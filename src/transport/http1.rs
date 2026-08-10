//! A small HTTP/1.1 server, bounded everywhere a peer can push.
//!
//! This exists because the transport needs three properties no general-purpose
//! sync HTTP crate offered, and all three are safety properties rather than
//! features:
//!
//! - **Nothing is allocated on a peer's say-so.** A `Content-Length` is a
//!   number an attacker chooses. It is compared against a ceiling and then
//!   thrown away; no buffer is ever sized from it.
//! - **Every request has a deadline, and it is not one the peer can renew.**
//!   A socket timeout is per-`read()`, so every byte that arrives resets it and
//!   a peer sending one byte at a time never meets one. The budget here starts
//!   with a request's first byte and covers its head *and* its body, so a
//!   declared body that never arrives - and one that arrives four bytes a
//!   second - both cost one connection until the deadline, not a thread
//!   forever.
//! - **Concurrency is capped.** One thread per live connection, and a ceiling
//!   on how many of those exist, so how many threads this process has is not a
//!   decision the network gets to make.
//!
//! The subset of HTTP/1.1 here is the subset MCP needs: `POST`, `GET` and
//! `DELETE` on one path, `Content-Length` and `chunked` request bodies,
//! keep-alive, and a whole response in one buffer. There is no pipelining
//! (a request is answered before the next is read), no compression, no
//! ranges, and no upgrade.
//!
//! **The one thing here that is not hand-written is the parser.** Those three
//! properties are about what a peer can make this process *hold*, and none of
//! them is an argument for hand-writing the byte-level grammar of a request
//! line and a header block. That grammar is where request smuggling lives, so
//! it is [`httparse`]'s - the parser hyper has been running in production for a
//! decade, at one crate and no transitive dependencies. It decides what a
//! request line is, what a header is, and where the head ends; this module
//! decides everything that then happens to those bytes:
//!
//! - **Framing.** `Content-Length` against `Transfer-Encoding`, conflicting
//!   lengths, unknown transfer codings. `httparse` reports headers, it does not
//!   adjudicate them, so [`framing_of`] still does - see its comments for which
//!   shapes are refused and why.
//! - **Ceilings.** How many bytes a head may occupy and how many fields it may
//!   carry, both charged before the parser is asked anything.
//! - **The body**, which `httparse` never touches: deadlines, the chunked
//!   decoder, and the rule that nothing is sized from a declared length.

use crate::types::{McpError, Result};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

/// Bytes buffered per connection while reading.
const READ_BUFFER: usize = 8 * 1024;

/// How much a connection being closed will read and discard so the peer sees
/// the response instead of a reset.
///
/// Closing a socket with unread bytes still in flight is what turns a `413`
/// into a `ECONNRESET` on the client, so the answer never arrives. Draining is
/// bounded by *both* of these, and by nothing the peer says.
const LINGER_BYTES: usize = 64 * 1024;
const LINGER_TIME: Duration = Duration::from_millis(250);

/// How long the drain waits for bytes that may not be coming.
///
/// A peer that declared a body and sent none has nothing in flight, so there is
/// nothing to discard and nothing that would cause a reset; one short look
/// establishes that. A peer that *did* send is drained until it stops, or until
/// the budget or the deadline above runs out.
const LINGER_POLL: Duration = Duration::from_millis(25);

/// How long the refusal thread will spend telling one peer it is not welcome.
const REFUSAL_TIME: Duration = Duration::from_secs(1);

/// How many refusals may be waiting at once before the rest are closed in
/// silence.
const REFUSAL_BACKLOG: usize = 64;

/// The longest a chunk-size line or a trailer may be.
const CHUNK_LINE_BYTES: usize = 128;

/// How many trailer fields a chunked body may carry.
const MAX_TRAILERS: usize = 16;

/// The shortest deadline this server will put on a socket.
///
/// `set_read_timeout(Some(Duration::ZERO))` is `EINVAL`, and the error goes
/// nowhere - which leaves the socket **blocking forever**, the exact opposite of
/// what a zero timeout asks for. One millisecond is the shortest deadline that
/// means what it says.
pub(crate) const MIN_TIMEOUT: Duration = Duration::from_millis(1);

/// The longest a request's budget can be, if the clock cannot express what was
/// configured.
///
/// Only reachable from a `Duration` large enough to overflow `Instant`, which
/// is a configuration mistake rather than a peer's doing - and panicking on the
/// connection thread is a worse answer than an hour.
const MAX_BUDGET: Duration = Duration::from_secs(3600);

/// How many empty lines may precede a request line.
///
/// RFC 9112 §2.2 asks a recipient to ignore "at least one" empty line before a
/// request line. `httparse::skip_empty_lines` ignores an unbounded run, and
/// [`read_head`] hands it the whole accumulator again on every round that ends
/// a line - so an unbounded run is a quadratic re-parse whose size the *peer*
/// chooses, for two bytes a line. Ignoring a few is politeness; ignoring all of
/// them is letting the network decide how much CPU a connection costs.
const MAX_LEADING_BLANK_LINES: usize = 8;

/// Live connections allowed at once, by default.
///
/// Each one costs a thread, so this is the ceiling on what a peer can make this
/// process hold. Generous enough that no real deployment meets it, finite so
/// that an attacker does not choose the number.
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 512;

/// What a peer is allowed to make this process hold.
#[derive(Clone, Debug)]
pub(crate) struct Limits {
    /// Live connections at once, across all peers.
    pub(crate) max_connections: usize,
    /// The whole request head - request line, every header, and the blank line.
    pub(crate) max_head_bytes: usize,
    /// How many header fields one request may carry.
    pub(crate) max_headers: usize,
    /// The largest body that will be read.
    pub(crate) max_body_bytes: usize,
    /// How long a peer has to deliver a whole request, head and body together,
    /// counted from its first byte.
    ///
    /// **Aggregate, not per-read.** A per-read deadline is renewed by every
    /// byte that arrives, so it bounds a peer that has *stopped* talking and
    /// says nothing at all about one that is talking slowly. This is the one a
    /// slow peer cannot renew.
    pub(crate) read_timeout: Duration,
    /// How long a kept-alive connection may sit between requests.
    pub(crate) idle_timeout: Duration,
    /// How long a write may take before the peer is assumed gone.
    pub(crate) write_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_head_bytes: 32 * 1024,
            max_headers: 100,
            max_body_bytes: crate::transport::MAX_MESSAGE_BYTES,
            read_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(120),
            write_timeout: Duration::from_secs(30),
        }
    }
}

/// A connection, plaintext or TLS, with the knobs the loop needs.
///
/// `Read`/`Write` alone would do for the bytes; the deadlines are the reason
/// this is a trait rather than a `Box<dyn Read + Write>`.
pub(crate) trait Socket: Read + Write + Send {
    fn set_read_timeout(&self, timeout: Option<Duration>);
    fn set_write_timeout(&self, timeout: Option<Duration>);
    /// Send a FIN, leaving the read side open so the peer's answer still
    /// arrives.
    fn shutdown_write(&self);
}

impl Socket for TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) {
        let _ = TcpStream::set_read_timeout(self, timeout);
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) {
        let _ = TcpStream::set_write_timeout(self, timeout);
    }

    fn shutdown_write(&self) {
        let _ = TcpStream::shutdown(self, Shutdown::Write);
    }
}

/// Put a socket on a read deadline, never a zero one.
///
/// See [`MIN_TIMEOUT`] for why zero is the one value that must not get through.
fn arm_read(io: &BufReader<Box<dyn Socket>>, timeout: Duration) {
    io.get_ref()
        .set_read_timeout(Some(timeout.max(MIN_TIMEOUT)));
}

/// [`arm_read`], for the other direction.
fn arm_write(io: &BufReader<Box<dyn Socket>>, timeout: Duration) {
    io.get_ref()
        .set_write_timeout(Some(timeout.max(MIN_TIMEOUT)));
}

/// The whole budget one request gets, from its first byte to its last.
///
/// This is the difference between bounding a peer that has *stopped* and
/// bounding one that is *slow*. A socket timeout is per-`read()`: a peer
/// sending one byte just under the deadline resets it and can do that forever,
/// which costs a connection - and at `max_connections` sockets, costs every
/// connection - for a few bytes a second. This deadline starts when a request
/// does and does not move.
///
/// It is enforced twice, because either alone has a hole. It is checked
/// *between* reads, which catches a peer that keeps delivering; and the
/// socket's own timeout is set from what is left of it, which catches one that
/// stops mid-read. Neither check can be renewed by anything the peer sends.
#[derive(Clone, Copy, Debug)]
struct Deadline {
    at: Instant,
}

impl Deadline {
    /// A budget of `total`, starting now.
    fn starting_now(total: Duration) -> Self {
        let now = Instant::now();
        let at = now
            .checked_add(total)
            .or_else(|| now.checked_add(MAX_BUDGET))
            .unwrap_or(now);
        Self { at }
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.at
    }

    /// Put the socket on what is left, so no single read can outlive the
    /// request that is spending it.
    fn arm(&self, io: &BufReader<Box<dyn Socket>>) {
        arm_read(io, self.at.saturating_duration_since(Instant::now()));
    }
}

/// The HTTP methods this transport distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Method {
    Get,
    Head,
    Post,
    Delete,
    /// Anything else, kept verbatim so a diagnostic can name it.
    Other(String),
}

impl Method {
    fn parse(token: &str) -> Self {
        match token {
            "GET" => Method::Get,
            "HEAD" => Method::Head,
            "POST" => Method::Post,
            "DELETE" => Method::Delete,
            other => Method::Other(other.to_string()),
        }
    }
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::Get => f.write_str("GET"),
            Method::Head => f.write_str("HEAD"),
            Method::Post => f.write_str("POST"),
            Method::Delete => f.write_str("DELETE"),
            Method::Other(other) => f.write_str(other),
        }
    }
}

/// The protocol revision the request line named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version {
    Http10,
    Http11,
}

/// How the body, if any, is framed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// No body at all.
    Empty,
    /// Exactly this many bytes. **A number the peer chose** - compared against
    /// a ceiling, never used as a size.
    Length(usize),
    Chunked,
}

/// Everything a request says before its body.
#[derive(Debug)]
pub(crate) struct Head {
    method: Method,
    target: String,
    version: Version,
    headers: Vec<(String, String)>,
    framing: Framing,
    expects_continue: bool,
    wants_close: bool,
}

impl Head {
    pub(crate) fn method(&self) -> &Method {
        &self.method
    }

    /// The request target as sent, query string and all.
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    /// A header's value, matched case-insensitively as HTTP requires.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Why a body could not be handed over.
#[derive(Debug)]
pub(crate) enum BodyError {
    /// Bigger than this server accepts. The declared length is never read, so
    /// the connection is finished after this.
    TooLarge,
    /// Framing the peer got wrong, or bytes that are not UTF-8.
    Malformed,
    /// The peer stopped talking.
    Incomplete,
    /// The peer kept talking, but not fast enough to finish inside the budget
    /// its request started with. Told apart from [`Incomplete`](Self::Incomplete)
    /// because "you were too slow" is a different thing for a client to act on
    /// than "your body ended early".
    TimedOut,
}

/// Whether this is a deadline rather than a broken socket.
///
/// The socket's own timeout is armed from what is left of the request's
/// budget, so one firing means the budget is spent - the same answer the
/// between-reads check gives, arrived at from inside a `read`.
fn is_timeout(error: &std::io::Error) -> bool {
    matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock)
}

/// The request body, read on demand and only up to a ceiling.
struct Body<'a> {
    io: &'a mut BufReader<Box<dyn Socket>>,
    framing: Framing,
    must_send_continue: bool,
    /// Whether every byte the framing promised has been consumed. A connection
    /// is only reusable when this is true - the alternative is guessing where
    /// the next request starts, which is how a desynchronised connection
    /// becomes a smuggling primitive.
    finished: bool,
    /// Whether a read was already attempted.
    started: bool,
    /// The budget this request's head was already spending. A body does not
    /// get a fresh one: the deadline covers a request, not a phase of one.
    deadline: Deadline,
    /// How long a `100 Continue` may take to go out.
    write_timeout: Duration,
}

impl Body<'_> {
    /// Read the whole body, or say why not.
    fn read(&mut self, limit: usize) -> std::result::Result<String, BodyError> {
        if self.started {
            return Err(BodyError::Malformed);
        }
        self.started = true;

        match self.framing {
            Framing::Empty => {
                self.finished = true;
                Ok(String::new())
            }
            // The whole point: an over-limit body is refused *without reading a
            // byte and without draining one*. Nothing here is sized from
            // `declared`.
            Framing::Length(declared) if declared > limit => Err(BodyError::TooLarge),
            Framing::Length(declared) => {
                self.send_continue()?;
                let mut buffer = Vec::new();
                read_exactly(self.io, declared, &mut buffer, self.deadline)?;
                self.finished = true;
                String::from_utf8(buffer).map_err(|_| BodyError::Malformed)
            }
            Framing::Chunked => {
                self.send_continue()?;
                let bytes = self.read_chunked(limit)?;
                String::from_utf8(bytes).map_err(|_| BodyError::Malformed)
            }
        }
    }

    /// Consume a body the handler never asked for, when doing so is cheap, so
    /// the connection survives to carry another request.
    ///
    /// "Cheap" is a length this server would have accepted anyway. An
    /// over-limit or chunked body is left alone and the connection closes -
    /// draining on a peer's terms is the thing this whole module exists to
    /// avoid.
    fn drain_if_cheap(&mut self, limit: usize) {
        if self.started || self.finished {
            return;
        }
        // A peer waiting for `100 Continue` has sent nothing, and nothing is
        // what it will send until told otherwise - which the handler, having
        // declined to read, never did. Draining it waits out the whole deadline
        // for bytes that were never in flight, and delivers an already-computed
        // `401`/`403`/`404` that much later.
        if self.must_send_continue {
            return;
        }
        let Framing::Length(declared) = self.framing else {
            return;
        };
        // The only thing between `read_exactly` and an unbounded read of a
        // length the *peer* chose. The main path is covered upstream - `read`
        // refuses an over-limit length before this can be reached - but a
        // request rejected before the handler read anything arrives here with
        // `started == false` and nothing else in the way.
        if declared > limit {
            return;
        }

        self.started = true;
        let mut sink = Vec::new();
        if read_exactly(self.io, declared, &mut sink, self.deadline).is_ok() {
            self.finished = true;
        }
    }

    /// Answer a `Expect: 100-continue` now that the body is actually wanted.
    ///
    /// Deliberately *after* the size check: telling a client to send a body
    /// this server has already decided to refuse is a wart, and on a declared
    /// length over the ceiling it would be an invitation to the exact flood
    /// being refused.
    fn send_continue(&mut self) -> std::result::Result<(), BodyError> {
        if !self.must_send_continue {
            return Ok(());
        }
        self.must_send_continue = false;

        // Nothing has written to this socket yet on a plaintext connection, so
        // without this it has no write deadline at all - 25 bytes into an empty
        // send buffer never blocks in practice, and "in practice" is not a
        // deadline.
        arm_write(self.io, self.write_timeout);
        let writer = self.io.get_mut();
        writer
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .and_then(|()| writer.flush())
            .map_err(|_| BodyError::Incomplete)
    }

    /// Decode a chunked body.
    ///
    /// **Every line ending here must be a CRLF.** RFC 9112 §2.2 permits a
    /// recipient to accept a bare LF, but that permission is scoped to "the
    /// start-line and fields"; §7.1's chunked grammar is strict CRLF, and
    /// bare-LF chunk framing is one of the better-known smuggling differentials
    /// precisely because front-ends disagree about it. The head keeps
    /// `httparse`'s leniency, which is hyper's and is a defensible thing to
    /// inherit. The body does not, for the same reason [`parse_chunk_size`]
    /// exists: a chunked body is where a disagreement about the end of a
    /// request gets written.
    fn read_chunked(&mut self, limit: usize) -> std::result::Result<Vec<u8>, BodyError> {
        let mut body: Vec<u8> = Vec::new();

        loop {
            let mut budget = CHUNK_LINE_BYTES;
            let size = match read_line(self.io, &mut budget, self.deadline) {
                Ok(Line::Text { text, crlf: true }) => {
                    parse_chunk_size(&text).ok_or(BodyError::Malformed)?
                }
                Ok(Line::Text { .. }) => return Err(BodyError::Malformed),
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(Line::TooLong | Line::Invalid) => return Err(BodyError::Malformed),
                Err(e) if is_timeout(&e) => return Err(BodyError::TimedOut),
                Err(_) => return Err(BodyError::Incomplete),
            };

            if size == 0 {
                self.read_trailers()?;
                self.finished = true;
                return Ok(body);
            }

            // Checked before a byte is read, so the accumulator can never be
            // grown past the ceiling by a peer's arithmetic.
            if size > limit.saturating_sub(body.len()) {
                return Err(BodyError::TooLarge);
            }

            read_exactly(self.io, size, &mut body, self.deadline)?;
            match read_line(self.io, &mut { 2usize }, self.deadline) {
                Ok(Line::Text { text, crlf: true }) if text.is_empty() => {}
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(_) => return Err(BodyError::Malformed),
                Err(e) if is_timeout(&e) => return Err(BodyError::TimedOut),
                Err(_) => return Err(BodyError::Incomplete),
            }
        }
    }

    fn read_trailers(&mut self) -> std::result::Result<(), BodyError> {
        for _ in 0..=MAX_TRAILERS {
            let mut budget = CHUNK_LINE_BYTES;
            match read_line(self.io, &mut budget, self.deadline) {
                // The blank line that ends the trailers is the byte the next
                // request starts after, so it is held to the same CRLF as the
                // rest of the framing.
                Ok(Line::Text { text, crlf: true }) if text.is_empty() => return Ok(()),
                Ok(Line::Text { crlf: true, .. }) => continue,
                Ok(Line::Text { .. }) => return Err(BodyError::Malformed),
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(Line::TooLong | Line::Invalid) => return Err(BodyError::Malformed),
                Err(e) if is_timeout(&e) => return Err(BodyError::TimedOut),
                Err(_) => return Err(BodyError::Incomplete),
            }
        }
        Err(BodyError::Malformed)
    }
}

/// One request, with its body still on the wire.
pub(crate) struct Request<'a> {
    head: Head,
    body: Body<'a>,
}

impl Request<'_> {
    pub(crate) fn method(&self) -> &Method {
        self.head.method()
    }

    pub(crate) fn target(&self) -> &str {
        self.head.target()
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.head.header(name)
    }

    /// Read the body, refusing anything over `limit`.
    pub(crate) fn read_body(&mut self, limit: usize) -> std::result::Result<String, BodyError> {
        self.body.read(limit)
    }
}

/// What the connection loop needs back once the handler has let go of the
/// request.
struct Handled {
    method: Method,
    version: Version,
    wants_close: bool,
    body_finished: bool,
}

/// A whole response, held in one buffer.
#[derive(Debug, Clone)]
pub(crate) struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    close: bool,
}

impl Response {
    pub(crate) fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            close: false,
        }
    }

    pub(crate) fn with_body(mut self, content_type: &str, body: Vec<u8>) -> Self {
        self.body = body;
        self.with_header("Content-Type", content_type)
    }

    /// Attach a header, unless the value could forge one.
    ///
    /// Nothing here builds a header value out of what a peer sent, so the
    /// dropping arm is unreachable in practice - but a header value carrying a
    /// newline is response splitting, and dropping one header beats it.
    pub(crate) fn with_header(mut self, name: &str, value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        if is_field_value(value) && is_token(name) {
            self.headers.push((name.to_string(), value.to_string()));
        }
        self
    }

    /// End the connection after this response.
    pub(crate) fn closing(mut self) -> Self {
        self.close = true;
        self
    }
}

/// The certificate and key a TLS listener serves.
#[cfg(feature = "tls")]
pub(crate) struct TlsSettings {
    pub(crate) certificate: Vec<u8>,
    pub(crate) private_key: Vec<u8>,
}

/// Serve forever: accept here, answer on one thread per connection.
///
/// `overloaded` is the answer to a connection arriving when
/// [`Limits::max_connections`] are already live. It is written by a thread of
/// its own, never here: writing to a peer is peer-paced, and the accept loop is
/// the one thread that must never wait on a peer.
pub(crate) fn serve<H>(
    addr: &str,
    #[cfg(feature = "tls")] tls: Option<TlsSettings>,
    limits: Limits,
    overloaded: Response,
    handle: H,
) -> Result<()>
where
    H: for<'a> Fn(&mut Request<'a>) -> Response + Send + Sync + 'static,
{
    // Before the socket exists, so a certificate this server cannot use refuses
    // to serve rather than quietly serving plaintext on the HTTPS port.
    #[cfg(feature = "tls")]
    let acceptor = Acceptor::new(tls)?;
    #[cfg(not(feature = "tls"))]
    let acceptor = Acceptor::Plain;

    let listener = TcpListener::bind(addr)
        .map_err(|e| McpError::Internal(format!("Failed to start HTTP server: {}", e)))?;

    let handle = Arc::new(handle);
    let limits = Arc::new(limits);
    let acceptor = Arc::new(acceptor);
    let live = Arc::new(AtomicUsize::new(0));
    let refuser = Refuser::start(render(
        &Method::Get,
        Version::Http11,
        overloaded.closing(),
        false,
    ));

    loop {
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            // Out of descriptors, or a connection that died between the SYN and
            // here. Neither is a reason to stop listening, and neither is a
            // reason to spin: a hot loop on EMFILE is its own outage.
            Err(_) => {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);

        let Some(slot) = Slot::claim(&live, limits.max_connections) else {
            // A TLS listener cannot say anything a peer would understand
            // without completing a handshake first, so there it is closed
            // instead of answered.
            if acceptor.is_tls() {
                drop(stream);
            } else {
                refuser.refuse(stream);
            }
            continue;
        };

        let (handle, limits, acceptor) = (
            Arc::clone(&handle),
            Arc::clone(&limits),
            Arc::clone(&acceptor),
        );
        let spawned = std::thread::Builder::new()
            .name("mcp-http".to_string())
            .spawn(move || {
                // Held for the connection's whole life; dropping it is what
                // gives the slot back, panic or not.
                let _slot = slot;
                if let Some(socket) = acceptor.accept(stream, &limits) {
                    serve_connection(socket, &limits, handle.as_ref());
                }
            });

        // A thread the OS will not give us is the platform saying it is full.
        // The connection is closed and the loop carries on, because an accept
        // loop that unwinds is a listener that never comes back.
        if spawned.is_err() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// One connection, from the first request line to the last.
fn serve_connection<H>(socket: Box<dyn Socket>, limits: &Limits, handle: &H)
where
    H: for<'a> Fn(&mut Request<'a>) -> Response,
{
    let mut io = BufReader::with_capacity(READ_BUFFER, socket);

    loop {
        // A connection between requests is idle on the peer's schedule; one in
        // the middle of a request is on ours. `read_head` makes that switch on
        // the first byte of the request - not here, and not after the head is
        // complete, because a peer that has begun a request is no longer idle.
        arm_read(&io, limits.idle_timeout);
        let (head, deadline) = match read_head(&mut io, limits) {
            Ok(Some(started)) => started,
            // A clean hangup, or a peer that went quiet. Neither is worth an
            // answer, and neither is an error.
            Ok(None) => return,
            Err(rejection) => {
                let _ = write_response(&mut io, &Method::Get, Version::Http11, rejection, limits);
                linger_close(&mut io);
                return;
            }
        };

        let handled = {
            let mut request = Request {
                body: Body {
                    io: &mut io,
                    framing: head.framing,
                    must_send_continue: head.expects_continue,
                    finished: matches!(head.framing, Framing::Empty),
                    started: false,
                    // The head's budget, not a new one. The peer does not get
                    // a fresh `read_timeout` for having reached the blank line.
                    deadline,
                    write_timeout: limits.write_timeout,
                },
                head,
            };

            // The backstop, not the policy. A handler is expected to contain
            // its own panics and answer with something a client can read; this
            // is here so that one which does not still produces a response
            // rather than a dropped connection, and so the connection thread
            // ends by choice rather than by unwinding.
            let response =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle(&mut request)))
                    .unwrap_or_else(|_| {
                        Response::new(500)
                            .with_body("text/plain", b"Internal Server Error\n".to_vec())
                            .closing()
                    });
            request.body.drain_if_cheap(limits.max_body_bytes);

            let handled = Handled {
                method: request.head.method.clone(),
                version: request.head.version,
                wants_close: request.head.wants_close,
                body_finished: request.body.finished,
            };
            (handled, response)
        };
        let (handled, response) = handled;

        // Reuse needs three yeses: the peer asked for it, the answer allows it,
        // and the connection is still framed where the next request starts.
        let reusable = handled.version == Version::Http11
            && !handled.wants_close
            && !response.close
            && handled.body_finished;

        let response = if reusable {
            response
        } else {
            response.closing()
        };

        let sent = write_response(&mut io, &handled.method, handled.version, response, limits);
        if !reusable || sent.is_err() {
            linger_close(&mut io);
            return;
        }
    }
}

/// Read the request head, and start the clock the whole request runs on.
///
/// `Ok(None)` is a connection that ended before a request started - a hangup
/// or an idle timeout, both ordinary. `Ok(Some(..))` carries the [`Deadline`]
/// the head was read under, because the body is read under the same one: the
/// budget belongs to the request, not to a phase of it.
///
/// The head is accumulated into a buffer of *our* size and handed to
/// [`httparse`] whole, again, as more of it arrives. `httparse` is the only
/// thing that decides where a head ends: the alternative is scanning for the
/// blank line here and disagreeing with the parser about which byte starts the
/// body, which is a desynchronised connection written by hand.
///
/// The accumulator is bounded by [`Limits::max_head_bytes`] and grows only as
/// bytes actually arrive, so - like everywhere else here - a peer's declared
/// intent never sizes a buffer.
fn read_head(
    io: &mut BufReader<Box<dyn Socket>>,
    limits: &Limits,
) -> std::result::Result<Option<(Head, Deadline)>, Response> {
    let mut raw: Vec<u8> = Vec::new();
    // How much of `raw` has been taken out of the reader's buffer. Everything
    // past it must stay there: it is the body, and the body is read from the
    // reader.
    let mut consumed = 0usize;
    // Unset until the first byte of the request arrives. Before that the
    // connection is idle, and idle runs on the peer's schedule; after it, the
    // request runs on ours.
    let mut clock: Option<Deadline> = None;

    loop {
        if raw.len() >= limits.max_head_bytes {
            return Err(too_long());
        }
        if clock.is_some_and(|deadline| deadline.expired()) {
            return Err(timed_out());
        }

        // Copy out of the reader's buffer without consuming it, so that the
        // bytes past the head are still there for the body to read.
        let (taken, ended_a_line) = {
            let available = match io.fill_buf() {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                // A deadline or a broken socket. Before a request started
                // neither is worth an answer; part-way into one, a peer that
                // ran out of time is owed the reason.
                Err(_) => {
                    return match clock {
                        Some(deadline) if deadline.expired() => Err(timed_out()),
                        _ => Ok(None),
                    };
                }
            };
            if available.is_empty() {
                // A clean hangup. Mid-head it is a peer that changed its mind,
                // which is still not an error worth answering.
                return Ok(None);
            }
            let room = limits.max_head_bytes - raw.len();
            let take = available.len().min(room);
            raw.extend_from_slice(&available[..take]);
            (take, available[..take].contains(&b'\n'))
        };

        // The first byte of a request starts its budget, and everything after
        // this - the rest of the head, then the body - spends that one budget.
        let deadline = *clock.get_or_insert_with(|| Deadline::starting_now(limits.read_timeout));
        deadline.arm(io);

        // See `MAX_LEADING_BLANK_LINES`: `httparse` skips an unbounded run of
        // these, and this loop re-parses from the front every round.
        if blank_lines_before_a_request(&raw, MAX_LEADING_BLANK_LINES) > MAX_LEADING_BLANK_LINES {
            return Err(bad_request(
                "more empty lines before a request line than this server ignores",
            ));
        }

        // A head ends with a newline, so a round that brought none cannot have
        // completed one, and re-parsing everything to be told so again is work
        // a slow peer would be choosing for us. This cannot disagree with
        // `httparse` about where a head ends - it only declines to ask.
        if ended_a_line {
            let mut fields = vec![httparse::EMPTY_HEADER; limits.max_headers];
            let mut parsed = httparse::Request::new(&mut fields);
            match parsed.parse(&raw) {
                Ok(httparse::Status::Complete(head_len)) => {
                    // `head_len` is past `consumed`: every earlier round was
                    // told `Partial` over a prefix of these same bytes, so the
                    // head cannot have ended inside one.
                    io.consume(head_len.saturating_sub(consumed));
                    return head_from(parsed).map(|head| Some((head, deadline)));
                }
                Ok(httparse::Status::Partial) => {}
                Err(e) => return Err(rejection(e)),
            }
        }

        io.consume(taken);
        consumed += taken;
    }
}

/// How many empty lines stand before the request line, counted no further than
/// `cap + 1`.
///
/// Stopping there is the point: the caller only needs to know whether the run
/// is over its ceiling, and counting the whole run every round would be the
/// same quadratic scan the ceiling exists to prevent.
fn blank_lines_before_a_request(raw: &[u8], cap: usize) -> usize {
    let mut at = 0;
    let mut lines = 0;
    while lines <= cap {
        if raw[at..].starts_with(b"\r\n") {
            at += 2;
        } else if raw[at..].starts_with(b"\n") {
            at += 1;
        } else {
            break;
        }
        lines += 1;
    }
    lines
}

/// Turn a parsed request line and header block into a [`Head`], deciding the
/// things `httparse` reports but does not adjudicate.
fn head_from(parsed: httparse::Request<'_, '_>) -> std::result::Result<Head, Response> {
    // `Complete` guarantees all three, but answering beats unwrapping.
    let (Some(method), Some(target), Some(version)) = (parsed.method, parsed.path, parsed.version)
    else {
        return Err(bad_request("a malformed request line"));
    };
    let version = match version {
        0 => Version::Http10,
        1 => Version::Http11,
        // `httparse` accepts no other version, so this is unreachable - and
        // answering rather than unwrapping is what keeps it that way.
        _ => return Err(status_only(505, "HTTP Version Not Supported").closing()),
    };

    // A header value is bytes, and RFC 9110 §5.5 still permits obs-text
    // (0x80-0xFF) in one. Nothing downstream has a use for a header that is not
    // text, and every one of them here is compared, logged or echoed as text,
    // so one that is not is refused rather than lossily repaired.
    let mut headers: Vec<(String, String)> = Vec::with_capacity(parsed.headers.len());
    for field in parsed.headers.iter() {
        let Ok(value) = std::str::from_utf8(field.value) else {
            return Err(bad_request("a header field value that is not text"));
        };
        headers.push((field.name.to_string(), value.to_string()));
    }

    let framing = framing_of(&headers)?;
    let connection = header_of(&headers, "connection").unwrap_or("");
    let wants_close = match version {
        Version::Http11 => has_token(connection, "close"),
        // "HTTP/1.0 clients ... persist only when keep-alive is asked for."
        Version::Http10 => !has_token(connection, "keep-alive"),
    };

    let expects_continue = match header_of(&headers, "expect") {
        None => false,
        Some(value) if value.eq_ignore_ascii_case("100-continue") => version == Version::Http11,
        // "A server that receives an Expect field-value other than
        // 100-continue MAY respond with a 417 (Expectation Failed) status."
        Some(_) => return Err(status_only(417, "Expectation Failed").closing()),
    };

    Ok(Head {
        method: Method::parse(method),
        target: target.to_string(),
        version,
        headers,
        framing,
        expects_continue,
        wants_close,
    })
}

/// What to answer a peer whose head `httparse` would not accept.
///
/// Every one of these ends the connection, because a head that did not parse
/// leaves no agreed answer to where the next one starts.
fn rejection(error: httparse::Error) -> Response {
    use httparse::Error;
    match error {
        // `HTTP/2.0`, `HTTP/1.9`, and a request line whose third token is not a
        // version at all. RFC 9110 §15.6.6 is what 505 is for.
        Error::Version => status_only(505, "HTTP Version Not Supported").closing(),
        // More fields than `max_headers`. Same answer as a head over its byte
        // budget, because it is the same kind of "too much".
        Error::TooManyHeaders => too_long(),
        // A field name that is not a token - which is also how obsolete line
        // folding arrives, and how a space before the colon does. Both are
        // smuggling primitives: RFC 9112 §5.2 lets a server reject an obs-fold
        // rather than rewrite it, and rejecting is the side a proxy cannot be
        // played off against.
        Error::HeaderName => bad_request("a header field name that is not a token"),
        Error::HeaderValue => bad_request("a header field value that is not text"),
        // An extra space in the request line, a method that is not a token, a
        // control character in the target.
        Error::Token => bad_request("a malformed request line"),
        // Not "a line ending that is not CRLF", which is what this used to say:
        // `httparse` raises `NewLine` for a `\r` that is *not* followed by a
        // `\n`, and accepts a bare `\n` (RFC 9112 §2.2 permits that in a head).
        // A stray carriage return is the thing a client can go and find.
        Error::NewLine => bad_request("a stray carriage return"),
        // `Error::Status` is response-only, so unreachable from here; the
        // wildcard is for whatever a later `httparse` adds.
        _ => bad_request("a malformed request head"),
    }
}

/// Decide how the body is framed, refusing every shape two parsers could read
/// differently.
fn framing_of(headers: &[(String, String)]) -> std::result::Result<Framing, Response> {
    let mut transfer_encoding = None;
    let mut content_length: Option<&str> = None;

    for (name, value) in headers {
        if name.eq_ignore_ascii_case("transfer-encoding") {
            if transfer_encoding.is_some() {
                return Err(bad_request("more than one `Transfer-Encoding`"));
            }
            transfer_encoding = Some(value.as_str());
        } else if name.eq_ignore_ascii_case("content-length") {
            // A repeated header with the same value is legal; two different
            // values are a smuggling attempt with a cover story.
            if content_length.is_some_and(|first| first != value) {
                return Err(bad_request("conflicting `Content-Length` headers"));
            }
            content_length = Some(value.as_str());
        }
    }

    match (transfer_encoding, content_length) {
        // RFC 9112 §6.1: "A sender MUST NOT send a Content-Length header field
        // in any message that contains a Transfer-Encoding header field."
        (Some(_), Some(_)) => Err(bad_request("both `Transfer-Encoding` and `Content-Length`")),
        (Some(encoding), None) => {
            if encoding.eq_ignore_ascii_case("chunked") {
                Ok(Framing::Chunked)
            } else {
                Err(status_only(501, "Not Implemented").closing())
            }
        }
        (None, Some(length)) => {
            // RFC 9112 §6.2: `Content-Length = 1*DIGIT`, and nothing else.
            // `usize::from_str` is looser than that - it accepts a leading `+`
            // - and a length this server reads as 5 while the proxy in front of
            // it rejects the field outright is two answers to where the request
            // ends.
            if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad_request("an unreadable `Content-Length`"));
            }
            let declared: usize = length
                .parse()
                .map_err(|_| bad_request("an unreadable `Content-Length`"))?;
            Ok(if declared == 0 {
                Framing::Empty
            } else {
                Framing::Length(declared)
            })
        }
        (None, None) => Ok(Framing::Empty),
    }
}

/// One line of chunked framing, charged against a shared byte budget.
///
/// Its callers are [`Body::read_chunked`] and [`Body::read_trailers`]; the head
/// is [`httparse`]'s and has not come through here since the grammar moved.
enum Line {
    Text {
        text: String,
        /// Whether the line ended with a CRLF rather than a bare LF.
        ///
        /// Reported rather than decided here because the two are not the same
        /// question everywhere: RFC 9112 §2.2 lets a recipient accept a bare LF
        /// in "the start-line and fields", and §7.1's chunked grammar is strict
        /// CRLF. The callers that frame a body require `true`.
        crlf: bool,
    },
    /// The peer hung up.
    Eof,
    /// The budget ran out before the line ended.
    TooLong,
    /// The line is not text this may contain.
    Invalid,
}

fn read_line(
    io: &mut BufReader<Box<dyn Socket>>,
    budget: &mut usize,
    deadline: Deadline,
) -> std::io::Result<Line> {
    // An exhausted budget is a line too long, not a connection that ended -
    // reading zero bytes because we asked for zero says nothing about the peer.
    if *budget == 0 {
        return Ok(Line::TooLong);
    }

    // Read a byte range at a time rather than through `read_until`, which loops
    // inside itself: the deadline has to be checked *between* reads, and one
    // socket timeout covering an inner loop bounds each read rather than the
    // line, which is `CHUNK_LINE_BYTES` × the timeout for a peer that dribbles.
    let mut raw = Vec::new();
    let terminated = loop {
        if deadline.expired() {
            return Err(std::io::Error::from(ErrorKind::TimedOut));
        }
        deadline.arm(io);

        let available = match io.fill_buf() {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if available.is_empty() {
            break false;
        }

        let room = available.len().min(*budget);
        let take = match available[..room].iter().position(|byte| *byte == b'\n') {
            Some(at) => at + 1,
            None => room,
        };
        raw.extend_from_slice(&available[..take]);
        io.consume(take);
        *budget -= take;

        if raw.ends_with(b"\n") {
            break true;
        }
        if *budget == 0 {
            break false;
        }
    };

    if raw.is_empty() {
        return Ok(Line::Eof);
    }
    if !terminated {
        return Ok(Line::TooLong);
    }

    raw.pop();
    let crlf = raw.last() == Some(&b'\r');
    if crlf {
        raw.pop();
    }
    // Framing is text. Anything else is either a wrong guess about the protocol
    // or an attempt to smuggle a control character through a trailer.
    match String::from_utf8(raw) {
        Ok(text)
            if text
                .bytes()
                .all(|b| b == b'\t' || (0x20..0x7f).contains(&b) || b >= 0x80) =>
        {
            Ok(Line::Text { text, crlf })
        }
        _ => Ok(Line::Invalid),
    }
}

/// Read exactly `count` bytes onto the end of `into`, growing as they arrive.
///
/// `count` is peer-declared, so nothing is reserved for it up front: a
/// `Content-Length` of 200000000000000000 costs the same here as one of 10.
///
/// The loop is here rather than in `read_to_end` for the same reason as
/// [`read_line`]'s: `deadline` has to be checked between reads, or a peer
/// delivering one byte at a time never meets one.
fn read_exactly(
    io: &mut BufReader<Box<dyn Socket>>,
    count: usize,
    into: &mut Vec<u8>,
    deadline: Deadline,
) -> std::result::Result<(), BodyError> {
    let mut outstanding = count;

    while outstanding > 0 {
        if deadline.expired() {
            return Err(BodyError::TimedOut);
        }
        deadline.arm(io);

        let available = match io.fill_buf() {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            // A hangup, a deadline and a broken socket all end this connection;
            // only the deadline gets a different word for it.
            Err(e) if is_timeout(&e) => return Err(BodyError::TimedOut),
            Err(_) => return Err(BodyError::Incomplete),
        };
        if available.is_empty() {
            return Err(BodyError::Incomplete);
        }

        let take = available.len().min(outstanding);
        into.extend_from_slice(&available[..take]);
        io.consume(take);
        outstanding -= take;
    }

    Ok(())
}

/// `1a3f` or `1a3f;ext=value` - the size, in hex, of the chunk that follows.
///
/// The hex and the chunk extensions are `httparse`'s to read. The one thing
/// added on top is a guard, because `httparse::parse_chunk_size` answers `0` -
/// *the last chunk* - to three lines that are not a chunk size at all:
///
/// ```text
/// "\r\n"     => Complete((2, 0))     a blank line
/// " \r\n"    => Complete((3, 0))     whitespace before the size
/// ";x\r\n"   => Complete((4, 0))     an extension with no size
/// ```
///
/// Reading any of those as "the body ends here" while a stricter parser in
/// front reads it as a malformed chunk is a disagreement about where the
/// request ends, which is the whole of request smuggling. So a chunk size must
/// start with a hex digit, and nothing else opens one.
fn parse_chunk_size(line: &str) -> Option<usize> {
    if !line.starts_with(|c: char| c.is_ascii_hexdigit()) {
        return None;
    }
    // `read_line` has already established the terminator was a CRLF and taken
    // it off; `httparse` needs one back to know the size is finished. Its own
    // bare-LF rejection cannot run on a line that has been through here, which
    // is exactly why requiring the CRLF is `read_chunked`'s job and not a
    // property inherited from the parser.
    let mut framed = String::with_capacity(line.len() + 2);
    framed.push_str(line);
    framed.push_str("\r\n");

    match httparse::parse_chunk_size(framed.as_bytes()) {
        Ok(httparse::Status::Complete((_, size))) => usize::try_from(size).ok(),
        // `Partial` is unreachable - the CRLF above is what it would be
        // waiting for - and is a malformed size either way.
        Ok(httparse::Status::Partial) | Err(_) => None,
    }
}

fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Whether a comma-separated header value carries this token.
fn has_token(value: &str, token: &str) -> bool {
    value
        .split(',')
        .any(|part| part.trim().eq_ignore_ascii_case(token))
}

/// An RFC 9110 token: what a header field name and a method are allowed to be.
fn is_token(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

/// Whether this can be a header value without forging the rest of the response.
fn is_field_value(text: &str) -> bool {
    text.bytes().all(|b| b == b'\t' || b >= 0x20)
}

fn bad_request(why: &str) -> Response {
    Response::new(400)
        .with_body("text/plain", format!("Bad Request: {why}\n").into_bytes())
        .closing()
}

fn too_long() -> Response {
    Response::new(431)
        .with_body("text/plain", b"Request Header Fields Too Large\n".to_vec())
        .closing()
}

/// A request that ran out of its budget part-way through.
///
/// Closing is not optional here: a request that is half-delivered leaves no
/// agreed answer to where the next one starts, which is the same reason every
/// other rejection in this file closes.
fn timed_out() -> Response {
    Response::new(408)
        .with_body("text/plain", b"Request Timeout\n".to_vec())
        .closing()
}

fn status_only(status: u16, reason: &str) -> Response {
    Response::new(status).with_body("text/plain", format!("{reason}\n").into_bytes())
}

/// Serialise a response, and say whether the connection continues.
fn write_response(
    io: &mut BufReader<Box<dyn Socket>>,
    method: &Method,
    version: Version,
    response: Response,
    limits: &Limits,
) -> std::io::Result<()> {
    arm_write(io, limits.write_timeout);
    let keep_alive = !response.close;
    let bytes = render(method, version, response, keep_alive);
    let writer = io.get_mut();
    writer.write_all(&bytes)?;
    writer.flush()
}

/// The bytes of one response.
fn render(method: &Method, version: Version, response: Response, keep_alive: bool) -> Vec<u8> {
    // 204 and 304 define no body, and a HEAD response carries its headers
    // without one.
    let bodiless = matches!(response.status, 204 | 304) || matches!(method, Method::Head);

    let mut out = Vec::with_capacity(response.body.len() + 256);
    let line = match version {
        Version::Http10 => "HTTP/1.0",
        Version::Http11 => "HTTP/1.1",
    };
    out.extend_from_slice(
        format!(
            "{} {} {}\r\n",
            line,
            response.status,
            reason_phrase(response.status)
        )
        .as_bytes(),
    );

    for (name, value) in &response.headers {
        out.extend_from_slice(format!("{}: {}\r\n", name, value).as_bytes());
    }
    if !matches!(response.status, 204 | 304) {
        out.extend_from_slice(format!("Content-Length: {}\r\n", response.body.len()).as_bytes());
    }
    out.extend_from_slice(if keep_alive {
        b"Connection: keep-alive\r\n"
    } else {
        b"Connection: close\r\n"
    });
    out.extend_from_slice(b"\r\n");

    if !bodiless {
        out.extend_from_slice(&response.body);
    }
    out
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        417 => "Expectation Failed",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "Status",
    }
}

/// End a connection so the peer sees the answer instead of a reset.
///
/// Closing a socket that still has unread bytes coming at it sends an RST, and
/// an RST discards whatever the peer had not read yet - including the `413`
/// explaining why. So: half-close, then read and throw away what is already in
/// flight, bounded by a byte budget *and* a deadline, neither of which the peer
/// gets a say in.
fn linger_close(io: &mut BufReader<Box<dyn Socket>>) {
    arm_read(io, LINGER_POLL);
    io.get_ref().shutdown_write();

    let deadline = Instant::now() + LINGER_TIME;
    let mut budget = LINGER_BYTES;
    let mut scratch = [0u8; 4096];

    while budget > 0 && Instant::now() < deadline {
        match io.get_mut().read(&mut scratch) {
            // Nothing there, or nothing more: whatever the peer promised, it is
            // not on its way, so closing cannot lose the answer.
            Ok(0) | Err(_) => return,
            Ok(read) => budget = budget.saturating_sub(read),
        }
    }
}

/// One live connection's claim on the ceiling.
struct Slot {
    live: Arc<AtomicUsize>,
}

impl Slot {
    fn claim(live: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        live.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
            (count < max).then_some(count + 1)
        })
        .ok()?;
        Some(Self {
            live: Arc::clone(live),
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Tells refused peers why, somewhere other than the accept loop.
///
/// Answering a refused connection inline is a write to a peer, and a peer
/// decides how fast it reads. The accept loop doing that is how a saturated
/// server becomes a deaf one.
struct Refuser {
    queue: SyncSender<TcpStream>,
}

impl Refuser {
    fn start(answer: Vec<u8>) -> Self {
        let (queue, refused) = sync_channel::<TcpStream>(REFUSAL_BACKLOG);

        // Detached on purpose: it lives exactly as long as the listener does,
        // and the listener never returns.
        let _ = std::thread::Builder::new()
            .name("mcp-http-refuse".to_string())
            .spawn(move || {
                for stream in refused {
                    let _ = stream.set_write_timeout(Some(REFUSAL_TIME));
                    let mut stream = stream;
                    if stream.write_all(&answer).is_ok() {
                        let _ = stream.flush();
                    }
                    // The refused peer sent a whole request nobody read, and
                    // closing on top of bytes still arriving resets the
                    // connection - which throws away the `503` explaining
                    // itself. Same bounded lingering close as everywhere else.
                    let mut io =
                        BufReader::with_capacity(READ_BUFFER, Box::new(stream) as Box<dyn Socket>);
                    linger_close(&mut io);
                }
            });

        Self { queue }
    }

    fn refuse(&self, stream: TcpStream) {
        // A full refusal queue means even saying "no" is backed up. The
        // connection is closed without an explanation, which is still an
        // answer, and it costs nothing.
        match self.queue.try_send(stream) {
            Ok(()) => {}
            Err(TrySendError::Full(stream) | TrySendError::Disconnected(stream)) => drop(stream),
        }
    }
}

/// How an accepted socket becomes a connection.
enum Acceptor {
    Plain,
    #[cfg(feature = "tls")]
    Tls(Arc<rustls::ServerConfig>),
}

impl Acceptor {
    #[cfg(feature = "tls")]
    fn new(tls: Option<TlsSettings>) -> Result<Self> {
        let Some(settings) = tls else {
            return Ok(Acceptor::Plain);
        };
        Ok(Acceptor::Tls(Arc::new(tls_config(settings)?)))
    }

    fn is_tls(&self) -> bool {
        match self {
            Acceptor::Plain => false,
            #[cfg(feature = "tls")]
            Acceptor::Tls(_) => true,
        }
    }

    fn accept(
        &self,
        stream: TcpStream,
        #[allow(unused)] limits: &Limits,
    ) -> Option<Box<dyn Socket>> {
        match self {
            Acceptor::Plain => Some(Box::new(stream)),
            #[cfg(feature = "tls")]
            Acceptor::Tls(config) => {
                // The handshake runs here, on the connection's own thread, and
                // under the connection's own deadline - never on the accept
                // loop, where a slow one would be everybody's problem.
                let _ = stream.set_read_timeout(Some(limits.read_timeout.max(MIN_TIMEOUT)));
                let _ = stream.set_write_timeout(Some(limits.write_timeout.max(MIN_TIMEOUT)));
                let connection = rustls::ServerConnection::new(Arc::clone(config)).ok()?;
                Some(Box::new(TlsSocket {
                    stream: rustls::StreamOwned::new(connection, stream),
                }))
            }
        }
    }
}

#[cfg(not(feature = "tls"))]
impl Acceptor {
    #[allow(dead_code)]
    fn unused(&self) {}
}

#[cfg(feature = "tls")]
fn tls_config(settings: TlsSettings) -> Result<rustls::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let bad = |what: &str| McpError::Internal(format!("Failed to start HTTP server: {what}"));

    let certificates: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut settings.certificate.as_slice())
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| bad(&format!("unreadable certificate chain: {e}")))?;
    if certificates.is_empty() {
        return Err(bad("the certificate chain is empty"));
    }

    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut settings.private_key.as_slice())
            .map_err(|e| bad(&format!("unreadable private key: {e}")))?
            .ok_or_else(|| bad("no private key in the PEM given"))?;

    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| bad(&format!("no usable TLS versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|e| {
            bad(&format!(
                "the certificate and key do not work together: {e}"
            ))
        })
}

#[cfg(feature = "tls")]
struct TlsSocket {
    stream: rustls::StreamOwned<rustls::ServerConnection, TcpStream>,
}

#[cfg(feature = "tls")]
impl Read for TlsSocket {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buffer)
    }
}

#[cfg(feature = "tls")]
impl Write for TlsSocket {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

#[cfg(feature = "tls")]
impl Socket for TlsSocket {
    fn set_read_timeout(&self, timeout: Option<Duration>) {
        let _ = self.stream.sock.set_read_timeout(timeout);
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) {
        let _ = self.stream.sock.set_write_timeout(timeout);
    }

    fn shutdown_write(&self) {
        let _ = self.stream.sock.shutdown(Shutdown::Write);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A socket backed by two buffers, so the parsing can be driven without a
    /// network.
    struct Fake {
        incoming: std::io::Cursor<Vec<u8>>,
        outgoing: Arc<Mutex<Vec<u8>>>,
    }

    impl Read for Fake {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.incoming.read(buffer)
        }
    }

    impl Write for Fake {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.outgoing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Socket for Fake {
        fn set_read_timeout(&self, _: Option<Duration>) {}
        fn set_write_timeout(&self, _: Option<Duration>) {}
        fn shutdown_write(&self) {}
    }

    /// A socket that hands back at most `chunk` bytes per read.
    ///
    /// [`Fake`] is a single `Cursor`, so `BufReader` always fills from it in one
    /// go and every head arrives whole - which is the one thing a network never
    /// does. This one splits a head at boundaries nobody chose, which is what
    /// the incremental parse loop in [`read_head`] exists for.
    struct Dribble {
        incoming: std::io::Cursor<Vec<u8>>,
        outgoing: Arc<Mutex<Vec<u8>>>,
        chunk: usize,
    }

    impl Read for Dribble {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let take = buffer.len().min(self.chunk);
            self.incoming.read(&mut buffer[..take])
        }
    }

    impl Write for Dribble {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.outgoing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Socket for Dribble {
        fn set_read_timeout(&self, _: Option<Duration>) {}
        fn set_write_timeout(&self, _: Option<Duration>) {}
        fn shutdown_write(&self) {}
    }

    /// A socket that waits before handing back each small piece.
    ///
    /// [`Fake`] and [`Dribble`] both answer instantly, which is the one thing a
    /// slow peer never does - so neither can measure a deadline. This one can,
    /// and deliberately **ignores `set_read_timeout` entirely**: a per-read
    /// deadline is what a slow peer defeats by sending a byte, so a test that
    /// let the socket enforce one would be measuring the wrong thing. The only
    /// thing that can stop it is a budget that does not move.
    struct Trickle {
        incoming: std::io::Cursor<Vec<u8>>,
        outgoing: Arc<Mutex<Vec<u8>>>,
        /// Bytes the *first* read hands over at once, with no wait - a head a
        /// real client put in one segment. Zero means trickle from the start.
        burst: usize,
        chunk: usize,
        gap: Duration,
    }

    impl Read for Trickle {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let take = match std::mem::replace(&mut self.burst, 0) {
                0 => {
                    std::thread::sleep(self.gap);
                    self.chunk
                }
                burst => burst,
            };
            let take = buffer.len().min(take);
            self.incoming.read(&mut buffer[..take])
        }
    }

    impl Write for Trickle {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.outgoing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Socket for Trickle {
        fn set_read_timeout(&self, _: Option<Duration>) {}
        fn set_write_timeout(&self, _: Option<Duration>) {}
        fn shutdown_write(&self) {}
    }

    /// A socket that hands over what it has and then goes quiet *without*
    /// hanging up - a client waiting to be told it may send its body.
    ///
    /// The opposite choice from [`Trickle`]: this one honours the deadline it
    /// is given, and answers `WouldBlock` when it runs out, exactly as a real
    /// socket does. That makes the wait itself the measurement - whatever an
    /// answer costs here, it costs because something read from a peer that had
    /// nothing to send.
    struct Mute {
        incoming: std::io::Cursor<Vec<u8>>,
        outgoing: Arc<Mutex<Vec<u8>>>,
        deadline: Mutex<Duration>,
    }

    impl Read for Mute {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            match self.incoming.read(buffer)? {
                0 => {
                    let wait = *self.deadline.lock().unwrap_or_else(|e| e.into_inner());
                    std::thread::sleep(wait);
                    Err(std::io::Error::from(ErrorKind::WouldBlock))
                }
                read => Ok(read),
            }
        }
    }

    impl Write for Mute {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.outgoing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Socket for Mute {
        fn set_read_timeout(&self, timeout: Option<Duration>) {
            if let Some(timeout) = timeout {
                *self.deadline.lock().unwrap_or_else(|e| e.into_inner()) = timeout;
            }
        }
        fn set_write_timeout(&self, _: Option<Duration>) {}
        fn shutdown_write(&self) {}
    }

    /// Drive one request through a peer that then goes quiet, and say how long
    /// the answer took to come back.
    fn drive_mute<H>(request: &[u8], limits: Limits, handle: H) -> (String, Duration)
    where
        H: for<'a> Fn(&mut Request<'a>) -> Response,
    {
        let outgoing = Arc::new(Mutex::new(Vec::new()));
        let socket = Mute {
            incoming: std::io::Cursor::new(request.to_vec()),
            outgoing: Arc::clone(&outgoing),
            deadline: Mutex::new(Duration::from_secs(1)),
        };

        let started = Instant::now();
        serve_connection(Box::new(socket), &limits, &handle);
        let took = started.elapsed();

        let written = outgoing.lock().unwrap_or_else(|e| e.into_inner()).clone();
        (String::from_utf8_lossy(&written).into_owned(), took)
    }

    /// Drive one connection's worth of bytes through the loop and return what
    /// came back.
    fn drive<H>(request: &[u8], limits: Limits, handle: H) -> String
    where
        H: for<'a> Fn(&mut Request<'a>) -> Response,
    {
        let outgoing = Arc::new(Mutex::new(Vec::new()));
        let socket = Fake {
            incoming: std::io::Cursor::new(request.to_vec()),
            outgoing: Arc::clone(&outgoing),
        };
        serve_connection(Box::new(socket), &limits, &handle);

        let written = outgoing.lock().unwrap_or_else(|e| e.into_inner()).clone();
        String::from_utf8_lossy(&written).into_owned()
    }

    /// The same, with the bytes handed over `chunk` at a time.
    fn drive_dribbled<H>(request: &[u8], chunk: usize, limits: Limits, handle: H) -> String
    where
        H: for<'a> Fn(&mut Request<'a>) -> Response,
    {
        let outgoing = Arc::new(Mutex::new(Vec::new()));
        let socket = Dribble {
            incoming: std::io::Cursor::new(request.to_vec()),
            outgoing: Arc::clone(&outgoing),
            chunk,
        };
        serve_connection(Box::new(socket), &limits, &handle);

        let written = outgoing.lock().unwrap_or_else(|e| e.into_inner()).clone();
        String::from_utf8_lossy(&written).into_owned()
    }

    /// The same, `burst` bytes at once and then one `chunk` every `gap`, with
    /// how long the whole thing took.
    fn drive_trickled<H>(
        request: &[u8],
        burst: usize,
        chunk: usize,
        gap: Duration,
        limits: Limits,
        handle: H,
    ) -> (String, Duration)
    where
        H: for<'a> Fn(&mut Request<'a>) -> Response,
    {
        let outgoing = Arc::new(Mutex::new(Vec::new()));
        let socket = Trickle {
            incoming: std::io::Cursor::new(request.to_vec()),
            outgoing: Arc::clone(&outgoing),
            burst,
            chunk,
            gap,
        };

        let started = Instant::now();
        serve_connection(Box::new(socket), &limits, &handle);
        let took = started.elapsed();

        let written = outgoing.lock().unwrap_or_else(|e| e.into_inner()).clone();
        (String::from_utf8_lossy(&written).into_owned(), took)
    }

    fn echo_body(limit: usize) -> impl for<'a> Fn(&mut Request<'a>) -> Response {
        // The same mapping `HttpServer::read_body` makes, so what these tests
        // see on the wire is what a client would.
        move |request: &mut Request| match request.read_body(limit) {
            Ok(body) => Response::new(200).with_body("text/plain", body.into_bytes()),
            Err(BodyError::TooLarge) => Response::new(413)
                .with_body("text/plain", b"too large".to_vec())
                .closing(),
            Err(BodyError::TimedOut) => Response::new(408)
                .with_body("text/plain", b"too slow".to_vec())
                .closing(),
            Err(_) => Response::new(400).with_body("text/plain", b"bad body".to_vec()),
        }
    }

    fn post(body: &str) -> Vec<u8> {
        format!(
            "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    #[test]
    fn a_request_is_read_and_answered() {
        let answer = drive(&post("hello"), Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
        assert!(answer.ends_with("\r\n\r\nhello"), "{answer}");
        assert!(answer.contains("Content-Length: 5\r\n"), "{answer}");
    }

    #[test]
    fn an_absurd_content_length_is_refused_without_being_believed() {
        // The finding this module exists for: `vec![0; declared]` on a number
        // the peer chose is an allocator abort, and the abort happened in a
        // destructor the handler could not see. Nothing here is sized from
        // `declared` - so a length no machine could hold costs one comparison.
        let request =
            b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 200000000000000000\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));

        assert!(answer.starts_with("HTTP/1.1 413 "), "{answer}");
        assert!(
            answer.contains("Connection: close"),
            "an oversized body ends the connection: {answer}"
        );
    }

    #[test]
    fn a_body_that_never_arrives_is_not_waited_for_after_the_answer() {
        // Same shape as the abort, one order of magnitude down: a declared body
        // the peer simply does not send. The answer goes out and the connection
        // ends; nothing drains a promise the peer has no intention of keeping.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 100000000\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));

        assert!(answer.starts_with("HTTP/1.1 413 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn a_declared_body_that_stops_early_is_incomplete_not_infinite() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 64\r\n\r\nshort";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn two_requests_share_one_connection() {
        let mut wire = post("one");
        wire.extend_from_slice(&post("two"));
        let answer = drive(&wire, Limits::default(), echo_body(1024));

        assert_eq!(answer.matches("HTTP/1.1 200 OK").count(), 2, "{answer}");
        assert!(answer.contains("\r\n\r\none"), "{answer}");
        assert!(answer.ends_with("two"), "{answer}");
    }

    #[test]
    fn a_head_split_across_reads_is_still_one_head() {
        // Every other test here hands the head over whole, because a `Cursor`
        // fills a `BufReader` in one go. A network does not: it splits a head
        // mid-token, mid-line and mid-terminator. Each of those rounds must
        // leave the parse loop asking for more rather than deciding, and the
        // sizes below straddle the `\r\n` (1), fall inside it (3) and land on
        // neither boundary (7).
        for chunk in [1, 3, 7] {
            let answer = drive_dribbled(&post("hello"), chunk, Limits::default(), echo_body(1024));
            assert!(
                answer.starts_with("HTTP/1.1 200 OK\r\n"),
                "chunk {chunk}: {answer}"
            );
            // The body arriving intact is the real assertion: it says the head
            // ended on the byte `httparse` chose, and that the bytes past it
            // were left in the reader for the body to find.
            assert!(answer.ends_with("\r\n\r\nhello"), "chunk {chunk}: {answer}");
        }
    }

    #[test]
    fn a_dribbled_connection_keeps_its_place_between_requests() {
        // What `consumed` is for. It tracks how much of the head was already
        // taken out of the reader across rounds, so the last round consumes the
        // remainder and not the whole head again. An off-by-one there does not
        // fail the first request - it eats the first bytes of the second, and
        // the failure surfaces one request later than the mistake.
        let mut wire = post("one");
        wire.extend_from_slice(&post("two"));

        for chunk in [1, 3, 7] {
            let answer = drive_dribbled(&wire, chunk, Limits::default(), echo_body(1024));
            assert_eq!(
                answer.matches("HTTP/1.1 200 OK").count(),
                2,
                "chunk {chunk}: {answer}"
            );
            assert!(answer.contains("\r\n\r\none"), "chunk {chunk}: {answer}");
            assert!(answer.ends_with("two"), "chunk {chunk}: {answer}");
        }
    }

    #[test]
    fn an_unread_body_does_not_desynchronise_the_next_request() {
        // A handler that rejects a request without reading its body must not
        // leave those bytes to be parsed as the next request line.
        let mut wire = post(r#"{"ignored":true}"#);
        wire.extend_from_slice(&post("second"));

        let answer = drive(&wire, Limits::default(), |request: &mut Request| {
            if request.target() == "/mcp" && request.header("content-length") == Some("16") {
                Response::new(403).with_body("text/plain", b"no".to_vec())
            } else {
                echo_body(1024)(request)
            }
        });

        assert!(answer.starts_with("HTTP/1.1 403 "), "{answer}");
        assert!(
            answer.contains("HTTP/1.1 200 OK"),
            "the connection stayed framed: {answer}"
        );
        assert!(answer.ends_with("second"), "{answer}");
    }

    #[test]
    fn an_undrained_body_ends_the_connection_rather_than_framing_the_next_request() {
        // `body_finished` in `serve_connection`'s reuse condition, which is the
        // one `&&` between a rejected request and request smuggling - and which
        // deleting failed **zero** tests before this one existed.
        //
        // Every other test that looks like it covers this misses in the same
        // way. `an_unread_body_does_not_desynchronise_the_next_request` drains
        // successfully, so `finished` is true and the guard is inert; the
        // over-ceiling tests answer `.closing()`, so `response.close`
        // short-circuits before the guard is consulted. The case that matters
        // is an **unfinished body with a response that does not ask to close**,
        // and it is what every `precheck` rejection produces: those return
        // before `read_body`, and `drain_if_cheap` declines any body it would
        // not have accepted anyway.
        //
        // So: a length over the ceiling, and a plain `404` that does not close.
        // The bytes after the head are then attacker-chosen and unframed. With
        // the guard, one request gets one answer and the connection ends;
        // without it, the residue below is read as the next request line and
        // the smuggled `POST` executes on a connection whose only request was a
        // `404`.
        let mut wire =
            b"POST /nope HTTP/1.1\r\nHost: x\r\nContent-Length: 20000000\r\n\r\n".to_vec();
        wire.extend_from_slice(&post("smuggled"));

        let answer = drive(&wire, Limits::default(), |_: &mut Request| {
            Response::new(404).with_body("text/plain", b"no\n".to_vec())
        });

        assert_eq!(
            answer.matches("HTTP/1.1 ").count(),
            1,
            "one request, one answer: {answer}"
        );
        assert!(answer.contains("Connection: close"), "{answer}");
        assert!(
            !answer.contains("smuggled"),
            "the smuggled request was executed: {answer}"
        );
    }

    #[test]
    fn a_declined_request_does_not_drain_a_body_over_the_ceiling() {
        // `drain_if_cheap`'s `if declared > limit { return; }`, which deleting
        // failed zero tests. On the main path it is shadowed - `Body::read`
        // refuses an over-limit length and sets `started`, so the drain returns
        // at its first line - but a request rejected *before* the handler read
        // anything arrives here with `started == false`, and then this `if` is
        // the only thing between `read_exactly` and a read of a length the peer
        // chose. That is the `EqualReader` shape this module exists because of,
        // re-created in our own code.
        //
        // The peer below declares 900 MB and sends nothing. If the drain
        // believes it, the answer waits out the whole deadline instead of going
        // out now.
        let wire = b"POST /nope HTTP/1.1\r\nHost: x\r\nContent-Length: 900000000\r\n\r\n";
        let limits = Limits {
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        };

        let (answer, took) = drive_mute(wire, limits, |_: &mut Request| {
            Response::new(404).with_body("text/plain", b"no\n".to_vec())
        });

        assert!(answer.starts_with("HTTP/1.1 404 "), "{answer}");
        assert!(
            took < Duration::from_secs(1),
            "the drain believed a peer-declared length and waited {took:?}"
        );
        // Not drained means not finished, which means the connection ends -
        // the right outcome, and the same one `body_finished` enforces.
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn a_body_cannot_be_read_twice() {
        // `Body::read`'s `if self.started` guard, which deleting also failed
        // zero tests. No in-tree handler reads twice; one that did would
        // consume the *next* pipelined request as this one's body and then set
        // `finished = true`, so the connection would be reused from the wrong
        // byte - a desync produced by the server rather than the peer.
        let mut wire = post("first");
        wire.extend_from_slice(&post("second"));

        let answer = drive(&wire, Limits::default(), |request: &mut Request| {
            let first = request.read_body(1024);
            let second = request.read_body(1024);
            match (first, second) {
                // The second read is refused, and the first still got its body.
                (Ok(body), Err(BodyError::Malformed)) => {
                    Response::new(200).with_body("text/plain", body.into_bytes())
                }
                (first, second) => Response::new(500).with_body(
                    "text/plain",
                    format!("{first:?} then {second:?}").into_bytes(),
                ),
            }
        });

        assert_eq!(
            answer.matches("HTTP/1.1 200 OK").count(),
            2,
            "the second request was eaten by the first handler: {answer}"
        );
        assert!(answer.contains("\r\n\r\nfirst"), "{answer}");
        assert!(answer.ends_with("second"), "{answer}");
    }

    #[test]
    fn a_chunked_body_is_decoded() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
                        5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("hello world"), "{answer}");
    }

    #[test]
    fn a_chunked_body_over_the_ceiling_stops_at_the_ceiling() {
        let chunk = "x".repeat(4096);
        let request = format!(
            "POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            chunk.len(),
            chunk
        );
        let answer = drive(request.as_bytes(), Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 413 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn a_head_bigger_than_its_budget_is_refused() {
        let padding = "x".repeat(4096);
        let request = format!("POST /mcp HTTP/1.1\r\nHost: x\r\nBig: {padding}\r\n\r\n");
        let limits = Limits {
            max_head_bytes: 512,
            ..Default::default()
        };
        let answer = drive(request.as_bytes(), limits, echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 431 "), "{answer}");
    }

    #[test]
    fn too_many_headers_is_refused() {
        let mut request = String::from("POST /mcp HTTP/1.1\r\nHost: x\r\n");
        for n in 0..64 {
            request.push_str(&format!("X-{n}: v\r\n"));
        }
        request.push_str("\r\n");

        let limits = Limits {
            max_headers: 8,
            ..Default::default()
        };
        let answer = drive(request.as_bytes(), limits, echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 431 "), "{answer}");
    }

    #[test]
    fn content_length_and_transfer_encoding_together_are_refused() {
        // The classic smuggling pair: two intermediaries disagree about where
        // the request ends, and the disagreement is the exploit.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 6\r\n\
                        Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn conflicting_content_lengths_are_refused() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 6\r\n\
                        Content-Length: 7\r\n\r\nabcdef";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn an_unreadable_content_length_is_refused() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: twelve\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn a_folded_header_is_refused() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nX-Thing: one\r\n  two\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn a_space_before_a_header_colon_is_refused() {
        // `X-Thing : v` is a field name a hop in front may read as `X-Thing`
        // and this server as `X-Thing `, or the reverse - which is how a field
        // gets past a proxy that is filtering on it. RFC 9112 §5.1 says a
        // server MUST reject it rather than pick a reading, and rejecting is
        // the only side that cannot be played off against the hop.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nX-Thing : v\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(
            answer.contains("a header field name that is not a token"),
            "refused as a name, not as some other 400: {answer}"
        );
    }

    #[test]
    fn a_signed_content_length_is_refused() {
        // RFC 9112 §6.2 is `1*DIGIT`. `usize::from_str` is looser and would
        // take the `+` and read 5, while a strict front-end rejects the field
        // outright - the same "two answers to where the request ends" as the
        // `Transfer-Encoding` pair above, spelled with one character.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\nhello";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(
            answer.contains("an unreadable `Content-Length`"),
            "{answer}"
        );
    }

    #[test]
    fn a_header_value_that_is_not_text_is_refused() {
        // obs-text (0x80-0xFF) is legal per RFC 9110 §5.5, so `httparse` parses
        // this head clean and hands the value back as bytes - the refusal is
        // ours. Nothing downstream has a use for a header that is not text:
        // every one of them is compared, logged or echoed as text, so a value
        // that is not gets refused rather than lossily repaired.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nX-Thing: \xff\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(
            answer.contains("a header field value that is not text"),
            "{answer}"
        );
    }

    #[test]
    fn a_control_character_in_a_header_value_is_refused() {
        // The other half of the pair above, and a different path to the same
        // answer: a NUL is not obs-text, so `httparse` refuses the head itself
        // and never reaches the text check. Both roads end at 400 because a
        // value a downstream reader might truncate at the NUL is a header two
        // parties would read differently.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nX-Thing: a\0b\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(
            answer.contains("a header field value that is not text"),
            "{answer}"
        );
    }

    #[test]
    fn an_unknown_transfer_encoding_is_not_implemented() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 501 "), "{answer}");
    }

    #[test]
    fn an_expectation_we_cannot_meet_is_refused() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nExpect: the-moon\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 417 "), "{answer}");
    }

    #[test]
    fn continue_is_sent_only_for_a_body_that_will_be_accepted() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\
                        Content-Length: 5\r\n\r\nhello";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(
            answer.starts_with("HTTP/1.1 100 Continue\r\n\r\n"),
            "{answer}"
        );
        assert!(answer.contains("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("hello"), "{answer}");
    }

    #[test]
    fn continue_is_not_sent_for_a_body_that_is_already_refused() {
        // Answering `100 Continue` and then `413` is an invitation to send the
        // very flood being refused.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\
                        Content-Length: 900000\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(!answer.contains("100 Continue"), "{answer}");
        assert!(answer.starts_with("HTTP/1.1 413 "), "{answer}");
    }

    #[test]
    fn a_rejected_continue_is_answered_now_rather_than_waited_out() {
        // A client that sent `Expect: 100-continue` is doing exactly what the
        // spec asks: waiting for permission before it sends a body. The handler
        // here declines to read, so permission never comes - and the drain then
        // sat on `read_exactly` for the whole deadline waiting for bytes that
        // were, correctly, never in flight. Measured at 3.0016 s against a
        // 3-second timeout, on every `400`/`401`/`403`/`404`/`405` - all of
        // which are on the unauthenticated path, so it was also a
        // connection-holding primitive for about a hundred bytes.
        let wire = b"POST /nope HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\
                     Content-Length: 40\r\n\r\n";
        let limits = Limits {
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        };

        // The peer hands over its head and then goes quiet without hanging up,
        // which is precisely what the spec told it to do. If the drain waits on
        // that, it waits out the whole thirty seconds - which is the default,
        // and so what a real deployment paid per rejected request.
        let (answer, took) = drive_mute(wire, limits, |_: &mut Request| {
            Response::new(404).with_body("text/plain", b"no\n".to_vec())
        });

        assert!(answer.starts_with("HTTP/1.1 404 "), "{answer}");
        assert!(
            took < Duration::from_secs(1),
            "the answer waited {took:?} for a body the client was told not to send"
        );
        assert!(!answer.contains("100 Continue"), "{answer}");
        // `finished` stays false, so the connection ends - which is right: the
        // peer's body was never framed onto the wire, and there is no agreed
        // answer to where a next request would start.
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn a_request_line_with_extra_spaces_is_refused() {
        let request = b"POST  /mcp HTTP/1.1\r\nHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn an_unsupported_http_version_is_refused() {
        let request = b"POST /mcp HTTP/2.0\r\nHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 505 "), "{answer}");
    }

    #[test]
    fn http_10_closes_unless_it_asks_not_to() {
        let request = b"POST /mcp HTTP/1.0\r\nHost: x\r\nContent-Length: 2\r\n\r\nhi";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.0 200 OK"), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
    }

    #[test]
    fn a_head_request_carries_headers_and_no_body() {
        let request = b"HEAD /mcp HTTP/1.1\r\nHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), |_: &mut Request| {
            Response::new(200).with_body("text/plain", b"hello".to_vec())
        });
        assert!(answer.contains("Content-Length: 5\r\n"), "{answer}");
        assert!(answer.ends_with("\r\n\r\n"), "no body on a HEAD: {answer}");
    }

    #[test]
    fn a_204_carries_no_length_and_no_body() {
        let request = b"DELETE /mcp HTTP/1.1\r\nHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), |_: &mut Request| {
            Response::new(204)
        });
        assert!(answer.starts_with("HTTP/1.1 204 No Content"), "{answer}");
        assert!(!answer.contains("Content-Length"), "{answer}");
    }

    #[test]
    fn a_header_value_cannot_forge_a_response() {
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), |_: &mut Request| {
            Response::new(200)
                .with_header("X-Ok", "fine")
                .with_header("X-Evil", "a\r\nX-Injected: yes")
        });
        assert!(answer.contains("X-Ok: fine"), "{answer}");
        assert!(!answer.contains("X-Injected"), "{answer}");
    }

    #[test]
    fn a_handler_that_panics_still_produces_a_response() {
        // The backstop under the transport's own `catch_unwind`. Without it a
        // panic anywhere in answering unwinds out of the connection loop, and
        // the client gets a dropped connection rather than a status - which is
        // strictly worse than the bare `500` this exists to replace.
        let answer = drive(&post("hi"), Limits::default(), |_: &mut Request| {
            panic!("handler exploded on purpose")
        });
        assert!(answer.starts_with("HTTP/1.1 500 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
        assert!(answer.ends_with("Internal Server Error\n"), "{answer}");
    }

    #[test]
    fn a_leading_blank_line_is_ignored() {
        let mut wire = b"\r\n".to_vec();
        wire.extend_from_slice(&post("hi"));
        let answer = drive(&wire, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
    }

    #[test]
    fn a_few_leading_blank_lines_are_ignored_and_a_flood_is_not() {
        // RFC 9112 §2.2 asks a recipient to ignore "at least one" empty line
        // before a request line. Ignoring a *few* is politeness; the ceiling is
        // where politeness stops.
        let mut wire = b"\r\n".repeat(MAX_LEADING_BLANK_LINES);
        wire.extend_from_slice(&post("hi"));
        let answer = drive(&wire, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");

        let mut wire = b"\r\n".repeat(MAX_LEADING_BLANK_LINES + 1);
        wire.extend_from_slice(&post("hi"));
        let answer = drive(&wire, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(answer.contains("more empty lines"), "{answer}");

        // Bare LFs count too. `httparse::skip_empty_lines` takes either, so a
        // ceiling that only saw CRLF would be one `\n` wide.
        let mut wire = b"\n".repeat(MAX_LEADING_BLANK_LINES + 1);
        wire.extend_from_slice(&post("hi"));
        let answer = drive(&wire, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn a_flood_of_leading_blank_lines_is_not_a_cpu_bill() {
        // The reason for the ceiling, and the measurement it was written
        // against. `read_head` hands the whole accumulator to `httparse` again
        // on every round that ends a line, and every `\r\n` ends one - so a
        // peer dribbling 32 KiB of them forces ~16k parses of an average 16 KiB
        // buffer. Measured here, same build, same machine: **1.07 s** of CPU
        // for 32 KiB of traffic, ending in a `431`. The peer picks the size.
        // Bounded, the same bytes cost 5.6 ms and end on the ninth line.
        //
        // A `Dribble` rather than a `Fake`, because the quadratic term is the
        // number of *rounds*, and a `Fake` delivers everything in one.
        let wire = b"\r\n".repeat(16 * 1024);
        let started = Instant::now();
        let answer = drive_dribbled(&wire, 1, Limits::default(), echo_body(1024));
        let took = started.elapsed();

        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(
            took < Duration::from_millis(50),
            "32 KiB of blank lines cost {took:?}"
        );
    }

    /// The budget the slowloris tests give a request, and a bound on how long
    /// answering one that blows it may take.
    ///
    /// The bound is the budget plus [`LINGER_TIME`], which is what the bounded
    /// close spends making sure the `408` arrives instead of a reset, plus
    /// slack for a sleeping thread waking up late. What it has to stay under is
    /// the time the peer's *whole* request would have taken - that difference
    /// is the finding.
    const SLOW_BUDGET: Duration = Duration::from_millis(60);
    const SLOW_ANSWER_BY: Duration = Duration::from_millis(600);

    fn slow_limits() -> Limits {
        Limits {
            read_timeout: SLOW_BUDGET,
            // Deliberately enormous. Whatever cuts these connections off, it is
            // not the idle clock: a peer that is talking is not idle.
            idle_timeout: Duration::from_secs(120),
            ..Default::default()
        }
    }

    #[test]
    fn a_head_dribbled_past_the_deadline_is_cut_off() {
        // Slowloris, and the reason the deadline is aggregate rather than
        // per-read. `Trickle` ignores `set_read_timeout` on purpose: it always
        // has one more byte, always a little late, so a per-`read()` deadline
        // is renewed forever and never fires. The only thing that can end this
        // is a budget that does not move.
        //
        // 55 bytes at 20 ms is 1.1 s of peer. The budget is 60 ms.
        let (answer, took) = drive_trickled(
            &post("hello"),
            0,
            1,
            Duration::from_millis(20),
            slow_limits(),
            echo_body(1024),
        );

        assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
        assert!(
            took < SLOW_ANSWER_BY,
            "cut off on the budget, not on the last byte: {took:?}"
        );
    }

    #[test]
    fn a_body_dribbled_past_the_deadline_is_cut_off() {
        // The other half, and the handoff between them. The head arrives in one
        // segment the way a real client sends it, and the body does not - so
        // what is being measured is the budget *surviving* `read_head` into
        // `read_exactly`. A deadline that restarted per phase would hand a peer
        // a fresh one for reaching the blank line.
        //
        // 40 body bytes at 30 ms is 1.2 s of peer, against the same 60 ms.
        let mut wire = b"POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 40\r\n\r\n".to_vec();
        let head = wire.len();
        wire.extend_from_slice(&b"x".repeat(40));

        let (answer, took) = drive_trickled(
            &wire,
            head,
            1,
            Duration::from_millis(30),
            slow_limits(),
            echo_body(1024),
        );

        assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
        assert!(answer.contains("Connection: close"), "{answer}");
        assert!(
            took < SLOW_ANSWER_BY,
            "cut off on the budget, not on the last byte: {took:?}"
        );
    }

    #[test]
    fn a_chunked_body_dribbled_past_the_deadline_is_cut_off() {
        // The third read loop, and the one with the most places to lose a
        // deadline: `read_chunked` alternates `read_line` and `read_exactly`,
        // and both of those used to loop *inside* themselves - where one socket
        // timeout bounds a read rather than a line, and a peer sets the pace.
        //
        // 26 body bytes at 40 ms is 1 s of peer, against the same 60 ms.
        let wire = b"POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
                     5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let head = wire.len() - 26;

        let (answer, took) = drive_trickled(
            wire,
            head,
            1,
            Duration::from_millis(40),
            slow_limits(),
            echo_body(1024),
        );

        assert!(answer.starts_with("HTTP/1.1 408 "), "{answer}");
        assert!(
            took < SLOW_ANSWER_BY,
            "cut off on the budget, not on the last byte: {took:?}"
        );
    }

    #[test]
    fn an_idle_connection_is_not_spending_a_request_budget() {
        // The other side of the same coin, and the reason the switch happens on
        // the *first byte* rather than at the top of the loop. A connection
        // sitting between requests is idle on the peer's schedule; only a
        // request that has started runs on ours. Here the wait before the first
        // byte is four times the whole request budget, and the request that
        // then arrives is answered rather than refused.
        let limits = Limits {
            read_timeout: Duration::from_millis(25),
            idle_timeout: Duration::from_secs(120),
            ..Default::default()
        };
        let (answer, _) = drive_trickled(
            &post("hello"),
            0,
            usize::MAX,
            Duration::from_millis(100),
            limits,
            echo_body(1024),
        );
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("\r\n\r\nhello"), "{answer}");
    }

    #[test]
    fn a_request_inside_its_budget_is_answered_normally() {
        // The deadline is not allowed to be the reason the ordinary case works
        // or fails. Same trickle as the tests above, a budget that fits.
        let limits = Limits {
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let (answer, _) = drive_trickled(
            &post("hello"),
            0,
            4,
            Duration::from_millis(1),
            limits,
            echo_body(1024),
        );
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("\r\n\r\nhello"), "{answer}");
    }

    #[test]
    fn each_request_on_a_connection_gets_its_own_budget() {
        // A budget spent by the first request must not be charged to the
        // second. `read_head` starts a fresh clock every time round the loop;
        // one hoisted out of it would close a perfectly healthy keep-alive
        // connection as soon as two slow-ish requests added up.
        //
        // Two 53-byte requests at 4 ms a byte: ~212 ms each, ~424 ms together,
        // against a 400 ms budget. One clock for the connection answers the
        // first and times out the second.
        let mut wire = post("one");
        wire.extend_from_slice(&post("two"));

        let limits = Limits {
            read_timeout: Duration::from_millis(400),
            ..Default::default()
        };
        let (answer, _) = drive_trickled(
            &wire,
            0,
            1,
            Duration::from_millis(4),
            limits,
            echo_body(1024),
        );
        assert_eq!(answer.matches("HTTP/1.1 200 OK").count(), 2, "{answer}");
        assert!(!answer.contains("408"), "{answer}");
    }

    #[test]
    fn a_connection_that_says_nothing_is_not_an_error() {
        let answer = drive(b"", Limits::default(), echo_body(1024));
        assert_eq!(answer, "", "a peer that opens and leaves gets no answer");
    }

    #[test]
    fn the_connection_ceiling_admits_exactly_its_size() {
        let live = Arc::new(AtomicUsize::new(0));
        let first = Slot::claim(&live, 2).expect("room for one");
        let second = Slot::claim(&live, 2).expect("room for two");
        assert!(Slot::claim(&live, 2).is_none(), "and no more than two");

        drop(first);
        assert!(
            Slot::claim(&live, 2).is_some(),
            "a finished connection gives its slot back"
        );
        drop(second);
    }

    #[test]
    fn a_panicking_handler_frees_its_slot() {
        // The slot lives on the connection thread, so a handler that unwinds
        // still gives it back - a pool that leaks a slot per panic eventually
        // admits nobody.
        let live = Arc::new(AtomicUsize::new(0));
        let slot = Slot::claim(&live, 1).expect("room");

        let handle = std::thread::spawn(move || {
            let _slot = slot;
            panic!("handler exploded on purpose");
        });
        assert!(handle.join().is_err());
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn chunk_sizes_are_read_as_hex_and_nothing_else() {
        assert_eq!(parse_chunk_size("1a"), Some(26));
        assert_eq!(parse_chunk_size("0"), Some(0));
        assert_eq!(parse_chunk_size("10;name=value"), Some(16));
        assert_eq!(parse_chunk_size(""), None);
        assert_eq!(parse_chunk_size("-1"), None);
        assert_eq!(parse_chunk_size("ffffffffffffffffff"), None);
    }

    #[test]
    fn a_chunk_size_must_open_with_a_hex_digit() {
        // The guard in `parse_chunk_size` is load-bearing, and this is the
        // measurement it was written against: `httparse` reads all three of
        // these as `0` - *the last chunk* - and not one of them is a chunk size
        // at all. Ending a body on a blank line while a stricter parser in
        // front reads that line as malformed is two answers to where the
        // request ends, which is the whole of request smuggling.
        for line in ["\r\n", " \r\n", ";x\r\n"] {
            assert!(
                matches!(
                    httparse::parse_chunk_size(line.as_bytes()),
                    Ok(httparse::Status::Complete((_, 0)))
                ),
                "`httparse` no longer reads {line:?} as the last chunk - the \
                 guard below can be reconsidered, but do not simply delete it"
            );
        }

        // What we answer instead. `read_line` has taken the CRLF off by here.
        assert_eq!(parse_chunk_size(""), None);
        assert_eq!(parse_chunk_size(" "), None);
        assert_eq!(parse_chunk_size(";x"), None);
    }

    #[test]
    fn chunked_framing_requires_a_crlf_at_every_line() {
        // RFC 9112 §2.2 lets a recipient accept a bare LF, and this server does
        // - in "the start-line and fields", which is the scope of that
        // permission and is `httparse`'s business. §7.1's chunked grammar is
        // strict CRLF, and bare-LF chunk framing is one of the better-known
        // smuggling differentials precisely because front-ends disagree about
        // it. This module's whole stated position is being the strict side.
        //
        // The claim that used to cover this was that `httparse` rejects a bare
        // LF in a chunk size - true of `httparse`, and never reached, because
        // `read_line` had already stripped the LF and `parse_chunk_size`
        // re-appended a CRLF before handing it over. The check the comment
        // relied on could not run. It is ours now, so it does.
        let head = "POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";

        // Bare LF after the chunk size, after the chunk data, and after the
        // last chunk - each on its own, so no case is passing for another's
        // reason.
        for body in [
            "5\nhello\r\n0\r\n\r\n",
            "5\r\nhello\n0\r\n\r\n",
            "5\r\nhello\r\n0\n\r\n",
            "5\r\nhello\r\n0\r\n\n",
        ] {
            let request = format!("{head}{body}");
            let answer = drive(request.as_bytes(), Limits::default(), echo_body(1024));
            assert!(
                answer.starts_with("HTTP/1.1 400 "),
                "bare LF accepted in {body:?}: {answer}"
            );
        }

        // And the all-CRLF version of the same body is still read.
        let request = format!("{head}5\r\nhello\r\n0\r\n\r\n");
        let answer = drive(request.as_bytes(), Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("hello"), "{answer}");
    }

    #[test]
    fn a_trailer_section_is_held_to_the_same_crlf() {
        // The blank line ending the trailers is the byte the next request
        // starts after, so it is framing like any other.
        let head = "POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";

        let request = format!("{head}5\r\nhello\r\n0\r\nX-Trailer: v\n\r\n");
        let answer = drive(request.as_bytes(), Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");

        // Trailers themselves are still parsed and discarded when well formed.
        let request = format!("{head}5\r\nhello\r\n0\r\nX-Trailer: v\r\n\r\n");
        let answer = drive(request.as_bytes(), Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.ends_with("hello"), "{answer}");
    }

    #[test]
    fn a_chunked_body_ending_on_a_blank_line_is_refused() {
        // The guard above, reached the way an attacker would rather than by
        // calling it directly. The body here is a blank line where a chunk size
        // belongs, followed by the blank line that ends the trailers - so
        // without the guard this is a *well-formed* zero-chunk body and the
        // answer is 200 with nothing in it. That 200 is the smuggle landing:
        // this server would have called the request finished here while a hop
        // in front reads the same bytes as a malformed chunk and keeps going.
        let request = b"POST /mcp HTTP/1.1\r\nHost: x\r\n\
                        Transfer-Encoding: chunked\r\n\r\n\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    }

    #[test]
    fn a_stray_carriage_return_is_named_as_one() {
        // What `Error::NewLine` actually is, and what the diagnostic used to
        // get wrong. `httparse` raises it for a `\r` with no `\n` behind it;
        // "a line ending that is not CRLF" described a bare LF, which is the
        // one thing it *accepts*.
        let request = b"POST /mcp HTTP/1.1\rHost: x\r\n\r\n";
        let answer = drive(request, Limits::default(), echo_body(1024));
        assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
        assert!(answer.contains("a stray carriage return"), "{answer}");
    }

    #[test]
    fn connection_tokens_are_matched_one_at_a_time() {
        assert!(has_token("keep-alive, Upgrade", "upgrade"));
        assert!(has_token("close", "close"));
        assert!(!has_token("keep-alive", "close"));
        assert!(!has_token("closely", "close"));
    }
}
