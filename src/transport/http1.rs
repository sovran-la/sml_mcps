//! A small HTTP/1.1 server, bounded everywhere a peer can push.
//!
//! This exists because the transport needs three properties no general-purpose
//! sync HTTP crate offered, and all three are safety properties rather than
//! features:
//!
//! - **Nothing is allocated on a peer's say-so.** A `Content-Length` is a
//!   number an attacker chooses. It is compared against a ceiling and then
//!   thrown away; no buffer is ever sized from it.
//! - **Every read has a deadline.** A peer that declares a body and sends none
//!   costs one connection until the deadline, not a thread forever.
//! - **Concurrency is capped.** One thread per live connection, and a ceiling
//!   on how many of those exist, so how many threads this process has is not a
//!   decision the network gets to make.
//!
//! The subset of HTTP/1.1 here is the subset MCP needs: `POST`, `GET` and
//! `DELETE` on one path, `Content-Length` and `chunked` request bodies,
//! keep-alive, and a whole response in one buffer. There is no pipelining
//! (a request is answered before the next is read), no compression, no
//! ranges, and no upgrade.

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
    /// How long a peer may take over a request head, or over a body once it
    /// has started one.
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
    /// The peer stopped talking, or ran out of time.
    Incomplete,
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
                read_exactly(self.io, declared, &mut buffer)?;
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
        let Framing::Length(declared) = self.framing else {
            return;
        };
        if declared > limit {
            return;
        }

        self.started = true;
        let mut sink = Vec::new();
        if read_exactly(self.io, declared, &mut sink).is_ok() {
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

        let writer = self.io.get_mut();
        writer
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .and_then(|()| writer.flush())
            .map_err(|_| BodyError::Incomplete)
    }

    fn read_chunked(&mut self, limit: usize) -> std::result::Result<Vec<u8>, BodyError> {
        let mut body: Vec<u8> = Vec::new();

        loop {
            let mut budget = CHUNK_LINE_BYTES;
            let size = match read_line(self.io, &mut budget) {
                Ok(Line::Text(line)) => parse_chunk_size(&line).ok_or(BodyError::Malformed)?,
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(Line::TooLong | Line::Invalid) => return Err(BodyError::Malformed),
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

            read_exactly(self.io, size, &mut body)?;
            match read_line(self.io, &mut { 2usize }) {
                Ok(Line::Text(line)) if line.is_empty() => {}
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(_) => return Err(BodyError::Malformed),
                Err(_) => return Err(BodyError::Incomplete),
            }
        }
    }

    fn read_trailers(&mut self) -> std::result::Result<(), BodyError> {
        for _ in 0..=MAX_TRAILERS {
            let mut budget = CHUNK_LINE_BYTES;
            match read_line(self.io, &mut budget) {
                Ok(Line::Text(line)) if line.is_empty() => return Ok(()),
                Ok(Line::Text(_)) => continue,
                Ok(Line::Eof) => return Err(BodyError::Incomplete),
                Ok(Line::TooLong | Line::Invalid) => return Err(BodyError::Malformed),
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
        // the middle of a request is on ours.
        io.get_ref().set_read_timeout(Some(limits.idle_timeout));
        let head = match read_head(&mut io, limits) {
            Ok(Some(head)) => head,
            // A clean hangup, or a peer that went quiet. Neither is worth an
            // answer, and neither is an error.
            Ok(None) => return,
            Err(rejection) => {
                let _ = write_response(&mut io, &Method::Get, Version::Http11, rejection, limits);
                linger_close(&mut io);
                return;
            }
        };
        io.get_ref().set_read_timeout(Some(limits.read_timeout));

        let handled = {
            let mut request = Request {
                body: Body {
                    io: &mut io,
                    framing: head.framing,
                    must_send_continue: head.expects_continue,
                    finished: matches!(head.framing, Framing::Empty),
                    started: false,
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

/// Read the request head, or the response that says why not.
///
/// `Ok(None)` is a connection that ended before a request started - a hangup
/// or an idle timeout, both ordinary.
fn read_head(
    io: &mut BufReader<Box<dyn Socket>>,
    limits: &Limits,
) -> std::result::Result<Option<Head>, Response> {
    let mut budget = limits.max_head_bytes;

    // "a server that is expecting to read a request line SHOULD ignore at
    // least one empty line (CRLF) received prior to the request-line."
    let request_line = loop {
        match read_line(io, &mut budget) {
            Ok(Line::Text(line)) if line.is_empty() => continue,
            Ok(Line::Text(line)) => break line,
            Ok(Line::Eof) => return Ok(None),
            Ok(Line::TooLong) => return Err(too_long()),
            Ok(Line::Invalid) => return Err(bad_request("a request line that is not text")),
            Err(e) if is_timeout(&e) => return Ok(None),
            Err(_) => return Ok(None),
        }
    };

    let (method, target, version) = parse_request_line(&request_line)?;

    let mut headers: Vec<(String, String)> = Vec::new();
    loop {
        let line = match read_line(io, &mut budget) {
            Ok(Line::Text(line)) => line,
            Ok(Line::Eof) => return Ok(None),
            Ok(Line::TooLong) => return Err(too_long()),
            Ok(Line::Invalid) => {
                return Err(bad_request("a header field that is not text"));
            }
            Err(_) => return Ok(None),
        };
        if line.is_empty() {
            break;
        }
        if headers.len() == limits.max_headers {
            return Err(too_long());
        }
        // Obsolete line folding. RFC 9112 §5.2: "A server that receives an
        // obs-fold ... MUST either reject the message ... or replace each
        // received obs-fold with one or more SP octets". Rejecting is the side
        // that cannot be used to smuggle a header past a proxy.
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(bad_request("obsolete header line folding"));
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(bad_request("a header field with no `:`"));
        };
        if !is_token(name) {
            return Err(bad_request("a header field name that is not a token"));
        }
        headers.push((
            name.to_string(),
            value.trim_matches([' ', '\t']).to_string(),
        ));
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

    Ok(Some(Head {
        method,
        target,
        version,
        headers,
        framing,
        expects_continue,
        wants_close,
    }))
}

fn parse_request_line(line: &str) -> std::result::Result<(Method, String, Version), Response> {
    // Exactly two spaces, and no more: a request line with extra whitespace is
    // parsed differently by different intermediaries, which is where request
    // smuggling starts.
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad_request("a malformed request line"));
    };
    if method.is_empty() || target.is_empty() || !is_token(method) {
        return Err(bad_request("a malformed request line"));
    }

    let version = match version {
        "HTTP/1.1" => Version::Http11,
        "HTTP/1.0" => Version::Http10,
        _ => {
            return Err(status_only(505, "HTTP Version Not Supported").closing());
        }
    };

    Ok((Method::parse(method), target.to_string(), version))
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

/// One line of a head, charged against a shared budget.
enum Line {
    Text(String),
    /// The peer hung up.
    Eof,
    /// The budget ran out before the line ended.
    TooLong,
    /// The line is not text a head may contain.
    Invalid,
}

fn read_line(io: &mut BufReader<Box<dyn Socket>>, budget: &mut usize) -> std::io::Result<Line> {
    // An exhausted budget is a head too long, not a connection that ended -
    // reading zero bytes because we asked for zero says nothing about the peer.
    if *budget == 0 {
        return Ok(Line::TooLong);
    }

    let mut raw = Vec::new();
    let read = (&mut *io)
        .take(*budget as u64)
        .read_until(b'\n', &mut raw)?;

    if read == 0 {
        return Ok(Line::Eof);
    }
    *budget -= read;
    if !raw.ends_with(b"\n") {
        return Ok(Line::TooLong);
    }

    raw.pop();
    if raw.last() == Some(&b'\r') {
        raw.pop();
    }
    // A head is text. Anything else is either a wrong guess about the protocol
    // or an attempt to smuggle a control character through a header.
    match String::from_utf8(raw) {
        Ok(line)
            if line
                .bytes()
                .all(|b| b == b'\t' || (0x20..0x7f).contains(&b) || b >= 0x80) =>
        {
            Ok(Line::Text(line))
        }
        _ => Ok(Line::Invalid),
    }
}

/// Read exactly `count` bytes onto the end of `into`, growing as they arrive.
///
/// `count` is peer-declared, so nothing is reserved for it up front: a
/// `Content-Length` of 200000000000000000 costs the same here as one of 10.
fn read_exactly(
    io: &mut BufReader<Box<dyn Socket>>,
    count: usize,
    into: &mut Vec<u8>,
) -> std::result::Result<(), BodyError> {
    let before = into.len();
    match (&mut *io).take(count as u64).read_to_end(into) {
        Ok(_) if into.len() - before == count => Ok(()),
        // A hangup, a deadline, and a broken socket are the same answer: the
        // body promised is not going to arrive, and this connection is over.
        Ok(_) | Err(_) => Err(BodyError::Incomplete),
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

/// `1a3f` or `1a3f;ext=value` - the size, in hex, of the chunk that follows.
fn parse_chunk_size(line: &str) -> Option<usize> {
    let digits = line.split(';').next()?.trim_end_matches([' ', '\t']);
    if digits.is_empty() || digits.len() > 16 || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(digits, 16)
        .ok()
        .and_then(|size| usize::try_from(size).ok())
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
    io.get_ref().set_write_timeout(Some(limits.write_timeout));
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
    io.get_ref().set_read_timeout(Some(LINGER_POLL));
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
                let _ = stream.set_read_timeout(Some(limits.read_timeout));
                let _ = stream.set_write_timeout(Some(limits.write_timeout));
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

    fn echo_body(limit: usize) -> impl for<'a> Fn(&mut Request<'a>) -> Response {
        move |request: &mut Request| match request.read_body(limit) {
            Ok(body) => Response::new(200).with_body("text/plain", body.into_bytes()),
            Err(BodyError::TooLarge) => Response::new(413)
                .with_body("text/plain", b"too large".to_vec())
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
    fn connection_tokens_are_matched_one_at_a_time() {
        assert!(has_token("keep-alive, Upgrade", "upgrade"));
        assert!(has_token("close", "close"));
        assert!(!has_token("keep-alive", "close"));
        assert!(!has_token("closely", "close"));
    }
}
