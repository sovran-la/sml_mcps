//! Newline-delimited JSON-RPC framing with an optional deadline.
//!
//! Both bidirectional transports speak the same wire format - one JSON object
//! per line - and both need to give up waiting when a client stops answering.
//! Getting that right is fiddly enough to be worth doing once:
//!
//! - A read that expires **must not lose bytes.** Half a message stays in the
//!   buffer and is completed by the next read, so a timeout costs a wait, not
//!   the connection.
//! - The deadline covers the whole message, not each syscall. A peer dribbling
//!   one byte at a time cannot extend the wait indefinitely.
//! - Bytes are accumulated raw and decoded only once a full line exists, so a
//!   multi-byte character split across two reads survives.
//!
//! [`BufRead::read_line`](std::io::BufRead::read_line) does none of this: its
//! contract leaves the buffer's contents unspecified after an error, which is
//! exactly the case a timeout produces.

use crate::types::{JsonRpcMessage, McpError, Result};
use std::io::{BufRead, BufReader, Read};
use std::time::{Duration, Instant};

/// A byte source whose reads can be made to expire.
///
/// Implementors arrange for [`Read::read`] to fail with
/// [`WouldBlock`](std::io::ErrorKind::WouldBlock) or
/// [`TimedOut`](std::io::ErrorKind::TimedOut) once the deadline has passed;
/// [`LineReader`] turns that into [`McpError::Timeout`].
pub(crate) trait DeadlineRead: Read {
    /// Apply `deadline` to subsequent reads, or `None` to block indefinitely.
    fn set_deadline(&mut self, deadline: Option<Instant>) -> std::io::Result<()>;
}

/// Largest message a transport will accept by default, in bytes.
///
/// Without a bound, a peer that streams bytes and never sends a newline grows
/// the buffer until the process is OOM-killed - and on a `UnixServer` anyone
/// who can connect gets their own reader, so N connections multiply it. The
/// framing rule ("**MUST NOT** contain embedded newlines") means a legitimate
/// message is exactly one line, so a generous cap costs nothing real.
///
/// Override it per server with
/// [`ServerConfig::max_message_bytes`](crate::ServerConfig::max_message_bytes),
/// which is threaded down to whichever transport the server is driven over.
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Reads newline-delimited messages, optionally under a deadline.
pub(crate) struct LineReader<R> {
    inner: BufReader<R>,
    /// Bytes of a message whose newline has not arrived yet.
    ///
    /// Survives a timeout: the next read continues where this one stopped.
    partial: Vec<u8>,
    /// Ceiling on `partial`.
    max_bytes: usize,
    /// Set when the current message blew the ceiling.
    ///
    /// Bytes keep being consumed and discarded until its newline arrives, so
    /// framing resynchronizes on the next message rather than the connection
    /// dying or the buffer growing.
    overflowed: bool,
    /// Whether a deadline is currently applied to the source.
    ///
    /// Tracked so an untimed read never touches the source's settings. That is
    /// not just an optimization: on macOS, `setsockopt(SO_RCVTIMEO)` against a
    /// unix socket whose peer has closed fails with `EINVAL`, so clearing a
    /// deadline that was never set would turn a clean disconnect into an IO
    /// error.
    armed: bool,
}

impl<R: DeadlineRead> LineReader<R> {
    pub(crate) fn new(source: R) -> Self {
        Self::with_limit(source, MAX_MESSAGE_BYTES)
    }

    /// A reader with a different ceiling.
    ///
    /// A zero ceiling would refuse every message, including the error response
    /// explaining why, so it is clamped to one byte.
    pub(crate) fn with_limit(source: R, max_bytes: usize) -> Self {
        Self {
            inner: BufReader::new(source),
            partial: Vec::new(),
            max_bytes: max_bytes.max(1),
            overflowed: false,
            armed: false,
        }
    }

    /// Change the ceiling on a reader already in use.
    ///
    /// Applies from the next message on: a message already half-accumulated
    /// keeps the budget it started with, since retroactively overflowing it
    /// would discard bytes that were legal when they arrived.
    pub(crate) fn set_limit(&mut self, max_bytes: usize) {
        self.max_bytes = max_bytes.max(1);
    }

    /// Push `deadline` down to the source, if that changes anything.
    fn arm(&mut self, deadline: Option<Instant>) -> Result<()> {
        match deadline {
            // Always re-applied: the remaining budget shrinks with every pass.
            Some(_) => {
                self.inner
                    .get_mut()
                    .set_deadline(deadline)
                    .map_err(McpError::Io)?;
                self.armed = true;
            }
            // Failing to *clear* a deadline is survivable - the worst case is a
            // spurious timeout later - and the source is often unsettable by
            // this point precisely because it just closed.
            None if self.armed => {
                let _ = self.inner.get_mut().set_deadline(None);
                self.armed = false;
            }
            None => {}
        }
        Ok(())
    }

    /// Read one message, waiting no longer than `timeout` in total.
    ///
    /// Returns [`McpError::TransportClosed`] at end of stream and
    /// [`McpError::Timeout`] if the deadline passes with a message still
    /// incomplete.
    pub(crate) fn read_message(&mut self, timeout: Option<Duration>) -> Result<JsonRpcMessage> {
        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            // Re-applied every pass so the remaining budget shrinks as bytes
            // trickle in; without that, a slow peer could hold the read open
            // for `timeout` per syscall rather than per message.
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return Err(timed_out(timeout));
                }
            }
            self.arm(deadline)?;

            let (line_ended, consumed) = {
                let available = match self.inner.fill_buf() {
                    Ok(bytes) => bytes,
                    Err(e) if is_timeout(&e) => return Err(timed_out(timeout)),
                    Err(e) if is_interrupt(&e) => continue,
                    Err(e) if is_disconnect(&e) => return Err(McpError::TransportClosed),
                    Err(e) => return Err(e.into()),
                };

                // Empty buffer with no error is end of stream. A partial
                // message at that point is a truncated write, not a message.
                if available.is_empty() {
                    return Err(McpError::TransportClosed);
                }

                match available.iter().position(|&byte| byte == b'\n') {
                    Some(index) => {
                        // Checked on this branch too, not just the one that
                        // keeps reading: appending a chunk that happens to
                        // contain the newline used to skip the ceiling
                        // entirely, so an accepted message could reach
                        // `max_bytes` plus one buffer refill. The constant now
                        // means what it says.
                        if !self.overflowed {
                            if self.partial.len() + index + 1 > self.max_bytes {
                                self.overflowed = true;
                                self.partial = Vec::new();
                            } else {
                                self.partial.extend_from_slice(&available[..=index]);
                            }
                        }
                        (true, index + 1)
                    }
                    None if self.overflowed => (false, available.len()),
                    None if self.partial.len() + available.len() > self.max_bytes => {
                        // Stop accumulating, but keep reading: the rest of this
                        // message still has to be skipped past so the next one
                        // starts on a message boundary.
                        self.overflowed = true;
                        self.partial = Vec::new();
                        (false, available.len())
                    }
                    None => {
                        self.partial.extend_from_slice(available);
                        (false, available.len())
                    }
                }
            };
            self.inner.consume(consumed);

            if line_ended {
                if std::mem::take(&mut self.overflowed) {
                    self.partial.clear();
                    return Err(McpError::InvalidMessage(format!(
                        "message exceeds the {} byte limit",
                        self.max_bytes
                    )));
                }
                let line = std::mem::take(&mut self.partial);
                let text = String::from_utf8(line)
                    .map_err(|e| McpError::InvalidMessage(format!("message is not UTF-8: {e}")))?;
                return JsonRpcMessage::parse(&text);
            }
        }
    }
}

fn timed_out(timeout: Option<Duration>) -> McpError {
    match timeout {
        Some(t) => McpError::Timeout(format!("no message within {t:?}")),
        None => McpError::Timeout("no message".to_string()),
    }
}

/// Did this read expire rather than fail?
///
/// A socket read timeout surfaces as `WouldBlock` on some platforms and
/// `TimedOut` on others, so both count.
fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// A signal arrived mid-read; retry rather than treating it as failure.
fn is_interrupt(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::Interrupted
}

/// True if an IO error means the peer went away, which is a clean close rather
/// than a hard error.
pub(crate) fn is_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A source that hands out scripted chunks, each optionally "arriving"
    /// only after a delay, so deadlines can be tested without real IO.
    struct Scripted {
        chunks: Arc<Mutex<Vec<Chunk>>>,
        deadline: Option<Instant>,
    }

    struct Chunk {
        bytes: Vec<u8>,
        /// When this chunk becomes readable.
        available_at: Instant,
    }

    impl Scripted {
        fn new(chunks: Vec<Chunk>) -> Self {
            Self {
                chunks: Arc::new(Mutex::new(chunks)),
                deadline: None,
            }
        }
    }

    impl Read for Scripted {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            loop {
                let mut chunks = self.chunks.lock().unwrap();
                let Some(next) = chunks.first() else {
                    return Ok(0); // end of stream
                };

                if Instant::now() >= next.available_at {
                    let chunk = chunks.remove(0);
                    let n = chunk.bytes.len().min(buf.len());
                    buf[..n].copy_from_slice(&chunk.bytes[..n]);
                    return Ok(n);
                }

                // Not here yet. Expire if we are past the deadline, otherwise
                // wait a beat and look again - which is what a blocking socket
                // read does.
                if let Some(deadline) = self.deadline {
                    if Instant::now() >= deadline {
                        return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
                    }
                }
                drop(chunks);
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }

    impl DeadlineRead for Scripted {
        fn set_deadline(&mut self, deadline: Option<Instant>) -> std::io::Result<()> {
            self.deadline = deadline;
            Ok(())
        }
    }

    fn now() -> Instant {
        Instant::now()
    }

    fn chunk(bytes: &str) -> Chunk {
        Chunk {
            bytes: bytes.as_bytes().to_vec(),
            available_at: now(),
        }
    }

    fn delayed(bytes: &str, delay: Duration) -> Chunk {
        Chunk {
            bytes: bytes.as_bytes().to_vec(),
            available_at: now() + delay,
        }
    }

    #[test]
    fn the_cap_is_not_overshot_by_a_chunk_that_carries_the_newline() {
        // The ceiling was only checked on the branch that keeps reading, so a
        // refill that happened to contain the terminating newline was appended
        // unchecked - an accepted message could reach the limit plus one whole
        // buffer refill, and the constant did not mean what it said.
        let mut reader = LineReader::with_limit(
            Scripted::new(vec![
                chunk(&"x".repeat(40)),
                chunk(&format!("{}\n", "y".repeat(40))),
            ]),
            50,
        );

        let error = reader.read_message(None).unwrap_err();
        assert!(
            matches!(&error, McpError::InvalidMessage(m) if m.contains("50 byte limit")),
            "80 bytes must not sneak past a 50 byte cap: {error}"
        );
        assert!(reader.partial.is_empty());
    }

    #[test]
    fn the_cap_can_be_changed_on_a_reader_already_in_use() {
        // `ServerConfig::max_message_bytes` is only real if it reaches the
        // transport the caller constructed, which is after the fact.
        let line = request_line(3);
        let mut reader = LineReader::with_limit(Scripted::new(vec![chunk(&line)]), 8);
        reader.set_limit(line.len());

        assert!(reader.read_message(None).is_ok());
    }

    #[test]
    fn a_zero_cap_is_clamped_rather_than_refusing_everything() {
        let line = request_line(4);
        let mut reader = LineReader::with_limit(Scripted::new(vec![chunk(&line)]), 0);

        // Still refused - one byte is a very small ceiling - but as an
        // answerable message rather than a reader that can never make progress.
        assert!(matches!(
            reader.read_message(None),
            Err(McpError::InvalidMessage(_))
        ));
        assert_eq!(reader.max_bytes, 1);
    }

    #[test]
    fn an_oversized_message_is_refused_without_growing_the_buffer() {
        // A peer that streams bytes and never sends a newline used to grow
        // `partial` until the process was OOM-killed - reachable by anyone who
        // can open a Unix socket, once per connection.
        let filler = "x".repeat(64);
        let mut reader = LineReader::with_limit(
            Scripted::new(vec![
                chunk(&filler),
                chunk(&filler),
                chunk(&filler),
                chunk("still going"),
                chunk("\n"),
                chunk(&request_line(7)),
            ]),
            100,
        );

        let error = reader.read_message(None).unwrap_err();
        assert!(
            matches!(&error, McpError::InvalidMessage(m) if m.contains("100 byte limit")),
            "{error}"
        );
        assert!(
            reader.partial.is_empty(),
            "the oversized bytes must not be retained"
        );

        // Framing resynchronizes: the next message parses normally.
        let recovered = reader.read_message(None).unwrap();
        let JsonRpcMessage::Request(request) = recovered else {
            panic!("expected a request");
        };
        assert_eq!(request.id, crate::types::RequestId::Number(7));
    }

    #[test]
    fn a_message_at_the_limit_is_still_accepted() {
        let line = request_line(1);
        let mut reader = LineReader::with_limit(Scripted::new(vec![chunk(&line)]), line.len());
        assert!(reader.read_message(None).is_ok());
    }

    fn request_line(id: i64) -> String {
        let msg = JsonRpcMessage::request(id, "ping", None);
        format!("{}\n", serde_json::to_string(&msg).unwrap())
    }

    #[test]
    fn reads_a_whole_message() {
        let mut reader = LineReader::new(Scripted::new(vec![chunk(&request_line(1))]));
        let message = reader.read_message(None).unwrap();
        assert_eq!(message, JsonRpcMessage::request(1i64, "ping", None));
    }

    #[test]
    fn reassembles_a_message_split_across_chunks() {
        let line = request_line(7);
        let (head, tail) = line.split_at(12);
        let mut reader = LineReader::new(Scripted::new(vec![chunk(head), chunk(tail)]));

        assert_eq!(
            reader.read_message(None).unwrap(),
            JsonRpcMessage::request(7i64, "ping", None)
        );
    }

    #[test]
    fn separates_messages_delivered_in_one_chunk() {
        let both = format!("{}{}", request_line(1), request_line(2));
        let mut reader = LineReader::new(Scripted::new(vec![chunk(&both)]));

        assert_eq!(
            reader.read_message(None).unwrap(),
            JsonRpcMessage::request(1i64, "ping", None)
        );
        assert_eq!(
            reader.read_message(None).unwrap(),
            JsonRpcMessage::request(2i64, "ping", None)
        );
    }

    #[test]
    fn end_of_stream_is_a_clean_close() {
        let mut reader = LineReader::new(Scripted::new(vec![]));
        assert!(matches!(
            reader.read_message(None),
            Err(McpError::TransportClosed)
        ));
    }

    #[test]
    fn expires_when_nothing_arrives() {
        let mut reader = LineReader::new(Scripted::new(vec![delayed(
            &request_line(1),
            Duration::from_secs(30),
        )]));

        let started = Instant::now();
        let err = reader.read_message(Some(Duration::from_millis(60)));
        assert!(matches!(err, Err(McpError::Timeout(_))), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5), "returned late");
    }

    #[test]
    fn a_timeout_keeps_the_bytes_it_already_read() {
        // The half that arrived must not be lost: dropping it would desync the
        // stream and every later message would fail to parse.
        let line = request_line(42);
        let (head, tail) = line.split_at(10);
        let mut reader = LineReader::new(Scripted::new(vec![
            chunk(head),
            delayed(tail, Duration::from_millis(120)),
        ]));

        assert!(matches!(
            reader.read_message(Some(Duration::from_millis(30))),
            Err(McpError::Timeout(_))
        ));

        // The tail lands, and the message completes from what was kept.
        assert_eq!(
            reader.read_message(Some(Duration::from_secs(5))).unwrap(),
            JsonRpcMessage::request(42i64, "ping", None)
        );
    }

    #[test]
    fn the_deadline_covers_the_message_not_each_chunk() {
        // Five chunks, each 40ms apart, against a 60ms budget. Per-read
        // timeouts would let this run to completion; a per-message deadline
        // must not.
        let line = request_line(1);
        let chunks = (0..5)
            .map(|i| Chunk {
                bytes: line.as_bytes()[i * 2..(i + 1) * 2].to_vec(),
                available_at: now() + Duration::from_millis(40 * (i as u64 + 1)),
            })
            .collect();

        let started = Instant::now();
        let mut reader = LineReader::new(Scripted::new(chunks));
        assert!(matches!(
            reader.read_message(Some(Duration::from_millis(60))),
            Err(McpError::Timeout(_))
        ));
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "waited {:?}, which is per-chunk not per-message",
            started.elapsed()
        );
    }

    #[test]
    fn a_zero_budget_expires_without_reading() {
        let mut reader = LineReader::new(Scripted::new(vec![chunk(&request_line(1))]));
        assert!(matches!(
            reader.read_message(Some(Duration::ZERO)),
            Err(McpError::Timeout(_))
        ));
    }

    #[test]
    fn survives_a_multibyte_character_split_across_chunks() {
        // "é" is two bytes. Decoding each chunk on its own would corrupt it.
        let msg =
            JsonRpcMessage::request(1i64, "ping", Some(serde_json::json!({ "text": "café" })));
        let line = format!("{}\n", serde_json::to_string(&msg).unwrap());
        let split = line.find('é').unwrap() + 1; // mid-character

        let mut reader = LineReader::new(Scripted::new(vec![
            chunk(std::str::from_utf8(&line.as_bytes()[..split]).unwrap_or("")),
            chunk(""),
        ]));
        // Build the split by bytes rather than str to actually cut the char.
        let mut reader2 = LineReader::new(Scripted::new(vec![
            Chunk {
                bytes: line.as_bytes()[..split].to_vec(),
                available_at: now(),
            },
            Chunk {
                bytes: line.as_bytes()[split..].to_vec(),
                available_at: now(),
            },
        ]));
        let _ = &mut reader;

        assert_eq!(reader2.read_message(None).unwrap(), msg);
    }

    #[test]
    fn rejects_a_line_that_is_not_utf8() {
        let mut reader = LineReader::new(Scripted::new(vec![Chunk {
            bytes: vec![0xff, 0xfe, b'\n'],
            available_at: now(),
        }]));
        assert!(matches!(
            reader.read_message(None),
            Err(McpError::InvalidMessage(_))
        ));
    }
}
