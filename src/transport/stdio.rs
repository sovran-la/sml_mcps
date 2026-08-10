//! Stdio Transport
//!
//! Communication over stdin/stdout for local MCP servers.

use crate::transport::Transport;
use crate::transport::line::{DeadlineRead, LineReader};
use crate::types::{JsonRpcMessage, Result};
use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

/// Stdio transport - reads from stdin, writes to stdout
pub struct StdioTransport {
    reader: LineReader<Stdin>,
    stdout: io::Stdout,
    /// Applied to the next read; `None` blocks indefinitely.
    read_timeout: Option<Duration>,
}

impl StdioTransport {
    pub fn new() -> Self {
        Self {
            reader: LineReader::new(Stdin::new()),
            stdout: io::stdout(),
            read_timeout: None,
        }
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for StdioTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        self.reader.read_message(self.read_timeout)
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        let mut handle = self.stdout.lock();
        serde_json::to_writer(&mut handle, message)?;
        writeln!(handle)?;
        handle.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // Nothing to close for stdio
        Ok(())
    }

    fn close_write(&mut self) -> Result<()> {
        // Can't half-close stdout without ending the process; no-op. The bridge
        // shutting down means main is about to return and the process exits.
        Ok(())
    }

    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        // stdin/stdout are process-global handles; a fresh StdioTransport
        // writes to the same stdout. The bridge only ever calls write() on it.
        Some(Box::new(StdioTransport::new()))
    }

    /// Supported on unix, where the deadline is enforced by `poll`ing stdin.
    ///
    /// Elsewhere there is no portable way to interrupt a blocking read of
    /// stdin, so this reports `false` and reads keep blocking.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<bool> {
        self.read_timeout = timeout;
        Ok(cfg!(unix))
    }

    fn set_max_message_bytes(&mut self, max: usize) {
        self.reader.set_limit(max);
    }
}

/// Reader over the process's standard input.
///
/// Deliberately *not* [`io::Stdin`]: that keeps its own buffer, and a message
/// sitting in it is invisible to `poll`, so a client that pipelines two
/// messages into one write would look idle and time out with its answer already
/// in hand. Reading the descriptor directly puts the only buffer in
/// [`LineReader`], where the deadline logic can see it.
///
/// The descriptor is borrowed, never owned: dropping this must not close the
/// process's stdin.
struct Stdin {
    /// Only consulted on unix, where `poll` can enforce it.
    #[cfg_attr(not(unix), allow(dead_code))]
    deadline: Option<Instant>,
    /// Used on targets with no `poll`, where reads block regardless.
    #[cfg(not(unix))]
    fallback: io::Stdin,
}

impl Stdin {
    fn new() -> Self {
        Self {
            deadline: None,
            #[cfg(not(unix))]
            fallback: io::stdin(),
        }
    }
}

#[cfg(unix)]
impl Read for Stdin {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        const STDIN: libc::c_int = 0;

        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !poll_readable(STDIN, remaining)? {
                // Same shape a socket read timeout takes, which is what
                // `LineReader` recognizes.
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }

        // SAFETY: `buf` is a valid writable slice of exactly `buf.len()` bytes,
        // and fd 0 is read-only here - nothing takes ownership of it.
        let n = unsafe { libc::read(STDIN, buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }
}

#[cfg(not(unix))]
impl Read for Stdin {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // No portable deadline; the caller was told as much by
        // `set_read_timeout` returning false.
        self.fallback.read(buf)
    }
}

impl DeadlineRead for Stdin {
    fn set_deadline(&mut self, deadline: Option<Instant>) -> io::Result<()> {
        self.deadline = deadline;
        Ok(())
    }
}

/// Wait for `fd` to become readable, returning false if `timeout` runs out.
#[cfg(unix)]
fn poll_readable(fd: libc::c_int, timeout: Duration) -> io::Result<bool> {
    let mut pending = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };

    // `poll` takes whole milliseconds. Rounding a sub-millisecond remainder
    // down to 0 would spin; rounding up costs at most a millisecond of extra
    // patience.
    let millis = timeout.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;
    let millis = if millis == 0 && !timeout.is_zero() {
        1
    } else {
        millis
    };

    // SAFETY: one initialized pollfd, and a length matching it.
    let ready = unsafe { libc::poll(&mut pending, 1, millis) };
    match ready {
        // Timed out with nothing readable.
        0 => Ok(false),
        n if n > 0 => Ok(true),
        _ => {
            let e = io::Error::last_os_error();
            // A signal interrupted the wait. Report readable so the caller
            // retries; the deadline check will stop it if the budget is gone.
            if e.kind() == io::ErrorKind::Interrupted {
                Ok(true)
            } else {
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonRpcMessage;

    #[test]
    fn test_message_roundtrip() {
        // Test that we can serialize and deserialize messages
        let msg = JsonRpcMessage::request(1i64, "test/method", None);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: JsonRpcMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn test_stdio_transport_new() {
        let _transport = StdioTransport::new();
        // Just verify it creates without panic
    }

    #[test]
    fn test_stdio_transport_default() {
        let _transport = StdioTransport::default();
        // Verify Default trait works
    }

    #[test]
    fn test_stdio_transport_close() {
        let mut transport = StdioTransport::new();
        // Close should succeed (it's a no-op for stdio)
        assert!(transport.close().is_ok());
    }

    #[test]
    fn test_read_timeout_is_supported_on_unix() {
        let mut transport = StdioTransport::new();
        assert_eq!(
            transport
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap(),
            cfg!(unix)
        );
        // Clearing it is always accepted.
        assert_eq!(transport.set_read_timeout(None).unwrap(), cfg!(unix));
    }

    #[cfg(unix)]
    #[test]
    fn test_poll_readable_times_out_on_an_idle_pipe() {
        // A pipe with nothing written to it is never readable, so poll must
        // report the timeout rather than block.
        let mut fds = [0 as libc::c_int; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read_end, write_end) = (fds[0], fds[1]);

        let started = Instant::now();
        assert!(!poll_readable(read_end, Duration::from_millis(40)).unwrap());
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert!(started.elapsed() < Duration::from_secs(2));

        // And it reports readable once a byte lands.
        assert_eq!(
            unsafe { libc::write(write_end, b"x".as_ptr().cast(), 1) },
            1
        );
        assert!(poll_readable(read_end, Duration::from_millis(500)).unwrap());

        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_poll_readable_rounds_a_sub_millisecond_budget_up() {
        // Truncating 500µs to 0ms would turn a wait into a busy spin.
        let mut fds = [0 as libc::c_int; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

        assert!(!poll_readable(fds[0], Duration::from_micros(500)).unwrap());
        // A genuinely zero budget still returns immediately.
        assert!(!poll_readable(fds[0], Duration::ZERO).unwrap());

        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
    }

    // Note: read() against real stdin can't be exercised here - the test
    // harness owns it. Framing and deadline behaviour are covered by
    // `transport::line`, which is the code both transports share.
}
