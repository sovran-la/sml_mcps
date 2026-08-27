//! Transport Layer
//!
//! Abstracts communication between client and server.

mod line;
mod origin;
mod socket_stream;
mod stdio;
mod stream;
mod tcp;

#[cfg(feature = "http")]
mod http;

#[cfg(feature = "http")]
mod http1;

#[cfg(unix)]
pub(crate) mod daemon_state;

#[cfg(unix)]
mod listener;

#[cfg(unix)]
mod signals;

#[cfg(unix)]
mod unix;

#[cfg(unix)]
mod unix_server;

pub use line::MAX_MESSAGE_BYTES;
pub use origin::OriginPolicy;
pub use stdio::StdioTransport;
pub use stream::StreamTransport;
pub use tcp::TcpTransport;

#[cfg(feature = "http")]
pub use http::{HttpServer, HttpTransport};

#[cfg(unix)]
pub use listener::{Listener, TcpSocketListener};

#[cfg(unix)]
pub(crate) use listener::UnixSocketListener;

#[cfg(unix)]
pub use unix::UnixTransport;

#[cfg(unix)]
pub use unix_server::{DEFAULT_MAX_CONNECTIONS, UnixServer};

// Shared with the bridge for stale-daemon detection (same PID-file convention).
#[cfg(unix)]
pub(crate) use unix_server::pid_path_for;

use crate::types::{JsonRpcMessage, Result};
use std::time::Duration;

/// Transport trait - sync read/write of JSON-RPC messages
pub trait Transport: Send + Sync {
    /// Read a single message from the transport
    ///
    /// Blocks until a message arrives, the peer hangs up
    /// ([`McpError::TransportClosed`](crate::McpError::TransportClosed)), or -
    /// if [`set_read_timeout`](Self::set_read_timeout) armed one - the deadline
    /// passes ([`McpError::Timeout`](crate::McpError::Timeout)).
    fn read(&mut self) -> Result<JsonRpcMessage>;

    /// Write a single message to the transport
    fn write(&mut self, message: &JsonRpcMessage) -> Result<()>;

    /// Close the transport
    fn close(&mut self) -> Result<()>;

    /// Half-close the write direction, signaling end-of-stream to the peer
    /// while leaving the read direction open.
    ///
    /// [`Bridge`](crate::Bridge) uses this on teardown: when the client hangs
    /// up, the request pump half-closes the upstream so the daemon sees EOF and
    /// can flush its in-flight responses, which the response pump then drains
    /// before the connection fully closes. A full [`close`](Self::close) here
    /// would discard those responses.
    ///
    /// Default: falls back to [`close`](Self::close) (full shutdown), which
    /// still signals EOF but cannot drain.
    fn close_write(&mut self) -> Result<()> {
        self.close()
    }

    /// Produce an independent write handle to the same sink.
    ///
    /// This enables full-duplex proxying in [`Bridge`](crate::Bridge): the two
    /// pump directions each own a non-overlapping handle (one reads the
    /// original, the other writes the clone), so a blocking read never holds a
    /// lock the writer needs.
    ///
    /// Returns `None` for transports that can't be cheaply split into
    /// independent read/write handles - those can't serve as a `Bridge`
    /// endpoint. Default: `None`.
    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        None
    }

    /// Bound how long [`read`](Self::read) may block, or `None` to restore
    /// indefinite blocking.
    ///
    /// A read that expires yields
    /// [`McpError::Timeout`](crate::McpError::Timeout) and keeps any bytes it
    /// already had, so the next read resumes mid-message rather than losing
    /// the connection's framing.
    ///
    /// Returns whether the transport can honor deadlines at all. `false` means
    /// the timeout was ignored and reads still block forever - the caller must
    /// decide whether that is acceptable rather than assume it was applied.
    /// Default: `Ok(false)`, since a transport that has not thought about this
    /// cannot deliver it.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<bool> {
        let _ = timeout;
        Ok(false)
    }

    /// Bound how many bytes one incoming message may occupy.
    ///
    /// A message over the limit is reported as
    /// [`McpError::InvalidMessage`](crate::McpError::InvalidMessage) and the
    /// framing resynchronizes, so the connection survives an oversized message
    /// rather than dying with it.
    ///
    /// [`Server`](crate::Server) pushes
    /// [`ServerConfig::max_message_bytes`](crate::ServerConfig::max_message_bytes)
    /// down through this, which is what makes the knob real for a transport the
    /// caller constructed. Default: ignored, for transports whose framing is
    /// bounded by something else (HTTP's `Content-Length`, say).
    fn set_max_message_bytes(&mut self, max: usize) {
        let _ = max;
    }
}

/// A boxed transport is a transport.
///
/// [`try_clone_writer`](Transport::try_clone_writer) hands back a `Box`, and
/// this is what lets that box be stored as an `Arc<Mutex<dyn Transport>>`
/// alongside the original rather than needing a parallel type for write
/// handles.
impl Transport for Box<dyn Transport> {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        (**self).read()
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        (**self).write(message)
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }

    fn close_write(&mut self) -> Result<()> {
        (**self).close_write()
    }

    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        (**self).try_clone_writer()
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<bool> {
        (**self).set_read_timeout(timeout)
    }

    fn set_max_message_bytes(&mut self, max: usize) {
        (**self).set_max_message_bytes(max)
    }
}
