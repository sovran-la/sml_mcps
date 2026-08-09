//! Transport Layer
//!
//! Abstracts communication between client and server.

mod line;
mod origin;
mod stdio;

#[cfg(feature = "http")]
mod http;

#[cfg(unix)]
mod unix;

#[cfg(unix)]
mod unix_server;

pub use origin::OriginPolicy;
pub use stdio::StdioTransport;

#[cfg(feature = "http")]
pub use http::{HttpServer, HttpTransport};

#[cfg(unix)]
pub use unix::UnixTransport;

#[cfg(unix)]
pub use unix_server::UnixServer;

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
}
