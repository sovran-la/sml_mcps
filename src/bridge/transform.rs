//! Rewriting what the client sends, on its way through the shim.
//!
//! The shim is the one process on the path that runs as the MCP client's
//! child. It sees the client's environment, its working directory and its
//! user, and it sees every message. The daemon sees neither: it builds one
//! context per connection, and a tool sees its own arguments. So a fact about
//! the client that a tool needs has to be written into the message by the
//! shim, and this is where that happens.
//!
//! A daemon that tags each tool call with the agent that made it, read from
//! the shim's environment:
//!
//! ```ignore
//! let caller = std::env::var("MY_AGENT_ID").ok();
//!
//! server.serve_daemon_with(
//!     DaemonOptions::new(socket).shim_transform(move |message| {
//!         let JsonRpcMessage::Request(request) = message else {
//!             return Ok(());
//!         };
//!         if request.method != "tools/call" {
//!             return Ok(());
//!         }
//!         if let Some(arguments) = request
//!             .params
//!             .as_mut()
//!             .and_then(|params| params.get_mut("arguments"))
//!             .and_then(Value::as_object_mut)
//!         {
//!             // Replace whatever the model put there, or take it out.
//!             match &caller {
//!                 Some(id) => arguments.insert("_caller".into(), id.clone().into()),
//!                 None => arguments.remove("_caller"),
//!             };
//!         }
//!         Ok(())
//!     }),
//!     move |conn_id| AppContext::new(conn_id),
//! )
//! ```
//!
//! A daemon shared by clients in different projects, told which one each
//! call came from - and refusing to guess when the shim cannot say:
//!
//! ```ignore
//! DaemonOptions::new(socket).shim_transform(|message| {
//!     let JsonRpcMessage::Request(request) = message else {
//!         return Ok(());
//!     };
//!     if request.method != "tools/call" {
//!         return Ok(());
//!     }
//!     let cwd = std::env::current_dir()
//!         .map_err(|e| McpError::Internal(format!("no working directory: {e}")))?;
//!     if let Some(arguments) = request
//!         .params
//!         .as_mut()
//!         .and_then(|params| params.get_mut("arguments"))
//!         .and_then(Value::as_object_mut)
//!     {
//!         arguments.insert("_cwd".into(), cwd.display().to_string().into());
//!     }
//!     Ok(())
//! })
//! ```
//!
//! # What it is given
//!
//! Every message the client sends, one at a time and in the order they were
//! read - requests, notifications, and the client's responses to requests the
//! server made (sampling, elicitation, roots). Nothing the daemon sends back
//! goes through it. A line the shim cannot parse never becomes a message, so
//! it is answered as it always was and the transform never sees it.
//!
//! # When it fails
//!
//! A message whose transform fails is not forwarded, in its original form or
//! any other. Failing closed is the point: a transform that exists to remove
//! something must not be routed around by making it fail.
//!
//! - A **request** is answered by the shim with a JSON-RPC error carrying the
//!   request's id and the transform's error, so the client is not left waiting.
//! - A **notification** or **response** is dropped. JSON-RPC has no way to
//!   answer either. A dropped response leaves the server's request to time out
//!   on its own deadline.
//!
//! A transform that **panics** is treated as one that returned an internal
//! error, message by message: the panic is caught, the message is answered or
//! dropped as above, and the session carries on. The panic is still reported
//! on the shim's stderr by the panic hook, which is where an MCP client keeps
//! its server's logs. Uncaught, it would have killed the thread that forwards
//! client messages and left the session hanging with nothing said.

use crate::types::{JsonRpcMessage, McpError, Result};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

/// A rewrite applied to every message the client sends, before the shim
/// forwards it. See [this module](self).
pub(crate) type ShimTransform = Arc<dyn Fn(&mut JsonRpcMessage) -> Result<()> + Send + Sync>;

/// What to do with one client message once the transform has seen it.
#[derive(Debug, PartialEq)]
pub(super) enum Verdict {
    /// Forward it, as the transform left it.
    Forward,
    /// Do not forward it; send this back to the client instead.
    Answer(JsonRpcMessage),
    /// Do not forward it, and there is nobody to tell.
    Drop,
}

/// Run `transform` over `message` and say what becomes of it.
pub(super) fn apply(transform: &ShimTransform, message: &mut JsonRpcMessage) -> Verdict {
    // Taken before the transform runs, which may rewrite the id along with
    // everything else. The answer has to reach the request the client sent.
    let id = match message {
        JsonRpcMessage::Request(request) => Some(request.id.clone()),
        JsonRpcMessage::Notification(_) | JsonRpcMessage::Response(_) => None,
    };

    let outcome = catch_unwind(AssertUnwindSafe(|| transform(message))).unwrap_or_else(|_| {
        Err(McpError::Internal(
            "the shim's message transform panicked".into(),
        ))
    });

    match (outcome, id) {
        (Ok(()), _) => Verdict::Forward,
        (Err(error), Some(id)) => {
            Verdict::Answer(JsonRpcMessage::error(id, error.to_jsonrpc_error()))
        }
        (Err(_), None) => Verdict::Drop,
    }
}

#[cfg(test)]
mod tests;
