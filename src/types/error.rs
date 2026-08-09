//! Error Types

use crate::types::JsonRpcError;
use thiserror::Error;

//
// MCP-defined JSON-RPC error codes
//
// The base JSON-RPC codes (-32700 parse, -32600 invalid request, -32601 method
// not found, -32602 invalid params, -32603 internal) live on `JsonRpcError`.
// These are the codes MCP layers on top.
//

/// Resource not found (`resources/read` against an unknown URI).
///
/// This is the *only* code in the implementation-defined `-32000..=-32099`
/// range that MCP assigns a meaning to, so nothing else may use it. The `data`
/// object carries the requested `uri`.
pub const RESOURCE_NOT_FOUND: i32 = -32002;

/// Server-side auth failure surfaced over a transport with no HTTP status.
///
/// MCP handles authorization at the HTTP layer (401/403), so the spec defines
/// no JSON-RPC code for it. This is a local extension, kept at its historical
/// value so existing clients keep recognizing it.
#[cfg(feature = "auth")]
pub const AUTH_ERROR: i32 = -32003;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum McpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Transport closed")]
    TransportClosed,

    /// A blocking wait gave up.
    ///
    /// Produced by a transport read that outlived its deadline, and by a
    /// server-initiated request whose client never answered. Distinguishable
    /// from [`McpError::Internal`] so a caller can retry rather than treat the
    /// session as broken.
    #[error("Timed out: {0}")]
    Timeout(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid params: {0}")]
    InvalidParams(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Tool error: {0}")]
    ToolError(String),

    #[error("Resource not found: {0}")]
    ResourceNotFound(String),

    #[error("Prompt not found: {0}")]
    PromptNotFound(String),

    /// A JSON-RPC error to replay exactly as-is.
    ///
    /// `tasks/result` "MUST return exactly what the underlying request would
    /// have returned", so a task that failed with a particular code and message
    /// has to reproduce them rather than be re-derived into something else.
    #[error("{}", .0.message)]
    Passthrough(JsonRpcError),

    #[cfg(feature = "auth")]
    #[error("Auth error: {0}")]
    Auth(String),
}

impl McpError {
    pub fn to_jsonrpc_error(&self) -> JsonRpcError {
        match self {
            McpError::Json(e) => JsonRpcError::parse_error(e.to_string()),
            McpError::InvalidMessage(msg) => JsonRpcError::invalid_request(msg),
            McpError::MethodNotFound(method) => {
                JsonRpcError::method_not_found(format!("Method not found: {}", method))
            }
            McpError::InvalidParams(msg) => JsonRpcError::invalid_params(msg),
            McpError::Internal(msg) => JsonRpcError::internal_error(msg),
            McpError::ToolError(msg) => JsonRpcError::new(-32000, msg),
            // MCP assigns -32002 to resource-not-found, and carries the URI in
            // `data` so clients can report which resource was missing.
            McpError::ResourceNotFound(uri) => {
                JsonRpcError::new(RESOURCE_NOT_FOUND, format!("Resource not found: {}", uri))
                    .with_data(serde_json::json!({ "uri": uri }))
            }
            // The spec classifies an unknown prompt name as invalid params, not
            // as its own code. It used to be reported as -32002 here, which
            // collided with resource-not-found and made a modern client read
            // "prompt not found" as "resource not found".
            McpError::PromptNotFound(name) => {
                JsonRpcError::invalid_params(format!("Prompt not found: {}", name))
                    .with_data(serde_json::json!({ "name": name }))
            }
            McpError::Passthrough(error) => error.clone(),
            McpError::Io(e) => JsonRpcError::internal_error(e.to_string()),
            McpError::TransportClosed => JsonRpcError::internal_error("Transport closed"),
            // MCP defines no code for a timeout, and the base set has no better
            // fit than "something went wrong on our side".
            McpError::Timeout(msg) => JsonRpcError::internal_error(format!("Timed out: {msg}")),
            #[cfg(feature = "auth")]
            McpError::Auth(msg) => JsonRpcError::new(AUTH_ERROR, format!("Auth error: {}", msg)),
        }
    }
}

pub type Result<T> = std::result::Result<T, McpError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let mcp_err = McpError::from(io_err);
        assert!(matches!(mcp_err, McpError::Io(_)));
        assert!(mcp_err.to_string().contains("IO error"));
    }

    #[test]
    fn test_json_error_conversion() {
        let json_err: serde_json::Error =
            serde_json::from_str::<String>("not valid json").unwrap_err();
        let mcp_err = McpError::from(json_err);
        assert!(matches!(mcp_err, McpError::Json(_)));
        assert!(mcp_err.to_string().contains("JSON error"));
    }

    #[test]
    fn test_transport_closed() {
        let err = McpError::TransportClosed;
        assert_eq!(err.to_string(), "Transport closed");
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32603); // internal error
    }

    #[test]
    fn test_invalid_message() {
        let err = McpError::InvalidMessage("bad message".into());
        assert!(err.to_string().contains("bad message"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32600); // invalid request
    }

    #[test]
    fn test_method_not_found() {
        let err = McpError::MethodNotFound("unknown/method".into());
        assert!(err.to_string().contains("unknown/method"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32601); // method not found
    }

    #[test]
    fn test_invalid_params() {
        let err = McpError::InvalidParams("missing required field".into());
        assert!(err.to_string().contains("missing required field"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32602); // invalid params
    }

    #[test]
    fn test_internal_error() {
        let err = McpError::Internal("something broke".into());
        assert!(err.to_string().contains("something broke"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32603); // internal error
    }

    #[test]
    fn test_tool_error() {
        let err = McpError::ToolError("tool failed".into());
        assert!(err.to_string().contains("tool failed"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32000); // custom error
    }

    #[test]
    fn test_resource_not_found() {
        let err = McpError::ResourceNotFound("file://missing".into());
        assert!(err.to_string().contains("file://missing"));
        let rpc_err = err.to_jsonrpc_error();
        // MCP's one implementation-defined code with an assigned meaning.
        assert_eq!(rpc_err.code, RESOURCE_NOT_FOUND);
        assert_eq!(rpc_err.code, -32002);
        assert_eq!(rpc_err.data.unwrap()["uri"], "file://missing");
    }

    #[test]
    fn test_prompt_not_found() {
        let err = McpError::PromptNotFound("missing-prompt".into());
        assert!(err.to_string().contains("missing-prompt"));
        let rpc_err = err.to_jsonrpc_error();
        // Invalid params, per the prompts spec.
        assert_eq!(rpc_err.code, -32602);
        assert_eq!(rpc_err.data.unwrap()["name"], "missing-prompt");
    }

    #[test]
    fn test_prompt_not_found_does_not_collide_with_resource_not_found() {
        // Regression guard for the -32002 collision: a client that maps -32002
        // to "resource not found" must never see it for a missing prompt.
        let prompt = McpError::PromptNotFound("p".into()).to_jsonrpc_error();
        let resource = McpError::ResourceNotFound("r".into()).to_jsonrpc_error();

        assert_ne!(prompt.code, resource.code);
        assert_ne!(prompt.code, RESOURCE_NOT_FOUND);
        assert_eq!(resource.code, RESOURCE_NOT_FOUND);
    }

    #[test]
    fn test_error_codes_are_distinct() {
        // Variants that mean different things to a client must not share a
        // code. (PromptNotFound is deliberately absent: the spec folds it into
        // the same -32602 that InvalidParams uses.)
        let codes = [
            McpError::ResourceNotFound("r".into())
                .to_jsonrpc_error()
                .code,
            McpError::ToolError("t".into()).to_jsonrpc_error().code,
            McpError::MethodNotFound("m".into()).to_jsonrpc_error().code,
            McpError::InvalidParams("p".into()).to_jsonrpc_error().code,
            McpError::Internal("i".into()).to_jsonrpc_error().code,
            McpError::InvalidMessage("m".into()).to_jsonrpc_error().code,
        ];

        let mut deduped = codes.to_vec();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            codes.len(),
            "duplicate error codes: {:?}",
            codes
        );
    }

    #[cfg(feature = "auth")]
    #[test]
    fn test_auth_error() {
        let err = McpError::Auth("invalid token".into());
        assert!(err.to_string().contains("invalid token"));
        let rpc_err = err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, AUTH_ERROR);
        assert_eq!(rpc_err.code, -32003);
    }

    #[test]
    fn test_json_parse_error_to_jsonrpc() {
        let json_err: serde_json::Error = serde_json::from_str::<String>("{").unwrap_err();
        let mcp_err = McpError::from(json_err);
        let rpc_err = mcp_err.to_jsonrpc_error();
        assert_eq!(rpc_err.code, -32700); // parse error
    }
}
