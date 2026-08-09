//! JSON-RPC 2.0 Types

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC version - always "2.0"
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct JsonRpcVersion(String);

impl Default for JsonRpcVersion {
    fn default() -> Self {
        JsonRpcVersion("2.0".to_owned())
    }
}

/// Request ID - can be number or string per JSON-RPC spec
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

impl From<i64> for RequestId {
    fn from(n: i64) -> Self {
        RequestId::Number(n)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        RequestId::String(s)
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestId::Number(n) => write!(f, "{}", n),
            RequestId::String(s) => write!(f, "{}", s),
        }
    }
}

/// A JSON-RPC message - request, response, or notification
///
/// Note: Order matters for serde untagged deserialization.
/// Request comes first (has method + id), then Notification (has method, no id),
/// then Response (has id, no method).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Notification(JsonRpcNotification),
    Response(JsonRpcResponse),
}

impl JsonRpcMessage {
    /// Create a request message
    pub fn request(
        id: impl Into<RequestId>,
        method: impl Into<String>,
        params: Option<Value>,
    ) -> Self {
        JsonRpcMessage::Request(JsonRpcRequest {
            id: id.into(),
            method: method.into(),
            params,
            jsonrpc: JsonRpcVersion::default(),
        })
    }

    /// Create a success response
    pub fn response(id: impl Into<RequestId>, result: Value) -> Self {
        JsonRpcMessage::Response(JsonRpcResponse {
            id: id.into(),
            result: Some(result),
            error: None,
            jsonrpc: JsonRpcVersion::default(),
        })
    }

    /// Create an error response
    pub fn error(id: impl Into<RequestId>, error: JsonRpcError) -> Self {
        JsonRpcMessage::Response(JsonRpcResponse {
            id: id.into(),
            result: None,
            error: Some(error),
            jsonrpc: JsonRpcVersion::default(),
        })
    }

    /// Create a notification (no response expected)
    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        JsonRpcMessage::Notification(JsonRpcNotification {
            method: method.into(),
            params,
            jsonrpc: JsonRpcVersion::default(),
        })
    }

    /// Parse one message off the wire.
    ///
    /// All transports go through here so that the batching rejection lives in
    /// exactly one place. MCP removed JSON-RPC batching in 2025-06-18: the body
    /// of a request "**MUST** be a single JSON-RPC *request*, *notification*,
    /// or *response*." A top-level array is well-formed JSON but not a valid
    /// message, so it gets `-32600 Invalid Request` with an explanation rather
    /// than a bare `-32700` parse error.
    pub fn parse(text: &str) -> crate::types::Result<Self> {
        if text.trim_start().starts_with('[') {
            return Err(crate::types::McpError::InvalidMessage(
                "JSON-RPC batching is not supported; send a single request, \
                 notification, or response per message"
                    .into(),
            ));
        }
        Ok(serde_json::from_str(text)?)
    }
}

/// JSON-RPC Request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub jsonrpc: JsonRpcVersion,
}

/// JSON-RPC Response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcResponse {
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub jsonrpc: JsonRpcVersion,
}

/// JSON-RPC Notification (no id, no response expected)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcNotification {
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub jsonrpc: JsonRpcVersion,
}

/// JSON-RPC Error
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    // Standard JSON-RPC error codes
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self::new(-32700, msg)
    }

    pub fn invalid_request(msg: impl Into<String>) -> Self {
        Self::new(-32600, msg)
    }

    pub fn method_not_found(msg: impl Into<String>) -> Self {
        Self::new(-32601, msg)
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self::new(-32602, msg)
    }

    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self::new(-32603, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = JsonRpcMessage::request(1i64, "tools/list", None);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"method\":\"tools/list\""));
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn test_response_serialization() {
        let resp = JsonRpcMessage::response(1i64, serde_json::json!({"tools": []}));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"result\""));
    }

    #[test]
    fn test_error_serialization() {
        let err = JsonRpcMessage::error(1i64, JsonRpcError::method_not_found("unknown method"));
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":-32601"));
    }

    #[test]
    fn test_request_id_types() {
        // Number ID
        let req1 = JsonRpcMessage::request(42i64, "test", None);
        let json1 = serde_json::to_string(&req1).unwrap();
        assert!(json1.contains("\"id\":42"));

        // String ID
        let req2 = JsonRpcMessage::request("abc-123".to_string(), "test", None);
        let json2 = serde_json::to_string(&req2).unwrap();
        assert!(json2.contains("\"id\":\"abc-123\""));
    }
}
