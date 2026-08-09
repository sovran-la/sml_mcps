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
///
/// [`RequestId::Null`] exists for one purpose: answering a message so malformed
/// that no id could be read from it. JSON-RPC 2.0 requires an error response
/// even then, and MCP says as much - "Error responses **MUST** include the same
/// ID as the request they correspond to (except in error cases where the ID
/// could not be read due a malformed request)". It is never a legal id on an
/// incoming request; [`JsonRpcMessage::parse`] rejects those.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
    /// Serializes as JSON `null`. Outgoing only - see the type docs.
    Null,
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

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        RequestId::String(s.to_owned())
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestId::Number(n) => write!(f, "{}", n),
            RequestId::String(s) => write!(f, "{}", s),
            RequestId::Null => f.write_str("null"),
        }
    }
}

/// A JSON-RPC message - request, response, or notification
///
/// Serialization is untagged: a message is just its envelope. Deserialization
/// does *not* rely on untagged variant order - [`JsonRpcMessage::from_value`]
/// discriminates on `method`/`id` so that a malformed message produces a
/// specific complaint instead of "no variant matched".
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
    /// All transports go through here so that message classification lives in
    /// exactly one place. Two error classes come out, and they are not the
    /// same thing:
    ///
    /// - text that is not JSON at all -> [`McpError::Json`], which the server
    ///   answers with `-32700 Parse error`
    /// - JSON that is not a JSON-RPC message -> [`McpError::InvalidMessage`],
    ///   answered with `-32600 Invalid Request`
    ///
    /// Either way the caller is expected to *answer*, not hang up.
    ///
    /// MCP removed JSON-RPC batching in 2025-06-18: the body of a request
    /// "**MUST** be a single JSON-RPC *request*, *notification*, or
    /// *response*." A top-level array is well-formed JSON but not a valid
    /// message, so it lands in the second class with an explanation.
    pub fn parse(text: &str) -> crate::types::Result<Self> {
        if text.trim_start().starts_with('[') {
            return Err(crate::types::McpError::InvalidMessage(
                "JSON-RPC batching is not supported; send a single request, \
                 notification, or response per message"
                    .into(),
            ));
        }
        Self::from_value(serde_json::from_str(text)?)
    }

    /// Classify an already-parsed JSON value as a JSON-RPC message.
    ///
    /// Discrimination is by shape - `method` means request or notification,
    /// `id` alone means response - rather than by serde's untagged fallback.
    /// Untagged deserialization can only say "no variant matched", which turns
    /// every small mistake (a float id, a stray key) into one opaque error, and
    /// silently reclassifies a request with an unusable id as a notification.
    pub fn from_value(value: Value) -> crate::types::Result<Self> {
        use crate::types::McpError;

        let Some(object) = value.as_object() else {
            return Err(McpError::InvalidMessage(format!(
                "a JSON-RPC message must be a JSON object, got {}",
                type_name_of(&value)
            )));
        };

        let has_method = object.contains_key("method");
        let id = object.get("id");

        match (has_method, id) {
            // "Unlike base JSON-RPC, the ID MUST NOT be null."
            (true, Some(Value::Null)) => Err(McpError::InvalidMessage(
                "request id must not be null".into(),
            )),
            (true, Some(_)) => serde_json::from_value(value)
                .map(JsonRpcMessage::Request)
                .map_err(|e| McpError::InvalidMessage(format!("invalid request: {e}"))),
            (true, None) => serde_json::from_value(value)
                .map(JsonRpcMessage::Notification)
                .map_err(|e| McpError::InvalidMessage(format!("invalid notification: {e}"))),
            (false, Some(_)) => serde_json::from_value(value)
                .map(JsonRpcMessage::Response)
                .map_err(|e| McpError::InvalidMessage(format!("invalid response: {e}"))),
            (false, None) => Err(McpError::InvalidMessage(
                "a JSON-RPC message needs either `method` (request/notification) \
                 or `id` (response)"
                    .into(),
            )),
        }
    }
}

/// The JSON type of `value`, for error messages.
fn type_name_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// JSON-RPC Request
///
/// Unknown keys are ignored rather than refused. A future revision that adds a
/// top-level field must not make every message from a newer peer unparseable,
/// and the envelope is discriminated by shape in
/// [`JsonRpcMessage::from_value`], so strictness here buys nothing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub jsonrpc: JsonRpcVersion,
}

/// JSON-RPC Response
///
/// Unknown keys are ignored; see [`JsonRpcRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub jsonrpc: JsonRpcVersion,
}

/// JSON-RPC Notification (no id, no response expected)
///
/// Unknown keys are ignored; see [`JsonRpcRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
