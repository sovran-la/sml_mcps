//! JSON-RPC 2.0 Types

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The only JSON-RPC revision MCP speaks.
pub const JSONRPC_VERSION: &str = "2.0";

/// JSON-RPC version - always "2.0"
///
/// Strict about the value and lenient about the field being there at all, which
/// is the way round that helps: `"jsonrpc": "1.0"` is a peer speaking a
/// protocol this server does not implement, while a *missing* `jsonrpc` is a
/// client with one bug, whose request is otherwise perfectly readable. Refusing
/// the second while accepting the first - which is what an unvalidated newtype
/// plus a required field produced - punishes the benign case and waves the real
/// mismatch through.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct JsonRpcVersion(String);

impl Default for JsonRpcVersion {
    fn default() -> Self {
        JsonRpcVersion(JSONRPC_VERSION.to_owned())
    }
}

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let version = String::deserialize(deserializer)?;
        if version != JSONRPC_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported JSON-RPC version `{version}`; MCP requires `{JSONRPC_VERSION}`"
            )));
        }
        Ok(JsonRpcVersion(version))
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
    ///
    /// A *request* that does not deserialize keeps its id, in
    /// [`McpError::InvalidRequest`], so the error response can be correlated
    /// with the request that caused it. Only a request: the id on a malformed
    /// *response* belongs to this server's own outgoing id space, and echoing
    /// it back as an error would read as a failure of the server's own request.
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
            (true, Some(id)) => {
                // Read before the parse attempt, because the parse is what
                // fails and the id is what makes the failure answerable. An id
                // that is not itself legal - a float, an object - leaves this
                // `None` and the answer carries `id: null`, which is the case
                // the spec's exception is actually for.
                let id = serde_json::from_value::<RequestId>(id.clone()).ok();
                serde_json::from_value(value)
                    .map(JsonRpcMessage::Request)
                    .map_err(|e| match id {
                        Some(id) => McpError::InvalidRequest {
                            id,
                            message: format!("invalid request: {e}"),
                        },
                        None => McpError::InvalidMessage(format!("invalid request: {e}")),
                    })
            }
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
    #[serde(default)]
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
    #[serde(default)]
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
    #[serde(default)]
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
    use crate::types::McpError;

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

    #[test]
    fn an_unparseable_request_keeps_the_id_it_could_read() {
        // "Error responses MUST include the same ID as the request they
        // correspond to (except in error cases where the ID could not be read
        // due a malformed request)." The id is right there.
        let error = JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","id":7,"method":123}"#).unwrap_err();

        let McpError::InvalidRequest { id, .. } = &error else {
            panic!("expected the id to survive: {error}");
        };
        assert_eq!(id, &RequestId::Number(7));
        assert_eq!(error.to_jsonrpc_error().code, -32600);
    }

    #[test]
    fn a_string_id_survives_an_unparseable_request_too() {
        let error =
            JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","id":"abc","method":[]}"#).unwrap_err();

        let McpError::InvalidRequest { id, .. } = &error else {
            panic!("expected the id to survive: {error}");
        };
        assert_eq!(id, &RequestId::String("abc".into()));
    }

    #[test]
    fn an_id_that_is_not_a_legal_id_does_not_survive() {
        // This is what the spec's exception is actually for: a float id is not
        // a JSON-RPC id, so there is nothing to echo and `null` is correct.
        for body in [
            r#"{"jsonrpc":"2.0","id":1.5,"method":123}"#,
            r#"{"jsonrpc":"2.0","id":{"a":1},"method":123}"#,
        ] {
            let error = JsonRpcMessage::parse(body).unwrap_err();
            assert!(
                matches!(error, McpError::InvalidMessage(_)),
                "{body} -> {error}"
            );
        }
    }

    #[test]
    fn a_malformed_response_does_not_get_its_id_echoed() {
        // A response's id lives in *this server's* outgoing id space. Echoing
        // it back on an error would read as that outgoing request failing.
        let outcome = JsonRpcMessage::parse(r#"{"id":3,"result":[1,2],"extra":true}"#);

        assert!(
            !matches!(outcome, Err(McpError::InvalidRequest { .. })),
            "a response is not a request"
        );
    }

    #[test]
    fn a_missing_jsonrpc_field_is_not_fatal() {
        // Forgetting `jsonrpc` is a common client bug, and the request is
        // otherwise perfectly readable. Refusing it while waving `"1.0"`
        // through had the strictness exactly backwards.
        let message = JsonRpcMessage::parse(r#"{"id":8,"method":"ping"}"#).unwrap();

        let JsonRpcMessage::Request(request) = message else {
            panic!("expected a request");
        };
        assert_eq!(request.id, RequestId::Number(8));
        assert_eq!(request.jsonrpc, JsonRpcVersion::default());
    }

    #[test]
    fn a_missing_jsonrpc_field_is_not_fatal_on_notifications_or_responses() {
        assert!(matches!(
            JsonRpcMessage::parse(r#"{"method":"notifications/initialized"}"#).unwrap(),
            JsonRpcMessage::Notification(_)
        ));
        assert!(matches!(
            JsonRpcMessage::parse(r#"{"id":1,"result":{}}"#).unwrap(),
            JsonRpcMessage::Response(_)
        ));
    }

    #[test]
    fn a_wrong_jsonrpc_version_is_refused_with_its_id() {
        // "All messages between MCP clients and servers MUST follow the
        // JSON-RPC 2.0 specification." A peer announcing 1.0 is announcing a
        // protocol this server does not implement; answering it as if it were
        // 2.0 hides a real mismatch.
        let error =
            JsonRpcMessage::parse(r#"{"jsonrpc":"1.0","id":9,"method":"ping"}"#).unwrap_err();

        let McpError::InvalidRequest { id, message } = &error else {
            panic!("expected an answerable refusal: {error}");
        };
        assert_eq!(id, &RequestId::Number(9));
        assert!(message.contains("1.0"), "{message}");
    }

    #[test]
    fn the_version_still_round_trips() {
        let request = JsonRpcMessage::parse(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#).unwrap();
        assert!(
            serde_json::to_string(&request)
                .unwrap()
                .contains("\"jsonrpc\":\"2.0\"")
        );
    }
}
