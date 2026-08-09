//! Server-initiated request/response round trips.
//!
//! Elicitation and sampling invert the usual direction: the *server* sends a
//! JSON-RPC request and waits for the client's response. In an async server
//! that is a future; in a sync one it needs care, because the thing that reads
//! from the transport is the main loop, and the code that wants the response
//! is a tool running inside that loop.
//!
//! The rule that makes this tractable: **at most one reader exists at a time.**
//! Either the main loop is blocked in `read()`, or it has handed control to a
//! tool that is doing its own reading. Never both. So whoever is reading takes
//! responsibility for everything it pulls off the wire:
//!
//! - the response it is waiting for -> returned to the waiter
//! - a response for some *other* in-flight request -> parked for that waiter
//! - anything else (a client request or notification) -> deferred, and
//!   replayed to the main loop before it reads again
//!
//! Nothing is dropped and nothing is delivered twice.
//!
//! This works on any bidirectional transport (stdio, Unix socket). It cannot
//! work on the HTTP transport, where a request carries exactly one message and
//! there is no back-channel: the read simply fails and the waiter gets a clear
//! error instead of hanging.

use crate::types::{JsonRpcMessage, JsonRpcResponse, McpError, RequestId, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Routing state shared between the server loop and in-flight waiters.
#[derive(Debug, Default)]
pub(crate) struct RequestBroker {
    next_id: AtomicU64,
    /// Responses that arrived while someone else was reading.
    parked: Mutex<HashMap<RequestId, JsonRpcResponse>>,
    /// Client-originated messages read by a waiter, owed back to the main loop.
    deferred: Mutex<VecDeque<JsonRpcMessage>>,
}

impl RequestBroker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Allocate an id for a server-initiated request.
    ///
    /// String ids with an `sml-` prefix cannot collide with the numeric ids
    /// clients conventionally use, which keeps the two id spaces disjoint even
    /// though JSON-RPC shares one namespace between both directions.
    pub(crate) fn next_request_id(&self) -> RequestId {
        RequestId::String(format!(
            "sml-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Take a previously parked response for `id`, if one arrived.
    pub(crate) fn take_parked(&self, id: &RequestId) -> Option<JsonRpcResponse> {
        self.parked.lock().ok()?.remove(id)
    }

    /// Park a response nobody is currently waiting on this call stack for.
    pub(crate) fn park(&self, response: JsonRpcResponse) {
        if let Ok(mut parked) = self.parked.lock() {
            parked.insert(response.id.clone(), response);
        }
    }

    /// Set aside a client-originated message for the main loop.
    pub(crate) fn defer(&self, message: JsonRpcMessage) {
        if let Ok(mut deferred) = self.deferred.lock() {
            deferred.push_back(message);
        }
    }

    /// Take the next message the main loop still owes itself, if any.
    ///
    /// The main loop calls this before reading, so messages that arrived while
    /// a tool was awaiting a response are handled in arrival order.
    pub(crate) fn next_deferred(&self) -> Option<JsonRpcMessage> {
        self.deferred.lock().ok()?.pop_front()
    }

    /// Turn a client's response into the result value, or the error it carried.
    pub(crate) fn into_result(response: JsonRpcResponse) -> Result<serde_json::Value> {
        if let Some(error) = response.error {
            return Err(McpError::Internal(format!(
                "client returned error {}: {}",
                error.code, error.message
            )));
        }
        response
            .result
            .ok_or_else(|| McpError::InvalidMessage("response has neither result nor error".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonRpcError;

    fn response(id: &str) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::String(id.into()),
            result: Some(serde_json::json!({ "ok": true })),
            error: None,
        }
    }

    #[test]
    fn request_ids_are_unique_and_prefixed() {
        let broker = RequestBroker::new();
        let a = broker.next_request_id();
        let b = broker.next_request_id();

        assert_ne!(a, b);
        for id in [&a, &b] {
            let RequestId::String(s) = id else {
                panic!("expected a string id");
            };
            assert!(s.starts_with("sml-"), "{s}");
        }
    }

    #[test]
    fn request_ids_never_collide_with_numeric_client_ids() {
        let broker = RequestBroker::new();
        for _ in 0..100 {
            assert!(matches!(broker.next_request_id(), RequestId::String(_)));
        }
    }

    #[test]
    fn parked_responses_round_trip_once() {
        let broker = RequestBroker::new();
        let id = RequestId::String("sml-0".into());

        assert!(broker.take_parked(&id).is_none());
        broker.park(response("sml-0"));

        assert!(broker.take_parked(&id).is_some());
        // Taken exactly once - a second waiter must not see it.
        assert!(broker.take_parked(&id).is_none());
    }

    #[test]
    fn parked_responses_are_keyed_by_id() {
        let broker = RequestBroker::new();
        broker.park(response("sml-0"));
        broker.park(response("sml-1"));

        assert!(
            broker
                .take_parked(&RequestId::String("sml-1".into()))
                .is_some()
        );
        assert!(
            broker
                .take_parked(&RequestId::String("sml-0".into()))
                .is_some()
        );
        assert!(
            broker
                .take_parked(&RequestId::String("sml-2".into()))
                .is_none()
        );
    }

    #[test]
    fn deferred_messages_replay_in_arrival_order() {
        let broker = RequestBroker::new();
        broker.defer(JsonRpcMessage::notification("a", None));
        broker.defer(JsonRpcMessage::notification("b", None));

        let JsonRpcMessage::Notification(first) = broker.next_deferred().unwrap() else {
            panic!("expected notification");
        };
        let JsonRpcMessage::Notification(second) = broker.next_deferred().unwrap() else {
            panic!("expected notification");
        };

        assert_eq!(first.method, "a");
        assert_eq!(second.method, "b");
        assert!(broker.next_deferred().is_none());
    }

    #[test]
    fn into_result_extracts_the_result() {
        let value = RequestBroker::into_result(response("sml-0")).unwrap();
        assert_eq!(value["ok"], true);
    }

    #[test]
    fn into_result_surfaces_client_errors() {
        let response = JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::String("sml-0".into()),
            result: None,
            error: Some(JsonRpcError::invalid_params("bad mode")),
        };
        let err = RequestBroker::into_result(response).unwrap_err();
        assert!(err.to_string().contains("-32602"), "{err}");
        assert!(err.to_string().contains("bad mode"), "{err}");
    }

    #[test]
    fn into_result_rejects_a_response_with_neither_field() {
        let response = JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::String("sml-0".into()),
            result: None,
            error: None,
        };
        assert!(matches!(
            RequestBroker::into_result(response),
            Err(McpError::InvalidMessage(_))
        ));
    }
}
