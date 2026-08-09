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
//! - a response for some *other* in-flight request -> handed to that waiter,
//!   whichever thread it is on, or parked if it is on this call stack
//! - anything else (a client request or notification) -> deferred, and
//!   replayed to the main loop before it reads again
//!
//! Nothing is dropped and nothing is delivered twice.
//!
//! ## Waiting from a thread that cannot read
//!
//! A task worker runs on its own thread, where nothing reads the transport. It
//! therefore cannot use the loop above. Instead it *registers* as a waiter and
//! blocks on a channel: it writes its own request (writes need no reader), and
//! whichever thread is reading hands the response over when it arrives.
//!
//! That is what makes `input_required` reachable. It relies on someone actually
//! reading, which is why `tasks/result` pumps the transport while it blocks
//! rather than only waiting on the store.
//!
//! This all works on any bidirectional transport (stdio, Unix socket). It cannot
//! work on the HTTP transport, where a request carries exactly one message and
//! there is no back-channel: the read simply fails and the waiter gets a clear
//! error instead of hanging.

use crate::types::{JsonRpcMessage, JsonRpcResponse, McpError, RequestId, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

/// How many abandoned request ids to remember.
///
/// Each one is a timed-out request whose answer may still turn up. Remembering
/// them is what stops a late response being parked for a waiter that has gone,
/// where it would sit until the process ended. The bound keeps a client that
/// never answers anything from turning that into a slow leak; forgetting the
/// oldest only costs one parked response.
const MAX_ABANDONED: usize = 64;

/// Routing state shared between the server loop and in-flight waiters.
#[derive(Debug, Default)]
pub(crate) struct RequestBroker {
    next_id: AtomicU64,
    /// Responses that arrived while someone else was reading.
    parked: Mutex<HashMap<RequestId, JsonRpcResponse>>,
    /// Client-originated messages read by a waiter, owed back to the main loop.
    deferred: Mutex<VecDeque<JsonRpcMessage>>,
    /// Requests nobody is waiting for any more, newest last.
    abandoned: Mutex<VecDeque<RequestId>>,
    /// Waiters on other threads, by the request each is waiting for.
    ///
    /// A response for one of these is handed straight over rather than parked,
    /// because the waiter is not on the reading thread and will never come back
    /// to collect it.
    waiters: Mutex<HashMap<RequestId, SyncSender<JsonRpcResponse>>>,
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

    /// Register a waiter that is not on the reading thread.
    ///
    /// The returned receiver yields the response once whoever is reading routes
    /// it here. The channel holds one message, so the reader hands it over
    /// without ever blocking.
    pub(crate) fn register_waiter(&self, id: &RequestId) -> Result<Receiver<JsonRpcResponse>> {
        let (sender, receiver) = sync_channel(1);
        self.waiters
            .lock()
            .map_err(|_| McpError::Internal("Request broker lock poisoned".into()))?
            .insert(id.clone(), sender);
        Ok(receiver)
    }

    /// Route a response to whoever is waiting for it.
    ///
    /// Prefers a registered off-thread waiter, since parking a response for a
    /// thread that is blocked on a channel would strand it. Falls back to
    /// parking, which is how a waiter further down this call stack collects it.
    pub(crate) fn deliver(&self, response: JsonRpcResponse) {
        let waiter = self
            .waiters
            .lock()
            .ok()
            .and_then(|mut waiters| waiters.remove(&response.id));

        match waiter {
            // A full or disconnected channel means the waiter gave up, so the
            // response has nowhere useful to go.
            Some(sender) => {
                let _ = sender.try_send(response);
            }
            None => self.park(response),
        }
    }

    /// Park a response nobody is currently waiting on this call stack for.
    ///
    /// A response to a request that has since been abandoned is dropped: its
    /// waiter gave up, so parking it would only fill the map with answers
    /// nobody will ever collect.
    pub(crate) fn park(&self, response: JsonRpcResponse) {
        if self.forget_abandoned(&response.id) {
            return;
        }
        if let Ok(mut parked) = self.parked.lock() {
            parked.insert(response.id.clone(), response);
        }
    }

    /// Stop waiting for `id`, so a late response to it is discarded.
    pub(crate) fn abandon(&self, id: &RequestId) {
        if let Ok(mut parked) = self.parked.lock() {
            parked.remove(id);
        }
        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.remove(id);
        }
        if let Ok(mut abandoned) = self.abandoned.lock() {
            abandoned.push_back(id.clone());
            while abandoned.len() > MAX_ABANDONED {
                abandoned.pop_front();
            }
        }
    }

    /// Was `id` abandoned? Removes it if so, since a response only arrives once.
    fn forget_abandoned(&self, id: &RequestId) -> bool {
        let Ok(mut abandoned) = self.abandoned.lock() else {
            return false;
        };
        match abandoned.iter().position(|known| known == id) {
            Some(index) => {
                abandoned.remove(index);
                true
            }
            None => false,
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
    fn an_abandoned_response_is_dropped_rather_than_parked() {
        let broker = RequestBroker::new();
        let id = RequestId::String("sml-0".into());

        broker.abandon(&id);
        broker.park(response("sml-0"));

        assert!(
            broker.take_parked(&id).is_none(),
            "nobody is waiting for this any more"
        );
    }

    #[test]
    fn abandoning_discards_a_response_that_already_arrived() {
        // The race that motivates this: the response lands between the waiter
        // deciding it has waited long enough and it saying so.
        let broker = RequestBroker::new();
        let id = RequestId::String("sml-0".into());

        broker.park(response("sml-0"));
        broker.abandon(&id);

        assert!(broker.take_parked(&id).is_none());
    }

    #[test]
    fn abandoning_one_request_does_not_affect_another() {
        let broker = RequestBroker::new();
        broker.abandon(&RequestId::String("sml-0".into()));

        broker.park(response("sml-1"));
        assert!(
            broker
                .take_parked(&RequestId::String("sml-1".into()))
                .is_some()
        );
    }

    #[test]
    fn an_abandoned_id_is_only_honored_once() {
        // Ids are never reused, so this is theoretical - but a stale entry that
        // swallowed a *later* response would be a genuinely confusing bug.
        let broker = RequestBroker::new();
        let id = RequestId::String("sml-0".into());

        broker.abandon(&id);
        broker.park(response("sml-0")); // dropped
        broker.park(response("sml-0")); // parked normally

        assert!(broker.take_parked(&id).is_some());
    }

    #[test]
    fn the_abandoned_list_stays_bounded() {
        // A client that answers nothing must not grow this without limit.
        let broker = RequestBroker::new();
        for i in 0..(MAX_ABANDONED * 4) {
            broker.abandon(&RequestId::String(format!("sml-{i}")));
        }

        assert_eq!(broker.abandoned.lock().unwrap().len(), MAX_ABANDONED);

        // The newest are the ones kept, since they are likeliest to still be
        // answered.
        let newest = RequestId::String(format!("sml-{}", MAX_ABANDONED * 4 - 1));
        broker.park(response(&format!("sml-{}", MAX_ABANDONED * 4 - 1)));
        assert!(broker.take_parked(&newest).is_none());
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
