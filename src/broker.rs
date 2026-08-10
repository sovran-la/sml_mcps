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

/// How many parked responses to keep.
///
/// A parked response is one a waiter further down the call stack will collect
/// on its way back up, so at any moment there are as many useful entries as
/// there are nested waiters - single digits, and in practice one. Keeping more
/// than a handful buys nothing, and keeping them all is a remote OOM: every
/// response a peer sends before the server has asked anything is unmatched by
/// construction, so a client can park arbitrarily large values, arbitrarily
/// many times, before the handshake.
const MAX_PARKED: usize = 16;

/// How many client-originated messages to hold for the main loop.
///
/// The queue only grows while something blocks the loop - a `tasks/result` on a
/// task with a client-chosen TTL, or a server-initiated round trip - and it
/// drains one message per iteration once that clears. Both windows are long
/// enough (an hour, two minutes) for a client to fill memory with well-formed
/// requests, so the queue has a ceiling and says so rather than growing.
const MAX_DEFERRED: usize = 256;

/// Routing state shared between the server loop and in-flight waiters.
#[derive(Debug, Default)]
pub(crate) struct RequestBroker {
    next_id: AtomicU64,
    /// Responses that arrived while someone else was reading, oldest first.
    ///
    /// A queue rather than a map: it is never more than [`MAX_PARKED`] long, so
    /// a scan is free, and insertion order is what eviction needs.
    parked: Mutex<VecDeque<JsonRpcResponse>>,
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
        let mut parked = self.parked.lock().ok()?;
        let at = parked.iter().position(|response| &response.id == id)?;
        parked.remove(at)
    }

    /// Is `id` one this broker handed out?
    ///
    /// Server-initiated ids are `sml-N` for an `N` this counter has already
    /// issued, and [`next_request_id`](Self::next_request_id) never reuses one.
    /// Anything else is an answer to a request the server never sent - a
    /// client's own id echoed back, or noise - and there is no waiter it could
    /// ever belong to.
    fn was_issued_here(&self, id: &RequestId) -> bool {
        let RequestId::String(text) = id else {
            return false;
        };
        text.strip_prefix("sml-")
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n < self.next_id.load(Ordering::Relaxed))
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
    /// Two things are dropped rather than kept:
    ///
    /// - a response to an id this broker never issued. Nothing can ever wait
    ///   for it, so it is garbage by construction - and *every* response is
    ///   garbage by construction until the server asks its first question,
    ///   which is what made an unbounded map a pre-handshake remote OOM.
    /// - a response to a request that has since been abandoned: its waiter gave
    ///   up, so parking it would only fill the queue with answers nobody will
    ///   ever collect.
    ///
    /// What survives both is bounded at [`MAX_PARKED`], oldest evicted first.
    pub(crate) fn park(&self, response: JsonRpcResponse) {
        if !self.was_issued_here(&response.id) {
            return;
        }
        if self.forget_abandoned(&response.id) {
            return;
        }
        if let Ok(mut parked) = self.parked.lock() {
            // Ids are never reused, so a duplicate is a peer answering twice.
            // The newer answer replaces the older in place rather than queuing
            // behind it, since `take_parked` would only ever return the first.
            if let Some(at) = parked.iter().position(|known| known.id == response.id) {
                parked[at] = response;
                return;
            }
            parked.push_back(response);
            while parked.len() > MAX_PARKED {
                parked.pop_front();
            }
        }
    }

    /// Stop waiting for `id`, so a late response to it is discarded.
    pub(crate) fn abandon(&self, id: &RequestId) {
        if let Ok(mut parked) = self.parked.lock() {
            parked.retain(|response| &response.id != id);
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
    ///
    /// Hands the message back as `Err` when there is no room for it, so the
    /// caller can tell the client rather than lose the request silently. The
    /// message refused is the *newest* - shedding at the door keeps whatever is
    /// already queued moving in arrival order, and gives the client the
    /// backpressure signal immediately instead of after an hour of queueing.
    #[allow(clippy::result_large_err)] // The `Err` *is* the message handed back.
    pub(crate) fn defer(&self, message: JsonRpcMessage) -> std::result::Result<(), JsonRpcMessage> {
        let Ok(mut deferred) = self.deferred.lock() else {
            return Err(message);
        };
        if deferred.len() >= MAX_DEFERRED {
            return Err(message);
        }
        deferred.push_back(message);
        Ok(())
    }

    /// Take the next message the main loop still owes itself, if any.
    ///
    /// The main loop calls this before reading, so messages that arrived while
    /// a tool was awaiting a response are handled in arrival order.
    pub(crate) fn next_deferred(&self) -> Option<JsonRpcMessage> {
        self.deferred.lock().ok()?.pop_front()
    }

    /// How many off-thread waiters are registered.
    ///
    /// Every one of them is a `SyncSender` held until its response arrives, so
    /// "did that exit path clean up?" is a question worth being able to ask.
    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.waiters
            .lock()
            .map(|waiters| waiters.len())
            .unwrap_or(0)
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

    /// A broker that has already handed out `count` ids, so `sml-0` through
    /// `sml-{count-1}` are answers it might legitimately receive.
    fn broker_awaiting(count: usize) -> RequestBroker {
        let broker = RequestBroker::new();
        for _ in 0..count {
            broker.next_request_id();
        }
        broker
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
        let broker = broker_awaiting(1);
        let id = RequestId::String("sml-0".into());

        assert!(broker.take_parked(&id).is_none());
        broker.park(response("sml-0"));

        assert!(broker.take_parked(&id).is_some());
        // Taken exactly once - a second waiter must not see it.
        assert!(broker.take_parked(&id).is_none());
    }

    #[test]
    fn parked_responses_are_keyed_by_id() {
        let broker = broker_awaiting(2);
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
        broker
            .defer(JsonRpcMessage::notification("a", None))
            .unwrap();
        broker
            .defer(JsonRpcMessage::notification("b", None))
            .unwrap();

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
        let broker = broker_awaiting(1);
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
        let broker = broker_awaiting(1);
        let id = RequestId::String("sml-0".into());

        broker.park(response("sml-0"));
        broker.abandon(&id);

        assert!(broker.take_parked(&id).is_none());
    }

    #[test]
    fn abandoning_one_request_does_not_affect_another() {
        let broker = broker_awaiting(2);
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
        let broker = broker_awaiting(1);
        let id = RequestId::String("sml-0".into());

        broker.abandon(&id);
        broker.park(response("sml-0")); // dropped
        broker.park(response("sml-0")); // parked normally

        assert!(broker.take_parked(&id).is_some());
    }

    #[test]
    fn a_response_to_an_id_the_server_never_issued_is_not_parked() {
        // The remote OOM: before the server asks its first question there is no
        // id it could be answering, so every response a peer sends is garbage.
        // Parking them was unbounded - ~48 messages of 8 MiB reached a
        // gigabyte, and the server kept answering pings throughout.
        let broker = RequestBroker::new();

        broker.park(response("sml-0")); // never issued
        broker.park(response("client-made-this-up"));
        broker.park(JsonRpcResponse {
            jsonrpc: Default::default(),
            id: RequestId::Number(7),
            result: Some(serde_json::json!({})),
            error: None,
        });

        assert_eq!(broker.parked.lock().unwrap().len(), 0);
    }

    #[test]
    fn a_response_to_an_id_beyond_the_counter_is_not_parked() {
        // Well-formed prefix, plausible shape, but the server has only issued
        // `sml-0` - so `sml-999` answers nothing.
        let broker = broker_awaiting(1);

        broker.park(response("sml-999"));

        assert!(
            broker
                .take_parked(&RequestId::String("sml-999".into()))
                .is_none()
        );
    }

    #[test]
    fn the_parked_queue_stays_bounded() {
        // Even ids the server did issue cannot accumulate without limit: a
        // client that answers every request twice, or a server whose waiters
        // all timed out, must not grow this forever.
        let broker = broker_awaiting(MAX_PARKED * 4);
        for i in 0..(MAX_PARKED * 4) {
            broker.park(response(&format!("sml-{i}")));
        }

        assert_eq!(broker.parked.lock().unwrap().len(), MAX_PARKED);

        // The newest survive; the oldest were evicted to make room.
        assert!(
            broker
                .take_parked(&RequestId::String("sml-0".into()))
                .is_none()
        );
        let newest = format!("sml-{}", MAX_PARKED * 4 - 1);
        assert!(broker.take_parked(&RequestId::String(newest)).is_some());
    }

    #[test]
    fn a_repeated_answer_replaces_rather_than_accumulates() {
        // Two answers to one id are one entry, not two - otherwise a peer that
        // answers in a loop fills the queue with duplicates of a single id and
        // evicts every other waiter's response.
        let broker = broker_awaiting(2);
        for _ in 0..100 {
            broker.park(response("sml-0"));
        }
        broker.park(response("sml-1"));

        assert_eq!(broker.parked.lock().unwrap().len(), 2);
        assert!(
            broker
                .take_parked(&RequestId::String("sml-1".into()))
                .is_some(),
            "the other waiter's answer survived the flood"
        );
    }

    #[test]
    fn the_deferred_queue_stays_bounded_and_says_so() {
        // While `tasks/result` blocks, everything the client sends is deferred
        // to a loop that cannot run. That window is up to an hour, so the queue
        // has to have an end - and refusing loudly is what lets the server
        // answer `-32603 overloaded` instead of dropping the request.
        let broker = RequestBroker::new();
        for i in 0..MAX_DEFERRED {
            broker
                .defer(JsonRpcMessage::request(i as i64, "ping", None))
                .expect("under the ceiling");
        }

        let refused = broker
            .defer(JsonRpcMessage::request(9999i64, "ping", None))
            .expect_err("the queue is full");

        // What comes back is the message that could not be queued, so the
        // caller knows which request to answer.
        let JsonRpcMessage::Request(request) = refused else {
            panic!("expected the request back");
        };
        assert_eq!(request.id, RequestId::Number(9999));
        assert_eq!(broker.deferred.lock().unwrap().len(), MAX_DEFERRED);
    }

    #[test]
    fn a_full_deferred_queue_still_replays_what_it_accepted() {
        // Shedding the newest keeps the queue a FIFO: nothing already accepted
        // is lost to make room for something newer.
        let broker = RequestBroker::new();
        for i in 0..MAX_DEFERRED {
            let _ = broker.defer(JsonRpcMessage::request(i as i64, "ping", None));
        }
        let _ = broker.defer(JsonRpcMessage::request(9999i64, "ping", None));

        let JsonRpcMessage::Request(first) = broker.next_deferred().unwrap() else {
            panic!("expected a request");
        };
        assert_eq!(first.id, RequestId::Number(0));
    }

    #[test]
    fn the_abandoned_list_stays_bounded() {
        // A client that answers nothing must not grow this without limit.
        let broker = broker_awaiting(MAX_ABANDONED * 4);
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
