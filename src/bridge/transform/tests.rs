use super::*;
use crate::bridge::Bridge;
use crate::transport::{Transport, UnixTransport};
use crate::types::{JsonRpcMessage, RequestId};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

fn transform(
    f: impl Fn(&mut JsonRpcMessage) -> Result<()> + Send + Sync + 'static,
) -> ShimTransform {
    Arc::new(f)
}

fn call(id: i64, arguments: Value) -> JsonRpcMessage {
    JsonRpcMessage::request(
        id,
        "tools/call",
        Some(json!({ "name": "echo", "arguments": arguments })),
    )
}

fn arguments(message: &JsonRpcMessage) -> Value {
    let JsonRpcMessage::Request(request) = message else {
        panic!("expected a request, got {message:?}");
    };
    request.params.as_ref().unwrap()["arguments"].clone()
}

/// Write `key` into a `tools/call`'s arguments, or take it out when `value`
/// is `None` - the shape of the hook the shim exists for.
fn stamp(key: &'static str, value: Option<&'static str>) -> ShimTransform {
    transform(move |message| {
        let JsonRpcMessage::Request(request) = message else {
            return Ok(());
        };
        if request.method != "tools/call" {
            return Ok(());
        }
        let arguments = request
            .params
            .as_mut()
            .and_then(|params| params.get_mut("arguments"))
            .and_then(Value::as_object_mut)
            .expect("a call with arguments");
        match value {
            Some(value) => arguments.insert(key.into(), value.into()),
            None => arguments.remove(key),
        };
        Ok(())
    })
}

/// A bridge between two socket pairs: the test speaks for the client on one
/// and for the daemon on the other.
struct Rig {
    client: UnixTransport,
    daemon: UnixTransport,
    bridge: thread::JoinHandle<Result<()>>,
}

impl Rig {
    fn new(transform: Option<ShimTransform>) -> Self {
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();
        for stream in [&client_peer, &upstream_peer] {
            // A message the bridge wrongly held back fails the test rather
            // than hanging it.
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
        }

        let client = UnixTransport::from_stream(client_bridge);
        let upstream = UnixTransport::from_stream(upstream_bridge);
        let bridge = thread::spawn(move || match transform {
            Some(transform) => crate::bridge::run_transformed(client, upstream, transform),
            None => Bridge::run(client, upstream),
        });

        Self {
            client: UnixTransport::from_stream(client_peer),
            daemon: UnixTransport::from_stream(upstream_peer),
            bridge,
        }
    }

    fn finish(self) {
        drop(self.client);
        drop(self.daemon);
        self.bridge.join().unwrap().unwrap();
    }
}

// ---- apply ---------------------------------------------------------------

#[test]
fn a_transform_that_succeeds_forwards_what_it_left() {
    let mut message = call(1, json!({ "a": 1 }));
    let verdict = apply(&stamp("_caller", Some("agent-7")), &mut message);

    assert_eq!(verdict, Verdict::Forward);
    assert_eq!(arguments(&message), json!({ "a": 1, "_caller": "agent-7" }));
}

#[test]
fn a_failed_request_is_answered_with_the_transforms_error() {
    let failing = transform(|_| Err(McpError::InvalidParams("no agent id".into())));
    let mut message = call(41, json!({}));

    let Verdict::Answer(JsonRpcMessage::Response(answer)) = apply(&failing, &mut message) else {
        panic!("a request has to be answered");
    };
    assert_eq!(answer.id, RequestId::Number(41));
    assert!(answer.result.is_none());
    let error = answer.error.unwrap();
    assert_eq!(error.code, -32602);
    assert!(error.message.contains("no agent id"), "{}", error.message);
}

#[test]
fn the_answer_goes_to_the_id_the_client_sent() {
    // A transform that rewrote the id before failing must not misaddress the
    // answer: the client is waiting on the id it chose.
    let rewrites_then_fails = transform(|message| {
        if let JsonRpcMessage::Request(request) = message {
            request.id = RequestId::Number(999);
        }
        Err(McpError::Internal("then failed".into()))
    });
    let mut message = JsonRpcMessage::request("abc", "tools/call", None);

    let Verdict::Answer(JsonRpcMessage::Response(answer)) =
        apply(&rewrites_then_fails, &mut message)
    else {
        panic!("a request has to be answered");
    };
    assert_eq!(answer.id, RequestId::String("abc".into()));
}

#[test]
fn a_failed_notification_or_response_is_dropped() {
    let failing = transform(|_| Err(McpError::Internal("no".into())));

    let mut note = JsonRpcMessage::notification("notifications/initialized", None);
    assert_eq!(apply(&failing, &mut note), Verdict::Drop);

    let mut reply = JsonRpcMessage::response(7i64, json!({ "roots": [] }));
    assert_eq!(apply(&failing, &mut reply), Verdict::Drop);
}

#[test]
fn a_panicking_transform_is_an_internal_error() {
    let panicking = transform(|_| panic!("transform bug"));

    let mut request = call(3, json!({}));
    let Verdict::Answer(JsonRpcMessage::Response(answer)) = apply(&panicking, &mut request) else {
        panic!("a request has to be answered");
    };
    assert_eq!(answer.id, RequestId::Number(3));
    let error = answer.error.unwrap();
    assert_eq!(error.code, -32603);
    assert!(error.message.contains("panicked"), "{}", error.message);

    let mut note = JsonRpcMessage::notification("notifications/cancelled", None);
    assert_eq!(apply(&panicking, &mut note), Verdict::Drop);
}

// ---- through the bridge --------------------------------------------------

#[test]
fn every_client_message_is_transformed_in_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let tag = {
        let seen = seen.clone();
        transform(move |message| {
            let n = {
                let mut seen = seen.lock().unwrap();
                seen.push(message.clone());
                seen.len()
            };
            // Tag each one with the order the transform saw it in.
            match message {
                JsonRpcMessage::Request(r) => r.params = Some(json!({ "seen": n })),
                JsonRpcMessage::Notification(n_) => n_.params = Some(json!({ "seen": n })),
                JsonRpcMessage::Response(r) => r.result = Some(json!({ "seen": n })),
            }
            Ok(())
        })
    };
    let mut rig = Rig::new(Some(tag));

    let sent = [
        JsonRpcMessage::request(1i64, "initialize", None),
        JsonRpcMessage::notification("notifications/initialized", None),
        // The client's answer to a request the server made.
        JsonRpcMessage::response("srv-1", json!({})),
        JsonRpcMessage::request(2i64, "tools/call", None),
        JsonRpcMessage::request(3i64, "ping", None),
    ];
    for message in &sent {
        rig.client.write(message).unwrap();
    }

    for n in 1..=sent.len() {
        let forwarded = rig.daemon.read().unwrap();
        let payload = match &forwarded {
            JsonRpcMessage::Request(r) => r.params.clone(),
            JsonRpcMessage::Notification(n) => n.params.clone(),
            JsonRpcMessage::Response(r) => r.result.clone(),
        };
        assert_eq!(payload, Some(json!({ "seen": n })), "{forwarded:?}");
    }
    assert_eq!(*seen.lock().unwrap(), sent);

    rig.finish();
}

#[test]
fn nothing_from_the_daemon_is_transformed() {
    let calls = Arc::new(Mutex::new(0usize));
    let counting = {
        let calls = calls.clone();
        transform(move |message| {
            *calls.lock().unwrap() += 1;
            *message = JsonRpcMessage::notification("rewritten", None);
            Ok(())
        })
    };
    let mut rig = Rig::new(Some(counting));

    let from_daemon = [
        JsonRpcMessage::response(1i64, json!({ "content": [] })),
        JsonRpcMessage::notification("notifications/message", Some(json!({ "level": "info" }))),
        JsonRpcMessage::request("srv-1", "roots/list", None),
        call(9, json!({ "_caller": "agent-7" })),
    ];
    for message in &from_daemon {
        rig.daemon.write(message).unwrap();
    }
    for message in &from_daemon {
        assert_eq!(&rig.client.read().unwrap(), message);
    }
    assert_eq!(*calls.lock().unwrap(), 0);

    rig.finish();
}

#[test]
fn a_transform_can_add_and_remove_a_key() {
    let mut rig = Rig::new(Some(stamp("_caller", None)));

    // Whatever a model put under the key is gone, and nothing else is.
    rig.client
        .write(&call(1, json!({ "task": "x", "_caller": "forged" })))
        .unwrap();
    assert_eq!(
        arguments(&rig.daemon.read().unwrap()),
        json!({ "task": "x" })
    );
    rig.finish();

    let mut rig = Rig::new(Some(stamp("_caller", Some("agent-7"))));
    rig.client
        .write(&call(1, json!({ "task": "x", "_caller": "forged" })))
        .unwrap();
    assert_eq!(
        arguments(&rig.daemon.read().unwrap()),
        json!({ "task": "x", "_caller": "agent-7" })
    );
    // Not a tool call: left alone.
    let ping = JsonRpcMessage::request(2i64, "ping", Some(json!({ "_caller": "x" })));
    rig.client.write(&ping).unwrap();
    assert_eq!(rig.daemon.read().unwrap(), ping);
    rig.finish();
}

#[test]
fn a_refused_request_is_answered_and_the_session_carries_on() {
    let refuse_secrets = transform(|message| match message {
        JsonRpcMessage::Request(r) if r.method == "secret" => {
            Err(McpError::InvalidParams("not through this shim".into()))
        }
        JsonRpcMessage::Notification(n) if n.method == "secret" => {
            Err(McpError::InvalidParams("nor this".into()))
        }
        JsonRpcMessage::Request(r) if r.method == "bug" => panic!("transform bug"),
        _ => Ok(()),
    });
    let mut rig = Rig::new(Some(refuse_secrets));

    rig.client
        .write(&JsonRpcMessage::request(1i64, "secret", None))
        .unwrap();
    let JsonRpcMessage::Response(answer) = rig.client.read().unwrap() else {
        panic!("the shim must answer a request it will not forward");
    };
    assert_eq!(answer.id, RequestId::Number(1));
    assert!(
        answer
            .error
            .unwrap()
            .message
            .contains("not through this shim")
    );

    rig.client
        .write(&JsonRpcMessage::notification("secret", None))
        .unwrap();

    rig.client
        .write(&JsonRpcMessage::request(2i64, "bug", None))
        .unwrap();
    let JsonRpcMessage::Response(answer) = rig.client.read().unwrap() else {
        panic!("a panicking transform still answers");
    };
    assert_eq!(answer.id, RequestId::Number(2));
    assert_eq!(answer.error.unwrap().code, -32603);

    // None of the three reached the daemon: the first thing it reads is the
    // request that came after them, so the pump survived all three.
    let after = JsonRpcMessage::request(3i64, "ping", None);
    rig.client.write(&after).unwrap();
    assert_eq!(rig.daemon.read().unwrap(), after);

    // And the way back still works.
    let reply = JsonRpcMessage::response(3i64, json!({}));
    rig.daemon.write(&reply).unwrap();
    assert_eq!(rig.client.read().unwrap(), reply);

    rig.finish();
}

/// What the daemon end receives, byte for byte, for `lines` written raw by the
/// client through a bridge built with `transform`.
fn forwarded_bytes(transform: Option<ShimTransform>, lines: &[&str]) -> Vec<String> {
    let (client_bridge, client_peer) = UnixStream::pair().unwrap();
    let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();
    upstream_peer
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let client = UnixTransport::from_stream(client_bridge);
    let upstream = UnixTransport::from_stream(upstream_bridge);
    let bridge = thread::spawn(move || match transform {
        Some(transform) => crate::bridge::run_transformed(client, upstream, transform),
        None => Bridge::run(client, upstream),
    });

    let mut raw = client_peer;
    for line in lines {
        raw.write_all(line.as_bytes()).unwrap();
        raw.write_all(b"\n").unwrap();
    }
    raw.flush().unwrap();

    let mut reader = BufReader::new(upstream_peer.try_clone().unwrap());
    let received = lines
        .iter()
        .map(|_| {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            line
        })
        .collect();

    drop(raw);
    drop(reader);
    drop(upstream_peer);
    bridge.join().unwrap().unwrap();
    received
}

#[test]
fn without_a_transform_forwarding_is_byte_for_byte_what_it_was() {
    // Key order, unicode, big numbers, extra members: whatever the plain
    // bridge does to a line, a bridge with a transform that changes nothing
    // must do exactly the same - so a server that installs no hook, and one
    // whose hook leaves a message alone, cannot tell the difference.
    let lines = [
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"z":1,"a":2}}"#,
        r#"{"method":"tools/call","jsonrpc":"2.0","id":"x","params":{"arguments":{"s":"é😀","n":12345678901234567890}}}"#,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        r#"{"jsonrpc":"2.0","id":"srv-1","result":{"roots":[]}}"#,
        r#"{"jsonrpc":"2.0","id":7,"error":{"code":-1,"message":"declined"}}"#,
    ];

    let plain = forwarded_bytes(None, &lines);
    let identity = forwarded_bytes(Some(transform(|_| Ok(()))), &lines);
    assert_eq!(plain, identity);
    assert_eq!(plain.len(), lines.len());
}
