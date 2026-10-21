//! [`DaemonOptions::shim_transform`](sml_mcps::DaemonOptions::shim_transform)
//! through real processes: the `shim_transform` example, launched the way an
//! MCP client launches it, relaunching itself as the daemon.
//!
//! The bridge's handling of a transform - order, failure, what it never sees -
//! is unit-tested in `src/bridge/transform/tests.rs`. What only a real process
//! pair can show is where the transform runs: in each shim, with that shim's
//! environment, and never in the daemon.

#![cfg(unix)]

use serde_json::{Value, json};
use sml_mcps::transport::{Transport, UnixTransport};
use sml_mcps::types::JsonRpcMessage;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};
use std::{fs, thread};

/// How long any one step gets before the test calls it a failure.
const PATIENCE: Duration = Duration::from_secs(15);

const CALLER_ENV: &str = "SHIM_TRANSFORM_CALLER";

fn temp_path(suffix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "sml_mcps_xform_{}_{}_{}",
        std::process::id(),
        n,
        suffix
    ))
}

/// Built once per test binary, for the reason `serve_daemon_e2e` gives.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "shim_transform"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(
        status.success(),
        "cargo build --example shim_transform failed"
    );

    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/examples/shim_transform");
    assert!(path.exists(), "example binary not found at {path:?}");
    path
});

/// Kill the daemon a test started, and tidy its files.
struct DaemonGuard(PathBuf);

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let pid_file = self.0.with_extension("pid");
        if let Some(pid) = fs::read_to_string(&pid_file)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
        {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(&pid_file);
    }
}

fn initialize_params() -> Value {
    json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": { "name": "e2e", "version": "1.0" }
    })
}

/// What `whoami` said, given `arguments`.
fn whoami_result(result: &Value) -> Value {
    serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
}

/// A shim process, its stdout drained by a thread so a hang times out.
struct Shim {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
}

impl Shim {
    fn start(socket: &Path, caller: Option<&str>) -> Self {
        let mut command = Command::new(&*EXAMPLE_BINARY);
        command
            .args(["--socket", socket.to_str().unwrap()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        match caller {
            Some(id) => command.env(CALLER_ENV, id),
            None => command.env_remove(CALLER_ENV),
        };
        let mut child = command.spawn().expect("failed to spawn shim");

        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });

        let mut shim = Self {
            stdin: child.stdin.take(),
            child,
            lines,
        };
        shim.request(1, "initialize", initialize_params());
        shim
    }

    fn request(&mut self, id: i64, method: &str, params: Value) -> Value {
        let message = JsonRpcMessage::request(id, method, Some(params));
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{}", serde_json::to_string(&message).unwrap()).unwrap();
        stdin.flush().unwrap();

        let line = self
            .lines
            .recv_timeout(PATIENCE)
            .unwrap_or_else(|e| panic!("shim never answered `{method}`: {e}"));
        let response: Value = serde_json::from_str(&line).unwrap();
        assert!(response.get("error").is_none(), "{response}");
        response["result"].clone()
    }

    fn whoami(&mut self, id: i64, arguments: Value) -> Value {
        whoami_result(&self.request(
            id,
            "tools/call",
            json!({ "name": "whoami", "arguments": arguments }),
        ))
    }

    fn finish(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "shim exited with {status}");
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("shim did not exit after its client hung up");
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn the_shim_stamps_its_caller_over_whatever_was_sent() {
    let socket = temp_path("stamp.sock");
    let _guard = DaemonGuard(socket.clone());

    let mut shim = Shim::start(&socket, Some("alice"));
    assert_eq!(
        shim.whoami(2, json!({ "_caller": "forged", "x": 1 })),
        json!({ "caller": "alice", "arguments": { "x": 1 } })
    );
    // With nothing there to replace, too.
    assert_eq!(
        shim.whoami(3, json!({})),
        json!({ "caller": "alice", "arguments": {} })
    );
    shim.finish();
}

#[test]
fn a_shim_with_no_caller_removes_a_forged_one() {
    let socket = temp_path("strip.sock");
    let _guard = DaemonGuard(socket.clone());

    let mut shim = Shim::start(&socket, None);
    assert_eq!(
        shim.whoami(2, json!({ "_caller": "forged", "x": 1 })),
        json!({ "caller": null, "arguments": { "x": 1 } })
    );
    shim.finish();
}

#[test]
fn each_shim_stamps_its_own_caller_on_one_daemon() {
    // The daemon was started by the first shim and inherited its environment.
    // If the transform ran anywhere but the shim, the second shim's calls would
    // come out as the first's.
    let socket = temp_path("two.sock");
    let _guard = DaemonGuard(socket.clone());

    let mut alice = Shim::start(&socket, Some("alice"));
    let mut bob = Shim::start(&socket, Some("bob"));
    let mut nobody = Shim::start(&socket, None);

    assert_eq!(alice.whoami(2, json!({}))["caller"], "alice");
    assert_eq!(
        bob.whoami(2, json!({ "_caller": "alice" }))["caller"],
        "bob"
    );
    assert_eq!(
        nobody.whoami(2, json!({ "_caller": "alice" }))["caller"],
        Value::Null
    );
    assert_eq!(alice.whoami(3, json!({}))["caller"], "alice");

    nobody.finish();
    bob.finish();
    alice.finish();
}

#[test]
fn the_daemon_itself_transforms_nothing() {
    // A connection straight to the socket, not through a shim, gets exactly
    // what it sent: the hook belongs to the shim, not to the server.
    let socket = temp_path("direct.sock");
    let _guard = DaemonGuard(socket.clone());

    let shim = Shim::start(&socket, Some("alice"));

    let mut direct = UnixTransport::connect(&socket).unwrap();
    for (id, method, params) in [
        (1, "initialize", initialize_params()),
        (
            2,
            "tools/call",
            json!({ "name": "whoami", "arguments": { "_caller": "forged" } }),
        ),
    ] {
        direct
            .write(&JsonRpcMessage::request(id, method, Some(params)))
            .unwrap();
        let JsonRpcMessage::Response(response) = direct.read().unwrap() else {
            panic!("expected a response");
        };
        if id == 2 {
            assert_eq!(whoami_result(&response.result.unwrap())["caller"], "forged");
        }
    }

    drop(direct);
    shim.finish();
}
