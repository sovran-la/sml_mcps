//! End-to-end tests for [`Server::serve_daemon`], through a real binary.
//!
//! The dispatch itself is unit-tested in `src/daemon.rs` with an injected
//! command line, which cannot cover the two things that only exist in a real
//! process: the double-fork, and a shim that relaunches its own executable.
//! Both need the `serve_daemon` example, spawned the way an MCP client would
//! spawn it.
//!
//! What is verified here:
//! - `--daemon` detaches (the launcher exits, a PID file appears, the daemon
//!   outlives it) and serves.
//! - `--foreground` serves *without* detaching and writes no PID file.
//! - a shim with no mode flag auto-starts the daemon and proxies stdio to it.
//! - two shim processes reach one daemon, which is the point of the pattern.
//! - the socket's directory is created if it does not exist.

#![cfg(unix)]

use serde_json::Value;
use sml_mcps::transport::{Transport, UnixTransport};
use sml_mcps::types::JsonRpcMessage;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};
use std::{fs, thread};

/// How long any single step gets before the test calls it a failure.
///
/// Generous, because a loaded CI box is slow, but finite: a hung shim must fail
/// the test rather than wedge the suite until someone notices.
const PATIENCE: Duration = Duration::from_secs(15);

/// Unique temp path per call, so tests never share a socket.
fn temp_path(suffix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("sml_mcps_e2e_{}_{}_{}", pid, n, suffix))
}

/// PID file path for a socket (`server.sock` -> `server.pid`).
fn pid_path(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    p.set_extension("pid");
    p
}

/// The `serve_daemon` example, built once for the whole test binary.
///
/// Built once rather than per test: cargo replaces the binary by unlinking and
/// re-linking it, so a test that checked `exists()` just before another build's
/// unlink would go on to spawn a path that had ceased to exist.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "serve_daemon"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(
        status.success(),
        "cargo build --example serve_daemon failed"
    );

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("serve_daemon");
    assert!(path.exists(), "example binary not found at {:?}", path);
    path
});

fn wait_for_file(path: &Path) -> bool {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

fn read_pid(path: &Path) -> Option<i32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Kill whatever daemon a test left behind, and tidy its files.
struct DaemonGuard {
    socket: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let pid_file = pid_path(&self.socket);
        if let Some(pid) = read_pid(&pid_file) {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_file(&pid_file);
    }
}

// ---- talking to the daemon over its socket --------------------------------

fn connect_retry(path: &Path) -> UnixTransport {
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Ok(t) = UnixTransport::connect(path) {
            return t;
        }
        assert!(
            Instant::now() < deadline,
            "nothing ever answered on {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn initialize_params() -> Value {
    serde_json::json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": { "name": "e2e", "version": "1.0" }
    })
}

/// Send a request over the socket and hand back its result payload.
fn socket_request(t: &mut UnixTransport, id: i64, method: &str, params: Option<Value>) -> Value {
    t.write(&JsonRpcMessage::request(id, method, params))
        .unwrap();
    match t.read().unwrap() {
        JsonRpcMessage::Response(r) => r.result.expect("result"),
        other => panic!("expected response, got {:?}", other),
    }
}

/// Initialize and call `increment`, returning the counter text.
fn increment_over_socket(socket: &Path, id: i64) -> String {
    let mut client = connect_retry(socket);
    socket_request(&mut client, 1, "initialize", Some(initialize_params()));
    let result = socket_request(
        &mut client,
        id,
        "tools/call",
        Some(serde_json::json!({ "name": "increment", "arguments": {} })),
    );
    result["content"][0]["text"].as_str().unwrap().to_string()
}

// ---- talking to a shim over its stdio --------------------------------------

/// A shim process, with its stdout drained by a thread.
///
/// The draining thread is what makes a broken shim fail the test instead of
/// hanging it: reads come from a channel, and a channel can time out where
/// `ChildStdout::read` cannot.
struct Shim {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
}

impl Shim {
    /// Launch the example with no mode flag - exactly how an MCP client does.
    fn start(socket: &Path) -> Self {
        let mut child = Command::new(&*EXAMPLE_BINARY)
            .args(["--socket", socket.to_str().unwrap()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn shim");

        let stdout = child.stdout.take().expect("shim stdout");
        let (tx, lines) = channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });

        Self {
            stdin: child.stdin.take(),
            child,
            lines,
        }
    }

    /// Send a request and hand back its result payload.
    fn request(&mut self, id: i64, method: &str, params: Option<Value>) -> Value {
        let message = JsonRpcMessage::request(id, method, params);
        let stdin = self.stdin.as_mut().expect("shim stdin");
        writeln!(stdin, "{}", serde_json::to_string(&message).unwrap()).unwrap();
        stdin.flush().unwrap();

        let line = match self.lines.recv_timeout(PATIENCE) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => panic!("shim never answered `{method}`"),
            Err(RecvTimeoutError::Disconnected) => panic!("shim hung up during `{method}`"),
        };

        let response: Value = serde_json::from_str(&line).expect("shim wrote a non-JSON line");
        assert!(
            response.get("error").is_none(),
            "`{method}` failed: {response}"
        );
        response["result"].clone()
    }

    fn initialize(&mut self) -> Value {
        self.request(1, "initialize", Some(initialize_params()))
    }

    fn call(&mut self, id: i64, tool: &str) -> String {
        let result = self.request(
            id,
            "tools/call",
            Some(serde_json::json!({ "name": tool, "arguments": {} })),
        );
        result["content"][0]["text"].as_str().unwrap().to_string()
    }

    /// Close stdin and wait for the shim to finish draining and exit.
    fn finish(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + PATIENCE;
        loop {
            match self.child.try_wait().unwrap() {
                Some(status) => {
                    assert!(status.success(), "shim exited with {status}");
                    return;
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                None => panic!("shim did not exit after its client hung up"),
            }
        }
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Launch `--daemon` and wait for the launcher to hand the terminal back.
///
/// Deliberately not `Command::status()`: a `serve_daemon` that stopped
/// detaching would never return, and the test would hang the suite instead of
/// reporting the regression it exists to catch.
fn launch_daemon(socket: &Path) {
    let mut child = Command::new(&*EXAMPLE_BINARY)
        .args(["--daemon", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to spawn daemon");

    let deadline = Instant::now() + PATIENCE;
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                assert!(status.success(), "the launcher exited with {status}");
                return;
            }
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            None => {
                let _ = child.kill();
                panic!("--daemon did not detach: the launcher never returned");
            }
        }
    }
}

// ---------------------------------------------------------------------------

#[test]
fn test_daemon_flag_detaches_and_serves() {
    let socket = temp_path("d.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    // The launcher double-forks and returns; the daemon it left behind is what
    // answers afterwards.
    launch_daemon(&socket);

    assert!(wait_for_file(&socket), "no socket");
    assert!(wait_for_file(&pid_path(&socket)), "no PID file");

    let pid = read_pid(&pid_path(&socket)).expect("unreadable PID file");
    assert!(
        alive(pid),
        "the daemon outlives the process that launched it"
    );

    assert_eq!(increment_over_socket(&socket, 2), "counter = 1");
}

#[test]
fn test_foreground_flag_serves_without_detaching() {
    let socket = temp_path("f.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let mut child = Command::new(&*EXAMPLE_BINARY)
        .args(["--foreground", "--socket", socket.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn foreground server");

    assert!(wait_for_file(&socket), "no socket");
    assert_eq!(increment_over_socket(&socket, 2), "counter = 1");

    // Still this process, not a detached grandchild - and no PID file, which is
    // what `serve` promises and `serve_daemon` does not.
    assert!(
        child.try_wait().unwrap().is_none(),
        "--foreground must not return while it is serving"
    );
    assert!(
        !pid_path(&socket).exists(),
        "--foreground writes no PID file"
    );

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn test_a_shim_auto_starts_the_daemon_and_proxies_to_it() {
    let socket = temp_path("s.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    assert!(!socket.exists(), "nothing is running yet");

    let mut shim = Shim::start(&socket);
    let init = shim.initialize();
    assert_eq!(init["serverInfo"]["name"], "serve-daemon-example");

    // The shim relaunched the example as `--daemon`, so a real daemon - PID
    // file and all - is now behind it.
    assert!(
        wait_for_file(&socket),
        "the shim should have started a daemon"
    );
    assert!(wait_for_file(&pid_path(&socket)), "no PID file");

    assert_eq!(shim.call(2, "increment"), "counter = 1");
    assert!(shim.call(3, "whoami").starts_with("conn-"));

    shim.finish();
}

#[test]
fn test_two_shims_reach_one_daemon() {
    // The reason the pattern exists: separate client processes, one copy of the
    // expensive state.
    let socket = temp_path("m.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };

    let mut first = Shim::start(&socket);
    first.initialize();
    assert_eq!(first.call(2, "increment"), "counter = 1");

    // The second shim finds the daemon already up and connects to it rather
    // than starting a second one.
    let mut second = Shim::start(&socket);
    second.initialize();
    assert_eq!(
        second.call(2, "increment"),
        "counter = 2",
        "the second shim should be talking to the first shim's daemon"
    );

    // One daemon, but a connection each.
    assert_ne!(first.call(3, "whoami"), second.call(3, "whoami"));

    // And a client that hangs up does not take the daemon with it.
    second.finish();
    assert_eq!(first.call(4, "increment"), "counter = 3");

    first.finish();
}

#[test]
fn test_the_daemon_creates_its_socket_directory() {
    let base = temp_path("dir");
    let socket = base.join("run/state/server.sock");
    let _guard = DaemonGuard {
        socket: socket.clone(),
    };
    assert!(!base.exists(), "two directories short of a home");

    launch_daemon(&socket);

    assert!(
        wait_for_file(&socket),
        "no socket under a created directory"
    );
    assert_eq!(increment_over_socket(&socket, 2), "counter = 1");

    drop(_guard);
    let _ = fs::remove_dir_all(&base);
}
