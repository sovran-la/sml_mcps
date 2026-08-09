//! Integration tests for daemon lifecycle and signal handling.
//!
//! These tests exercise the full daemon/shim stack by spawning the
//! `unix_server` example binary and verifying:
//! - `serve_daemon` detaches correctly (socket + PID file appear)
//! - `serve` (foreground) responds to tool calls
//! - SIGTERM/SIGINT trigger clean shutdown (socket + PID removed)
//! - daemon survives parent process exit

#![cfg(unix)]

use sml_mcps::transport::{Transport, UnixTransport};
use sml_mcps::types::JsonRpcMessage;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{fs, thread};

/// Unique temp socket path per test.
fn temp_socket() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("sml_mcps_integ_{}_{}.sock", pid, n))
}

/// PID file path for a socket (mirrors unix_server.rs logic).
fn pid_path(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    p.set_extension("pid");
    p
}

/// The `unix_server` example, built once for the whole test binary.
///
/// Building per test raced: every test runs in its own thread, cargo replaces
/// `target/debug/examples/unix_server` by unlinking and re-linking it, and a
/// test that checked `exists()` just before another build's unlink would go on
/// to spawn a path that had ceased to exist. Building once behind a
/// `LazyLock` removes the window along with four redundant builds.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "unix_server"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "cargo build --example unix_server failed");

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("unix_server");
    assert!(path.exists(), "example binary not found at {:?}", path);
    path
});

/// Path to the built example.
fn example_binary() -> PathBuf {
    EXAMPLE_BINARY.clone()
}

/// Wait for a file to appear on disk, with timeout.
fn wait_for_file(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Wait for a file to disappear from disk, with timeout.
fn wait_for_removal(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Read the PID from a PID file.
fn read_pid(path: &Path) -> Option<i32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Send a signal to a PID.
fn kill(pid: i32, signal: libc::c_int) -> bool {
    unsafe { libc::kill(pid, signal) == 0 }
}

/// Check if a process is alive.
fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Wait for a process to exit, with timeout.
///
/// A daemon removes its socket and PID file and *then* returns from `serve`,
/// unwinds, and exits. Those are separate moments, so checking liveness the
/// instant the files disappear is a race - one the machine loses whenever it is
/// busy enough to deschedule the daemon in between.
fn wait_for_death(pid: i32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !alive(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Connect to a socket, retrying until it's ready.
fn connect_retry(path: &Path, timeout: Duration) -> Option<UnixTransport> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(t) = UnixTransport::connect(path) {
            return Some(t);
        }
        thread::sleep(Duration::from_millis(25));
    }
    None
}

/// Send MCP initialize handshake.
fn initialize(t: &mut UnixTransport) {
    let req = JsonRpcMessage::request(
        1i64,
        "initialize",
        Some(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "integ-test", "version": "1.0" }
        })),
    );
    t.write(&req).unwrap();
    let resp = t.read().unwrap();
    assert!(matches!(resp, JsonRpcMessage::Response(_)));
}

/// Call a tool and return the text content from the response.
fn call_tool(t: &mut UnixTransport, id: i64, name: &str) -> String {
    let req = JsonRpcMessage::request(
        id,
        "tools/call",
        Some(serde_json::json!({ "name": name, "arguments": {} })),
    );
    t.write(&req).unwrap();
    match t.read().unwrap() {
        JsonRpcMessage::Response(r) => {
            let result = r.result.expect("expected result");
            result["content"][0]["text"]
                .as_str()
                .expect("text content")
                .to_string()
        }
        other => panic!("expected response, got {:?}", other),
    }
}

/// RAII cleanup: kill daemon + remove socket/PID on drop.
struct DaemonGuard {
    socket: PathBuf,
    pid_file: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(pid) = read_pid(&self.pid_file) {
            let _ = kill(pid, libc::SIGKILL);
        }
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_file(&self.pid_file);
    }
}

// ---------------------------------------------------------------------------

#[test]
fn test_serve_daemon_detaches() {
    let bin = example_binary();
    let sock = temp_socket();
    let pid_file = pid_path(&sock);
    let _guard = DaemonGuard {
        socket: sock.clone(),
        pid_file: pid_file.clone(),
    };

    // serve_daemon should fork and return immediately.
    let status = Command::new(&bin)
        .args(["--daemon", "--socket", sock.to_str().unwrap()])
        .status()
        .expect("failed to spawn daemon");
    assert!(status.success(), "daemon spawn should exit 0");

    // Socket and PID file should appear.
    assert!(
        wait_for_file(&sock, Duration::from_secs(5)),
        "socket not created"
    );
    assert!(
        wait_for_file(&pid_file, Duration::from_secs(5)),
        "PID file not created"
    );

    // PID should be alive.
    let pid = read_pid(&pid_file).expect("couldn't read PID");
    assert!(alive(pid), "daemon PID {} should be alive", pid);
}

#[test]
fn test_daemon_responds_to_tool_calls() {
    let bin = example_binary();
    let sock = temp_socket();
    let pid_file = pid_path(&sock);
    let _guard = DaemonGuard {
        socket: sock.clone(),
        pid_file: pid_file.clone(),
    };

    let status = Command::new(&bin)
        .args(["--daemon", "--socket", sock.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());

    let mut client =
        connect_retry(&sock, Duration::from_secs(5)).expect("failed to connect to daemon");
    initialize(&mut client);

    let result = call_tool(&mut client, 2, "increment");
    assert!(
        result.contains("counter"),
        "expected counter response, got: {}",
        result
    );

    let who = call_tool(&mut client, 3, "whoami");
    assert!(who.starts_with("conn-"), "expected conn-id, got: {}", who);
}

#[test]
fn test_sigterm_clean_shutdown() {
    let bin = example_binary();
    let sock = temp_socket();
    let pid_file = pid_path(&sock);
    let _guard = DaemonGuard {
        socket: sock.clone(),
        pid_file: pid_file.clone(),
    };

    let status = Command::new(&bin)
        .args(["--daemon", "--socket", sock.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(wait_for_file(&sock, Duration::from_secs(5)));
    assert!(wait_for_file(&pid_file, Duration::from_secs(5)));

    let pid = read_pid(&pid_file).expect("couldn't read PID");
    assert!(alive(pid));

    // Send SIGTERM.
    assert!(kill(pid, libc::SIGTERM), "failed to send SIGTERM");

    // Daemon should exit and clean up.
    assert!(
        wait_for_removal(&sock, Duration::from_secs(5)),
        "socket should be removed after SIGTERM"
    );
    assert!(
        wait_for_removal(&pid_file, Duration::from_secs(5)),
        "PID file should be removed after SIGTERM"
    );
    assert!(
        wait_for_death(pid, Duration::from_secs(5)),
        "daemon should exit after SIGTERM"
    );
}

#[test]
fn test_sigint_clean_shutdown() {
    let bin = example_binary();
    let sock = temp_socket();
    let pid_file = pid_path(&sock);
    let _guard = DaemonGuard {
        socket: sock.clone(),
        pid_file: pid_file.clone(),
    };

    let status = Command::new(&bin)
        .args(["--daemon", "--socket", sock.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(wait_for_file(&sock, Duration::from_secs(5)));
    assert!(wait_for_file(&pid_file, Duration::from_secs(5)));

    let pid = read_pid(&pid_file).expect("couldn't read PID");

    assert!(kill(pid, libc::SIGINT), "failed to send SIGINT");

    assert!(
        wait_for_removal(&sock, Duration::from_secs(5)),
        "socket should be removed after SIGINT"
    );
    assert!(
        wait_for_removal(&pid_file, Duration::from_secs(5)),
        "PID file should be removed after SIGINT"
    );
    assert!(
        wait_for_death(pid, Duration::from_secs(5)),
        "daemon should exit after SIGINT"
    );
}

#[test]
fn test_daemon_survives_parent_exit() {
    let bin = example_binary();
    let sock = temp_socket();
    let pid_file = pid_path(&sock);
    let _guard = DaemonGuard {
        socket: sock.clone(),
        pid_file: pid_file.clone(),
    };

    // Spawn daemon from a child process that exits immediately.
    let mut child = Command::new(&bin)
        .args(["--daemon", "--socket", sock.to_str().unwrap()])
        .spawn()
        .expect("failed to spawn");
    let exit = child.wait().unwrap();
    assert!(exit.success());

    // Parent (child process) is gone. Daemon should still be alive.
    assert!(wait_for_file(&sock, Duration::from_secs(5)));
    assert!(wait_for_file(&pid_file, Duration::from_secs(5)));
    let pid = read_pid(&pid_file).expect("couldn't read PID");
    assert!(alive(pid), "daemon should survive parent exit");

    // Verify it still responds.
    let mut client = connect_retry(&sock, Duration::from_secs(5))
        .expect("failed to connect after parent exited");
    initialize(&mut client);
    let result = call_tool(&mut client, 2, "increment");
    assert!(result.contains("counter"));
}
