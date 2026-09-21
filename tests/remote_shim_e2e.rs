//! End-to-end tests for the remote shim, through real processes.
//!
//! The dispatch is unit-tested in `src/daemon.rs` against an injected command
//! line and an injected client transport. What only exists in a real process:
//! the shim's *own* stdio carrying MCP traffic, the exit status and stderr a
//! failed connection produces, and that `--connect` on a real command line
//! reaches `serve_daemon_with` through `std::env::args`.
//!
//! The `multi_listener` example is both halves: launched with `--daemon --tcp
//! <addr>` it is the daemon on the "other machine" (loopback, here); launched
//! with `--connect <addr>` it is the shim an MCP client would spawn.

#![cfg(unix)]

use serde_json::Value;
use sml_mcps::types::JsonRpcMessage;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};
use std::{fs, thread};

/// How long any single step gets before the test calls it a failure.
const PATIENCE: Duration = Duration::from_secs(15);

/// The `multi_listener` example, built once for the whole test binary.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "multi_listener"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(
        status.success(),
        "cargo build --example multi_listener failed"
    );

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("multi_listener");
    assert!(path.exists(), "example binary not found at {path:?}");
    path
});

/// Unique temp path per call, so tests never share a socket.
fn temp_path(suffix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "sml_mcps_remote_{}_{}_{}",
        std::process::id(),
        n,
        suffix
    ))
}

/// PID file path for a socket (the crate's own convention).
fn pid_path(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    p.set_extension("pid");
    p
}

/// A TCP port held open until the caller is ready to give it away.
fn reserve_port() -> (SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, listener)
}

/// An address nothing answers on.
fn refused_addr() -> SocketAddr {
    let (addr, held) = reserve_port();
    drop(held);
    addr
}

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

/// Kill the daemon and remove what it left behind, whatever the test did.
struct DaemonGuard {
    socket: PathBuf,
    pid_file: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(pid) = read_pid(&self.pid_file) {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_file(&self.pid_file);
    }
}

/// Start the example as a detached daemon with a TCP listener, and wait for
/// its socket. Hands back the guard and the TCP address to dial.
fn start_daemon() -> (DaemonGuard, SocketAddr) {
    let socket = temp_path("daemon.sock");
    let guard = DaemonGuard {
        socket: socket.clone(),
        pid_file: pid_path(&socket),
    };
    let (tcp_addr, held) = reserve_port();
    drop(held);

    let status = Command::new(&*EXAMPLE_BINARY)
        .args(["--daemon", "--socket", socket.to_str().unwrap()])
        .args(["--tcp", &tcp_addr.to_string(), "--idle-ms", "30000"])
        .status()
        .expect("failed to spawn the example daemon");
    assert!(status.success(), "the daemon launcher should exit 0");
    assert!(wait_for_file(&socket), "the daemon never bound its socket");
    assert!(wait_for_file(&guard.pid_file));

    (guard, tcp_addr)
}

// ---- talking to a shim over its stdio --------------------------------------

/// A shim process, with its stdout drained by a thread.
struct Shim {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    stderr: Receiver<String>,
}

impl Shim {
    /// Launch the example as `--connect <addr>` - exactly how an MCP client
    /// launches an installed remote entry - with `$HOME` pointed at `home`.
    fn start(addr: &str, home: &Path, extra: &[&str]) -> Self {
        let mut child = Command::new(&*EXAMPLE_BINARY)
            .args(["--connect", addr])
            .args(extra)
            .env("HOME", home)
            .env_remove("XDG_RUNTIME_DIR")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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

        let stderr_pipe = child.stderr.take().expect("shim stderr");
        let (etx, stderr) = channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr_pipe).lines() {
                let Ok(line) = line else { return };
                if etx.send(line).is_err() {
                    return;
                }
            }
        });

        Self {
            stdin: child.stdin.take(),
            child,
            lines,
            stderr,
        }
    }

    /// Send a request and hand back its result payload.
    fn request(&mut self, id: i64, method: &str, params: Option<Value>) -> Value {
        let message = JsonRpcMessage::request(id, method, params);
        let stdin = self.stdin.as_mut().expect("shim stdin");
        writeln!(stdin, "{}", serde_json::to_string(&message).unwrap()).unwrap();
        stdin.flush().unwrap();

        loop {
            let line = match self.lines.recv_timeout(PATIENCE) {
                Ok(line) => line,
                Err(RecvTimeoutError::Timeout) => panic!("shim never answered `{method}`"),
                Err(RecvTimeoutError::Disconnected) => {
                    let stderr: Vec<String> = self.stderr.try_iter().collect();
                    panic!("shim hung up during `{method}`: {stderr:?}")
                }
            };

            let response: Value = serde_json::from_str(&line).expect("shim wrote a non-JSON line");
            // A notification between request and answer is the daemon
            // talking, not the answer.
            if response.get("id").is_none() {
                continue;
            }
            assert!(
                response.get("error").is_none(),
                "`{method}` failed: {response}"
            );
            return response["result"].clone();
        }
    }

    fn initialize(&mut self) -> Value {
        self.request(
            1,
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "remote-shim-test", "version": "1.0" }
            })),
        )
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
    fn finish(mut self) -> (i32, String) {
        drop(self.stdin.take());
        self.wait()
    }

    /// Wait for the shim to exit, with a deadline, and collect its stderr.
    fn wait(mut self) -> (i32, String) {
        let deadline = Instant::now() + PATIENCE;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                panic!("shim did not exit");
            }
            thread::sleep(Duration::from_millis(20));
        };
        let stderr: Vec<String> = self.stderr.iter().collect();
        (status.code().expect("shim was killed"), stderr.join("\n"))
    }
}

/// A `$HOME` with nothing in it, to prove the shim leaves it that way.
fn empty_home() -> tempfile::TempDir {
    tempfile::TempDir::new().unwrap()
}

/// Whether anything at all appeared under `home`.
fn home_is_still_empty(home: &Path) -> bool {
    fs::read_dir(home).unwrap().next().is_none()
}

// ---- tests -----------------------------------------------------------------

#[test]
fn a_remote_shim_proxies_stdio_to_a_tcp_daemon() {
    let (_daemon, tcp_addr) = start_daemon();
    let home = empty_home();

    let mut shim = Shim::start(&tcp_addr.to_string(), home.path(), &[]);
    let init = shim.initialize();
    assert_eq!(init["serverInfo"]["name"], "multi-listener-example");
    assert_eq!(shim.call(2, "increment"), "counter = 1");
    assert_eq!(shim.call(3, "increment"), "counter = 2");

    let (code, stderr) = shim.finish();
    assert_eq!(code, 0, "{stderr}");
    assert!(
        home_is_still_empty(home.path()),
        "a remote shim must write nothing under $HOME"
    );
}

#[test]
fn a_remote_shim_resolves_a_hostname() {
    // `localhost:<port>`, not `127.0.0.1:<port>`: the real use is a MagicDNS
    // name, and the shim has to leave resolution to the resolver.
    let (_daemon, tcp_addr) = start_daemon();
    let home = empty_home();

    let mut shim = Shim::start(&format!("localhost:{}", tcp_addr.port()), home.path(), &[]);
    let init = shim.initialize();
    assert_eq!(init["serverInfo"]["name"], "multi-listener-example");
    assert_eq!(shim.call(2, "increment"), "counter = 1");

    let (code, _) = shim.finish();
    assert_eq!(code, 0);
}

#[test]
fn two_remote_shims_share_one_daemon() {
    let (_daemon, tcp_addr) = start_daemon();
    let home = empty_home();

    let mut first = Shim::start(&tcp_addr.to_string(), home.path(), &[]);
    let mut second = Shim::start(&tcp_addr.to_string(), home.path(), &[]);
    first.initialize();
    second.initialize();

    assert_eq!(first.call(2, "increment"), "counter = 1");
    assert_eq!(second.call(2, "increment"), "counter = 2");
    assert_ne!(first.call(3, "whoami"), second.call(3, "whoami"));

    first.finish();
    second.finish();
}

#[test]
fn a_remote_shim_that_cannot_connect_exits_non_zero_naming_the_address() {
    let addr = refused_addr().to_string();
    let home = empty_home();

    let started = Instant::now();
    let shim = Shim::start(&addr, home.path(), &[]);
    let (code, stderr) = shim.wait();

    assert_ne!(code, 0, "a refused connection must not exit 0");
    assert!(
        stderr.contains("cannot connect to") && stderr.contains(&addr),
        "stderr must name the address: {stderr}"
    );
    // No retry loop, no respawn wait.
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        home_is_still_empty(home.path()),
        "a failed remote shim must not have created a socket directory"
    );
}

#[test]
fn a_remote_shim_given_a_name_that_does_not_resolve_exits_non_zero() {
    let home = empty_home();

    let shim = Shim::start("no-such-host.invalid:7211", home.path(), &[]);
    let (code, stderr) = shim.wait();

    assert_ne!(code, 0);
    assert!(
        stderr.contains("cannot connect to no-such-host.invalid:7211"),
        "{stderr}"
    );
    assert!(home_is_still_empty(home.path()));
}

#[test]
fn a_remote_shim_never_spawns_a_daemon() {
    // The strongest form of "never spawns": point it at nothing, and check
    // that no daemon appeared on the socket path the Unix shim would have
    // used - the one under `$HOME`, which stays empty.
    let addr = refused_addr().to_string();
    let home = empty_home();

    let shim = Shim::start(&addr, home.path(), &[]);
    let (code, _) = shim.wait();
    assert_ne!(code, 0);

    // Give a spawned daemon, had there been one, a moment to bind.
    thread::sleep(Duration::from_millis(300));
    assert!(
        home_is_still_empty(home.path()),
        "a socket or PID file under $HOME means a daemon was spawned"
    );
}

#[test]
fn connect_with_daemon_is_refused_before_anything_is_touched() {
    let addr = refused_addr().to_string();
    let home = empty_home();

    let shim = Shim::start(&addr, home.path(), &["--daemon"]);
    let (code, stderr) = shim.wait();

    assert_ne!(code, 0);
    assert!(
        stderr.contains("--connect") && stderr.contains("--daemon"),
        "{stderr}"
    );
    assert!(home_is_still_empty(home.path()));
}

#[test]
fn connect_with_socket_is_refused() {
    let addr = refused_addr().to_string();
    let home = empty_home();
    let socket = temp_path("refused.sock");

    let shim = Shim::start(&addr, home.path(), &["--socket", socket.to_str().unwrap()]);
    let (code, stderr) = shim.wait();

    assert_ne!(code, 0);
    assert!(
        stderr.contains("--connect") && stderr.contains("--socket"),
        "{stderr}"
    );
    assert!(!socket.exists());
    assert!(!pid_path(&socket).exists());
}
