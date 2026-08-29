//! The socket claim, driven from outside the crate: two servers race to the
//! same path through nothing but the public API, and exactly one may win.
//!
//! This is the end-to-end shape of the orphaned-daemon bug. Before the claim,
//! the loser of this race could misread the winner's just-bound socket as
//! stale - `UnixListener::bind` is `bind` then `listen`, and a connect landing
//! between them is refused like an abandoned socket - delete the winner's
//! socket file, and bind its own. Two live servers, one of them parked on a
//! nameless inode forever. The deterministic mid-bind pins live next to the
//! claim itself (`socket.rs`, `unix_server.rs`, `bridge.rs`, where the
//! bind-but-not-listening state can be held open); what this file pins is the
//! invariant a user of the crate actually relies on: whatever the
//! interleaving, one path serves one daemon, and the machine converges.

#![cfg(unix)]

use sml_mcps::transport::{Transport, UnixTransport};
use sml_mcps::types::JsonRpcMessage;
use sml_mcps::{Result, ServerConfig, UnixServer};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Unique temp socket path per round.
fn temp_socket() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("sml_mcps_claim_{}_{}.sock", std::process::id(), n))
}

fn config() -> ServerConfig {
    ServerConfig {
        name: "claim-race".into(),
        version: "1.0.0".into(),
        instructions: None,
        ..Default::default()
    }
}

/// Connect a client transport, retrying until a server is listening.
fn connect_retry(path: &Path) -> UnixTransport {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match UnixTransport::connect(path) {
            Ok(t) => return t,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("could not connect to {}: {}", path.display(), e),
        }
    }
}

/// Handshake, proving whoever owns the path is a working server.
fn initialize(t: &mut UnixTransport) {
    let request = JsonRpcMessage::request(
        1,
        "initialize",
        Some(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "claim-race", "version": "1.0" }
        })),
    );
    t.write(&request).unwrap();
    assert!(matches!(t.read(), Ok(JsonRpcMessage::Response(_))));
}

/// Join a server thread, or fail the test if it never exits.
///
/// A thread that never exits here IS the bug's signature: a server on a
/// nameless inode never sees the connection that starts its idle countdown.
fn join_within(handle: thread::JoinHandle<Result<()>>, what: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "{what} never exited - a server is parked on an unreachable socket, \
             which is the orphaned-daemon outcome"
        );
        thread::sleep(Duration::from_millis(25));
    }
    handle.join().expect("server thread panicked")
}

#[test]
fn two_servers_racing_leave_one_reachable() {
    for round in 0..5 {
        let sock = temp_socket();
        let barrier = Arc::new(Barrier::new(3));

        let racer = |sock: PathBuf, barrier: Arc<Barrier>| {
            thread::spawn(move || {
                // The idle timeout is the winner's way home once the client
                // below hangs up; the loser never serves and never needs it.
                let server: UnixServer<()> =
                    UnixServer::new(config()).idle_timeout(Duration::from_millis(300));
                barrier.wait();
                server.serve(&sock, |_conn_id| ())
            })
        };

        let a = racer(sock.clone(), barrier.clone());
        let b = racer(sock.clone(), barrier.clone());
        barrier.wait();

        // Whoever won, the path answers a real handshake.
        let mut client = connect_retry(&sock);
        initialize(&mut client);
        drop(client);

        let outcome_a = join_within(a, "server A");
        let outcome_b = join_within(b, "server B");
        assert!(
            outcome_a.is_ok() != outcome_b.is_ok(),
            "round {round}: exactly one server may serve one path; \
             got A: {outcome_a:?} / B: {outcome_b:?}"
        );

        let _ = std::fs::remove_file(&sock);
    }
}
