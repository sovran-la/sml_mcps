# Listener Abstraction + Shutdown Guard

**Date:** 2026-08-26
**Authors:** Brandon Sneed, Porter
**Branch:** bsneed/listener-abstraction

## Goal

Two additive capabilities on the `serve_daemon` path, both asked for by
sovran-agents (a background-agent daemon built on `Server::serve_daemon`):

1. **Listener abstraction.** One daemon, one connection ceiling, one idle
   clock - but connections arriving from more than one listener. The Unix
   socket as today, plus optionally TCP, plus a generic path for streams the
   application has already wrapped (mTLS, in sovran-agents' case). sml_mcps
   must not grow a TLS dependency: it takes anything that is `Read + Write +
   Send`.

2. **Shutdown guard.** Idle shutdown is connection-based today: no clients for
   the idle window and the daemon exits. A daemon supervising child processes
   that outlive the client session needs to say "not yet" - idle exit should
   fire only when connections are idle *and* the application is not busy.

Both must be invisible to the three shipped UDS-only consumers (memory,
filesystem, nemo). Existing public API and behaviour unchanged; this is a
FEATURE bump, `0.5.0` -> `0.6.0`, not published from this branch.

## Current State

`Server::serve_daemon(socket_path, idle_timeout, context_factory)` dispatches
on `argv` (`--daemon` / `--foreground` / shim) and hands the daemon branches to
`UnixServer`, which:

- binds one `UnixListener` at `socket_path` in `UnixServer::run`,
- runs `accept_loop` on the calling thread, spawning a thread per admitted
  connection up to `max_connections` (default 64),
- keeps `ConnState { active, generation, shutdown }` behind a `Mutex` +
  `Condvar`,
- runs an optional `idle_watcher` thread that exits the daemon after
  `idle_timeout` with `active == 0` and no new arrivals,
- installs SIGTERM/SIGINT handlers whose watcher thread sets `shutdown` and
  wakes the accept loop.

Two things in there are hard-wired to "one Unix socket": the accept loop takes
a `&UnixListener`, and *waking* a parked `accept()` is a `UnixStream::connect`
to the daemon's own socket path - open-coded in both the idle watcher and the
signal watcher.

`UnixTransport` is the only line-framed socket transport; `Transport` requires
`Send + Sync` and offers optional `try_clone_writer` (independent write handle)
and `set_read_timeout` (deadlines), which together decide whether task workers
may make server-initiated requests.

## Design

### 1. `Listener` - the seam

```rust
pub trait Listener: Send + Sync + 'static {
    /// Block until a client connects; hand back a transport speaking to it.
    fn accept(&self) -> std::io::Result<Box<dyn Transport>>;

    /// Make a thread parked in `accept` return promptly.
    fn wake(&self) -> std::io::Result<()>;

    /// How this listener names itself in the daemon's startup line.
    fn describe(&self) -> String;
}
```

Three deliberate choices:

- **`accept` yields a `Transport`, not a stream.** A listener that has to do
  work before the stream is usable - a TLS handshake, a proxy-protocol header -
  does it inside `accept`, and a listener that knows how to split its stream
  hands back a transport that says so. It also means the mTLS path needs
  nothing from this crate but the trait: the consumer's `accept` runs its own
  handshake and boxes the result.
- **`io::Result`, not `Result`.** The accept loop already distinguishes
  `ErrorKind::Interrupted` (retry at once) from everything else (back off 10ms,
  re-check shutdown). Narrowing to `McpError` would throw that away. A
  non-IO failure - a rejected client certificate - maps to `io::Error::other`
  and is treated as "this connection failed, keep listening", which is right.
- **`wake` is required.** The daemon joins its accept threads on the way out. A
  listener that cannot be woken wedges shutdown, so the contract is explicit
  rather than defaulted to a no-op. Both built-ins implement it by connecting
  to themselves - the same trick the crate already uses.

### 2. Built-in implementations

| type | `accept` yields | `wake` |
|---|---|---|
| `UnixSocketListener` (private) | `UnixTransport` | `UnixStream::connect(path)` |
| `TcpSocketListener` (public) | `TcpTransport` | `TcpStream::connect(local_addr)` |
| whatever the consumer writes | `StreamTransport`, or their own | theirs |

`UnixSocketListener` is the daemon's existing primary socket, wrapped. It stays
private: `UnixServer` still owns binding, the private-directory check, the
stale-socket check, the PID file, and the unlink on exit, and none of that is
the listener's to duplicate.

`TcpSocketListener::bind(addr)` binds and remembers the resolved local address
(so `bind("127.0.0.1:0")` in a test can be asked what port it got). `wake`
connects to that address, substituting loopback when the bound IP is
unspecified - `0.0.0.0` is a bind address, not a connect address.

### 3. Transports

`TcpTransport` is `UnixTransport` over `TcpStream`: same newline framing, same
`try_clone_writer`, same `SO_RCVTIMEO` deadlines, plus `TCP_NODELAY` (one
JSON-RPC message per line is exactly the traffic Nagle punishes). Rather than
copy ninety lines, both are built on one private generic:

```rust
pub(crate) trait SocketStream: Read + Write + Send + Sync + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

pub(crate) struct SocketTransport<S: SocketStream> { /* LineReader + writer */ }
```

`UnixTransport` becomes a newtype over `SocketTransport<UnixStream>` with its
three constructors and its `Transport` impl unchanged in signature and
behaviour. Its existing test module is kept verbatim - that suite, passing
untouched, is the evidence that the refactor changed nothing.

`StreamTransport` is the generic path: any `Read + Write + Send + 'static`,
newline-framed the same way. It is honest about what it cannot do -
`try_clone_writer` returns `None` and `set_read_timeout` returns `Ok(false)` -
because an opaque stream (a rustls `StreamOwned`) is one object with one state
machine and cannot be split or deadlined from outside. `Server::start` already
handles both: it falls back to writing through the read handle and refuses
server-initiated requests from task workers (`can_pump == false`), which is the
same deal the HTTP transport gets.

`Transport` requires `Sync`, and `Box<dyn Read + Write + Send>` is not `Sync`,
so `StreamTransport` holds its stream in a `Mutex` and reaches it with
`Mutex::get_mut()` from the `&mut self` methods - no locking at runtime, and no
`unsafe impl Sync` to keep true across future edits.

### 4. One accept loop, N listeners

`accept_loop` takes `&dyn Listener` instead of `&UnixListener`; nothing else
about it changes. `UnixServer::run` then:

1. binds the primary socket exactly as today and wraps it in
   `UnixSocketListener`,
2. collects `[primary] + extras` into `Vec<Arc<dyn Listener>>`,
3. spawns one thread per *extra* listener running the same `accept_loop`,
4. runs the primary's `accept_loop` on the calling thread - so `serve()` still
   blocks and `serve_daemon()` still returns only when the daemon is done,
5. on return: sets `shutdown`, wakes every listener, joins the extra threads,
   then cleans up the socket and PID file.

`ConnState` is shared across all of them, so `max_connections` stays a ceiling
on the *process* and the idle clock counts a client as a client whichever
listener it arrived on. Both are the point.

The open-coded self-connects collapse into one helper:

```rust
fn request_shutdown(shared: &Shared, listeners: &[Arc<dyn Listener>])
```

which sets the flag, notifies the condvar, and calls `wake()` on each listener.
The idle watcher, the signal watcher, and `run`'s teardown all call it - so
"stop the daemon" is one code path that knows about every listener, instead of
three copies that each know about the socket path.

Signal handling moves to `src/transport/signals.rs` and takes an
`FnOnce() + Send` to run when a signal lands, instead of reaching into
`ConnState` and the socket path itself. That is what lets a signal wake every
listener; extracting it also takes 160 lines out of a 1600-line file.

An extra listener's thread that fails (only possible via a poisoned
`ConnState`) logs and exits; the daemon carries on with the listeners it has
left. A poisoned lock already makes `shutdown_requested` answer "yes", so the
primary loop is on its way out anyway.

### 5. The shutdown guard

A predicate, not an RAII handle:

```rust
impl<C> UnixServer<C> {
    pub fn busy_check<F>(self, is_busy: F) -> Self
    where F: Fn() -> bool + Send + Sync + 'static;
}
```

Why the predicate wins here: the application already knows whether it is busy -
sovran-agents has a registry it can ask - so a callback is a one-line
adaptation of state that exists, while a handle means threading an object into
every spawn path and pairing acquire/release correctly. A leaked RAII guard is
a daemon that never exits, and it fails silently. A predicate has one failure
mode, it is the application's own function, and it is trivially testable.

Semantics, and they are deliberately narrow:

- Consulted **only** by the idle watcher, and only at the moment it is about to
  commit to exiting. SIGTERM and SIGINT are not vetoable - "exit now" means
  now.
- Called **without** the `ConnState` lock held. It is application code; running
  it under our mutex invites a deadlock and a slow predicate would stall every
  accept.
- A veto costs one more idle window. The watcher re-arms and asks again after
  `idle_timeout`, so the poll interval is a knob that already exists rather
  than a new one.
- After a veto the watcher re-reads `ConnState` before committing, because the
  world may have moved while the predicate ran.
- No guard configured -> the predicate is `None` -> the branch is not taken and
  the code path is byte-for-byte today's.

A panicking predicate kills the watcher thread, which leaves the daemon up
forever. That is the safe direction to fail (work survives; an operator can
still SIGTERM) and it is documented rather than caught.

### 6. Reaching it from `Server::serve_daemon`

`serve_daemon` takes three positional arguments and there are now four more
knobs, so the additive form is an options object rather than a longer list:

```rust
pub struct DaemonOptions { /* ... */ }

impl DaemonOptions {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self;
    pub fn idle_timeout(self, d: Duration) -> Self;
    pub fn max_connections(self, n: usize) -> Self;
    pub fn listener(self, l: impl Listener) -> Self;
    pub fn busy_check(self, f: impl Fn() -> bool + Send + Sync + 'static) -> Self;
}

impl<C: Send + Sync + 'static> Server<C> {
    pub fn serve_daemon_with<F>(self, options: DaemonOptions, context_factory: F) -> Result<()>
    where F: Fn(&str) -> C + Send + Sync + 'static;
}
```

`serve_daemon(path, idle, f)` becomes `serve_daemon_with(DaemonOptions::new(path)
.idle_timeout(idle), f)` - one call site, so the two cannot drift. Note the
difference in `idle_timeout`: `DaemonOptions` defaults to *no* idle timeout
(run until told to stop), which is what a resident service wants;
`serve_daemon`'s existing signature keeps requiring one.

## Files

New:
- `src/transport/listener.rs` - `Listener`, `UnixSocketListener`, `TcpSocketListener`
- `src/transport/socket_stream.rs` - `SocketStream`, `SocketTransport<S>`
- `src/transport/tcp.rs` - `TcpTransport`
- `src/transport/stream.rs` - `StreamTransport`
- `src/transport/signals.rs` - signal handlers, moved out of `unix_server.rs`
- `examples/multi_listener.rs` - UDS + TCP + a busy check, in one daemon
- `tests/multi_listener_e2e.rs` - the example driven end to end

Modified:
- `src/transport/unix.rs` - delegate to `SocketTransport`, API unchanged
- `src/transport/unix_server.rs` - N listeners, `busy_check`, `request_shutdown`
- `src/transport/mod.rs`, `src/lib.rs` - module declarations and re-exports
- `src/daemon.rs` - `DaemonOptions`, `serve_daemon_with`
- `README.md`, `Cargo.toml` (0.5.0 -> 0.6.0)

## Tests

Unit, per module:
- `TcpSocketListener`: binds, reports its port, accepts, wakes a parked accept,
  loopback substitution for an unspecified bind address.
- `StreamTransport`: round-trip over a socket pair, framing across several
  messages, EOF, message-size ceiling, and the two honest `None`/`false`
  answers.
- `SocketTransport`/`TcpTransport`: the `UnixTransport` suite's shape over TCP -
  split writers, deadlines that expire and recover, partial messages preserved.
- `ConnState`/idle watcher: veto blocks the exit, a predicate that stops
  vetoing lets the next window exit, `None` never asks.

Integration:
- **Multi-listener accept**: one daemon, UDS and TCP up at once, a client on
  each, both reaching the same shared state; the connection ceiling counted
  across both.
- **Generic stream path**: a listener built on `StreamTransport` over a plain
  `TcpStream` - what a TLS-wrapped stream looks like to this crate - serving a
  real tool call.
- **Guard semantics**: busy blocks idle exit past several windows; not-busy
  exits within one; no guard configured exits exactly as today.
- **No regression**: `tests/daemon_lifecycle.rs` and `tests/serve_daemon_e2e.rs`
  pass unmodified.

`cargo fmt`, `cargo clippy --all-targets --all-features -- -D warnings`, and a
release build all clean.

## Non-goals

- **Per-connection peer identity.** The context factory still receives only a
  `conn_id`. sovran-agents' trust boundary is the mTLS handshake - a connection
  that got accepted is a connection that presented a valid client certificate -
  so nothing downstream needs to ask *which* peer. Adding a richer factory
  would be a second public entry point for a question nobody is asking yet.
- **TLS in this crate.** The `tls` feature stays what it is: HTTPS for the HTTP
  transport. The daemon path takes wrapped streams and knows nothing about how
  they were wrapped.
- **Renaming `UnixServer`.** It grows non-Unix listeners and keeps its name,
  because the alternative is a breaking change to three shipped consumers for a
  noun.
