# The Socket Claim

**Date:** 2026-08-29
**Authors:** Brandon Sneed, Porter
**Branch:** bsneed/socket-stale-probe

## Origin

Found in the field on 2026-08-29, during the sovran-agents connect-first work
(`sovran.la/agents`, `docs/plans/serve-connect-first.md`). Their
`tests/serve_startup.rs` raced two `serve` shims and, about one full-file run
in five, got two daemons on one socket path:

```
exactly one of them should have become the daemon; these did:
90580 .../sovran_agents --daemon --socket /var/.../home/socket
90581 .../sovran_agents --daemon --socket /var/.../home/socket
```

`lsof` on the pair: one held a socket at the path with both shims attached;
the other held a *different* socket at the same path with no connections -
parked in `accept` on an inode with no name. That one is permanent: the idle
watcher never fires because the countdown starts on connection activity and
nothing can ever connect, and SIGTERM cannot reach it because shutdown wakes a
parked `accept` by connecting to the listener's own path, which has been
deleted out from under it. Six of these accumulated in an afternoon and every
one needed `kill -9`.

## The bug

`prepare_socket_path` in `src/transport/unix_server.rs` decides whether an
existing socket file is stale with a single connect:

```rust
match UnixStream::connect(path) {
    Ok(_)  => Err("already in use by a live server"),
    Err(_) => { std::fs::remove_file(path)?; Ok(()) }   // "stale"
}
```

One refused connection is treated as proof that nothing is there. It is not.
`UnixListener::bind` is two syscalls - `bind`, then `listen` - and a connect
landing between them is refused exactly the way an abandoned socket file
refuses. When two processes race to become the daemon, the loser can misread
the winner's just-bound socket as stale, delete the live daemon's socket file,
and bind its own at the same path. Two daemons, one orphaned forever.

The client side of this crate already half-knows the problem:
`Bridge::probe_socket` probes three times with a gap, because on the BSDs a
listener with a full backlog refuses the same way a dead socket does, and a
backlog drains where an abandoned socket does not. But retries only shrink the
window. A daemon preempted between `bind` and `listen` can sit there longer
than any probe schedule, and `Bridge`'s own `clear_socket` - the shim-side
unlink - would then delete a live daemon's socket just the same.

sovran-agents shielded itself with an flock-based daemon lock (`src/startup.rs`
in that repo), which is the prior art for this fix. But every other server
built on `serve_daemon` - nemo, memory, filesystem - is still exposed. The fix
belongs here, in the shared crate, and the four downstream servers must get it
without code changes.

## Design: one claim, held for the daemon's life

Guessing about staleness from connect results cannot be made sound - the
kernel offers no way to ask "is somebody between `bind` and `listen` on this
path". So stop guessing. Make the delete-and-bind sequence exclusive:

**The claim.** A new crate-internal type, `SocketLock` in `src/socket.rs`: an
`flock(LOCK_EX | LOCK_NB)` on `<socket>.lock` (sibling of the socket, next to
the `.pid` file, in the same `0700` directory). The rules:

1. A daemon takes the claim *before* it probes, deletes, or binds anything at
   the socket path, and holds it until it has stopped serving and unlinked its
   own socket.
2. Nobody unlinks a socket file without holding its claim.
   `prepare_socket_path` now *requires* the claim - it takes `&SocketLock` as
   an argument, so the compiler enforces rule 2 inside the crate.
3. `Bridge`'s `clear_socket` (the shim-side unlink) takes the claim
   non-blocking around its probe-and-delete. If the claim is held, the socket
   is not stale - somebody live owns the path - so it deletes nothing and lets
   the ordinary connect/wait/spawn logic find the owner.

Under the claim, the one-connect staleness check becomes sound for every
socket this crate created: a cooperating daemon between `bind` and `listen` is
holding the claim, so no claim-holder can ever observe that state from
outside. A refused connect under the claim is a socket nobody owns - the
leftovers of a `kill -9` - and deleting it is correct.

Why an flock and not a smarter probe:

- **It lives on the open file description.** The kernel releases it when the
  last descriptor closes, including a daemon killed with `-9`. There is no
  stale-lock state to reason about and nothing to clean up. (The `.lock` file
  itself is left in place forever - unlinking it would reopen the race, since
  a new claimer could open the old inode while a fresh file takes the name.)
- **It survives what the daemon does to itself.** `serve_daemon` forks before
  `run`, so the claim is taken in the process that serves; `serve` takes it on
  the calling thread. Either way it is held by the process that owns the
  socket.
- **It excludes threads too.** flock exclusion is per open file description,
  so two `UnixServer::serve` calls in one test process arbitrate the same way
  two daemons do.

### The loser's exit

A process that cannot take the claim does not wait and does not retry: it
returns an error from `run`, naming what it found - "already in use by a live
server" when the socket answers (the message the crate has always used for
that case), "being claimed by another server" when it does not (the holder is
mid-startup). This is the behavior `Bridge::auto_start` already composes with:
the shim that spawned the losing daemon ignores its exit status and polls the
socket, which the winner binds milliseconds later. The losing daemon standing
down with an error *is* the success path for the machine.

sovran-agents' `claim` waits a `RACE_WINDOW` for a wedged holder and retries
the lock; that is right for a binary whose lock spans certificate minting and
a TCP bind. Here the claim covers microseconds - probe, unlink, bind - and the
caller above (`auto_start`) already owns the waiting. Failing fast is simpler
and composes with it.

### What the claim does not cover

A listener at the socket path that never took the claim - a daemon from a
build before this change, or a foreign process squatting on the path - is
still detected by the connect probe when it is listening (refused-to-bind, as
always), but if caught exactly between its `bind` and `listen` it can still be
misread as stale. That window closes when the downstream servers rebuild
against this crate, which all four will before anything ships; it cannot be
closed from one side only.

## Changes

- `src/socket.rs`: `SocketLock` (crate-internal), `lock_path_for`.
- `src/transport/unix_server.rs`: `run` takes the claim after
  `ensure_private_parent_dir`, before `prepare_socket_path`, and holds it
  through `cleanup`. `prepare_socket_path` takes `&SocketLock`.
- `src/bridge.rs`: `clear_socket` takes the claim non-blocking; a held claim
  means "not stale, do not touch".
- No public API change. Additive only; the four downstream servers get the
  protection by rebuilding.
- Version: `0.6.0` -> `0.6.1` (FIX), unpublished.

## Test inventory

The window is bind-to-listen, which is two adjacent syscalls in the wild - so
the tests do not race it, they *hold* it: `libc::socket` + `libc::bind` with
no `listen` is exactly the kernel state of a daemon mid-bind, and it stays
that way for as long as a test wants.

Unit, `unix_server.rs`:

| test | what it pins |
|---|---|
| a mid-bind socket survives a racing server | **the pin.** Winner holds the claim with a bound-not-listening socket; a second claim fails, the file survives, and after `listen` the winner is still reachable |
| the claim is released with the server | a served-and-exited socket path can be claimed again - restarts work |
| a dead daemon's leftovers are cleared under the claim | socket file + lock file with no live holder: claim succeeds, stale socket removed, bind proceeds |
| a live server still refuses by name | claim held and socket answering yields the "already in use by a live server" error, unchanged |
| existing `prepare_socket_path` tests | same answers as before, now asked under a claim |

Unit, `bridge.rs`:

| test | what it pins |
|---|---|
| clear_socket leaves a claimed socket alone | shim side of the same pin: claim held + mid-bind socket, `clear_socket` deletes nothing |
| clear_socket still clears the genuinely dead | no holder: probe-dead socket and pid file removed, as before |

Integration, `tests/daemon_lifecycle.rs` (public API only):

| test | what it pins |
|---|---|
| two servers racing leave one reachable | two `UnixServer::serve` calls released by a barrier: exactly one serves and answers a handshake, the other returns an error, and the machine converges |

Verification beyond the suite: the pin is proven by revert (restore the old
behavior, watch the mid-bind tests fail with the socket deleted), each new
guard is mutation-checked (delete it, watch a test fail), the full suite runs
five times for flakiness, and sovran-agents builds and tests against the
modified crate. All tests use temp paths; no test touches a real daemon home.
