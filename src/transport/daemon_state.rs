//! How a daemon decides it is idle, and how it is told to stop.
//!
//! Three things that belong together and to no single listener:
//!
//! - [`ConnState`] - how many connections are live, how many have ever
//!   arrived, and whether the daemon is on its way out. One count for the
//!   whole daemon, because threads and descriptors are the process's.
//! - [`idle_watcher`] - the thread that decides the daemon has nothing left to
//!   do, and the application's chance to disagree.
//! - [`request_shutdown`] - the one way to stop a daemon, which is also the
//!   only thing that knows about every listener.
//!
//! [`UnixServer`](crate::UnixServer) owns the listeners and the accept loops;
//! this owns the arithmetic they share.

use crate::transport::Listener;
use crate::types::{McpError, Result};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Asks the application whether it still has work in hand.
///
/// See [`UnixServer::busy_check`](crate::UnixServer::busy_check). Shared rather than owned because the idle
/// watcher runs on its own thread and the daemon holds one of these across
/// both.
pub(crate) type BusyCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Connection bookkeeping, shared by every accept loop and both watchers.
pub(crate) type Shared = Arc<(Mutex<ConnState>, Condvar)>;

/// Every listener the daemon is serving on, **the primary Unix socket first**.
///
/// That order is load-bearing in one place: the primary's accept loop runs on
/// the calling thread, so `serve` blocks exactly the way it always has, and
/// every other listener gets a thread of its own.
pub(crate) type Listeners = Vec<Arc<dyn Listener>>;

/// Shared connection bookkeeping, guarded by a `Mutex` and paired with a
/// `Condvar` so the idle watcher can sleep until something changes.
#[derive(Default)]
pub(crate) struct ConnState {
    /// Number of currently-live connections.
    pub(crate) active: usize,
    /// Bumped on every accept. Lets the idle watcher tell "still idle" from
    /// "a new client arrived and left during my timeout window".
    pub(crate) generation: u64,
    /// Set when the server should stop accepting and exit.
    pub(crate) shutdown: bool,
}

/// What became of a connection that has just been accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// It has a slot, and `active` counts it.
    Admitted,
    /// The daemon already holds [`max_connections`](crate::UnixServer::max_connections) of them.
    Full,
    /// The daemon is on its way out and is not serving anything more.
    ShuttingDown,
}

impl ConnState {
    /// Take a slot for a newly accepted connection, or say why not.
    ///
    /// The ceiling is what stands between this daemon and a client that
    /// reconnects in a loop: every live connection is a thread and two
    /// descriptors, and a process that runs out of either is not the process
    /// that opened them. Without it the loop walked itself to `EMFILE`, where
    /// `accept` fails and the daemon used to exit.
    ///
    /// One count for the whole daemon, not one per listener: threads and
    /// descriptors are a process's, and a ceiling that a peer could multiply
    /// by opening a second kind of connection would not be one.
    pub(crate) fn admit(&mut self, max: usize) -> Admission {
        if self.shutdown {
            return Admission::ShuttingDown;
        }
        if self.active >= max {
            return Admission::Full;
        }

        self.active += 1;
        // Only for a connection that is being served. The generation is what
        // tells the idle watcher a client arrived, and a refused one did not:
        // counting it would let a reconnect loop hold an unused daemon open
        // forever.
        self.generation += 1;

        Admission::Admitted
    }
}

/// Decrements the active-connection count when a connection thread exits,
/// even on panic (RAII). Without this an unwinding handler would leave the
/// daemon thinking a client is still connected and never idle-out.
pub(crate) struct ActiveGuard(pub(crate) Shared);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let (lock, cv) = &*self.0;
        if let Ok(mut state) = lock.lock() {
            state.active = state.active.saturating_sub(1);
            cv.notify_all();
        }
    }
}

/// Ask the daemon to stop, and unpark anything waiting for a client.
///
/// Both watchers and the teardown in `run` go through here, so "stop the
/// daemon" is one code path that knows about every listener rather than three
/// that each know about the socket path.
///
/// The flag goes up before the knocking, so a connection a `wake` produces is
/// refused by the accept loop rather than served.
pub(crate) fn request_shutdown(shared: &Shared, listeners: &[Arc<dyn Listener>]) {
    {
        let (lock, cv) = &**shared;
        if let Ok(mut state) = lock.lock() {
            state.shutdown = true;
            cv.notify_all();
        }
    }

    for listener in listeners {
        let _ = listener.wake();
    }
}

/// Every listener, as one line for the daemon's startup message.
pub(crate) fn describe(listeners: &[Arc<dyn Listener>]) -> String {
    listeners
        .iter()
        .map(|listener| listener.describe())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Take a slot for an accepted connection, under the lock.
pub(crate) fn admit(shared: &Shared, max: usize) -> Result<Admission> {
    let (lock, cv) = &**shared;
    let mut state = lock
        .lock()
        .map_err(|_| McpError::Internal("conn state lock poisoned".into()))?;

    let admission = state.admit(max);
    if admission == Admission::Admitted {
        cv.notify_all();
    }

    Ok(admission)
}

/// Give a slot back, for the one exit that has no [`ActiveGuard`] to do it.
pub(crate) fn release(shared: &Shared) {
    let (lock, cv) = &**shared;
    if let Ok(mut state) = lock.lock() {
        state.active = state.active.saturating_sub(1);
        cv.notify_all();
    }
}

/// Whether a shutdown has been asked for.
///
/// A poisoned lock answers "yes": something panicked while holding the state,
/// and a loop that cannot read it any more is a loop with no way out.
pub(crate) fn shutdown_requested(shared: &Shared) -> bool {
    let (lock, _) = &**shared;
    lock.lock().map(|state| state.shutdown).unwrap_or(true)
}

/// Idle watcher thread. Sleeps until the daemon has been idle (zero
/// connections) for `timeout`, then requests shutdown and wakes the listeners.
/// A new connection arriving during the window cancels the countdown, and so
/// does a `busy` that answers `true`.
pub(crate) fn idle_watcher(
    shared: Shared,
    timeout: Duration,
    listeners: Listeners,
    busy: Option<BusyCheck>,
) {
    let (lock, cv) = &*shared;
    loop {
        let mut state = match lock.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        // Wait until idle (or shutdown).
        while state.active > 0 && !state.shutdown {
            state = match cv.wait(state) {
                Ok(g) => g,
                Err(_) => return,
            };
        }
        if state.shutdown {
            return;
        }

        // Idle now. Remember the generation, then wait out the timeout. If a
        // connection arrives it bumps `generation` and notifies, waking us early.
        let gen_at_idle = state.generation;
        let (mut state, res) = match cv.wait_timeout(state, timeout) {
            Ok(pair) => pair,
            Err(_) => return,
        };

        if state.shutdown {
            return;
        }
        if !(res.timed_out() && state.active == 0 && state.generation == gen_at_idle) {
            // Activity arrived; loop and re-evaluate from the top.
            continue;
        }

        // Nobody has connected for the whole window. The connection count is
        // the daemon's own measure of idleness; the application may have a
        // better one, and this is the only moment it is asked for it.
        if let Some(check) = busy.as_ref() {
            // Never under our lock: it is application code, it may take locks
            // of its own, and holding this one would stall every accept.
            drop(state);
            let busy_now = check();

            state = match lock.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            if state.shutdown {
                return;
            }
            // A veto costs another window, not the daemon. So does a client
            // that arrived while the predicate was running.
            if busy_now || state.active != 0 || state.generation != gen_at_idle {
                continue;
            }
        }

        // Genuinely idle for the whole window, and nothing is holding us.
        drop(state);
        request_shutdown(&shared, &listeners);
        return;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::transport::Transport;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Instant;

    //
    // The connection ceiling
    //
    #[test]
    fn test_a_free_slot_is_taken_and_counted() {
        let mut state = ConnState::default();

        assert_eq!(state.admit(2), Admission::Admitted);

        assert_eq!(state.active, 1);
        assert_eq!(
            state.generation, 1,
            "a served connection is activity the idle watcher must see"
        );
    }
    #[test]
    fn test_a_connection_over_the_ceiling_is_refused() {
        let mut state = ConnState::default();
        assert_eq!(state.admit(2), Admission::Admitted);
        assert_eq!(state.admit(2), Admission::Admitted);

        assert_eq!(state.admit(2), Admission::Full, "and no more than two");
        assert_eq!(state.active, 2, "a refusal costs no slot");
        assert_eq!(
            state.generation, 2,
            "nor does it count as a client arriving: a reconnect loop must not \
             hold an unused daemon open"
        );
    }
    #[test]
    fn test_a_released_slot_can_be_taken_again() {
        let mut state = ConnState::default();
        state.admit(1);
        assert_eq!(state.admit(1), Admission::Full);

        // What `ActiveGuard::drop` does when a connection thread ends.
        state.active -= 1;

        assert_eq!(state.admit(1), Admission::Admitted);
    }
    #[test]
    fn test_a_shutdown_outranks_a_free_slot() {
        // The idle watcher and the signal watcher both wake the accept loop by
        // connecting to it. That connection must end the loop, not be served -
        // however much room there is.
        let mut state = ConnState {
            shutdown: true,
            ..Default::default()
        };

        assert_eq!(state.admit(64), Admission::ShuttingDown);
        assert_eq!(state.active, 0);
    }
    #[test]
    fn test_a_ceiling_of_zero_admits_nobody() {
        // Unreachable through the builder, which raises zero to one. Here so
        // that the arithmetic says what it means on its own.
        assert_eq!(ConnState::default().admit(0), Admission::Full);
    }
    #[test]
    fn test_admit_notifies_the_idle_watcher_only_when_it_admits() {
        let shared: Arc<(Mutex<ConnState>, Condvar)> =
            Arc::new((Mutex::new(ConnState::default()), Condvar::new()));

        assert_eq!(admit(&shared, 1).unwrap(), Admission::Admitted);
        assert_eq!(admit(&shared, 1).unwrap(), Admission::Full);
        assert_eq!(shared.0.lock().unwrap().active, 1);

        release(&shared);
        assert_eq!(shared.0.lock().unwrap().active, 0);
        assert_eq!(
            admit(&shared, 1).unwrap(),
            Admission::Admitted,
            "the slot a failed spawn gave back is usable"
        );
    }
    #[test]
    fn test_shutdown_requested_reads_the_shared_flag() {
        let shared: Arc<(Mutex<ConnState>, Condvar)> =
            Arc::new((Mutex::new(ConnState::default()), Condvar::new()));
        assert!(!shutdown_requested(&shared));

        shared.0.lock().unwrap().shutdown = true;
        assert!(
            shutdown_requested(&shared),
            "an accept loop retrying a failing listener has to notice this"
        );
    }
    //
    // Shutting down, and the idle watcher that decides when
    //
    /// A listener that never accepts anything and remembers being woken.
    pub(crate) struct RecordingListener {
        pub(crate) woken: Arc<AtomicUsize>,
    }

    impl Listener for RecordingListener {
        fn accept(&self) -> io::Result<Box<dyn Transport>> {
            Err(io::Error::other("this listener never accepts"))
        }

        fn wake(&self) -> io::Result<()> {
            self.woken.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn describe(&self) -> String {
            "recording://test".to_string()
        }
    }
    /// Fresh connection bookkeeping, as `run` builds it.
    fn conn_state() -> Shared {
        Arc::new((Mutex::new(ConnState::default()), Condvar::new()))
    }
    /// Block until `shutdown` is set, or give up and say so.
    fn wait_for_shutdown(shared: &Shared, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if shared.0.lock().unwrap().shutdown {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }
    #[test]
    fn test_request_shutdown_flags_the_daemon_and_knocks_on_every_listener() {
        let shared = conn_state();
        let woken = Arc::new(AtomicUsize::new(0));
        let listeners: Listeners = (0..3)
            .map(|_| {
                Arc::new(RecordingListener {
                    woken: woken.clone(),
                }) as Arc<dyn Listener>
            })
            .collect();

        request_shutdown(&shared, &listeners);

        assert!(shared.0.lock().unwrap().shutdown);
        assert_eq!(
            woken.load(Ordering::SeqCst),
            3,
            "a listener left parked in `accept` is a daemon that cannot exit"
        );
    }
    #[test]
    fn test_a_listener_that_will_not_wake_does_not_stop_the_others() {
        /// The failure a real listener has when its socket is already gone.
        struct Deaf;
        impl Listener for Deaf {
            fn accept(&self) -> io::Result<Box<dyn Transport>> {
                Err(io::Error::other("no"))
            }
            fn wake(&self) -> io::Result<()> {
                Err(io::Error::other("cannot reach myself"))
            }
            fn describe(&self) -> String {
                "deaf://test".to_string()
            }
        }

        let shared = conn_state();
        let woken = Arc::new(AtomicUsize::new(0));
        let listeners: Listeners = vec![
            Arc::new(Deaf),
            Arc::new(RecordingListener {
                woken: woken.clone(),
            }),
        ];

        request_shutdown(&shared, &listeners);

        assert_eq!(
            woken.load(Ordering::SeqCst),
            1,
            "the second listener must still be knocked on"
        );
    }
    #[test]
    fn test_describe_names_every_listener() {
        let woken = Arc::new(AtomicUsize::new(0));
        let listeners: Listeners = vec![
            Arc::new(RecordingListener {
                woken: woken.clone(),
            }),
            Arc::new(RecordingListener { woken }),
        ];

        assert_eq!(describe(&listeners), "recording://test, recording://test");
        assert_eq!(describe(&[]), "");
    }
    #[test]
    fn test_the_idle_watcher_exits_a_daemon_nobody_is_using() {
        // The baseline the guard is measured against: no busy check, no
        // connections, one window.
        let shared = conn_state();
        let watching = {
            let shared = shared.clone();
            thread::spawn(move || idle_watcher(shared, Duration::from_millis(50), Vec::new(), None))
        };

        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
    }
    #[test]
    fn test_a_busy_check_that_says_no_changes_nothing() {
        // The guard is not a delay: an application that is not busy gets the
        // same idle exit it would have got without one.
        let shared = conn_state();
        let asked = Arc::new(AtomicUsize::new(0));
        let counter = asked.clone();

        let watching = {
            let shared = shared.clone();
            let check: BusyCheck = Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                false
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
        assert_eq!(
            asked.load(Ordering::SeqCst),
            1,
            "asked once, at the moment it mattered"
        );
    }
    #[test]
    fn test_a_busy_application_keeps_the_daemon_alive() {
        // The whole point: no connections for window after window, and the
        // daemon stays up because the work it is supervising has not finished.
        let shared = conn_state();
        let busy = Arc::new(AtomicBool::new(true));
        let asked = Arc::new(AtomicUsize::new(0));

        let watching = {
            let shared = shared.clone();
            let busy = busy.clone();
            let asked = asked.clone();
            let check: BusyCheck = Arc::new(move || {
                asked.fetch_add(1, Ordering::SeqCst);
                busy.load(Ordering::SeqCst)
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        // Several windows go by with nobody connected...
        thread::sleep(Duration::from_millis(400));
        assert!(
            !shared.0.lock().unwrap().shutdown,
            "a busy application must outlive its idle timeout"
        );
        assert!(
            asked.load(Ordering::SeqCst) >= 2,
            "and the veto has to be re-asked, not taken once and cached"
        );

        // ...and the moment the work is done, the next window ends it.
        busy.store(false, Ordering::SeqCst);
        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
    }
    #[test]
    fn test_a_client_arriving_during_the_veto_cancels_the_exit() {
        // The predicate runs without the lock held, so the world can move
        // while it is thinking. A connection that arrived in that window is a
        // connection, and the daemon must not exit out from under it.
        let shared = conn_state();
        let arrived = Arc::new(AtomicBool::new(false));

        let watching = {
            let shared = shared.clone();
            let state = shared.clone();
            let arrived = arrived.clone();
            let check: BusyCheck = Arc::new(move || {
                // Exactly the race: a client lands while the application is
                // being asked, and answers "not busy".
                if !arrived.swap(true, Ordering::SeqCst) {
                    let (lock, cv) = &*state;
                    let mut conns = lock.lock().unwrap();
                    conns.admit(8);
                    cv.notify_all();
                }
                false
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        thread::sleep(Duration::from_millis(300));
        assert!(
            !shared.0.lock().unwrap().shutdown,
            "a connection that arrived during the check is still a connection"
        );

        // Let it go, and the daemon leaves as it should.
        release(&shared);
        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
    }
    #[test]
    fn test_the_busy_check_is_not_asked_while_a_client_is_connected() {
        // Connections are the daemon's own measure of idleness and they come
        // first: the application is only consulted once that measure says the
        // daemon would otherwise leave.
        let shared = conn_state();
        shared.0.lock().unwrap().admit(8);

        let asked = Arc::new(AtomicUsize::new(0));
        let watching = {
            let shared = shared.clone();
            let asked = asked.clone();
            let check: BusyCheck = Arc::new(move || {
                asked.fetch_add(1, Ordering::SeqCst);
                false
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        thread::sleep(Duration::from_millis(300));
        assert_eq!(
            asked.load(Ordering::SeqCst),
            0,
            "nothing to veto while a client is being served"
        );

        release(&shared);
        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
    }
    #[test]
    fn test_the_busy_check_runs_without_the_state_lock_held() {
        // It is application code and it may take locks of its own. Running it
        // under ours would deadlock a predicate that so much as looked at the
        // daemon, and would stall every accept while it thought.
        let shared = conn_state();
        let was_free = Arc::new(AtomicBool::new(false));

        let watching = {
            let shared = shared.clone();
            let state = shared.clone();
            let was_free = was_free.clone();
            let check: BusyCheck = Arc::new(move || {
                // `try_lock` rather than `lock`: a test that deadlocks tells
                // you far less than one that fails.
                was_free.store(state.0.try_lock().is_ok(), Ordering::SeqCst);
                false
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
        assert!(
            was_free.load(Ordering::SeqCst),
            "the connection state was locked while the application was asked"
        );
    }
    #[test]
    fn test_the_idle_watcher_wakes_the_listeners_on_its_way_out() {
        // Its own exit is the daemon's exit, and the accept loops are parked.
        let shared = conn_state();
        let woken = Arc::new(AtomicUsize::new(0));
        let listeners: Listeners = vec![Arc::new(RecordingListener {
            woken: woken.clone(),
        })];

        let watching = {
            let shared = shared.clone();
            thread::spawn(move || idle_watcher(shared, Duration::from_millis(50), listeners, None))
        };

        assert!(wait_for_shutdown(&shared, Duration::from_secs(5)));
        watching.join().unwrap();
        assert_eq!(woken.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn test_a_shutdown_already_asked_for_ends_the_watcher_without_a_veto() {
        // A signal is not vetoable, and this is the shape of that: the flag is
        // up before the watcher looks, and it leaves without asking anyone.
        let shared = conn_state();
        shared.0.lock().unwrap().shutdown = true;

        let asked = Arc::new(AtomicUsize::new(0));
        let watching = {
            let shared = shared.clone();
            let asked = asked.clone();
            let check: BusyCheck = Arc::new(move || {
                asked.fetch_add(1, Ordering::SeqCst);
                true
            });
            thread::spawn(move || {
                idle_watcher(shared, Duration::from_millis(50), Vec::new(), Some(check))
            })
        };

        watching.join().unwrap();
        assert_eq!(asked.load(Ordering::SeqCst), 0);
    }
}
