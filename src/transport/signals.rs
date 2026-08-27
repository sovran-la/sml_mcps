//! SIGTERM and SIGINT, turned into an ordinary function call.
//!
//! A signal handler may only use async-signal-safe operations, which rules out
//! locking a mutex, notifying a condvar, or connecting a socket - everything a
//! daemon actually wants to do when it is told to stop. The self-pipe trick is
//! the standard way out: the handler writes one byte to a pipe, and a thread
//! blocked on the other end does the real work with the whole language
//! available to it.
//!
//! What "the real work" is belongs to the caller, which is why this takes a
//! callback. The daemon's is "flag the shutdown and wake every listener", and
//! that list is not something signal handling should know about.

use crate::types::{McpError, Result};
use std::io;
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;

/// Write end of the self-pipe used by the signal handler. -1 when inactive.
static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

/// Signal handler: writes a byte to the self-pipe so the signal watcher
/// thread can trigger a clean shutdown. Only uses async-signal-safe ops.
extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    let fd = SIGNAL_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        unsafe {
            libc::write(fd, b"x" as *const u8 as *const libc::c_void, 1);
        }
    }
}

/// Create a pipe for signal delivery. Returns (read_fd, write_fd).
fn create_signal_pipe() -> Result<(i32, i32)> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error().into());
    }
    Ok((fds[0], fds[1]))
}

/// Install SIGTERM/SIGINT handlers and spawn a watcher thread that runs
/// `on_signal` when one arrives.
///
/// `on_signal` runs on the watcher thread, once, with no signal-safety
/// constraints on it - the handler itself has already done the only
/// async-signal-safe thing that happens here.
///
/// On drop the returned guard puts the previous handlers back, closes the
/// pipe, and joins the watcher - which the closed pipe is what ends.
pub(crate) fn install_signal_handlers<F>(on_signal: F) -> Result<SignalGuard>
where
    F: FnOnce() + Send + 'static,
{
    let (sig_read, sig_write) = create_signal_pipe()?;

    // Publish the write fd so the signal handler can reach it.
    SIGNAL_WRITE_FD.store(sig_write, Ordering::SeqCst);

    // Register handlers. Save previous handlers for restoration.
    let prev_term;
    let prev_int;
    unsafe {
        prev_term = libc::signal(
            libc::SIGTERM,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
        prev_int = libc::signal(
            libc::SIGINT,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
    }

    let watcher = thread::Builder::new()
        .name("signal-watcher".into())
        .spawn(move || {
            // Block until the signal handler writes, or the pipe closes.
            let mut buf = [0u8; 1];
            let n = unsafe { libc::read(sig_read, buf.as_mut_ptr() as *mut libc::c_void, 1) };
            unsafe {
                libc::close(sig_read);
            }

            if n > 0 {
                on_signal();
            }
        });

    let watcher = match watcher {
        Ok(watcher) => watcher,
        Err(e) => {
            // Nobody will ever read this pipe now, and the handler is holding
            // a descriptor pointing into it: without this, both ends leak for
            // the life of the process and every later signal writes into a
            // pipe with no reader. Undo the installation in reverse.
            restore_signal_handlers(prev_term, prev_int, sig_write);
            // SAFETY: the watcher never started, so nothing else owns the read
            // end - it is closed here rather than by the thread that would
            // have been blocked on it.
            unsafe {
                libc::close(sig_read);
            }

            return Err(McpError::Internal(format!(
                "failed to spawn signal watcher: {}",
                e
            )));
        }
    };

    Ok(SignalGuard {
        write_fd: sig_write,
        prev_term,
        prev_int,
        watcher: Some(watcher),
    })
}

/// Put the previous signal handlers back, unpublish the pipe, and close the
/// write end of it.
///
/// Shared by [`SignalGuard::drop`] and the spawn-failure path above, because an
/// installation that never got its watcher has to give back exactly what a
/// finished one does - and the version that ran when the watcher failed to
/// start gave back nothing at all.
///
/// The order is deliberate: the handlers go back first, so that clearing the
/// descriptor underneath the handler cannot race a signal arriving.
fn restore_signal_handlers(
    prev_term: libc::sighandler_t,
    prev_int: libc::sighandler_t,
    write_fd: i32,
) {
    unsafe {
        libc::signal(libc::SIGTERM, prev_term);
        libc::signal(libc::SIGINT, prev_int);
    }

    SIGNAL_WRITE_FD.store(-1, Ordering::SeqCst);
    unsafe {
        libc::close(write_fd);
    }
}

/// RAII guard that restores signal handlers and cleans up the pipe on drop.
pub(crate) struct SignalGuard {
    write_fd: i32,
    prev_term: libc::sighandler_t,
    prev_int: libc::sighandler_t,
    watcher: Option<thread::JoinHandle<()>>,
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        // Closing the write end is also how the watcher is told to stop: if it
        // is still blocked on `read`, it gets EOF and exits cleanly.
        restore_signal_handlers(self.prev_term, self.prev_int, self.write_fd);

        if let Some(w) = self.watcher.take() {
            let _ = w.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    #[test]
    fn test_restoring_the_handlers_closes_the_pipe_it_published() {
        // What the spawn-failure path leans on. Without it, a daemon whose
        // watcher thread never started leaked both ends of the pipe and left
        // the signal handler holding a descriptor nobody would ever read.
        let (read_fd, write_fd) = create_signal_pipe().unwrap();
        let published = SIGNAL_WRITE_FD.swap(write_fd, Ordering::SeqCst);

        // `SIG_DFL` both ways: nothing in this suite installs a handler that
        // outlives its own server, and nothing here sends a signal.
        restore_signal_handlers(libc::SIG_DFL, libc::SIG_DFL, write_fd);

        assert_eq!(
            SIGNAL_WRITE_FD.load(Ordering::SeqCst),
            -1,
            "the handler must not be left pointing at a closed pipe"
        );
        assert!(is_closed(write_fd), "the write end must not be left open");
        assert!(
            !is_closed(read_fd),
            "the read end belongs to whoever is blocked on it, or to the \
             spawn-failure path"
        );

        // Which is what that path then does with it.
        unsafe { libc::close(read_fd) };
        assert!(is_closed(read_fd));

        SIGNAL_WRITE_FD.store(published, Ordering::SeqCst);
    }

    #[test]
    fn test_a_signal_pipe_has_two_usable_ends() {
        let (read_fd, write_fd) = create_signal_pipe().unwrap();

        assert!(!is_closed(read_fd));
        assert!(!is_closed(write_fd));
        assert_ne!(read_fd, write_fd);

        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
    }

    /// Whether `fd` is closed, asked the only way a process can ask.
    fn is_closed(fd: i32) -> bool {
        unsafe { libc::fcntl(fd, libc::F_GETFD) == -1 }
    }

    #[test]
    fn test_a_byte_on_the_pipe_runs_the_callback() {
        // The watcher's whole job, exercised without raising a real signal:
        // this suite is threaded and process-wide signal state is not a thing
        // one test gets to borrow. What the handler does is write that byte,
        // and it is tested end to end against a live daemon in
        // `tests/daemon_lifecycle.rs`.
        let fired = Arc::new(AtomicBool::new(false));
        let flag = fired.clone();

        let guard = install_signal_handlers(move || flag.store(true, Ordering::SeqCst)).unwrap();

        let written =
            unsafe { libc::write(guard.write_fd, b"x" as *const u8 as *const libc::c_void, 1) };
        assert_eq!(written, 1);

        let deadline = Instant::now() + Duration::from_secs(5);
        while !fired.load(Ordering::SeqCst) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(fired.load(Ordering::SeqCst), "the watcher never ran it");

        drop(guard);
    }

    #[test]
    fn test_dropping_the_guard_ends_the_watcher_without_running_the_callback() {
        // A daemon that exited for its own reasons must not have its shutdown
        // callback fired on the way out: `run` has already torn down by then,
        // and waking listeners that are gone is at best noise.
        let fired = Arc::new(AtomicBool::new(false));
        let flag = fired.clone();

        let guard = install_signal_handlers(move || flag.store(true, Ordering::SeqCst)).unwrap();
        // Joins the watcher, so there is no window left for it to run in.
        drop(guard);

        assert!(!fired.load(Ordering::SeqCst));
        assert_eq!(
            SIGNAL_WRITE_FD.load(Ordering::SeqCst),
            -1,
            "and the handler is left pointing at nothing"
        );
    }
}
