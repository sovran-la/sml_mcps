//! Where a daemon's socket goes, and who is allowed to be behind it.
//!
//! A Unix socket is a file, so everything that is true of a file in a
//! world-writable directory is true of it. `/tmp/myapp/server.sock` is a path
//! any account on the machine can create *first*: bind a socket there, wait for
//! the shim to connect, and read - or answer - every message of the session,
//! including whatever the tools were handed. The daemon that meant to own the
//! path then refuses to start, because something is already listening on it.
//!
//! Two things close that:
//!
//! - [`user_socket_path`] puts the socket in a directory belonging to the user
//!   running the server - `$XDG_RUNTIME_DIR` where the system provides one, and
//!   `~/.local/state` where it does not - and
//!   [`ensure_private_parent_dir`] creates it `0700`.
//! - [`owned_by_current_user`] is what the client side checks before it trusts
//!   a socket that is already there, for the paths this crate did not choose.
//!
//! Unix-only, like everything that needs it.

use crate::types::{McpError, Result};
use std::ffi::OsString;
use std::fs::{DirBuilder, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

/// Directory under `$HOME` for state a program keeps between runs.
///
/// The XDG Base Directory spec's `$XDG_STATE_HOME` default, which is where a
/// socket belongs on a system with no `$XDG_RUNTIME_DIR` - it is per-user, and
/// unlike `~/.cache` nothing sweeps it while a daemon is running.
const STATE_HOME: &str = ".local/state";

/// The socket path an application should use, in a directory only this user can
/// reach.
///
/// `app` and `file` are name components, not paths: `user_socket_path("myapp",
/// "server.sock")` is one directory holding one socket.
///
/// Resolved in this order:
///
/// 1. `$XDG_RUNTIME_DIR/<app>/<file>` - the system-provided per-user directory,
///    already `0700`, and cleaned up at logout. This is the right answer where
///    it exists, which is most Linux desktops.
/// 2. `$HOME/.local/state/<app>/<file>` - everywhere else, macOS included.
/// 3. `<temp>/<app>-<uid>/<file>` - only when the process has neither, which
///    means a daemon with no home directory. The `<uid>` is what keeps two
///    accounts from meeting in a shared temporary directory; the directory
///    itself is still created `0700`.
///
/// The directory is not created here - [`Server::serve_daemon`](crate::Server::serve_daemon)
/// and [`UnixServer`](crate::UnixServer) both create it, privately, before they
/// bind.
///
/// ```no_run
/// use sml_mcps::user_socket_path;
///
/// let socket = user_socket_path("my-mcp", "server.sock");
/// assert!(socket.ends_with("my-mcp/server.sock"));
/// ```
pub fn user_socket_path(app: &str, file: &str) -> PathBuf {
    socket_path_from(
        std::env::var_os("XDG_RUNTIME_DIR"),
        std::env::var_os("HOME"),
        &std::env::temp_dir(),
        current_uid(),
        app,
        file,
    )
}

/// [`user_socket_path`] over supplied values.
///
/// The environment is process-global and tests run threaded inside one process,
/// so taking it as arguments is the only way each arm is testable.
fn socket_path_from(
    runtime_dir: Option<OsString>,
    home: Option<OsString>,
    temp_dir: &Path,
    uid: u32,
    app: &str,
    file: &str,
) -> PathBuf {
    // An empty value counts as unset: a socket under `/` is not a thing, and
    // neither is one in a directory named by an empty variable.
    let usable = |value: Option<OsString>| value.filter(|v| !v.is_empty()).map(PathBuf::from);

    if let Some(runtime) = usable(runtime_dir) {
        return runtime.join(app).join(file);
    }

    if let Some(home) = usable(home) {
        return home.join(STATE_HOME).join(app).join(file);
    }

    temp_dir.join(format!("{app}-{uid}")).join(file)
}

/// Create the directory `socket_path` will be bound inside, reachable only by
/// this user.
///
/// A socket is exactly as private as the directory holding it, so anything
/// created here is created `0700`. A directory that already exists is left as
/// it is: the mode of `$HOME`, or of a runtime directory the system set up, is
/// not this crate's to change.
///
/// A daemon calls this before it forks, so a directory that cannot be created
/// is reported to whoever ran the command rather than to `/dev/null`.
pub(crate) fn ensure_private_parent_dir(socket_path: &Path) -> Result<()> {
    // A bare filename has a parent of `""`, which is the current directory and
    // is not ours to create. `create_dir_all` happens to no-op on that too, so
    // this guard is saying what we mean rather than standing between anyone and
    // a bug - which is still better than resting on a special case buried in
    // the standard library.
    let Some(parent) = socket_path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };

    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .map_err(|e| {
            McpError::Internal(format!(
                "failed to create socket directory {}: {}",
                parent.display(),
                e
            ))
        })
}

/// Whether `path` belongs to the user running this process.
///
/// `None` means the question could not be answered - the path is not there, the
/// link at it dangles, or the directory holding it cannot be searched. What no
/// caller may do is read that as "it is mine": a socket this user does not own
/// is one another account may be listening on, and everything said to it is
/// said to them.
///
/// Both ends of a symlink are asked about, because they can have different
/// owners and each is a way in: a link another account dropped at our path is
/// hostile whatever it points at, and a link of ours that names somebody else's
/// socket is what a connect would actually reach.
///
/// This is a check with a window in it: the file could be replaced between the
/// answer and the connect. Closing that window entirely takes peer credentials
/// on an established connection; what this is for is the case that needs no
/// race at all, where the attacker's socket is simply sitting there first - and
/// for the paths where it matters, [`user_socket_path`] means no other account
/// can create anything there to begin with.
pub(crate) fn owned_by_current_user(path: &Path) -> Option<bool> {
    let uid = current_uid();

    if std::fs::symlink_metadata(path).ok()?.uid() != uid {
        return Some(false);
    }

    Some(std::fs::metadata(path).ok()?.uid() == uid)
}

/// The exclusive right to touch what is at a socket path.
///
/// An `flock(LOCK_EX | LOCK_NB)` on the socket's sibling `.lock` file. The
/// rules it enforces, between every process built on this crate:
///
/// 1. A daemon takes the claim before it probes, removes, or binds anything at
///    the socket path, and holds it until it has stopped serving and unlinked
///    its own socket.
/// 2. Nobody unlinks a socket file without holding its claim.
///
/// Those two rules are what make "a refused connect means a stale socket"
/// sound. Without them it is a guess, and a wrong one: `UnixListener::bind` is
/// `bind` then `listen`, two syscalls, and a connect landing between them is
/// refused exactly the way an abandoned socket file refuses. A process that
/// believed that refusal deleted a live daemon's socket file and bound its
/// own, leaving the live daemon serving an inode with no name - unreachable
/// to every client and to SIGTERM (which wakes a parked `accept` by
/// connecting to the now-deleted path), forever. Under the claim that state
/// cannot be observed from outside: whoever is mid-bind is holding it.
///
/// The lock lives on the open file description, so the kernel releases it when
/// the last descriptor closes - including a holder killed with `-9`. There is
/// no stale-lock state and nothing to clean up. The `.lock` file itself is
/// never unlinked: removing it would reopen the race, because a new claimer
/// could lock the old inode while a fresh file takes the name.
#[derive(Debug)]
pub(crate) struct SocketLock {
    /// Held for the flock, never read or written; released on drop.
    _file: File,
}

impl SocketLock {
    /// Take the claim on `socket_path`, or `None` if another process (or
    /// thread - flock excludes per open file description) already holds it.
    ///
    /// `Err` is reserved for the lock file being unopenable or the flock
    /// failing for some reason other than being held - answers that mean the
    /// machine, not the race.
    pub(crate) fn acquire(socket_path: &Path) -> Result<Option<Self>> {
        let path = lock_path_for(socket_path);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                McpError::Internal(format!(
                    "could not open {} to claim the socket path: {}",
                    path.display(),
                    e
                ))
            })?;

        // SAFETY: `flock` reads a descriptor and a flag and touches no memory.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(Self { _file: file }));
        }

        let refused = std::io::Error::last_os_error();
        match refused.raw_os_error() {
            Some(libc::EWOULDBLOCK) => Ok(None),
            // A lock that cannot be taken for any other reason is not a busy
            // socket, and pretending it is would let a second daemon through
            // on a filesystem that does not do flock.
            _ => Err(McpError::Internal(format!(
                "could not lock {}: {}",
                path.display(),
                refused
            ))),
        }
    }
}

/// Compute the claim-file path for a socket path: replace the extension with
/// `lock` (e.g. `server.sock` -> `server.lock`), next to the `.pid` file.
fn lock_path_for(socket_path: &Path) -> PathBuf {
    let mut p = socket_path.to_path_buf();
    p.set_extension("lock");
    p
}

/// The real user id of this process.
fn current_uid() -> u32 {
    // SAFETY: `getuid` reads a process attribute and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A unique path in the temporary directory, per test.
    fn temp_path(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);

        std::env::temp_dir().join(format!(
            "sml_mcps_socket_{}_{}_{}",
            std::process::id(),
            tag,
            N.fetch_add(1, Ordering::SeqCst)
        ))
    }

    /// The mode of a directory, without the file-type bits.
    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    //
    // Choosing the path
    //

    #[test]
    fn a_runtime_directory_is_used_when_the_system_provides_one() {
        let path = socket_path_from(
            Some("/run/user/501".into()),
            Some("/home/x".into()),
            Path::new("/tmp"),
            501,
            "my-mcp",
            "server.sock",
        );

        assert_eq!(path, PathBuf::from("/run/user/501/my-mcp/server.sock"));
    }

    #[test]
    fn without_one_the_socket_lives_under_the_home_directory() {
        let path = socket_path_from(
            None,
            Some("/home/x".into()),
            Path::new("/tmp"),
            501,
            "my-mcp",
            "server.sock",
        );

        assert_eq!(
            path,
            PathBuf::from("/home/x/.local/state/my-mcp/server.sock")
        );
    }

    #[test]
    fn an_empty_variable_counts_as_unset() {
        // Otherwise `XDG_RUNTIME_DIR=` puts the socket at `/my-mcp/`, which is
        // both wrong and unwritable.
        assert_eq!(
            socket_path_from(
                Some("".into()),
                Some("/home/x".into()),
                Path::new("/tmp"),
                501,
                "my-mcp",
                "server.sock"
            ),
            PathBuf::from("/home/x/.local/state/my-mcp/server.sock")
        );

        assert_eq!(
            socket_path_from(
                Some("".into()),
                Some("".into()),
                Path::new("/tmp"),
                501,
                "my-mcp",
                "server.sock"
            ),
            PathBuf::from("/tmp/my-mcp-501/server.sock")
        );
    }

    #[test]
    fn with_neither_the_fallback_is_per_user() {
        // A shared temporary directory is the case this whole module exists
        // for, so the one path that lands in it does not land in it *bare*:
        // two accounts running the same server get two directories.
        let mine = socket_path_from(None, None, Path::new("/tmp"), 501, "my-mcp", "server.sock");
        let theirs = socket_path_from(None, None, Path::new("/tmp"), 502, "my-mcp", "server.sock");

        assert_eq!(mine, PathBuf::from("/tmp/my-mcp-501/server.sock"));
        assert_ne!(mine, theirs);
    }

    #[test]
    fn the_socket_is_always_in_a_directory_of_its_own() {
        // Whichever arm answers, the parent is the application's alone - which
        // is what makes creating it `0700` meaningful.
        for path in [
            socket_path_from(
                Some("/run/user/501".into()),
                None,
                Path::new("/tmp"),
                501,
                "my-mcp",
                "server.sock",
            ),
            socket_path_from(
                None,
                Some("/home/x".into()),
                Path::new("/tmp"),
                501,
                "my-mcp",
                "server.sock",
            ),
            socket_path_from(None, None, Path::new("/tmp"), 501, "my-mcp", "server.sock"),
        ] {
            assert_eq!(path.file_name().unwrap(), "server.sock", "{path:?}");
            assert!(path.parent().unwrap().to_string_lossy().contains("my-mcp"));
        }
    }

    #[test]
    fn the_public_helper_agrees_with_the_environment_it_is_given() {
        // Reads the real environment, so all it can assert is the shape - the
        // arms themselves are tested above, where the values are ours.
        let path = user_socket_path("my-mcp", "server.sock");

        assert!(path.is_absolute(), "{path:?}");
        assert!(path.ends_with("my-mcp/server.sock"), "{path:?}");
    }

    //
    // Creating the directory
    //

    #[test]
    fn the_socket_directory_is_created_for_this_user_alone() {
        let base = temp_path("private");
        let socket = base.join("nested/server.sock");

        ensure_private_parent_dir(&socket).unwrap();

        assert!(socket.parent().unwrap().is_dir());
        assert_eq!(
            mode_of(&base.join("nested")),
            0o700,
            "a socket is only as private as the directory holding it"
        );
        assert_eq!(mode_of(&base), 0o700, "every directory made on the way");
        assert!(!socket.exists(), "the socket itself is the listener's job");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn a_directory_that_already_exists_keeps_its_own_mode() {
        // `~`, or a runtime directory the system set up, is not this crate's
        // to re-permission.
        let base = temp_path("existing");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o755)).unwrap();

        ensure_private_parent_dir(&base.join("server.sock")).unwrap();

        assert_eq!(mode_of(&base), 0o755);

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn creating_the_directory_twice_is_not_an_error() {
        let base = temp_path("twice");
        let socket = base.join("server.sock");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("keep-me"), b"x").unwrap();

        ensure_private_parent_dir(&socket).unwrap();
        ensure_private_parent_dir(&socket).unwrap();

        assert!(base.join("keep-me").exists());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn a_socket_path_with_no_directory_is_not_an_error() {
        // `parent()` of a bare filename is `""` - the current directory, which
        // exists and is not ours to create.
        ensure_private_parent_dir(Path::new("server.sock")).unwrap();
        ensure_private_parent_dir(Path::new("/server.sock")).unwrap();
    }

    #[test]
    fn an_unusable_socket_directory_is_reported() {
        // A file sitting where the directory belongs. Failing here is the
        // point: the alternative is a daemon that forks, detaches, and only
        // then discovers it cannot bind - with its stderr already at
        // /dev/null.
        let blocker = temp_path("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();

        let error = ensure_private_parent_dir(&blocker.join("server.sock")).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to create socket directory"),
            "{error}"
        );

        let _ = std::fs::remove_file(&blocker);
    }

    //
    // Who owns what is there
    //

    #[test]
    fn a_file_this_process_made_is_this_users() {
        let path = temp_path("ours");
        std::fs::write(&path, b"x").unwrap();

        assert_eq!(owned_by_current_user(&path), Some(true));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_path_that_is_not_there_answers_nothing() {
        // Which is the ordinary case on a first start, and must not be read as
        // either "mine" or "someone else's".
        assert_eq!(owned_by_current_user(&temp_path("absent")), None);
    }

    #[test]
    fn a_file_belonging_to_another_account_is_not_ours() {
        // `/` is root's on every machine this runs on - unless the suite is
        // running as root, where the question has no answer worth asserting.
        if current_uid() == 0 {
            return;
        }

        assert_eq!(owned_by_current_user(Path::new("/")), Some(false));
    }

    #[test]
    fn a_link_of_ours_to_a_file_of_ours_is_ours() {
        let target = temp_path("link-target");
        let link = temp_path("link");
        std::fs::write(&target, b"x").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert_eq!(owned_by_current_user(&link), Some(true));

        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn a_link_of_ours_naming_someone_elses_file_is_not_ours() {
        // The end a connect would actually reach. A path can be ours and still
        // name a socket that is not - which is the thing worth refusing, and
        // the reason the target is asked about as well as the link.
        if current_uid() == 0 {
            return;
        }

        let link = temp_path("link-to-theirs");
        std::os::unix::fs::symlink("/", &link).unwrap();

        assert_eq!(
            std::fs::symlink_metadata(&link).unwrap().uid(),
            current_uid(),
            "the link itself is ours, so only the target can answer this"
        );
        assert_eq!(owned_by_current_user(&link), Some(false));

        let _ = std::fs::remove_file(&link);
    }

    #[test]
    fn a_link_that_dangles_answers_nothing() {
        // There is no file to own. Not "mine", and not "someone else's" either:
        // the callers that remove a stale socket need to be able to remove this
        // one.
        let link = temp_path("dangling");
        std::os::unix::fs::symlink(temp_path("never-existed"), &link).unwrap();

        assert_eq!(owned_by_current_user(&link), None);

        let _ = std::fs::remove_file(&link);
    }
}
