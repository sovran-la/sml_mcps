//! Publish a complete PID with one atomic rename, never an empty live file.
use crate::types::Result;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(crate) fn write_pid_file(path: &Path) -> Result<()> {
    let mut pending = PendingPid::create(path)?;
    writeln!(pending.file, "{}", std::process::id())?;
    pending.publish(path)?;
    Ok(())
}

/// The temporary file lives beside its destination so rename cannot cross
/// filesystems. create_new refuses collisions and symlinks; Drop removes an
/// incomplete file on every failure path.
struct PendingPid {
    path: PathBuf,
    file: File,
}
impl PendingPid {
    fn create(destination: &Path) -> io::Result<Self> {
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| io::Error::other(e.to_string()))?;
        let nonce = u128::from_ne_bytes(random);
        let path = parent.join(format!(".sml-mcps-pid-{nonce:032x}"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        Ok(Self { path, file })
    }

    fn publish(mut self, destination: &Path) -> io::Result<()> {
        self.file.flush()?;
        fs::rename(&self.path, destination)
    }
}
impl Drop for PendingPid {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn target_is_absent_until_the_complete_pid_is_published() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("daemon.pid");
        let mut pending = PendingPid::create(&target).unwrap();
        // Hold the exact create-before-write window open deterministically.
        assert!(!target.exists());
        pending.file.write_all(b"12").unwrap();
        assert!(!target.exists());
        pending.file.write_all(b"345\n").unwrap();
        assert!(!target.exists());
        pending.publish(&target).unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "12345\n");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn existing_pid_is_unchanged_until_replacement_is_complete() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("daemon.pid");
        fs::write(&target, "12345\n").unwrap();
        let mut pending = PendingPid::create(&target).unwrap();
        pending.file.write_all(b"67").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "12345\n");
        pending.file.write_all(b"890\n").unwrap();
        pending.publish(&target).unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "67890\n");
    }

    #[test]
    fn abandoned_write_preserves_existing_pid_and_removes_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("daemon.pid");
        fs::write(&target, "12345\n").unwrap();
        let mut pending = PendingPid::create(&target).unwrap();
        pending.file.write_all(b"67").unwrap();
        drop(pending);
        assert_eq!(fs::read_to_string(&target).unwrap(), "12345\n");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_rename_cleans_up_without_removing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("daemon.pid");
        fs::create_dir(&target).unwrap();
        assert!(write_pid_file(&target).is_err());
        assert!(target.is_dir());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn writes_current_pid_with_private_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("daemon.pid");
        write_pid_file(&target).unwrap();
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            format!("{}\n", std::process::id())
        );
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn destination_symlink_is_replaced_without_touching_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other");
        let target = dir.path().join("daemon.pid");
        fs::write(&other, "do not truncate").unwrap();
        symlink(&other, &target).unwrap();
        write_pid_file(&target).unwrap();
        assert!(
            !fs::symlink_metadata(&target)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(&other).unwrap(), "do not truncate");
    }

    #[test]
    fn missing_parent_returns_error_without_creating_directories() {
        let dir = tempfile::tempdir().unwrap();
        assert!(write_pid_file(&dir.path().join("missing/daemon.pid")).is_err());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
