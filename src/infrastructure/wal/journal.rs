use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use anyhow::{bail, Context, Result};

use crate::infrastructure::wal::{
    segment::{create_wal_directory, sync_directory},
    types::WalOffset,
};

#[derive(Default)]
pub(super) struct Maintenance {
    pub committed: Option<WalOffset>,
    pub pending_deleted_bytes: u64,
}

pub(crate) struct Journal {
    dir: PathBuf,
    _lock: File,
    maintenance: Mutex<Maintenance>,
}

impl Journal {
    pub(crate) fn open(dir: PathBuf) -> Result<Self> {
        create_wal_directory(&dir)?;
        let lock_path = dir.join(".journal.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("opening journal lock {}", lock_path.display()))?;
        fs4::FileExt::try_lock(&lock)
            .with_context(|| format!("acquiring exclusive journal lock {}", lock_path.display()))?;
        check_directory_capabilities(&dir)?;

        Ok(Self {
            dir,
            _lock: lock,
            maintenance: Mutex::new(Maintenance::default()),
        })
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(super) fn maintenance(&self) -> Result<MutexGuard<'_, Maintenance>> {
        self.maintenance
            .lock()
            .map_err(|_| anyhow::anyhow!("journal maintenance lock was poisoned"))
    }
}

fn check_directory_capabilities(dir: &Path) -> Result<()> {
    let mut probe = None;
    for attempt in 0..16 {
        let name = format!(".journal-probe-{}-{attempt}", std::process::id());
        let source = dir.join(format!("{name}.tmp"));
        let destination = dir.join(format!("{name}.renamed"));
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&source)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("creating journal probe in {}", dir.display()))
            }
        };
        let target = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
        {
            Ok(target) => target,
            Err(error) => {
                drop(file);
                fs::remove_file(&source)
                    .with_context(|| format!("removing journal probe {}", source.display()))?;
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(error)
                    .with_context(|| format!("creating journal probe in {}", dir.display()));
            }
        };
        probe = Some((source, destination, file, target));
        break;
    }

    let (source, destination, mut file, target) =
        probe.with_context(|| format!("no available journal probe name in {}", dir.display()))?;
    let result = (|| -> Result<()> {
        drop(target);
        file.write_all(b"journal-probe")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&source, &destination)?;
        sync_directory(dir)?;
        let mut bytes = [0; 13];
        File::open(&destination)?.read_exact(&mut bytes)?;
        if &bytes != b"journal-probe" {
            bail!("journal probe read returned different bytes");
        }
        Ok(())
    })()
    .with_context(|| {
        format!(
            "checking journal read/write/sync/rename in {}",
            dir.display()
        )
    });

    let mut cleanup = Ok(());
    for path in [&source, &destination] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                let error =
                    anyhow::Error::new(error).context(format!("removing {}", path.display()));
                cleanup = match cleanup {
                    Ok(()) => Err(error),
                    Err(previous) => {
                        Err(previous.context(format!("additional cleanup failure: {error:#}")))
                    }
                };
            }
        }
    }
    if cleanup.is_ok() {
        cleanup = sync_directory(dir)
            .with_context(|| format!("syncing journal directory {}", dir.display()));
    }
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("probe cleanup failed: {cleanup:#}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, process::Command};

    use super::*;

    const CHILD_TEST: &str = "infrastructure::wal::journal::tests::test_journal_ownership_child";
    const TEST_DIR: &str = "INGEST_TEST_JOURNAL_DIR";
    const TEST_MODE: &str = "INGEST_TEST_JOURNAL_MODE";

    #[test]
    fn test_journal_ownership_child() -> Result<()> {
        let Some(dir) = env::var_os(TEST_DIR) else {
            return Ok(());
        };
        match env::var(TEST_MODE)?.as_str() {
            "contended" => {
                assert!(Journal::open(PathBuf::from(dir)).is_err());
                println!("journal ownership contention checked");
            }
            "exit" => {
                let _journal = Journal::open(PathBuf::from(dir))?;
                std::process::exit(42);
            }
            "hold" => {
                let dir = PathBuf::from(dir);
                let _journal = Journal::open(dir.clone())?;
                fs::write(dir.join("child-ready"), b"ready")?;
                loop {
                    std::thread::park();
                }
            }
            mode => anyhow::bail!("unexpected journal ownership test mode {mode}"),
        }
        Ok(())
    }

    #[test]
    fn test_journal_ownership_blocks_other_process_and_recovers_after_exit() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let journal = Journal::open(dir.path().to_path_buf())?;
        let child = |mode: &str| {
            Command::new(env::current_exe().expect("test executable"))
                .args(["--exact", CHILD_TEST, "--nocapture"])
                .env(TEST_DIR, dir.path())
                .env(TEST_MODE, mode)
                .output()
        };

        let contention = child("contended")?;
        assert!(contention.status.success());
        assert!(String::from_utf8_lossy(&contention.stdout)
            .contains("journal ownership contention checked"));
        drop(journal);

        let exited = child("exit")?;
        assert_eq!(exited.status.code(), Some(42));
        assert!(dir.path().join(".journal.lock").exists());
        let _reopened = Journal::open(dir.path().to_path_buf())?;
        Ok(())
    }

    #[test]
    fn test_journal_ownership_recovers_after_forced_process_termination() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut child = Command::new(env::current_exe()?)
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(TEST_DIR, dir.path())
            .env(TEST_MODE, "hold")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let ready = dir.path().join("child-ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let started = ready.exists();
        let blocked = started && Journal::open(dir.path().to_path_buf()).is_err();
        child.kill()?;
        child.wait()?;
        assert!(started, "child did not acquire the journal");
        assert!(blocked, "live child must retain exclusive ownership");
        let _reopened = Journal::open(dir.path().to_path_buf())?;
        fs::remove_file(ready)?;
        Ok(())
    }

    #[test]
    fn test_journal_ownership_probe_preserves_preexisting_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        for attempt in 0..16 {
            let path = dir.path().join(format!(
                ".journal-probe-{}-{attempt}.tmp",
                std::process::id()
            ));
            fs::write(path, b"existing")?;
        }

        assert!(Journal::open(dir.path().to_path_buf()).is_err());
        for attempt in 0..16 {
            let path = dir.path().join(format!(
                ".journal-probe-{}-{attempt}.tmp",
                std::process::id()
            ));
            assert_eq!(fs::read(path)?, b"existing");
        }
        Ok(())
    }

    #[test]
    fn test_journal_ownership_is_independent_for_both_directories() -> Result<()> {
        let root = tempfile::tempdir()?;
        let wal_dir = root.path().join("wal");
        let quarantine_dir = root.path().join("quarantine");
        let wal = Journal::open(wal_dir.clone())?;
        let quarantine = Journal::open(quarantine_dir.clone())?;

        assert!(Journal::open(wal_dir.clone()).is_err());
        assert!(Journal::open(quarantine_dir.clone()).is_err());
        drop(wal);
        assert!(Journal::open(wal_dir).is_ok());
        assert!(Journal::open(quarantine_dir).is_err());
        drop(quarantine);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_journal_ownership_rejects_unwritable_directory_for_effective_user() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500))?;
        let probe_path = dir.path().join("permission-probe");
        let can_create = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe_path)
        {
            Ok(file) => {
                drop(file);
                fs::remove_file(probe_path)?;
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => false,
            Err(error) => return Err(error.into()),
        };
        let result = if can_create {
            None
        } else {
            Some(Journal::open(dir.path().to_path_buf()))
        };
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
        if let Some(result) = result {
            assert!(result.is_err(), "unwritable journal must fail at startup");
        }
        Ok(())
    }
}
