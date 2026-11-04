use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};

#[cfg(test)]
use crate::infrastructure::wal::test_support::{run_test_io_hook, TestIoHookSlot, TestIoStep};
use crate::infrastructure::wal::types::WalOffset;

/// Reads the current commit cursor from the given directory.
/// Returns `Ok(None)` if the cursor file does not exist.
///
/// # Errors
/// Returns an error if the cursor cannot be read or has an invalid length.
pub fn read_cursor(dir: &Path) -> Result<Option<WalOffset>> {
    let cursor_path = dir.join("commit.cursor");

    let mut file = match File::open(&cursor_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("opening WAL cursor {}", cursor_path.display()))
        }
    };

    let file_len = file.metadata()?.len();

    if file_len != 16 {
        return Err(anyhow!(
            "Invalid cursor file length: expected 16 bytes, got {}",
            file_len
        ));
    }

    let mut buf = [0u8; 16];
    file.read_exact(&mut buf)?;
    let offset = WalOffset::from_bytes(buf);
    Ok(Some(offset))
}

/// Syncs a temporary cursor file, renames it, and syncs the containing directory.
///
/// # Errors
/// Returns an error if writing, syncing, or renaming the cursor fails.
pub fn write_cursor(dir: &Path, offset: WalOffset) -> Result<()> {
    write_cursor_inner(
        dir,
        offset,
        #[cfg(test)]
        None,
    )
}

pub(super) fn write_cursor_inner(
    dir: &Path,
    offset: WalOffset,
    #[cfg(test)] test_io_hook: Option<&TestIoHookSlot>,
) -> Result<()> {
    let cursor_path_tmp = dir.join("commit.cursor.tmp");
    let cursor_path_final = dir.join("commit.cursor");
    let mut file = File::create(&cursor_path_tmp)?;
    file.write_all(&offset.to_bytes())?;
    #[cfg(test)]
    if let Some(hook) = test_io_hook {
        run_test_io_hook(hook, TestIoStep::CursorFileSync)?;
    }
    file.sync_all()
        .with_context(|| format!("syncing WAL cursor {}", cursor_path_tmp.display()))?;
    drop(file);
    std::fs::rename(&cursor_path_tmp, &cursor_path_final)
        .with_context(|| format!("renaming WAL cursor {}", cursor_path_final.display()))?;
    #[cfg(test)]
    if let Some(hook) = test_io_hook {
        run_test_io_hook(hook, TestIoStep::CursorDirectorySync)?;
    }
    crate::infrastructure::wal::segment::sync_directory(dir)
        .with_context(|| format!("syncing WAL cursor directory {}", dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_wal_sync_cursor_sync_failures_are_reported() -> Result<()> {
        for failed_step in [TestIoStep::CursorFileSync, TestIoStep::CursorDirectorySync] {
            let dir = tempdir()?;
            let old = WalOffset {
                segment_id: 1,
                byte_offset: 10,
            };
            let new = WalOffset {
                segment_id: 2,
                byte_offset: 20,
            };
            write_cursor(dir.path(), old)?;
            let hook: TestIoHookSlot = std::sync::Arc::new(std::sync::Mutex::new(None));
            *hook.lock().unwrap() = Some(std::sync::Arc::new(move |step| {
                if step == failed_step {
                    return Err(std::io::Error::other("injected cursor sync failure"));
                }
                Ok(())
            }));

            let result = write_cursor_inner(dir.path(), new, Some(&hook));

            assert!(result.is_err(), "{failed_step:?} failure was ignored");
            if failed_step == TestIoStep::CursorFileSync {
                assert_eq!(read_cursor(dir.path())?, Some(old));
            }
        }
        Ok(())
    }

    #[test]
    fn test_read_cursor_file_states_returns_expected_offsets() -> Result<()> {
        let dir = tempdir()?;
        let cursor_path = dir.path().join("commit.cursor");

        assert!(read_cursor(dir.path())?.is_none());

        File::create(&cursor_path)?;
        assert!(read_cursor(dir.path()).is_err());

        let offset = WalOffset {
            segment_id: 42,
            byte_offset: 100,
        };
        let mut file = File::create(&cursor_path)?;
        file.write_all(&offset.to_bytes())?;
        assert_eq!(read_cursor(dir.path())?, Some(offset));

        Ok(())
    }

    #[test]
    fn test_write_cursor_valid_offset_round_trips() -> Result<()> {
        let dir = tempdir()?;
        let offset = WalOffset {
            segment_id: 42,
            byte_offset: 100,
        };
        write_cursor(dir.path(), offset)?;
        assert_eq!(read_cursor(dir.path())?, Some(offset));
        Ok(())
    }

    #[test]
    fn test_write_cursor_over_existing_temp_file_commits_latest_offset() -> Result<()> {
        let dir = tempdir()?;
        let offset1 = WalOffset {
            segment_id: 42,
            byte_offset: 100,
        };
        let offset2 = WalOffset {
            segment_id: 43,
            byte_offset: 200,
        };

        // Simulate concurrent writes by writing to the temp file directly
        let cursor_path_tmp = dir.path().join("commit.cursor.tmp");
        let mut file = File::create(&cursor_path_tmp)?;
        file.write_all(&offset1.to_bytes())?;

        write_cursor(dir.path(), offset2)?;

        assert_eq!(read_cursor(dir.path())?, Some(offset2));
        Ok(())
    }

    #[test]
    fn test_write_cursor_then_read_cursor_returns_written_offset() -> Result<()> {
        let dir = tempdir()?;
        let offset = WalOffset {
            segment_id: 42,
            byte_offset: 100,
        };
        write_cursor(dir.path(), offset)?;
        let read_offset = read_cursor(dir.path())?;
        assert_eq!(read_offset, Some(offset));
        Ok(())
    }
}
