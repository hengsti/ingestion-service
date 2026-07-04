use crate::infrastructure::wal::types::WalOffset;
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// Reads the current commit cursor from the given directory.
/// Returns `Ok(None)` if the cursor file does not exist or is empty.
pub fn read_cursor(dir: &Path) -> Result<Option<WalOffset>> {
    let cursor_path = dir.join("commit.cursor");

    if !cursor_path.exists() {
        return Ok(None);
    }

    let mut file = match File::open(cursor_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(anyhow!("Failed to open cursor file: {}", e)),
    };

    let file_len = file.metadata()?.len();

    if file_len == 0 {
        return Ok(None);
    }

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

/// Writes the given `WalOffset` to the commit cursor file in the specified directory.
/// This operation is atomic: it first writes to a temporary file and then renames it to the final cursor file.
pub fn write_cursor(dir: &Path, offset: WalOffset) -> Result<()> {
    let cursor_path_tmp = dir.join("commit.cursor.tmp");
    let cursor_path_final = dir.join("commit.cursor");
    let mut file = File::create(&cursor_path_tmp)?;
    file.write_all(&offset.to_bytes())?;
    std::fs::rename(&cursor_path_tmp, &cursor_path_final)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_read_cursor_file_states_returns_expected_offsets() -> Result<()> {
        let dir = tempdir()?;
        let cursor_path = dir.path().join("commit.cursor");

        assert!(read_cursor(dir.path())?.is_none());

        File::create(&cursor_path)?;
        assert!(read_cursor(dir.path())?.is_none());

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
