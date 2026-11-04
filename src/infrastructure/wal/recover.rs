use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::infrastructure::wal::codec;

pub(super) fn validate_committed_offset(path: &Path, offset: u64) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("opening committed WAL segment {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut payload = Vec::new();
    let mut position = 0u64;

    while position < offset {
        match codec::decode_for_recovery(&mut reader, &mut payload)? {
            codec::DecodeOutcome::ValidRecord(_, consumed) => {
                position += consumed as u64;
            }
            codec::DecodeOutcome::CleanEof => {
                bail!("committed WAL cursor {offset} points beyond a complete record");
            }
            codec::DecodeOutcome::FrameCorrupt => {
                bail!("committed WAL segment contains a corrupt record at {position}");
            }
        }
    }

    if position != offset {
        bail!("committed WAL cursor {offset} is not on a record boundary");
    }
    Ok(())
}

/// Scans `path` and returns the byte offset immediately after the last
/// **complete** record, i.e. the length the active segment must be truncated to
/// so the next append lands on a frame boundary.
///
/// A crash can leave a partially written record at EOF (a torn length prefix or
/// a short payload). Resuming the writer at `metadata().len()` would place the
/// next record *after* those torn bytes, permanently mis-framing the reader.
/// This scan finds the safe resume point instead.
///
/// An incomplete final frame is treated as a torn tail. Complete corrupt frames
/// and invalid lengths fail recovery rather than allowing later records to pass
/// the damaged record.
///
/// Returns `0` for a missing or empty file.
///
/// # Errors
/// Returns an error if the segment cannot be read, contains an invalid length,
/// or contains a complete corrupt record.
pub fn last_valid_offset(path: &Path) -> Result<u64> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("opening WAL segment {}", path.display())),
    };

    let mut reader = BufReader::new(file);
    let mut valid_len: u64 = 0;
    let mut payload = Vec::new();

    loop {
        match codec::decode_for_recovery(&mut reader, &mut payload)? {
            codec::DecodeOutcome::ValidRecord(_, consumed) => {
                valid_len += consumed as u64;
            }
            codec::DecodeOutcome::CleanEof => {
                break;
            }
            codec::DecodeOutcome::FrameCorrupt => {
                bail!(
                    "WAL segment {} contains a corrupt record at {valid_len}",
                    path.display()
                );
            }
        }
    }

    Ok(valid_len)
}

/// Physically truncates `path` to `len` bytes.
///
/// The shortened file is synced before returning. Callers should only invoke
/// this when `len` is strictly less than the current file length.
///
/// # Errors
/// Returns an error if the segment cannot be opened or resized.
pub fn truncate_to(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("opening WAL segment for truncation {}", path.display()))?;
    file.set_len(len)
        .with_context(|| format!("truncating WAL segment {} to {len} bytes", path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing truncated WAL segment {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::encode_into;
    use crate::infrastructure::wal::test_support::sample_event;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    /// Writes `n` framed records to a fresh segment file, returning its path and
    /// the total number of bytes written.
    fn write_records(dir: &Path, n: u64) -> (std::path::PathBuf, u64) {
        let path = dir.join("00000000000000000001.log");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut buf = Vec::new();
        let mut total = 0u64;
        for i in 0..n {
            encode_into(&mut buf, &sample_event(i)).unwrap();
            file.write_all(&buf).unwrap();
            total += buf.len() as u64;
        }
        (path, total)
    }

    #[test]
    fn test_last_valid_offset_missing_file_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.log");
        assert_eq!(last_valid_offset(&path).unwrap(), 0);
    }

    #[test]
    fn test_last_valid_offset_empty_file_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000001.log");
        fs::File::create(&path).unwrap();
        assert_eq!(last_valid_offset(&path).unwrap(), 0);
    }

    #[test]
    fn test_last_valid_offset_clean_file_returns_full_length() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 5);
        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn test_last_valid_offset_torn_length_prefix_returns_offset_before_prefix() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8, 0u8]).unwrap();
        drop(f);

        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn test_last_valid_offset_full_prefix_partial_payload_returns_offset_before_prefix() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        drop(f);

        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn test_truncate_to_torn_tail_shrinks_file() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        drop(f);
        assert!(fs::metadata(&path).unwrap().len() > total);

        truncate_to(&path, total).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), total);

        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn test_last_valid_offset_decodable_corrupt_record_fails_without_skipping() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000001.log");

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut buf = Vec::new();
        encode_into(&mut buf, &sample_event(0)).unwrap();
        let valid1_size = buf.len();
        file.write_all(&buf).unwrap();

        encode_into(&mut buf, &sample_event(1)).unwrap();
        let valid2_size = buf.len();
        file.write_all(&buf).unwrap();

        let valid_offset_before_corrupt = (valid1_size + valid2_size) as u64;

        // Write a corrupt record (valid frame but invalid payload):
        // A valid 4-byte length prefix followed by garbage data that won't
        // deserialize as a WalEvent
        let corrupt_len: u32 = 20;
        file.write_all(&corrupt_len.to_le_bytes()).unwrap();
        file.write_all(&[0xFF; 20]).unwrap(); // Garbage that bincode cannot deserialize
        drop(file);

        file = OpenOptions::new().append(true).open(&path).unwrap();
        encode_into(&mut buf, &sample_event(2)).unwrap();
        let valid3_size = buf.len();
        file.write_all(&buf).unwrap();

        encode_into(&mut buf, &sample_event(3)).unwrap();
        let valid4_size = buf.len();
        file.write_all(&buf).unwrap();
        drop(file);

        let final_valid_offset = valid_offset_before_corrupt
            + 4
            + corrupt_len as u64
            + valid3_size as u64
            + valid4_size as u64;

        assert!(
            last_valid_offset(&path).is_err(),
            "recovery must stop at a complete corrupt record"
        );
        assert_eq!(fs::metadata(path).unwrap().len(), final_valid_offset);
    }
}
