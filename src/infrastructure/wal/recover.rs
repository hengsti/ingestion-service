use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result};

use crate::infrastructure::wal::codec;

/// Scans `path` and returns the byte offset of the end of the last **complete**
/// record, i.e. the length the active segment must be truncated to so that the
/// next append lands immediately after the last durable record.
///
/// A crash can leave a partially written record at EOF (a torn length prefix or
/// a short payload). Resuming the writer at `metadata().len()` would place the
/// next record *after* those torn bytes, permanently mis-framing the reader.
/// This scan finds the safe resume point instead.
///
/// Decoding stops on the first of:
/// - `Ok(None)` — clean EOF or a torn tail (short read).
/// - `Err(_)` — a record whose framed payload fails to decode; the remainder is
///   treated as torn and discarded.
///
/// Returns `0` for a missing or empty file.
///
/// # Errors
/// Returns an error if the segment exists but cannot be opened or read.
pub fn last_valid_offset(path: &Path) -> Result<u64> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("opening WAL segment {}", path.display())),
    };

    let mut reader = BufReader::new(file);
    let mut valid_len: u64 = 0;

    loop {
        match codec::decode_from(&mut reader) {
            Ok(Some((_, consumed))) => valid_len += consumed as u64,
            Ok(None) => break,
            Err(_) => break,
        }
    }

    Ok(valid_len)
}

/// Physically truncates `path` to `len` bytes.
///
/// No fsync is issued — this matches the WAL's page-cache durability contract.
/// Callers should only invoke this when `len` is strictly less than the current
/// file length (a torn tail was detected).
///
/// # Errors
/// Returns an error if the segment cannot be opened or resized.
pub fn truncate_to(path: &Path, len: u64) -> Result<()> {
    OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("opening WAL segment for truncation {}", path.display()))?
        .set_len(len)
        .with_context(|| format!("truncating WAL segment {} to {len} bytes", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::encode_into;
    use crate::infrastructure::wal::types::WalEvent;
    use crate::model::messages::message::HandledMessage;
    use crate::model::messages::status::StatusMessage;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    fn sample_event(seq: u64) -> WalEvent {
        WalEvent {
            topic: format!("smarthome/dev-{seq}/status"),
            ts_ms: 1_700_000_000_000 + seq as i64,
            message: HandledMessage::Status(StatusMessage {
                device_id: format!("dev-{seq}"),
                device_class: String::from("test"),
                fw_version: String::from("1.0.0"),
                ip: String::from("10.0.0.1"),
                rssi: -50,
                time_ms: 0,
                time_iso: String::from("2024-01-01T00:00:00Z"),
                time_valid: true,
                uptime: 1,
                free_mem: 1,
                ssid: String::from("ssid"),
            }),
        }
    }

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
    fn last_valid_offset_missing_file_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.log");
        assert_eq!(last_valid_offset(&path).unwrap(), 0);
    }

    #[test]
    fn last_valid_offset_empty_file_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000001.log");
        fs::File::create(&path).unwrap();
        assert_eq!(last_valid_offset(&path).unwrap(), 0);
    }

    #[test]
    fn last_valid_offset_clean_file_returns_full_length() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 5);
        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn last_valid_offset_torn_length_prefix_returns_offset_before_prefix() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        // Append a partial (2-byte) length prefix.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8, 0u8]).unwrap();
        drop(f);

        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn last_valid_offset_full_prefix_partial_payload_returns_offset_before_prefix() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        // Append a complete length prefix promising 999 bytes but no payload.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        drop(f);

        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }

    #[test]
    fn truncate_to_shrinks_file() {
        let dir = tempdir().unwrap();
        let (path, total) = write_records(dir.path(), 3);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        drop(f);
        assert!(fs::metadata(&path).unwrap().len() > total);

        truncate_to(&path, total).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), total);

        // The truncated file decodes cleanly to exactly the valid length.
        assert_eq!(last_valid_offset(&path).unwrap(), total);
    }
}
