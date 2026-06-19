use anyhow::{anyhow, Context, Result};
use std::io::Read;

use crate::infrastructure::wal::types::WalEvent;

const MAX_WAL_RECORD_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Outcome of attempting to decode a WAL record.
pub enum DecodeOutcome {
    /// Valid record decoded successfully.
    ValidRecord(#[allow(dead_code)] WalEvent, usize),
    /// Clean EOF or torn tail (incomplete frame).
    CleanEof,
    /// Frame was complete but payload deserialization failed (decodable-corrupt).
    /// Includes the number of bytes consumed so recovery can skip this record.
    FrameCorrupt(usize),
}

pub fn encode_into(buf: &mut Vec<u8>, event: &WalEvent) -> Result<()> {
    buf.clear();
    buf.extend_from_slice(&[0u8; 4]); // Placeholder for the length of the encoded event

    bincode::serialize_into(&mut *buf, event)?;

    let payload_len = u32::try_from(buf.len() - 4).context("WAL record exceeds u32::MAX bytes")?;
    buf[0..4].copy_from_slice(&payload_len.to_le_bytes());

    Ok(())
}

/// Decodes one `[u32 LE len][payload]` framed record from `r`.
///
/// On success returns `Some((event, bytes_consumed))`, where `bytes_consumed`
/// is `4 + payload_len` — the exact byte width of the record on disk. The
/// caller uses it to advance its read cursor without re-measuring the payload.
///
/// Returns `Ok(None)` on a clean EOF *or* a torn tail (short read of either
/// the length prefix or the payload). Both are non-fatal: a torn tail is
/// truncated during recovery (`recover::truncate_to`) before the writer reopens.
///
/// Convenience wrapper over [`decode_into`] that allocates a fresh payload
/// buffer per call — used by recovery and tests. The hot read path uses
/// [`decode_into`] with a reused buffer instead.
#[allow(dead_code)]
pub fn decode_from<R: Read>(r: &mut R) -> Result<Option<(WalEvent, usize)>> {
    let mut payload = Vec::new();
    decode_into(r, &mut payload)
}

/// Decodes one framed record from `r`, reusing `payload` as the scratch buffer
/// for the record body instead of allocating a fresh `Vec` per record.
///
/// Semantics match [`decode_from`]; the only difference is that the caller owns
/// and reuses `payload` across calls, removing a per-record allocation on the
/// subscription's hot read path. `payload` is left holding the decoded record's
/// bytes (and retains its capacity) on success.
pub fn decode_into<R: Read>(r: &mut R, payload: &mut Vec<u8>) -> Result<Option<(WalEvent, usize)>> {
    let mut len_buf = [0u8; 4];

    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    ensure_len_within_limit(len)?;

    payload.clear();
    payload.resize(len, 0);
    match r.read_exact(payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let event: WalEvent = bincode::deserialize(payload)?;
    Ok(Some((event, 4 + len)))
}

/// Decodes one record for recovery purposes, distinguishing between frame errors
/// and deserialization errors.
///
/// - Returns `DecodeOutcome::ValidRecord` on successful deserialization.
/// - Returns `DecodeOutcome::CleanEof` on I/O EOF or torn frame.
/// - Returns `DecodeOutcome::FrameCorrupt` if frame was complete but payload
///   deserialization failed; includes the bytes consumed so recovery can skip
///   this record and continue scanning.
pub fn decode_for_recovery<R: Read>(r: &mut R, payload: &mut Vec<u8>) -> Result<DecodeOutcome> {
    let mut len_buf = [0u8; 4];

    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(DecodeOutcome::CleanEof)
        }
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if ensure_len_within_limit(len).is_err() {
        let consumed = 4 + len;
        return if discard_bytes(r, len)? {
            Ok(DecodeOutcome::FrameCorrupt(consumed))
        } else {
            Ok(DecodeOutcome::CleanEof)
        };
    }

    payload.clear();
    payload.resize(len, 0);
    match r.read_exact(payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(DecodeOutcome::CleanEof)
        }
        Err(e) => return Err(e.into()),
    }

    // Frame is complete; try to deserialize. If deserialization fails, report it
    // as a frame-corrupt record (not as an I/O error), so recovery can skip it.
    match bincode::deserialize::<WalEvent>(payload) {
        Ok(event) => Ok(DecodeOutcome::ValidRecord(event, 4 + len)),
        Err(_) => Ok(DecodeOutcome::FrameCorrupt(4 + len)),
    }
}

fn ensure_len_within_limit(len: usize) -> Result<()> {
    if len > MAX_WAL_RECORD_PAYLOAD_BYTES {
        return Err(anyhow!(
            "WAL record length {len} exceeds max {MAX_WAL_RECORD_PAYLOAD_BYTES} bytes"
        ));
    }

    Ok(())
}

fn discard_bytes<R: Read>(r: &mut R, mut remaining: usize) -> Result<bool> {
    let mut scratch = [0u8; 8192];

    while remaining > 0 {
        let to_read = remaining.min(scratch.len());
        match r.read_exact(&mut scratch[..to_read]) {
            Ok(()) => remaining -= to_read,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::types::WalEvent;

    #[test]
    fn test_encode() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            line_protocol: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_decode() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            line_protocol: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.line_protocol, decoded_event.line_protocol);
        assert_eq!(consumed, cursor.position() as usize);
    }

    #[test]
    fn test_encode_decode() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            line_protocol: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();
        let encoded_len = buf.len();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.line_protocol, decoded_event.line_protocol);
        assert_eq!(consumed, encoded_len);
    }

    #[test]
    fn test_empty_reader() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let result = decode_from(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_into_reuses_buffer_across_records() {
        let event_a = WalEvent {
            topic: String::from("topic-a"),
            ts_ms: 1,
            line_protocol: String::from("device_status,device_id=device-a rssi=-42i 1"),
        };
        let event_b = WalEvent {
            topic: String::from("topic-b-longer-than-a"),
            ts_ms: 2,
            line_protocol: String::from(
                "device_status,device_id=device-b-longer-than-a rssi=-42i 2",
            ),
        };

        let mut wire = Vec::new();
        let mut enc = Vec::new();
        encode_into(&mut enc, &event_a).unwrap();
        wire.extend_from_slice(&enc);
        encode_into(&mut enc, &event_b).unwrap();
        wire.extend_from_slice(&enc);

        let mut cursor = std::io::Cursor::new(wire);
        let mut payload = Vec::new();

        let (a, _) = decode_into(&mut cursor, &mut payload).unwrap().unwrap();
        assert_eq!(a.topic, event_a.topic);
        let cap_after_first = payload.capacity();

        let (b, _) = decode_into(&mut cursor, &mut payload).unwrap().unwrap();
        assert_eq!(b.topic, event_b.topic);

        // The same buffer was reused; decoding a larger second record may grow
        // it but must never shrink it, and the first decode allocated capacity.
        assert!(cap_after_first > 0);
        assert!(payload.capacity() >= cap_after_first);

        assert!(decode_into(&mut cursor, &mut payload).unwrap().is_none());
    }

    #[test]
    fn test_truncated_event() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            line_protocol: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();

        // Truncate the buffer to simulate an incomplete event
        let truncated_len = buf.len() - 5; // Remove last 5 bytes
        let truncated_buf = &buf[..truncated_len];
        let mut cursor = std::io::Cursor::new(truncated_buf.to_vec());

        let result = decode_from(&mut cursor).unwrap();
        assert!(result.is_none()); // Should return None due to incomplete event
    }

    #[test]
    fn decode_into_oversized_length_prefix_returns_error_without_growing_payload_buffer() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&2_000_000u32.to_le_bytes());

        let mut cursor = std::io::Cursor::new(wire);
        let mut payload = Vec::with_capacity(16);
        let cap_before = payload.capacity();

        let err =
            decode_into(&mut cursor, &mut payload).expect_err("oversized frame must be rejected");
        assert!(
            err.to_string().contains("WAL record length"),
            "unexpected error: {err:#}"
        );
        assert_eq!(payload.capacity(), cap_before);
    }

    #[test]
    fn decode_for_recovery_oversized_length_prefix_keeps_small_payload_buffer() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&2_000_000u32.to_le_bytes());

        let mut cursor = std::io::Cursor::new(wire);
        let mut payload = Vec::with_capacity(16);
        let cap_before = payload.capacity();

        let outcome = decode_for_recovery(&mut cursor, &mut payload).unwrap();
        assert!(matches!(outcome, DecodeOutcome::CleanEof));
        assert_eq!(payload.capacity(), cap_before);
    }
}
