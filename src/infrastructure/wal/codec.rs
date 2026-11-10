use anyhow::{anyhow, Context, Result};
use std::io::Read;

use crate::infrastructure::wal::types::WalEvent;

pub(crate) const MAX_WAL_RECORD_PAYLOAD_BYTES: usize = 1024 * 1024;
const WAL_EVENT_FIXED_BYTES: usize = 24;
const WAL_FRAME_HEADER_BYTES: usize = 4;
// Old readers reject this as an oversized frame, preventing unsafe rollback replay.
const TARGETED_FRAME: u32 = 1 << 31;

pub(crate) fn wal_record_frame_size(topic_bytes: usize, payload_bytes: usize) -> Result<usize> {
    wal_record_frame_size_with_target(topic_bytes, payload_bytes, None)
}

pub(crate) fn wal_record_frame_size_with_target(
    topic_bytes: usize,
    payload_bytes: usize,
    target: Option<&str>,
) -> Result<usize> {
    let body_bytes = WAL_EVENT_FIXED_BYTES
        .checked_add(topic_bytes)
        .and_then(|size| size.checked_add(payload_bytes))
        .and_then(|size| size.checked_add(target.map_or(0, |value| 8 + value.len())))
        .context("WAL record size estimate overflowed")?;
    if body_bytes > MAX_WAL_RECORD_PAYLOAD_BYTES {
        return Err(anyhow!(
            "WAL record body size {body_bytes} exceeds max {MAX_WAL_RECORD_PAYLOAD_BYTES} bytes"
        ));
    }
    body_bytes
        .checked_add(WAL_FRAME_HEADER_BYTES)
        .context("WAL frame size estimate overflowed")
}

/// Outcome of attempting to decode a WAL record.
pub enum DecodeOutcome {
    /// Valid record decoded successfully.
    ValidRecord(#[allow(dead_code)] WalEvent, usize),
    /// Clean EOF or torn tail (incomplete frame).
    CleanEof,
    /// Frame was complete but payload deserialization failed (decodable-corrupt).
    FrameCorrupt,
}

pub fn encode_into(buf: &mut Vec<u8>, event: &WalEvent) -> Result<()> {
    wal_record_frame_size_with_target(
        event.topic.len(),
        event.payload.len(),
        event.sink_target.as_deref(),
    )?;
    buf.clear();
    buf.extend_from_slice(&[0u8; 4]); // Placeholder for the length of the encoded event

    let legacy = (&event.topic, event.ts_ms, &event.payload);
    if let Some(target) = &event.sink_target {
        bincode::serialize_into(&mut *buf, &(legacy.0, legacy.1, legacy.2, target))?;
    } else {
        bincode::serialize_into(&mut *buf, &legacy)?;
    }

    let payload_len = u32::try_from(buf.len() - 4).context("WAL record exceeds u32::MAX bytes")?;
    let prefix = payload_len
        | if event.sink_target.is_some() {
            TARGETED_FRAME
        } else {
            0
        };
    buf[0..4].copy_from_slice(&prefix.to_le_bytes());

    Ok(())
}

/// Decodes one `[u32 LE len][payload]` framed record from `r`.
///
/// On success returns `Some((event, bytes_consumed))`, where `bytes_consumed`
/// is `4 + payload_len` — the exact byte width of the record on disk. The
/// caller uses it to advance its read cursor without re-measuring the payload.
///
/// Returns `Ok(None)` on a clean EOF. A short frame is an error; only startup
/// recovery treats an incomplete final frame as a repairable tail.
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

    if !read_prefix_or_eof(r, &mut len_buf)? {
        return Ok(None);
    }
    let prefix = u32::from_le_bytes(len_buf);
    let len = (prefix & !TARGETED_FRAME) as usize;
    ensure_len_within_limit(len)?;

    payload.clear();
    payload.resize(len, 0);
    match r.read_exact(payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(e).context("truncated WAL record payload")
        }
        Err(e) => return Err(e.into()),
    }
    let event = deserialize_event(payload, prefix & TARGETED_FRAME != 0)?;
    Ok(Some((event, 4 + len)))
}

fn read_prefix_or_eof<R: Read>(r: &mut R, prefix: &mut [u8; 4]) -> Result<bool> {
    let mut read = 0;
    while read < prefix.len() {
        match r.read(&mut prefix[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => anyhow::bail!("truncated WAL record length prefix"),
            Ok(bytes) => read += bytes,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }

    Ok(true)
}

/// Decodes one record for recovery purposes, distinguishing between frame errors
/// and deserialization errors.
///
/// - Returns `DecodeOutcome::ValidRecord` on successful deserialization.
/// - Returns `DecodeOutcome::CleanEof` on I/O EOF or torn frame.
/// - Returns `DecodeOutcome::FrameCorrupt` if a complete frame's payload fails
///   deserialization; recovery must preserve and report this corruption.
pub fn decode_for_recovery<R: Read>(r: &mut R, payload: &mut Vec<u8>) -> Result<DecodeOutcome> {
    let mut len_buf = [0u8; 4];

    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(DecodeOutcome::CleanEof)
        }
        Err(e) => return Err(e.into()),
    }
    let prefix = u32::from_le_bytes(len_buf);
    let len = (prefix & !TARGETED_FRAME) as usize;
    ensure_len_within_limit(len)?;

    payload.clear();
    payload.resize(len, 0);
    match r.read_exact(payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(DecodeOutcome::CleanEof)
        }
        Err(e) => return Err(e.into()),
    }

    // Frame is complete; try to deserialize and preserve any corruption for recovery.
    match deserialize_event(payload, prefix & TARGETED_FRAME != 0) {
        Ok(event) => Ok(DecodeOutcome::ValidRecord(event, 4 + len)),
        Err(_) => Ok(DecodeOutcome::FrameCorrupt),
    }
}

fn deserialize_event(payload: &[u8], targeted: bool) -> Result<WalEvent> {
    use bincode::Options;
    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_WAL_RECORD_PAYLOAD_BYTES as u64)
        .reject_trailing_bytes();
    let (topic, ts_ms, payload, sink_target) = if targeted {
        let (topic, ts_ms, payload, target): (String, i64, String, String) =
            options.deserialize(payload)?;
        crate::config::routes::validate_sink_target(&target)?;
        (topic, ts_ms, payload, Some(target))
    } else {
        let (topic, ts_ms, payload) = options.deserialize(payload)?;
        (topic, ts_ms, payload, None)
    };
    Ok(WalEvent {
        topic,
        ts_ms,
        payload,
        sink_target,
    })
}

fn ensure_len_within_limit(len: usize) -> Result<()> {
    if len > MAX_WAL_RECORD_PAYLOAD_BYTES {
        return Err(anyhow!(
            "WAL record length {len} exceeds max {MAX_WAL_RECORD_PAYLOAD_BYTES} bytes"
        ));
    }

    Ok(())
}

#[allow(dead_code)]
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
    fn targeted_frame_round_trips_and_counts_destination_bytes() -> Result<()> {
        let mut event = crate::infrastructure::wal::test_support::sample_event(0);
        event.sink_target = Some("synthetic-bucket".to_owned());
        let mut frame = Vec::new();
        encode_into(&mut frame, &event)?;
        assert_eq!(
            frame.len(),
            wal_record_frame_size_with_target(
                event.topic.len(),
                event.payload.len(),
                event.sink_target.as_deref()
            )?
        );
        let prefix = u32::from_le_bytes(frame[..4].try_into()?);
        assert_ne!(prefix & TARGETED_FRAME, 0);
        assert!(
            prefix as usize > MAX_WAL_RECORD_PAYLOAD_BYTES,
            "old readers must refuse targeted frames"
        );
        let (decoded, consumed) =
            decode_from(&mut std::io::Cursor::new(&frame))?.context("decoded frame")?;
        assert_eq!(consumed, frame.len());
        assert_eq!(decoded.sink_target, event.sink_target);
        assert_eq!(decoded.payload, event.payload);
        event.payload = "x".repeat(
            MAX_WAL_RECORD_PAYLOAD_BYTES
                - WAL_EVENT_FIXED_BYTES
                - event.topic.len()
                - 8
                - event.sink_target.as_ref().context("target")?.len(),
        );
        encode_into(&mut frame, &event)?;
        assert_eq!(frame.len(), MAX_WAL_RECORD_PAYLOAD_BYTES + 4);
        event.payload.push('x');
        assert!(encode_into(&mut frame, &event).is_err());
        assert!(wal_record_frame_size_with_target(usize::MAX, 1, Some("target")).is_err());
        Ok(())
    }

    #[test]
    fn legacy_frame_bytes_remain_compatible_for_quarantine() -> Result<()> {
        let event = crate::infrastructure::wal::test_support::sample_event(0);
        let body = bincode::serialize(&(&event.topic, event.ts_ms, &event.payload))?;
        let mut expected = (body.len() as u32).to_le_bytes().to_vec();
        expected.extend_from_slice(&body);
        let mut frame = Vec::new();
        encode_into(&mut frame, &event)?;
        assert_eq!(frame, expected);
        let (decoded, _) =
            decode_from(&mut std::io::Cursor::new(&frame))?.context("legacy frame")?;
        assert!(decoded.sink_target.is_none());
        Ok(())
    }

    #[test]
    fn wal_record_size_estimate_matches_bincode_frame() -> Result<()> {
        let event = WalEvent {
            sink_target: None,
            topic: "topic".to_owned(),
            ts_ms: 123,
            payload: "line".to_owned(),
        };
        let expected = bincode::serialized_size(&(&event.topic, event.ts_ms, &event.payload))?
            as usize
            + WAL_FRAME_HEADER_BYTES;

        assert_eq!(
            wal_record_frame_size(event.topic.len(), event.payload.len())?,
            expected
        );
        Ok(())
    }

    #[test]
    fn test_wal_sync_codec_enforces_body_limit_with_framing_overhead() -> Result<()> {
        let mut event = WalEvent {
            sink_target: None,
            topic: "t".to_owned(),
            ts_ms: 1,
            payload: "x".repeat(MAX_WAL_RECORD_PAYLOAD_BYTES - WAL_EVENT_FIXED_BYTES - 1),
        };
        let mut frame = Vec::new();

        encode_into(&mut frame, &event)?;
        assert_eq!(
            frame.len(),
            MAX_WAL_RECORD_PAYLOAD_BYTES + WAL_FRAME_HEADER_BYTES
        );
        assert!(decode_from(&mut std::io::Cursor::new(&frame))?.is_some());

        event.payload.push('x');
        assert!(encode_into(&mut frame, &event).is_err());
        Ok(())
    }

    #[test]
    fn test_encode() {
        let event = WalEvent {
            sink_target: None,
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            payload: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_decode() {
        let event = WalEvent {
            sink_target: None,
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            payload: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.payload, decoded_event.payload);
        assert_eq!(consumed, cursor.position() as usize);
    }

    #[test]
    fn test_encode_decode() {
        let event = WalEvent {
            sink_target: None,
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            payload: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();
        let encoded_len = buf.len();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.payload, decoded_event.payload);
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
            sink_target: None,
            topic: String::from("topic-a"),
            ts_ms: 1,
            payload: String::from("device_status,device_id=device-a rssi=-42i 1"),
        };
        let event_b = WalEvent {
            sink_target: None,
            topic: String::from("topic-b-longer-than-a"),
            ts_ms: 2,
            payload: String::from("device_status,device_id=device-b-longer-than-a rssi=-42i 2"),
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
    fn test_truncated_event_returns_error() {
        let event = WalEvent {
            sink_target: None,
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            payload: String::from("device_status,device_id=device-123 rssi=-42i 123456789"),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();

        // Truncate the buffer to simulate an incomplete event
        let truncated_len = buf.len() - 5; // Remove last 5 bytes
        let truncated_buf = &buf[..truncated_len];
        let mut cursor = std::io::Cursor::new(truncated_buf.to_vec());

        let result = decode_from(&mut cursor);
        assert!(result.is_err(), "a truncated frame is not clean EOF");
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
    fn decode_for_recovery_oversized_length_prefix_fails_without_growing_payload_buffer() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&2_000_000u32.to_le_bytes());

        let mut cursor = std::io::Cursor::new(wire);
        let mut payload = Vec::with_capacity(16);
        let cap_before = payload.capacity();

        let error = match decode_for_recovery(&mut cursor, &mut payload) {
            Err(error) => error,
            Ok(_) => panic!("an invalid frame length must not be treated as a torn tail"),
        };
        assert!(error.to_string().contains("WAL record length"));
        assert_eq!(payload.capacity(), cap_before);
    }
}
