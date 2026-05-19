use anyhow::{Context, Result};
use std::io::Read;

use crate::infrastructure::wal::types::WalEvent;

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
/// the length prefix or the payload). Both are non-fatal: the writer will
/// overwrite a torn tail on the next append.
pub fn decode_from<R: Read>(r: &mut R) -> Result<Option<(WalEvent, usize)>> {
    let mut len_buf = [0u8; 4];

    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    match r.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let event: WalEvent = bincode::deserialize(&payload)?;
    Ok(Some((event, 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::types::WalEvent;
    use crate::model::messages::message::HandledMessage;
    use crate::model::messages::status::StatusMessage;

    fn create_status_message() -> StatusMessage {
        StatusMessage {
            device_id: String::from("device-123"),
            device_class: String::from("temperature"),
            fw_version: String::from("1.0.0"),
            ip: String::from("192.168.1.1"),
            rssi: -42,
            time_ms: 123456789,
            time_iso: String::from("2024-06-01T12:34:56Z"),
            time_valid: true,
            uptime: 3600,
            free_mem: 1024,
            ssid: String::from("MyWiFi"),
        }
    }

    #[test]
    fn test_encode() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            message: HandledMessage::Status(create_status_message()),
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
            message: HandledMessage::Status(create_status_message()),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.message, decoded_event.message);
        assert_eq!(consumed, cursor.position() as usize);
    }

    #[test]
    fn test_encode_decode() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            message: HandledMessage::Status(create_status_message()),
        };
        let mut buf = Vec::new();
        encode_into(&mut buf, &event).unwrap();
        let encoded_len = buf.len();

        let mut cursor = std::io::Cursor::new(buf);
        let (decoded_event, consumed) = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.message, decoded_event.message);
        assert_eq!(consumed, encoded_len);
    }

    #[test]
    fn test_empty_reader() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let result = decode_from(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_truncated_event() {
        let event = WalEvent {
            topic: String::from("test-topic"),
            ts_ms: 123456789,
            message: HandledMessage::Status(create_status_message()),
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
}
