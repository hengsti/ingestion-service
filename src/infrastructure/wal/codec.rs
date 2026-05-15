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

pub fn decode_from<R: Read>(r: &mut R) -> Result<Option<WalEvent>> {
    let mut len_buf = [0u8; 4];

    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None), // No more events to read
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    match r.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None), // Incomplete event, treat as end of stream
        Err(e) => return Err(e.into()),
    }
    let event: WalEvent = bincode::deserialize(&payload)?;
    Ok(Some(event))
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
        let decoded_event = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.message, decoded_event.message);
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

        let mut cursor = std::io::Cursor::new(buf);
        let decoded_event = decode_from(&mut cursor).unwrap().unwrap();
        assert_eq!(event.ts_ms, decoded_event.ts_ms);
        assert_eq!(event.topic, decoded_event.topic);
        assert_eq!(event.message, decoded_event.message);
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
