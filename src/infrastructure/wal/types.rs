use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::model::messages::message::HandledMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct WalOffset {
    pub segment_id: u64,
    pub byte_offset: u64,
}

impl WalOffset {
    pub fn to_bytes(self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.segment_id.to_be_bytes());
        buf[8..].copy_from_slice(&self.byte_offset.to_be_bytes());
        buf
    }

    pub fn from_bytes(buf: [u8; 16]) -> Self {
        let segment_id = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let byte_offset = u64::from_be_bytes(buf[8..].try_into().unwrap());
        WalOffset {
            segment_id,
            byte_offset,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEvent {
    pub topic: String,
    pub ts_ms: i64,
    pub message: HandledMessage,
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Byte offset of this record's *start*. Production code never reads it —
    /// the forwarder commits using `offset_after` — hence `#[allow(dead_code)]`.
    /// It is retained because the WAL test suite asserts record-start framing
    /// and monotonicity (e.g. the first record starts at byte 0, and successive
    /// starts strictly increase), invariants that `offset_after` cannot express.
    #[allow(dead_code)]
    pub offset: WalOffset,
    pub offset_after: WalOffset, // offset of "next" record's start (i.e. this record's end)
    pub event: WalEvent,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub dir: PathBuf,
    pub segment_bytes: u64,    // rotation threshold for segment files
    pub queue_capacity: usize, // bounded mpsc into the writer
}

/// Reason a non-blocking append was rejected. Each variant carries the rejected
/// event so callers can recover it (mirrors `tokio`'s `TrySendError`); the
/// pipeline currently routes to DLQ and drops the payload.
#[derive(Debug)]
pub enum TryAppendError {
    #[allow(dead_code)]
    Full(WalEvent),
    #[allow(dead_code)]
    Closed(WalEvent),
}
