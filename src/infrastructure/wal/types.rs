use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::model::messages::message::HandledMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct WalOffset {
    pub segment_id: u64,
    pub byte_offset: u64,
}

impl WalOffset {
    pub fn to_bytes(&self) -> [u8; 16] {
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
    pub offset: WalOffset,       // offset of "this" record's start
    pub offset_after: WalOffset, // offset of "next" record's start (i.e. this record's end)
    pub event: WalEvent,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub dir: PathBuf,
    pub segment_bytes: u64,    // rotation threshold for segment files
    pub queue_capacity: usize, // bounded mpsc into the writer
}

#[derive(Debug)]
pub enum TryAppendError {
    Full(WalEvent),
    Closed(WalEvent),
}
