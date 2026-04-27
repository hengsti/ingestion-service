use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::model::messages::message::HandledMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct WalOffset {
    pub segment_id: u64,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEvent {
    pub topic: String,
    pub ts_ms: i64,
    pub message: HandledMessage,
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub offset: WalOffset, // offset of "this" record's start
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
