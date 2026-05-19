use std::fs;

use anyhow::{Context, Result};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::infrastructure::wal::{
    cursor::read_cursor,
    segment::{list_segments, segment_path},
    subscription::WalSubscription,
    types::{TryAppendError, WalEvent, WalOffset, WalOptions},
    writer::spawn_writer,
};

pub struct Wal {
    tx: mpsc::Sender<WalEvent>,
}

impl Wal {
    pub async fn open(options: WalOptions) -> Result<(Self, WalSubscription)> {
        fs::create_dir_all(&options.dir)
            .with_context(|| format!("creating WAL dir {}", options.dir.display()))?;

        let segments = list_segments(&options.dir)
            .with_context(|| format!("listing WAL segments in {}", options.dir.display()))?;

        let (active_id, active_len) = match segments.last().copied() {
            None => (1u64, 0u64),
            Some(id) => {
                let len = fs::metadata(segment_path(&options.dir, id))
                    .with_context(|| format!("stat WAL segment {id}"))?
                    .len();
                (id, len)
            }
        };

        let read_start = match read_cursor(&options.dir)? {
            Some(off) => off,
            None => WalOffset {
                segment_id: segments.first().copied().unwrap_or(active_id),
                byte_offset: 0,
            },
        };

        let handle = spawn_writer(
            options.dir.clone(),
            active_id,
            active_len,
            options.segment_bytes,
            options.queue_capacity,
        )?;

        let subscription = WalSubscription::new(
            options.dir,
            handle.head.clone(),
            handle.notify.clone(),
            read_start,
        );
        Ok((Self { tx: handle.tx }, subscription))
    }

    pub fn try_append(&self, event: WalEvent) -> Result<(), TryAppendError> {
        self.tx.try_send(event).map_err(|err| match err {
            TrySendError::Full(ev) => TryAppendError::Full(ev),
            TrySendError::Closed(ev) => TryAppendError::Closed(ev),
        })
    }
}
