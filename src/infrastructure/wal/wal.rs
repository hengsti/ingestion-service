use std::fs;
use std::sync::{
    mpsc::{SyncSender, TrySendError},
    Arc,
};

use anyhow::{Context, Result};
use tokio::sync::Notify;
use tracing::warn;

use crate::infrastructure::wal::{
    capacity::{self, Capacity, SEGMENT_ALLOWANCE_BYTES},
    codec::wal_record_frame_size,
    cursor::read_cursor,
    journal::Journal,
    recover::{last_valid_offset, truncate_to, validate_committed_offset},
    segment::{list_segments, segment_path, sync_directory},
    subscription::WalSubscription,
    types::{AppendDurableError, TryAppendError, WalEvent, WalOffset, WalOptions},
    writer::{spawn_writer, WriteRequest, WriterState},
};

pub struct Wal {
    tx: SyncSender<WriteRequest>,
    state: Arc<WriterState>,
    capacity: Arc<Capacity>,
    writer_notify: Arc<Notify>,
    #[cfg(test)]
    pub(crate) test_io_hook: crate::infrastructure::wal::test_support::TestIoHookSlot,
}

impl Wal {
    pub async fn open(options: WalOptions) -> Result<(Self, WalSubscription)> {
        tokio::task::spawn_blocking(move || Self::open_blocking(options))
            .await
            .context("WAL initialization task failed")?
    }

    fn open_blocking(options: WalOptions) -> Result<(Self, WalSubscription)> {
        if options.queue_capacity == 0 {
            anyhow::bail!("WAL_QUEUE_CAPACITY must be greater than zero");
        }
        capacity::validate_limit("WAL_MAX_BYTES", options.max_bytes, options.segment_bytes)?;
        let journal = Arc::new(Journal::open(options.dir.clone())?);

        let segments = list_segments(&options.dir)
            .with_context(|| format!("listing WAL segments in {}", options.dir.display()))?;

        let committed = read_cursor(&options.dir)?;
        if let Some(off) = committed {
            if segments.binary_search(&off.segment_id).is_err() {
                return Err(anyhow::anyhow!(
                    "WAL cursor points to a missing segment: cursor={off:?}, segments={segments:?}"
                ));
            }
            validate_committed_offset(
                &segment_path(&options.dir, off.segment_id),
                off.byte_offset,
            )?;
        }

        for &id in segments.iter().take(segments.len().saturating_sub(1)) {
            if committed.is_some_and(|offset| id < offset.segment_id) {
                continue;
            }
            let path = segment_path(&options.dir, id);
            let file_len = fs::metadata(&path)
                .with_context(|| format!("stat sealed WAL segment {id}"))?
                .len();
            let valid_len = last_valid_offset(&path)?;
            if valid_len != file_len {
                anyhow::bail!(
                    "sealed WAL segment {id} has an incomplete record at offset {valid_len}"
                );
            }
            let file = fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .with_context(|| format!("opening recovered WAL segment {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("syncing recovered WAL segment {}", path.display()))?;
        }

        let (active_id, active_len) = match segments.last().copied() {
            None => (1u64, 0u64),
            Some(id) => {
                let path = segment_path(&options.dir, id);
                let file_len = fs::metadata(&path)
                    .with_context(|| format!("stat WAL segment {id}"))?
                    .len();
                let valid = last_valid_offset(&path)?;
                if valid < file_len {
                    truncate_to(&path, valid)
                        .with_context(|| format!("truncating torn WAL segment {id}"))?;
                    warn!(
                        segment_id = id,
                        reclaimed_bytes = file_len - valid,
                        "healed torn WAL tail on recovery"
                    );
                }
                (id, valid)
            }
        };
        let read_start = committed.unwrap_or(WalOffset {
            segment_id: segments.first().copied().unwrap_or(active_id),
            byte_offset: 0,
        });

        let mut removed = false;
        for id in segments
            .iter()
            .copied()
            .take_while(|id| *id < read_start.segment_id)
        {
            let path = segment_path(&options.dir, id);
            fs::remove_file(&path)
                .with_context(|| format!("reclaiming committed WAL segment {}", path.display()))?;
            removed = true;
        }
        if removed {
            sync_directory(&options.dir).with_context(|| {
                format!("syncing reclaimed WAL directory {}", options.dir.display())
            })?;
        }
        let capacity = Capacity::recover(&options.dir, options.max_bytes)?;
        let retained_previous = if active_len == 0 {
            match segments.iter().rev().nth(1).copied() {
                Some(id) if id >= read_start.segment_id => Some(
                    fs::metadata(segment_path(&options.dir, id))
                        .with_context(|| format!("stat retained WAL segment {id}"))?
                        .len(),
                ),
                _ => None,
            }
        } else {
            None
        };
        capacity.validate_recovered_active(active_len, retained_previous)?;

        let handle = spawn_writer(
            Arc::clone(&journal),
            Arc::clone(&capacity),
            active_id,
            active_len,
            options.segment_bytes,
            options.queue_capacity,
        )?;

        let subscription = WalSubscription::new(
            journal,
            Arc::clone(&capacity),
            handle.head.clone(),
            handle.notify.clone(),
            handle.state.clone(),
            read_start,
        );
        Ok((
            Self {
                tx: handle.tx,
                state: handle.state,
                capacity,
                writer_notify: handle.notify,
                #[cfg(test)]
                test_io_hook: handle.test_io_hook,
            },
            subscription,
        ))
    }

    /// Returns the terminal writer failure, if the writer has failed.
    pub fn writer_failure(&self) -> Option<&str> {
        self.state.failure()
    }

    /// Returns accounted and maximum journal bytes without scanning the filesystem.
    pub fn capacity_bytes(&self) -> (u64, u64) {
        self.capacity.snapshot()
    }

    /// Shares the journal's live capacity state with monitoring consumers.
    pub fn capacity_handle(&self) -> Arc<Capacity> {
        Arc::clone(&self.capacity)
    }

    pub(crate) fn capacity_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.capacity.notified()
    }

    /// Subscribes to durable writer progress or termination.
    pub(crate) fn writer_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.writer_notify.notified()
    }

    // `TryAppendError` carries the rejected `WalEvent` (mirrors `TrySendError`) so
    // callers can recover the payload; the error path is cold (queue saturated),
    // so the large-Err size is acceptable over boxing on the hot append path.
    #[allow(dead_code)]
    #[allow(clippy::result_large_err)]
    pub fn try_append(&self, event: WalEvent) -> Result<(), TryAppendError> {
        if self.writer_failure().is_some() || self.state.stopped() {
            return Err(TryAppendError::Closed(event));
        }
        let frame_bytes = match wal_record_frame_size(event.topic.len(), event.payload.len()) {
            Ok(bytes) => bytes as u64,
            Err(_) => return Err(TryAppendError::TooLarge(event)),
        };
        let Some(reservation) = self.capacity.reserve(frame_bytes + SEGMENT_ALLOWANCE_BYTES) else {
            return Err(TryAppendError::Capacity(event));
        };
        let req = WriteRequest {
            event,
            durable_ack: None,
            reservation: Some(reservation),
        };
        self.tx.try_send(req).map_err(|err| match err {
            TrySendError::Full(req) => TryAppendError::Full(req.event),
            TrySendError::Disconnected(req) => TryAppendError::Closed(req.event),
        })
    }

    #[allow(clippy::result_large_err)]
    /// Appends one event and waits for the writer's file-sync boundary.
    ///
    /// The writer flushes and syncs the group before resolving its receipts.
    /// Actual persistence depends on the filesystem and storage honoring sync.
    ///
    /// # Errors
    /// Returns the event if it is too large, journal capacity or the queue is
    /// full, or the writer is closed. Returns a durability error on sync failure.
    pub async fn append_durable(&self, event: WalEvent) -> Result<(), AppendDurableError> {
        if self.writer_failure().is_some() || self.state.stopped() {
            return Err(AppendDurableError::Closed(event));
        }
        let frame_bytes = match wal_record_frame_size(event.topic.len(), event.payload.len()) {
            Ok(bytes) => bytes as u64,
            Err(_) => return Err(AppendDurableError::TooLarge(event)),
        };
        let Some(reservation) = self.capacity.reserve(frame_bytes + SEGMENT_ALLOWANCE_BYTES) else {
            return Err(AppendDurableError::Capacity(event));
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let req = WriteRequest {
            event,
            durable_ack: Some(ack_tx),
            reservation: Some(reservation),
        };

        self.tx.try_send(req).map_err(|err| match err {
            TrySendError::Full(req) => AppendDurableError::Full(req.event),
            TrySendError::Disconnected(req) => AppendDurableError::Closed(req.event),
        })?;

        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason)) => Err(AppendDurableError::Durability(reason)),
            Err(_) => Err(AppendDurableError::Durability(String::from(
                "wal writer exited before durability ack",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::{
        segment::list_segments,
        test_support::sample_event,
        types::{WalEntry, WalOptions},
    };
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::tempdir;

    fn opts(dir: &std::path::Path, segment_bytes: u64, queue_capacity: usize) -> WalOptions {
        WalOptions {
            dir: dir.to_path_buf(),
            max_bytes: 128 * 1024 * 1024,
            segment_bytes,
            queue_capacity,
        }
    }

    async fn recv_one(sub: &mut WalSubscription, ms: u64) -> Option<WalEntry> {
        match tokio::time::timeout(Duration::from_millis(ms), sub.next()).await {
            Ok(Ok(entry)) => entry,
            Ok(Err(error)) => panic!("WAL subscription failed: {error:#}"),
            Err(_) => None,
        }
    }

    fn maximum_event() -> WalEvent {
        WalEvent {
            topic: "t".to_string(),
            ts_ms: 1,
            payload: "x"
                .repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES - 25),
        }
    }

    #[tokio::test]
    async fn test_journal_ownership_rejects_another_writer_until_reader_drops() -> Result<()> {
        let dir = tempdir()?;
        let (wal, sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;

        assert!(
            Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err(),
            "a second writer must not open the same journal"
        );

        let state = wal.state.clone();
        drop(wal);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err(),
            "the reader must retain journal ownership after the writer exits"
        );

        drop(sub);
        let (_wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_rejects_an_append_that_exceeds_budget() -> Result<()> {
        let dir = tempdir()?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;
        let (wal, _sub) = Wal::open(options).await?;
        let event = maximum_event();

        assert!(wal.append_durable(event.clone()).await.is_ok());
        assert!(wal.append_durable(event.clone()).await.is_ok());
        match wal.append_durable(event.clone()).await {
            Err(AppendDurableError::Capacity(rejected)) => {
                assert_eq!(rejected.topic, event.topic);
                assert_eq!(rejected.payload, event.payload);
            }
            other => panic!("expected capacity pressure with the original event, got {other:?}"),
        }
        assert!(wal.capacity_bytes().0 <= wal.capacity_bytes().1);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_rejects_a_zero_writer_queue_before_opening() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("wal");
        let error = match Wal::open(opts(&path, 1024 * 1024, 0)).await {
            Ok(_) => panic!("a zero-slot WAL writer queue must be rejected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("WAL_QUEUE_CAPACITY"));
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_fill_reclaim_and_readmit() -> Result<()> {
        let dir = tempdir()?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;
        let (wal, mut sub) = Wal::open(options).await?;
        let event = maximum_event();
        wal.append_durable(event.clone())
            .await
            .expect("first append");
        wal.append_durable(event.clone())
            .await
            .expect("second append");
        let rejected = match wal.append_durable(event).await {
            Err(AppendDurableError::Capacity(event)) => event,
            other => panic!("expected capacity pressure, got {other:?}"),
        };
        let full = wal.capacity_bytes().0;

        let first = recv_one(&mut sub, 2000).await.expect("first record");
        let second = recv_one(&mut sub, 2000).await.expect("second record");
        sub.commit(first.offset_after).await?;
        sub.commit(second.offset_after).await?;
        assert!(wal.capacity_bytes().0 < full);
        wal.append_durable(rejected)
            .await
            .expect("readmitted event");
        assert!(wal.capacity_bytes().0 <= wal.capacity_bytes().1);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_restart_counts_uncommitted_segments() -> Result<()> {
        let dir = tempdir()?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;
        let (wal, sub) = Wal::open(options.clone()).await?;
        let event = maximum_event();
        wal.append_durable(event.clone())
            .await
            .expect("first append");
        wal.append_durable(event.clone())
            .await
            .expect("second append");
        let state = Arc::clone(&wal.state);
        drop(wal);
        drop(sub);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.stopped() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;

        let (reopened, _sub) = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match Wal::open(options.clone()).await {
                    Ok(opened) => break Ok(opened),
                    Err(error)
                        if error
                            .to_string()
                            .contains("acquiring exclusive journal lock") =>
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(error) => break Err(error),
                }
            }
        })
        .await??;
        assert!(matches!(
            reopened.append_durable(event).await,
            Err(AppendDurableError::Capacity(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_recovered_oversized_active_segment_requires_headroom() -> Result<()>
    {
        let dir = tempdir()?;
        let event = maximum_event();
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &event)?;
        let mut file = fs::File::create(segment_path(dir.path(), 1))?;
        file.write_all(&frame)?;
        file.write_all(&frame)?;
        file.sync_all()?;
        drop(file);
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: (frame.len() * 2) as u64,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;

        match Wal::open(options).await {
            Err(error) => assert!(format!("{error:#}").contains("recovered active segment")),
            Ok(_) => panic!("a retained oversized active segment would prevent rotation"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_recovered_metadata_requires_rotation_headroom() -> Result<()> {
        let dir = tempdir()?;
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &maximum_event())?;
        fs::write(segment_path(dir.path(), 1), frame)?;
        fs::write(dir.path().join(".journal.lock"), vec![0; 512 * 1024])?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = capacity::minimum_capacity(options.segment_bytes)?;

        match Wal::open(options).await {
            Err(error) => assert!(format!("{error:#}").contains("recovered active segment")),
            Ok(_) => panic!("recovered metadata must not consume rotation headroom"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_minimum_supports_rotation_after_restart() -> Result<()> {
        let dir = tempdir()?;
        let event = maximum_event();
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &event)?;
        fs::write(segment_path(dir.path(), 1), &frame)?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: frame.len() as u64,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = capacity::minimum_capacity(options.segment_bytes)?;

        let (wal, _sub) = Wal::open(options).await?;
        assert!(wal.append_durable(event).await.is_ok());
        assert!(wal.capacity_bytes().0 <= wal.capacity_bytes().1);
        Ok(())
    }

    async fn restart_after_interrupted_rotation(trailing_bytes: &[u8]) -> Result<()> {
        let dir = tempdir()?;
        let event = maximum_event();
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &event)?;
        fs::write(segment_path(dir.path(), 1), &frame)?;
        fs::write(segment_path(dir.path(), 2), trailing_bytes)?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: frame.len() as u64,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = capacity::minimum_capacity(options.segment_bytes)?;

        let (wal, mut sub) = Wal::open(options).await?;
        let admitted = wal.append_durable(event.clone()).await;
        assert!(
            admitted.is_ok(),
            "maximum record must be admitted after recovery"
        );
        let entry = recv_one(&mut sub, 2000)
            .await
            .context("readmitted record")?;
        assert_eq!(entry.event.payload, event.payload);
        sub.commit(entry.offset_after).await?;
        assert_eq!(list_segments(dir.path())?, vec![2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_restart_after_empty_rotation_readmits_maximum_record() -> Result<()>
    {
        restart_after_interrupted_rotation(&[]).await
    }

    #[tokio::test]
    async fn test_wal_capacity_restart_after_torn_rotation_readmits_maximum_record() -> Result<()> {
        restart_after_interrupted_rotation(&[32, 0, 0, 0, b'p', b'a', b'r', b't']).await
    }

    #[tokio::test]
    async fn test_wal_capacity_restart_keeps_an_empty_segment_referenced_by_cursor() -> Result<()> {
        let dir = tempdir()?;
        let event = maximum_event();
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &event)?;
        let mut previous = frame.clone();
        previous.extend_from_slice(&frame);
        fs::write(segment_path(dir.path(), 1), previous)?;
        fs::File::create(segment_path(dir.path(), 2))?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 2,
                byte_offset: 0,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = capacity::minimum_capacity(options.segment_bytes)?;

        let (wal, mut sub) = Wal::open(options).await?;
        assert_eq!(list_segments(dir.path())?, vec![2]);
        wal.append_durable(event.clone())
            .await
            .expect("append to the retained segment");
        let entry = recv_one(&mut sub, 2000).await.context("retained record")?;
        assert_eq!(entry.event.payload, event.payload);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_restart_requires_headroom_for_oversized_preceding_segment(
    ) -> Result<()> {
        let dir = tempdir()?;
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &maximum_event())?;
        let mut file = fs::File::create(segment_path(dir.path(), 1))?;
        file.write_all(&frame)?;
        file.write_all(&frame)?;
        file.sync_all()?;
        drop(file);
        fs::File::create(segment_path(dir.path(), 2))?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: (frame.len() * 2) as u64,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;
        let minimum =
            capacity::METADATA_RESERVE_BYTES + 3 * SEGMENT_ALLOWANCE_BYTES + 3 * frame.len() as u64;

        match Wal::open(options).await {
            Err(error) => assert!(format!("{error:#}").contains(&minimum.to_string())),
            Ok(_) => panic!("a retained oversized segment would prevent admission"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_reclaims_committed_segments_before_startup_admission() -> Result<()>
    {
        let dir = tempdir()?;
        let event = maximum_event();
        let mut frame = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut frame, &event)?;
        for id in 1..=3 {
            fs::write(segment_path(dir.path(), id), &frame)?;
        }
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 3,
                byte_offset: 0,
            },
        )?;
        let mut options = opts(dir.path(), 1024 * 1024, 8);
        options.max_bytes = 3 * 1024 * 1024;

        let (wal, _sub) = Wal::open(options).await?;
        assert_eq!(list_segments(dir.path())?, vec![3]);
        assert!(wal.append_durable(event).await.is_ok());
        assert!(wal.capacity_bytes().0 <= wal.capacity_bytes().1);
        Ok(())
    }

    #[tokio::test]
    async fn test_try_append_then_next_round_trips_single_event() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();

        let ev = sample_event(1);
        wal.try_append(ev.clone()).unwrap();

        let got = recv_one(&mut sub, 500).await.expect("event should arrive");
        assert_eq!(got.event.topic, ev.topic);
        assert_eq!(got.event.ts_ms, ev.ts_ms);
        assert_eq!(got.offset.segment_id, 1);
        assert_eq!(got.offset.byte_offset, 0);
        assert!(got.offset_after.byte_offset > 0);
    }

    #[tokio::test]
    async fn test_append_durable_writer_flush_visibility_returns_after_readable() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();
        let ev = sample_event(1);

        wal.append_durable(ev.clone()).await.unwrap();

        let bytes = fs::read(dir.path().join("00000000000000000001.log")).unwrap();
        let mut cur = std::io::Cursor::new(bytes);
        let (decoded, _) = crate::infrastructure::wal::codec::decode_from(&mut cur)
            .unwrap()
            .expect("append_durable return implies WAL bytes are flushed and readable");
        assert_eq!(decoded.ts_ms, ev.ts_ms);
        assert_eq!(decoded.topic, ev.topic);
    }

    #[tokio::test]
    async fn test_wal_sync_oversized_record_is_rejected_before_append() -> Result<()> {
        let dir = tempdir()?;
        let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;
        let mut event = sample_event(1);
        event.payload = "x".repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES);

        let result = wal.append_durable(event).await;

        assert!(result.is_err(), "an unreadable record must not be accepted");
        assert_eq!(fs::metadata(segment_path(dir.path(), 1))?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_oversized_try_append_is_rejected_before_enqueue() -> Result<()> {
        let dir = tempdir()?;
        let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;
        let mut event = sample_event(1);
        event.payload = "x".repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES);

        assert!(wal.try_append(event).is_err());
        assert_eq!(fs::metadata(segment_path(dir.path(), 1))?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_committed_torn_tail_is_rejected_without_truncation() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut record = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut record, &sample_event(1))?;
        let mut file = fs::File::create(&path)?;
        file.write_all(&record)?;
        file.write_all(&512u32.to_le_bytes())?;
        file.write_all(b"partial")?;
        drop(file);
        let original_len = fs::metadata(&path)?.len();
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: original_len + 100,
            },
        )?;

        let result = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await;

        assert!(
            result.is_err(),
            "an inconsistent committed cursor must fail"
        );
        assert_eq!(
            fs::metadata(path)?.len(),
            original_len,
            "recovery must preserve the segment for diagnosis"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_committed_record_survives_torn_tail_and_reopen() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut committed = Vec::new();
        let mut uncommitted = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut committed, &sample_event(1))?;
        crate::infrastructure::wal::codec::encode_into(&mut uncommitted, &sample_event(2))?;
        let mut bytes = committed.clone();
        bytes.extend_from_slice(&uncommitted);
        bytes.extend_from_slice(&512u32.to_le_bytes());
        bytes.extend_from_slice(b"partial");
        fs::write(&path, &bytes)?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: committed.len() as u64,
            },
        )?;

        let (wal, sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;
        assert_eq!(fs::read(&path)?, [committed, uncommitted].concat());
        assert!(wal.append_durable(sample_event(3)).await.is_ok());
        let state = wal.state.clone();
        drop(wal);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        drop(sub);

        let (_wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await?;
        for sequence in 2..=3 {
            let entry = recv_one(&mut sub, 500).await.expect("uncommitted record");
            assert_eq!(entry.event.ts_ms, sample_event(sequence).ts_ms);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_cursor_inside_record_is_rejected() -> Result<()> {
        let dir = tempdir()?;
        let mut record = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut record, &sample_event(1))?;
        fs::write(segment_path(dir.path(), 1), record)?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: 1,
            },
        )?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_cursor_in_missing_segment_is_rejected() -> Result<()> {
        let dir = tempdir()?;
        fs::File::create(segment_path(dir.path(), 1))?;
        fs::File::create(segment_path(dir.path(), 3))?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 2,
                byte_offset: 0,
            },
        )?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_empty_cursor_rejects_recovery_without_truncating() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut bytes = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut bytes, &sample_event(1))?;
        bytes.extend_from_slice(&512u32.to_le_bytes());
        bytes.extend_from_slice(b"partial");
        fs::write(&path, &bytes)?;
        fs::File::create(dir.path().join("commit.cursor"))?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        assert_eq!(fs::read(path)?, bytes);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_open_rejects_unsyncable_older_segment() -> Result<()> {
        let dir = tempdir()?;
        fs::create_dir(segment_path(dir.path(), 1))?;
        fs::File::create(segment_path(dir.path(), 2))?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_corrupt_committed_record_is_preserved_and_rejected() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut bytes = 8u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0xFF; 8]);
        fs::write(&path, &bytes)?;
        crate::infrastructure::wal::cursor::write_cursor(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: bytes.len() as u64,
            },
        )?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        assert_eq!(fs::read(path)?, bytes);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_corrupt_sealed_segment_is_preserved_and_rejected() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut bytes = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut bytes, &sample_event(1))?;
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFF; 8]);
        fs::write(&path, &bytes)?;
        fs::File::create(segment_path(dir.path(), 2))?;

        let result = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await;

        assert!(
            result.is_err(),
            "a sealed corrupt segment must fail startup"
        );
        assert_eq!(fs::read(path)?, bytes, "recovery must preserve the segment");
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_torn_sealed_segment_is_preserved_and_rejected() -> Result<()> {
        let dir = tempdir()?;
        let path = segment_path(dir.path(), 1);
        let mut bytes = Vec::new();
        crate::infrastructure::wal::codec::encode_into(&mut bytes, &sample_event(1))?;
        bytes.extend_from_slice(&512u32.to_le_bytes());
        bytes.extend_from_slice(b"partial");
        fs::write(&path, &bytes)?;
        fs::File::create(segment_path(dir.path(), 2))?;

        assert!(Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.is_err());
        assert_eq!(fs::read(path)?, bytes, "sealed tails must not be truncated");
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_rotation_reopens_and_replays_legacy_frames() -> Result<()> {
        const LEGACY_FRAME: &[u8] = &[
            26, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b't', 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, b'p',
        ];
        let dir = tempdir()?;
        fs::write(segment_path(dir.path(), 1), LEGACY_FRAME)?;
        let (wal, sub) = Wal::open(opts(dir.path(), LEGACY_FRAME.len() as u64, 8)).await?;
        let next = sample_event(2);

        assert!(wal.append_durable(next.clone()).await.is_ok());
        let state = wal.state.clone();
        drop(wal);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        drop(sub);

        let (_wal, mut sub) = Wal::open(opts(dir.path(), LEGACY_FRAME.len() as u64, 8)).await?;
        let first = recv_one(&mut sub, 500).await.expect("legacy record");
        let second = recv_one(&mut sub, 500).await.expect("rotated record");
        assert_eq!((first.event.topic.as_str(), first.event.ts_ms), ("t", 1));
        assert_eq!(first.event.payload, "p");
        assert_eq!(second.event.topic, next.topic);
        assert_eq!(second.event.payload, next.payload);
        assert_eq!(second.offset.segment_id, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_writer_failure_is_visible_and_wakes_subscription() -> Result<()> {
        let dir = tempdir()?;
        let (wal, mut sub) = Wal::open(opts(dir.path(), 1, 8)).await?;
        assert!(wal.append_durable(sample_event(1)).await.is_ok());
        assert!(recv_one(&mut sub, 500).await.is_some());
        let mut waiting = Box::pin(sub.next());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut waiting)
                .await
                .is_err()
        );
        fs::create_dir(segment_path(dir.path(), 2))?;

        assert!(wal.append_durable(sample_event(2)).await.is_err());
        assert!(wal.writer_failure().is_some());
        assert!(matches!(
            wal.try_append(sample_event(3)),
            Err(TryAppendError::Closed(_))
        ));
        assert!(matches!(
            wal.append_durable(sample_event(4)).await,
            Err(AppendDurableError::Closed(_))
        ));
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut waiting).await?;
        assert!(
            outcome.is_err(),
            "writer failure must not look like clean EOF"
        );
        assert!(!wal.capacity_handle().is_writable());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_new_nested_directory_reopens_records() -> Result<()> {
        let parent = tempdir()?;
        let dir = parent.path().join("new").join("wal");
        let (wal, sub) = Wal::open(opts(&dir, 1024 * 1024, 8)).await?;
        assert!(wal.append_durable(sample_event(1)).await.is_ok());
        let state = wal.state.clone();
        drop(wal);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !state.stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        drop(sub);

        let (_wal, mut sub) = Wal::open(opts(&dir, 1024 * 1024, 8)).await?;
        assert_eq!(
            recv_one(&mut sub, 500)
                .await
                .expect("replayed record")
                .event
                .ts_ms,
            sample_event(1).ts_ms
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_open_reopen_without_commit_replays_all_records_with_monotonic_offsets() {
        let dir = tempdir().unwrap();

        {
            let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 256)).await.unwrap();
            for i in 0..100 {
                wal.try_append(sample_event(i)).unwrap();
                // Yield so the writer task can drain the bounded queue.
                tokio::task::yield_now().await;
            }
            drop(wal);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let (_wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 256)).await.unwrap();
        let mut prev: Option<WalOffset> = None;
        for i in 0..100 {
            let entry = recv_one(&mut sub, 500)
                .await
                .unwrap_or_else(|| panic!("missing record {i}"));
            assert_eq!(entry.event.ts_ms, sample_event(i).ts_ms);
            if let Some(p) = prev {
                assert!(
                    entry.offset > p,
                    "offsets must be monotonic: prev={p:?}, curr={:?}",
                    entry.offset
                );
            }
            prev = Some(entry.offset);
        }
    }

    #[tokio::test]
    async fn test_commit_halfway_reopen_resumes_subscription_at_committed_offset() {
        let dir = tempdir().unwrap();

        let resume_at = {
            let (wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 32)).await.unwrap();
            for i in 0..9 {
                wal.try_append(sample_event(i)).unwrap();
                tokio::task::yield_now().await;
            }
            wal.append_durable(sample_event(9)).await.unwrap();

            let mut last_after = None;
            for _ in 0..5 {
                let entry = recv_one(&mut sub, 500).await.unwrap();
                last_after = Some(entry.offset_after);
            }
            let cutoff = last_after.unwrap();
            sub.commit(cutoff).await.unwrap();

            drop(wal);
            tokio::time::sleep(Duration::from_millis(100)).await;
            cutoff
        };

        let (_wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 32)).await.unwrap();
        for i in 5..10 {
            let entry = recv_one(&mut sub, 500)
                .await
                .unwrap_or_else(|| panic!("missing post-commit record {i}"));
            assert_eq!(entry.event.ts_ms, sample_event(i).ts_ms);
            assert!(entry.offset >= resume_at);
        }
    }

    #[tokio::test]
    async fn test_try_append_segment_rotation_produces_multiple_files_on_disk() {
        let dir = tempdir().unwrap();

        let (wal, _sub) = Wal::open(opts(dir.path(), 512, 64)).await.unwrap();
        for i in 0..50 {
            wal.try_append(sample_event(i)).unwrap();
            tokio::task::yield_now().await;
        }
        drop(wal);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let segments = list_segments(dir.path()).unwrap();
        assert!(
            segments.len() >= 2,
            "expected rotation to produce >=2 segments, got {segments:?}"
        );
    }

    #[tokio::test]
    async fn test_commit_past_segment_boundary_deletes_older_segments() {
        let dir = tempdir().unwrap();

        // Force rotation with a tight segment_bytes.
        let (wal, mut sub) = Wal::open(opts(dir.path(), 512, 64)).await.unwrap();
        for i in 0..49 {
            wal.try_append(sample_event(i)).unwrap();
            tokio::task::yield_now().await;
        }
        wal.append_durable(sample_event(49)).await.unwrap();

        let mut commit_at = None;
        for _ in 0..50 {
            let entry = recv_one(&mut sub, 500).await.unwrap();
            if entry.offset.segment_id >= 2 {
                commit_at = Some(entry.offset);
                break;
            }
        }
        let cutoff = commit_at.expect("never crossed into segment 2");
        sub.commit(cutoff).await.unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert!(
            segments.iter().all(|&id| id >= cutoff.segment_id),
            "older segments should be deleted, got {segments:?}, cutoff segment {}",
            cutoff.segment_id
        );
    }

    #[tokio::test]
    async fn test_open_torn_trailing_record_stops_replay_cleanly() {
        let dir = tempdir().unwrap();

        {
            let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();
            wal.try_append(sample_event(1)).unwrap();
            tokio::task::yield_now().await;
            drop(wal);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let path = dir.path().join("00000000000000000001.log");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        drop(f);

        let (_wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();
        let good = recv_one(&mut sub, 500).await.expect("good record");
        assert_eq!(good.event.ts_ms, sample_event(1).ts_ms);

        // The torn tail must not produce more events; subscription should block.
        let after = recv_one(&mut sub, 150).await;
        assert!(after.is_none(), "replay should stop at torn tail");
    }

    #[tokio::test]
    async fn test_open_torn_tail_heals_and_subsequent_append_replays_cleanly() {
        let dir = tempdir().unwrap();

        {
            let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();
            wal.try_append(sample_event(1)).unwrap();
            tokio::task::yield_now().await;
            drop(wal);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let path = dir.path().join("00000000000000000001.log");
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&999u32.to_le_bytes()).unwrap();
            drop(f);
        }

        // Reopen: `open` must truncate the torn tail. Append a second good record.
        {
            let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();
            wal.try_append(sample_event(2)).unwrap();
            tokio::task::yield_now().await;
            drop(wal);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let (_wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 16)).await.unwrap();

        let first = recv_one(&mut sub, 500).await.expect("first good record");
        assert_eq!(first.event.ts_ms, sample_event(1).ts_ms);

        let second = recv_one(&mut sub, 500).await.expect("second good record");
        assert_eq!(second.event.ts_ms, sample_event(2).ts_ms);
        assert!(
            second.offset > first.offset,
            "offsets must be monotonic: first={:?}, second={:?}",
            first.offset,
            second.offset
        );

        let after = recv_one(&mut sub, 150).await;
        assert!(after.is_none(), "replay should block after the last record");
    }

    #[tokio::test]
    async fn test_next_live_subscription_after_writer_shutdown_drains_all_records() {
        let dir = tempdir().unwrap();

        let (wal, mut sub) = Wal::open(opts(dir.path(), 1024 * 1024, 256)).await.unwrap();
        for i in 0..20 {
            wal.try_append(sample_event(i)).unwrap();
            tokio::task::yield_now().await;
        }

        // Close the WAL: the writer flushes its final batch and exits, leaving
        // the live subscription to drain. Every appended record must surface
        // before `next()` returns `None`.
        drop(wal);

        let mut count = 0u64;
        while let Some(entry) = recv_one(&mut sub, 500).await {
            assert_eq!(entry.event.ts_ms, sample_event(count).ts_ms);
            count += 1;
        }
        assert_eq!(count, 20, "graceful drain must yield every appended record");
    }

    #[tokio::test]
    async fn test_try_append_queue_saturated_returns_full() {
        let dir = tempdir().unwrap();
        // queue_capacity = 1 with a real writer thread draining concurrently:
        // flood the channel until a `try_send` observes the buffer full. The
        // producer's tight loop outruns the writer's encode + write, so this is
        // deterministic without relying on cooperative scheduling.
        let (wal, _sub) = Wal::open(opts(dir.path(), 1024 * 1024, 1)).await.unwrap();

        let mut hit = None;
        for i in 0..10_000 {
            if let Err(err) = wal.try_append(sample_event(i)) {
                hit = Some(err);
                break;
            }
        }

        match hit.expect("queue should saturate under a tight flood") {
            TryAppendError::Full(_) => {}
            TryAppendError::Closed(_) => panic!("expected Full, got Closed"),
            TryAppendError::TooLarge(_) => panic!("expected Full, got TooLarge"),
            TryAppendError::Capacity(_) => panic!("expected Full, got Capacity"),
        }
    }

    #[tokio::test]
    async fn test_open_corrupt_cursor_file_returns_error() {
        let dir = tempdir().unwrap();
        let cursor_path = dir.path().join("commit.cursor");
        fs::write(&cursor_path, [1u8, 2u8, 3u8]).unwrap();

        let err = match Wal::open(opts(dir.path(), 1024 * 1024, 16)).await {
            Ok(_) => panic!("corrupt cursor must fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("Invalid cursor file length: expected 16 bytes, got 3"),
            "{err:#}"
        );
    }
}
