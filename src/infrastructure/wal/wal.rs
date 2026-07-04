use std::fs;
use std::sync::{
    mpsc::{SyncSender, TrySendError},
    Arc,
};

use anyhow::{Context, Result};
use tracing::warn;

use crate::infrastructure::wal::{
    cursor::read_cursor,
    recover::{last_valid_offset, truncate_to},
    segment::{list_segments, segment_path},
    subscription::WalSubscription,
    types::{AppendDurableError, TryAppendError, WalEvent, WalOffset, WalOptions},
    writer::{spawn_writer, WriteRequest},
};

pub struct Wal {
    tx: SyncSender<WriteRequest>,
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

        let read_start = match read_cursor(&options.dir)? {
            Some(off) => {
                let first_id = segments.first().copied().unwrap_or(active_id);
                if off.segment_id < first_id || off.segment_id > active_id {
                    return Err(anyhow::anyhow!(
                        "WAL cursor points outside available segments: cursor={off:?}, segments={segments:?}"
                    ));
                }
                off
            }
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
            Arc::from(options.dir),
            handle.head.clone(),
            handle.notify.clone(),
            read_start,
        );
        Ok((Self { tx: handle.tx }, subscription))
    }

    // `TryAppendError` carries the rejected `WalEvent` (mirrors `TrySendError`) so
    // callers can recover the payload; the error path is cold (queue saturated),
    // so the large-Err size is acceptable over boxing on the hot append path.
    #[allow(dead_code)]
    #[allow(clippy::result_large_err)]
    pub fn try_append(&self, event: WalEvent) -> Result<(), TryAppendError> {
        let req = WriteRequest {
            event,
            durable_ack: None,
        };
        self.tx.try_send(req).map_err(|err| match err {
            TrySendError::Full(req) => TryAppendError::Full(req.event),
            TrySendError::Disconnected(req) => TryAppendError::Closed(req.event),
        })
    }

    #[allow(clippy::result_large_err)]
    /// Appends one event and waits for the writer's flush boundary.
    ///
    /// This guarantee is bounded to `BufWriter::flush`: bytes are written out of
    /// process memory and become readable from the WAL file, but no storage-media
    /// fsync (`sync_data`) is performed here.
    pub async fn append_durable(&self, event: WalEvent) -> Result<(), AppendDurableError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let req = WriteRequest {
            event,
            durable_ack: Some(ack_tx),
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
            segment_bytes,
            queue_capacity,
        }
    }

    async fn recv_one(sub: &mut WalSubscription, ms: u64) -> Option<WalEntry> {
        tokio::time::timeout(Duration::from_millis(ms), sub.next())
            .await
            .ok()
            .flatten()
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
            for i in 0..10 {
                wal.try_append(sample_event(i)).unwrap();
                tokio::task::yield_now().await;
            }

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
        for i in 0..50 {
            wal.try_append(sample_event(i)).unwrap();
            tokio::task::yield_now().await;
        }

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
