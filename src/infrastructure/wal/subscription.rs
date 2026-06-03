use std::fs;
use std::sync::Arc;
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use metrics::counter;
use tokio::sync::Notify;
use tracing::warn;

use crate::infrastructure::wal::{codec, cursor};
use crate::infrastructure::wal::{
    segment,
    types::{WalEntry, WalOffset},
    writer::AtomicWalOffset,
};

pub struct WalSubscription {
    dir: PathBuf,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
    cur_segment_id: u64,
    cur_byte_offset: u64,
    cur_reader: Option<BufReader<File>>,
    /// Reusable scratch buffer for the record payload, passed to
    /// [`codec::decode_into`] so the hot read path doesn't allocate a fresh
    /// `Vec` per record. Retains capacity across `next()` calls.
    payload_buf: Vec<u8>,
    /// Highest segment id below which we've already reclaimed (GC'd) on commit.
    /// Lets `commit` skip the per-commit `readdir` unless the committed segment
    /// has advanced. Initialized one below `start.segment_id` so exactly one GC
    /// runs on the first commit, reclaiming any segments a prior crash left
    /// behind after writing the cursor but before finishing deletion.
    last_gc_segment_id: u64,
}

impl WalSubscription {
    pub(super) fn new(
        dir: PathBuf,
        head: Arc<AtomicWalOffset>,
        notify: Arc<Notify>,
        start: WalOffset,
    ) -> Self {
        Self {
            dir,
            head,
            notify,
            cur_segment_id: start.segment_id,
            cur_byte_offset: start.byte_offset,
            cur_reader: None,
            payload_buf: Vec::new(),
            last_gc_segment_id: start.segment_id.saturating_sub(1),
        }
    }

    pub async fn next(&mut self) -> Option<WalEntry> {
        loop {
            // (1) Lazily (re)open the active segment file at the current
            //     read cursor. We drop & reopen on every EOF so that bytes
            //     the writer appended after our last read become visible.
            if self.cur_reader.is_none() {
                let path = segment::segment_path(&self.dir, self.cur_segment_id);
                let mut file = match File::open(&path) {
                    Ok(f) => f,

                    // Segment file doesn't exist yet — the writer hasn't rolled
                    // into it. Fall through to wait-or-advance and retry.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        if !self.wait_or_advance().await {
                            return None;
                        }
                        continue;
                    }

                    // Any other open error is transient from our point of view;
                    // log and retry once we get a notification.
                    Err(e) => {
                        warn!(
                            error = %e,
                            path = %path.display(),
                            "WAL subscription: failed to open segment; retrying after notify"
                        );
                        if !self.wait_or_advance().await {
                            return None;
                        }
                        continue;
                    }
                };

                // Seek to where we left off inside this segment. `cur_byte_offset == 0`
                // happens on the very first read of a segment, no seek needed.
                if self.cur_byte_offset > 0 {
                    if let Err(e) = file.seek(SeekFrom::Start(self.cur_byte_offset)) {
                        warn!(error = %e, "WAL subscription: seek failed; retrying after notify");
                        if !self.wait_or_advance().await {
                            return None;
                        }
                        continue;
                    }
                }

                // Wrap the seeked file in a buffered reader — 64 KiB matches the
                // writer's BufWriter and keeps decode_from cheap.
                self.cur_reader = Some(BufReader::with_capacity(64 * 1024, file));
            }

            // (2) Remember the offset *before* the decode so we can hand it back
            //     to the caller as `WalEntry::offset` (start of this record).
            let record_start = WalOffset {
                segment_id: self.cur_segment_id,
                byte_offset: self.cur_byte_offset,
            };

            let reader = self
                .cur_reader
                .as_mut()
                .expect("reader just initialised above");

            // (3) Decode one record. Three outcomes:
            //       Ok(Some) – a full record materialised: advance cursor + return.
            //       Ok(None) – clean EOF or torn tail: wait for the writer.
            //       Err      – a fully-framed record whose payload won't decode.
            match codec::decode_into(reader, &mut self.payload_buf) {
                Ok(Some((event, bytes_consumed))) => {
                    // Advance our cursor past the record we just consumed and
                    // compute the post-record offset for the forwarder to commit.
                    self.cur_byte_offset += bytes_consumed as u64;
                    let offset_after = WalOffset {
                        segment_id: self.cur_segment_id,
                        byte_offset: self.cur_byte_offset,
                    };

                    return Some(WalEntry {
                        offset: record_start,
                        offset_after,
                        event,
                    });
                }

                // Reader saw EOF (or a torn tail). Drop it so we reopen the file
                // on the next iteration — BufReader otherwise caches the old EOF.
                Ok(None) => {
                    self.cur_reader = None;
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }

                // A fully-framed record whose payload failed to decode. Torn
                // tails are healed at `open` (recovery truncation), so this is
                // either the *active* tail mid-flush or a durable-but-corrupt
                // record with committed data after it. The two are handled
                // differently: wait for the former, skip the latter.
                Err(e) => {
                    self.cur_reader = None;

                    let head = self.head.load();
                    let at_active_tail = head.segment_id == self.cur_segment_id
                        && record_start.byte_offset >= head.byte_offset;

                    if at_active_tail {
                        // Not yet marked durable by the writer's head — treat as
                        // a torn active tail and wait, preserving prior behavior.
                        warn!(
                            error = %e,
                            "WAL subscription: decode error at active tail; treating as torn, waiting"
                        );
                        if !self.wait_or_advance().await {
                            return None;
                        }
                        continue;
                    }

                    // Durable poison: committed bytes follow this record. Retrying
                    // the same offset would stall the pipeline forever, so skip it.
                    warn!(
                        error = %e,
                        segment_id = self.cur_segment_id,
                        byte_offset = self.cur_byte_offset,
                        "WAL subscription: corrupt durable record; skipping"
                    );
                    counter!("wal_subscription_corrupt_skipped_total").increment(1);
                    if !self.skip_corrupt_record().await {
                        return None;
                    }
                }
            }
        }
    }

    /// Advances the read cursor past a corrupt-but-durable record at the current
    /// offset so the pipeline isn't stalled re-reading it forever.
    ///
    /// Reads the record's 4-byte length prefix directly to compute its on-disk
    /// width and skips `4 + len`. If the prefix is unreadable (defensive — a
    /// durable record's prefix is always present), it falls back to
    /// `wait_or_advance`, which rolls to the next segment if the writer has
    /// sealed this one. Returns `false` only when there is nothing left to read.
    async fn skip_corrupt_record(&mut self) -> bool {
        let path = segment::segment_path(&self.dir, self.cur_segment_id);
        match read_len_prefix(&path, self.cur_byte_offset) {
            Some(len) => {
                self.cur_byte_offset += 4 + u64::from(len);
                true
            }
            None => self.wait_or_advance().await,
        }
    }

    /// Marks `up_to` as durably processed by the downstream sink.
    ///
    /// Persists the cursor atomically and garbage-collects every segment file
    /// strictly older than `up_to.segment_id`. Safe to call repeatedly with
    /// the same or older offsets — re-commits simply rewrite the cursor and
    /// find nothing left to delete.
    ///
    /// # Errors
    /// Returns an error only if the cursor file cannot be written. Segment
    /// deletion failures are logged but non-fatal: the watermark is already
    /// durable, and a later commit will retry the reclaim.
    pub async fn commit(&mut self, up_to: WalOffset) -> Result<()> {
        // (1) Persist the new read watermark first. `write_cursor` does a
        //     write-tmp + rename, so a crash mid-commit either leaves the old
        //     cursor intact or atomically swaps in the new one — never torn.
        cursor::write_cursor(&self.dir, up_to)
            .with_context(|| format!("writing WAL cursor to {}", self.dir.display()))?;

        // (2) Reclaim disk: drop every segment whose id is strictly below the
        //     committed segment. The segment that *contains* the committed
        //     offset is kept — it may still hold uncommitted records past
        //     `up_to.byte_offset`. To avoid a `readdir` on every commit, only
        //     scan when the committed segment has advanced past the last one we
        //     reclaimed; commits that stay within the same segment have nothing
        //     new to delete. `list_segments` returns ids ascending, so once we
        //     hit one >= up_to.segment_id we can stop scanning.
        if up_to.segment_id > self.last_gc_segment_id {
            for seg_id in segment::list_segments(&self.dir)? {
                if seg_id >= up_to.segment_id {
                    break;
                }

                let path = segment::segment_path(&self.dir, seg_id);
                match fs::remove_file(&path) {
                    Ok(()) => {}

                    // A prior commit already removed it — commit is idempotent.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}

                    // Non-fatal: the watermark is already persisted. Log so a
                    // filling disk is visible; the next commit will try again.
                    Err(e) => {
                        warn!(
                            error = %e,
                            path = %path.display(),
                            "WAL subscription: failed to delete segment; will retry on next commit"
                        );
                    }
                }
            }
            self.last_gc_segment_id = up_to.segment_id;
        }

        Ok(())
    }

    async fn wait_or_advance(&mut self) -> bool {
        // Register interest *before* inspecting `head`/closure so a flush
        // notification that races with these checks can't be lost.
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let head = self.head.load();

        // The writer rotated into a newer segment: the current one is fully
        // written and flushed, so move on and read the next segment.
        if head.segment_id > self.cur_segment_id {
            self.cur_segment_id += 1;
            self.cur_byte_offset = 0;
            return true;
        }

        // Writer is gone and there is nothing newer to read: terminate. The
        // `enable()` above guarantees the writer's shutdown `notify_waiters`
        // is observed even if it races with this check.
        if Arc::strong_count(&self.notify) == 1 {
            // The writer has finished its final flush, so `head` is now durable
            // and final. Re-load it and drain any records we haven't consumed
            // yet before terminating — this guarantees a graceful shutdown
            // delivers every committed record instead of dropping the tail when
            // the reader hasn't caught up to the writer's final offset.
            let head = self.head.load();
            if head.segment_id > self.cur_segment_id {
                self.cur_segment_id += 1;
                self.cur_byte_offset = 0;
                return true;
            }
            return head.byte_offset > self.cur_byte_offset;
        }

        // Otherwise wait for the writer to flush more durable bytes (or a torn
        // tail stays unread until overwritten). We intentionally do *not* spin
        // on `head.byte_offset > cur_byte_offset`: those bytes may be buffered
        // or torn, in which case a reopen would read EOF and loop forever.
        notified.await;
        true
    }
}

/// Reads the 4-byte little-endian length prefix of a record at `offset` in the
/// segment at `path`. Returns `None` if the file or prefix can't be read.
fn read_len_prefix(path: &Path, offset: u64) -> Option<u32> {
    let mut file = File::open(path).ok()?;
    file.seek(SeekFrom::Start(offset)).ok()?;
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).ok()?;
    Some(u32::from_le_bytes(len_buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::encode_into;
    use crate::infrastructure::wal::segment::segment_filename;
    use crate::infrastructure::wal::types::WalEvent;
    use crate::model::messages::message::HandledMessage;
    use crate::model::messages::status::StatusMessage;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::tempdir;

    fn sample_event(seq: u64) -> WalEvent {
        WalEvent {
            topic: format!("smarthome/dev-{seq}/status"),
            ts_ms: 1_700_000_000_000 + seq as i64,
            message: HandledMessage::Status(StatusMessage {
                device_id: format!("dev-{seq}"),
                device_class: String::from("test"),
                fw_version: String::from("1.0.0"),
                ip: String::from("10.0.0.1"),
                rssi: -50,
                time_ms: 0,
                time_iso: String::from("2024-01-01T00:00:00Z"),
                time_valid: true,
                uptime: 1,
                free_mem: 1,
                ssid: String::from("ssid"),
            }),
        }
    }

    async fn recv_one(sub: &mut WalSubscription, ms: u64) -> Option<WalEntry> {
        tokio::time::timeout(Duration::from_millis(ms), sub.next())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn corrupt_record_in_sealed_segment_is_skipped_and_following_record_yielded() {
        let dir = tempdir().unwrap();

        // Build segment 1 by hand: good record A, a fully-framed but corrupt
        // record (len=8, undecodable payload), then good record C.
        let seg1 = dir.path().join(segment_filename(1));
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&seg1)
            .unwrap();
        let mut buf = Vec::new();
        encode_into(&mut buf, &sample_event(1)).unwrap();
        f.write_all(&buf).unwrap();
        f.write_all(&8u32.to_le_bytes()).unwrap();
        f.write_all(&[0xFFu8; 8]).unwrap();
        encode_into(&mut buf, &sample_event(3)).unwrap();
        f.write_all(&buf).unwrap();
        drop(f);

        // A second, empty segment makes segment 1 "sealed" (head points past it),
        // so the corrupt record is classified as durable poison, not a torn tail.
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dir.path().join(segment_filename(2)))
            .unwrap();

        let head = Arc::new(AtomicWalOffset::new(WalOffset {
            segment_id: 2,
            byte_offset: 0,
        }));
        let notify = Arc::new(Notify::new());
        // Keep an extra reference so the subscription parks (instead of
        // terminating) once it reaches the empty active segment.
        let _writer_notify = notify.clone();

        let mut sub = WalSubscription::new(
            dir.path().to_path_buf(),
            head,
            notify,
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            },
        );

        let a = recv_one(&mut sub, 500).await.expect("record A");
        assert_eq!(a.event.ts_ms, sample_event(1).ts_ms);

        let c = recv_one(&mut sub, 500).await.expect("record C after skip");
        assert_eq!(c.event.ts_ms, sample_event(3).ts_ms);
        assert_eq!(c.offset.segment_id, 1);

        // Nothing else durable: the subscription parks on the empty segment 2.
        assert!(recv_one(&mut sub, 150).await.is_none());
    }

    fn make_segment(dir: &Path, id: u64) {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dir.join(segment_filename(id)))
            .unwrap();
    }

    fn new_sub(dir: &Path, start: WalOffset) -> WalSubscription {
        WalSubscription::new(
            dir.to_path_buf(),
            Arc::new(AtomicWalOffset::new(start)),
            Arc::new(Notify::new()),
            start,
        )
    }

    fn remaining_segments(dir: &Path) -> Vec<u64> {
        crate::infrastructure::wal::segment::list_segments(dir).unwrap()
    }

    #[tokio::test]
    async fn commit_reclaims_segments_below_committed_segment() {
        let dir = tempdir().unwrap();
        for id in 1..=3 {
            make_segment(dir.path(), id);
        }

        let mut sub = new_sub(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            },
        );

        // Commit within segment 1: nothing older than 1 exists, keeps 1..=3.
        sub.commit(WalOffset {
            segment_id: 1,
            byte_offset: 10,
        })
        .await
        .unwrap();
        assert_eq!(remaining_segments(dir.path()), vec![1, 2, 3]);

        // Commit into segment 3: segments 1 and 2 are reclaimed, 3 is kept.
        sub.commit(WalOffset {
            segment_id: 3,
            byte_offset: 0,
        })
        .await
        .unwrap();
        assert_eq!(remaining_segments(dir.path()), vec![3]);
    }

    #[tokio::test]
    async fn first_commit_reclaims_crash_leftover_segments_below_start() {
        let dir = tempdir().unwrap();
        // A prior run left segments 0 and 1 behind after advancing the cursor
        // into segment 2 but before finishing GC.
        for id in 0..=2 {
            make_segment(dir.path(), id);
        }

        let mut sub = new_sub(
            dir.path(),
            WalOffset {
                segment_id: 2,
                byte_offset: 0,
            },
        );

        // The very first commit (even at the start segment) must run GC once and
        // reclaim the stale 0 and 1.
        sub.commit(WalOffset {
            segment_id: 2,
            byte_offset: 5,
        })
        .await
        .unwrap();
        assert_eq!(remaining_segments(dir.path()), vec![2]);
    }
}
