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
    types::{WalEntry, WalEvent, WalOffset},
    writer::AtomicWalOffset,
};

pub struct WalSubscription {
    dir: Arc<Path>,
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
        dir: Arc<Path>,
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
            // Offload the blocking open/seek/decode to a blocking thread so a
            // slow disk can't stall a runtime worker. The reader and payload
            // scratch buffer are moved in and handed back to preserve the cached
            // `BufReader` (no per-record reopen) and the reusable `Vec` (no
            // per-record allocation). `dir` is an `Arc<Path>`, so the clone is a
            // refcount bump, not a path allocation.
            let dir = Arc::clone(&self.dir);
            let segment_id = self.cur_segment_id;
            let byte_offset = self.cur_byte_offset;
            let reader = self.cur_reader.take();
            let payload = std::mem::take(&mut self.payload_buf);

            let (outcome, reader, payload) = tokio::task::spawn_blocking(move || {
                read_one(&dir, segment_id, byte_offset, reader, payload)
            })
            .await
            .expect("WAL read_one blocking task panicked");

            self.cur_reader = reader;
            self.payload_buf = payload;

            match outcome {
                // A full record materialised: advance the cursor past it, compute
                // the post-record offset for the forwarder to commit, and return.
                ReadOutcome::Record {
                    event,
                    bytes_consumed,
                } => {
                    let record_start = WalOffset {
                        segment_id: self.cur_segment_id,
                        byte_offset: self.cur_byte_offset,
                    };
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

                // Segment file doesn't exist yet — the writer hasn't rolled into
                // it. Wait-or-advance and retry.
                ReadOutcome::NotFound => {
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }

                // Transient open error from our point of view; log and retry once
                // we get a notification.
                ReadOutcome::OpenError { path, error } => {
                    warn!(
                        error = %error,
                        path = %path.display(),
                        "WAL subscription: failed to open segment; retrying after notify"
                    );
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }

                ReadOutcome::SeekError(error) => {
                    warn!(error = %error, "WAL subscription: seek failed; retrying after notify");
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }

                // Reader saw EOF (or a torn tail). `read_one` already dropped the
                // reader so the next iteration reopens the file and sees bytes the
                // writer appended after our last read.
                ReadOutcome::Eof => {
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }

                // A fully-framed record whose payload failed to decode. Torn tails
                // are healed at `open` (recovery truncation), so this is either the
                // *active* tail mid-flush or a durable-but-corrupt record with
                // committed data after it. Wait for the former, skip the latter.
                ReadOutcome::Corrupt(e) => {
                    let head = self.head.load();
                    let at_active_tail = head.segment_id == self.cur_segment_id
                        && self.cur_byte_offset >= head.byte_offset;

                    if at_active_tail {
                        // Not yet marked durable by the writer's head — treat as a
                        // torn active tail and wait, preserving prior behavior.
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
        let dir = Arc::clone(&self.dir);
        let segment_id = self.cur_segment_id;
        let byte_offset = self.cur_byte_offset;
        let len = tokio::task::spawn_blocking(move || {
            let path = segment::segment_path(&dir, segment_id);
            read_len_prefix(&path, byte_offset)
        })
        .await
        .expect("WAL skip_corrupt_record blocking task panicked");

        match len {
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
        let dir = Arc::clone(&self.dir);
        // Only scan for reclaimable segments when the committed segment has
        // advanced past the last one we GC'd; commits that stay within the same
        // segment have nothing new to delete. Computed before the move so the
        // blocking closure can capture `dir` by value.
        let run_gc = up_to.segment_id > self.last_gc_segment_id;

        // The cursor write (write-tmp + rename) and the segment GC are blocking
        // filesystem ops; run them on a blocking thread so they can't stall a
        // runtime worker.
        tokio::task::spawn_blocking(move || -> Result<()> {
            // (1) Persist the new read watermark first. `write_cursor` does a
            //     write-tmp + rename, so a crash mid-commit either leaves the old
            //     cursor intact or atomically swaps in the new one — never torn.
            cursor::write_cursor(&dir, up_to)
                .with_context(|| format!("writing WAL cursor to {}", dir.display()))?;

            // (2) Reclaim disk: drop every segment whose id is strictly below the
            //     committed segment. The segment that *contains* the committed
            //     offset is kept — it may still hold uncommitted records past
            //     `up_to.byte_offset`. `list_segments` returns ids ascending, so
            //     once we hit one >= up_to.segment_id we can stop scanning.
            if run_gc {
                for seg_id in segment::list_segments(&dir)? {
                    if seg_id >= up_to.segment_id {
                        break;
                    }

                    let path = segment::segment_path(&dir, seg_id);
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
            }

            Ok(())
        })
        .await
        .expect("WAL commit blocking task panicked")?;

        // Only advance the GC watermark once the cursor write succeeded (the `?`
        // above propagates a write failure without updating it, matching the
        // original commit semantics).
        if run_gc {
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

/// Outcome of a single blocking read attempt performed off-runtime by
/// [`read_one`]. The async [`WalSubscription::next`] inspects this to drive its
/// control flow (advance, wait, or skip) without ever touching the disk itself.
//
// `Record` (the hot, common case) carries the decoded `WalEvent` inline. Boxing
// it to equalise the variant sizes would add a heap allocation per record on the
// read path, so we accept the larger enum — mirroring the `result_large_err`
// trade-off made for `WalEvent` on the append path in `wal.rs`.
#[allow(clippy::large_enum_variant)]
enum ReadOutcome {
    /// A full record decoded; `bytes_consumed` is its on-disk width.
    Record {
        event: WalEvent,
        bytes_consumed: usize,
    },
    /// Segment file not present yet — the writer hasn't rolled into it.
    NotFound,
    /// The segment couldn't be opened (transient); carries the path for logging.
    OpenError {
        path: PathBuf,
        error: std::io::Error,
    },
    /// Seeking to the read cursor failed (transient).
    SeekError(std::io::Error),
    /// Clean EOF or a torn tail — wait for the writer to append more.
    Eof,
    /// A fully-framed record whose payload won't decode.
    Corrupt(anyhow::Error),
}

/// Performs one blocking read step: lazily (re)opens the segment at `byte_offset`
/// when `reader` is `None`, then decodes a single record. Ownership of the
/// `BufReader` and the payload scratch buffer is passed in and handed back so the
/// caller can cache them across calls. Runs entirely on a blocking thread.
///
/// The returned reader is `Some` only when a record decoded (so it stays cached);
/// on EOF, corruption, or any open/seek failure it is dropped here (closing the
/// file descriptor off-runtime) and returned as `None`, forcing a reopen.
fn read_one(
    dir: &Path,
    segment_id: u64,
    byte_offset: u64,
    mut reader: Option<BufReader<File>>,
    mut payload: Vec<u8>,
) -> (ReadOutcome, Option<BufReader<File>>, Vec<u8>) {
    if reader.is_none() {
        let path = segment::segment_path(dir, segment_id);
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return (ReadOutcome::NotFound, None, payload);
            }
            Err(e) => {
                return (ReadOutcome::OpenError { path, error: e }, None, payload);
            }
        };

        // Seek to where we left off inside this segment. `byte_offset == 0`
        // happens on the very first read of a segment, no seek needed.
        if byte_offset > 0 {
            if let Err(e) = file.seek(SeekFrom::Start(byte_offset)) {
                return (ReadOutcome::SeekError(e), None, payload);
            }
        }

        // 64 KiB matches the writer's BufWriter and keeps decode cheap.
        reader = Some(BufReader::with_capacity(64 * 1024, file));
    }

    let r = reader.as_mut().expect("reader just initialised above");
    match codec::decode_into(r, &mut payload) {
        Ok(Some((event, bytes_consumed))) => (
            ReadOutcome::Record {
                event,
                bytes_consumed,
            },
            reader,
            payload,
        ),
        Ok(None) => (ReadOutcome::Eof, None, payload),
        Err(e) => (ReadOutcome::Corrupt(e), None, payload),
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
            Arc::from(dir.path()),
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
            Arc::from(dir),
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
