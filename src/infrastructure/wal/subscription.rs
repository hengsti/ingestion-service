use std::fs;
use std::sync::Arc;
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result};
use tokio::sync::Notify;

#[cfg(test)]
use crate::infrastructure::wal::test_support::{run_test_io_hook, TestIoHookSlot, TestIoStep};
use crate::infrastructure::wal::{
    capacity::{Capacity, SEGMENT_ALLOWANCE_BYTES},
    journal::Journal,
    segment,
    types::{WalEntry, WalEvent, WalOffset},
    writer::{AtomicWalOffset, WriterState},
};
use crate::infrastructure::wal::{codec, cursor};

pub struct WalSubscription {
    journal: Arc<Journal>,
    capacity: Arc<Capacity>,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
    writer_state: Arc<WriterState>,
    #[cfg(test)]
    test_io_hook: Option<TestIoHookSlot>,
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
        journal: Arc<Journal>,
        capacity: Arc<Capacity>,
        head: Arc<AtomicWalOffset>,
        notify: Arc<Notify>,
        writer_state: Arc<WriterState>,
        start: WalOffset,
    ) -> Self {
        Self {
            journal,
            capacity,
            head,
            notify,
            writer_state,
            #[cfg(test)]
            test_io_hook: None,
            cur_segment_id: start.segment_id,
            cur_byte_offset: start.byte_offset,
            cur_reader: None,
            payload_buf: Vec::new(),
            last_gc_segment_id: start.segment_id.saturating_sub(1),
        }
    }

    /// Reads the next record at or before the writer's synced head.
    ///
    /// # Errors
    /// Returns an error when a segment or record inside the durable range cannot
    /// be read. The reader position remains at the offending record.
    pub async fn next(&mut self) -> Result<Option<WalEntry>> {
        self.next_before(None).await
    }

    /// Reads the next record without waiting past a fixed durable snapshot.
    pub(crate) async fn next_until(&mut self, through: WalOffset) -> Result<Option<WalEntry>> {
        self.next_before(Some(through)).await
    }

    async fn next_before(&mut self, through: Option<WalOffset>) -> Result<Option<WalEntry>> {
        loop {
            let durable_head = self.head.load();
            let head = match through {
                Some(through) if through <= durable_head => through,
                Some(through) => {
                    anyhow::bail!("WAL snapshot {through:?} exceeds durable head {durable_head:?}")
                }
                None => durable_head,
            };
            let segment_id = self.cur_segment_id;
            let byte_offset = self.cur_byte_offset;
            if segment_id > head.segment_id
                || (segment_id == head.segment_id && byte_offset > head.byte_offset)
            {
                anyhow::bail!(
                    "WAL reader position {segment_id}:{byte_offset} exceeds durable head {head:?}"
                );
            }

            if segment_id == head.segment_id && byte_offset == head.byte_offset {
                if through.is_some() {
                    return Ok(None);
                }
                if !self.wait_or_advance().await {
                    if self.writer_state.failure().is_some() {
                        anyhow::bail!("WAL writer failed at {segment_id}:{byte_offset}");
                    }
                    return Ok(None);
                }
                continue;
            }

            // Offload the blocking open/seek/decode to a blocking thread so a
            // slow disk can't stall a runtime worker. The reader and payload
            // scratch buffer are moved in and handed back to preserve the cached
            // `BufReader` (no per-record reopen) and the reusable `Vec` (no
            // per-record allocation). `dir` is an `Arc<Path>`, so the clone is a
            // refcount bump, not a path allocation.
            let journal = Arc::clone(&self.journal);
            let reader = self.cur_reader.take();
            let payload = std::mem::take(&mut self.payload_buf);
            let max_bytes = if segment_id == head.segment_id {
                Some(head.byte_offset - byte_offset)
            } else {
                None
            };

            let (outcome, reader, payload) = tokio::task::spawn_blocking(move || {
                read_one(
                    journal.dir(),
                    segment_id,
                    byte_offset,
                    max_bytes,
                    reader,
                    payload,
                )
            })
            .await
            .with_context(|| format!("WAL read task failed at {segment_id}:{byte_offset}"))?;

            self.cur_reader = reader;
            self.payload_buf = payload;

            match outcome {
                // A full record materialised: advance the cursor past it, compute
                // the post-record offset for the forwarder to commit, and return.
                ReadOutcome::Record {
                    event,
                    bytes_consumed,
                } => {
                    let Some(byte_offset_after) =
                        self.cur_byte_offset.checked_add(bytes_consumed as u64)
                    else {
                        self.cur_reader = None;
                        anyhow::bail!("WAL reader offset overflowed at {segment_id}:{byte_offset}");
                    };
                    if segment_id == head.segment_id && byte_offset_after > head.byte_offset {
                        self.cur_reader = None;
                        anyhow::bail!(
                            "WAL record at {segment_id}:{byte_offset} extends past durable head {head:?}"
                        );
                    }
                    let record_start = WalOffset {
                        segment_id,
                        byte_offset,
                    };
                    self.cur_byte_offset = byte_offset_after;
                    let offset_after = WalOffset {
                        segment_id,
                        byte_offset: self.cur_byte_offset,
                    };

                    return Ok(Some(WalEntry {
                        offset: record_start,
                        offset_after,
                        event,
                    }));
                }

                ReadOutcome::NotFound => anyhow::bail!(
                    "WAL segment {segment_id} is missing at durable offset {byte_offset}"
                ),
                ReadOutcome::OpenError(error) => anyhow::bail!(
                    "WAL segment {segment_id} open failed at durable offset {byte_offset}: {}",
                    error.kind()
                ),
                ReadOutcome::SeekError(error) => anyhow::bail!(
                    "WAL segment {segment_id} seek failed at durable offset {byte_offset}: {}",
                    error.kind()
                ),
                ReadOutcome::IoError(error) => {
                    return Err(error).context(format!(
                        "WAL read failed at durable offset {segment_id}:{byte_offset}"
                    ));
                }
                ReadOutcome::Eof => {
                    if segment_id < head.segment_id {
                        self.cur_segment_id = segment_id
                            .checked_add(1)
                            .context("WAL segment identifier overflowed")?;
                        self.cur_byte_offset = 0;
                        self.cur_reader = None;
                        continue;
                    }
                    anyhow::bail!(
                        "unexpected WAL EOF at durable offset {segment_id}:{byte_offset} (head {head:?})"
                    );
                }
                ReadOutcome::Corrupt(error) => {
                    return Err(error).context(format!(
                        "WAL record is corrupt at durable offset {segment_id}:{byte_offset}"
                    ));
                }
            }
        }
    }

    pub(crate) fn durable_head(&self) -> WalOffset {
        self.head.load()
    }

    /// Marks `up_to` as durably processed by the downstream sink.
    ///
    /// Syncs the cursor and garbage-collects segments strictly older than
    /// `up_to.segment_id`. Callers must commit monotonically advancing offsets;
    /// retrying the same offset is safe.
    ///
    /// # Errors
    /// Returns an error if `up_to` exceeds the synced head or cursor persistence
    /// or segment reclamation fails. A persisted cursor is safe to retry.
    pub async fn commit(&mut self, up_to: WalOffset) -> Result<()> {
        let head = self.head.load();
        if up_to > head {
            anyhow::bail!("WAL commit {up_to:?} exceeds durable head {head:?}");
        }
        let journal = Arc::clone(&self.journal);
        let capacity = Arc::clone(&self.capacity);
        // Only scan for reclaimable segments when the committed segment has
        // advanced past the last one we GC'd; commits that stay within the same
        // segment have nothing new to delete. Computed before the move so the
        // blocking closure can capture `dir` by value.
        let run_gc = up_to.segment_id > self.last_gc_segment_id;
        #[cfg(test)]
        let test_io_hook = self.test_io_hook.clone();

        // The cursor write (write-tmp + rename) and the segment GC are blocking
        // filesystem ops; run them on a blocking thread so they can't stall a
        // runtime worker.
        tokio::task::spawn_blocking(move || -> Result<()> {
            let dir = journal.dir();
            let mut maintenance = journal.maintenance()?;
            let committed = match maintenance.committed {
                Some(offset) => Some(offset),
                None => cursor::read_cursor(dir)?,
            };
            if committed.is_some_and(|offset| up_to < offset) {
                anyhow::bail!("WAL commit {up_to:?} precedes the persisted cursor");
            }
            // A failed directory sync can still leave the newer cursor visible.
            maintenance.committed = Some(up_to);
            // Persist and sync the cursor before reclaiming older segments.
            #[cfg(test)]
            let cursor_result = match test_io_hook.as_ref() {
                Some(hook) => cursor::write_cursor_inner(dir, up_to, Some(hook)),
                None => cursor::write_cursor(dir, up_to),
            };
            #[cfg(not(test))]
            let cursor_result = cursor::write_cursor(dir, up_to);
            cursor_result.with_context(|| format!("writing WAL cursor to {}", dir.display()))?;

            // (2) Reclaim disk: drop every segment whose id is strictly below the
            //     committed segment. The segment that *contains* the committed
            //     offset is kept — it may still hold uncommitted records past
            //     `up_to.byte_offset`. `list_segments` returns ids ascending, so
            //     once we hit one >= up_to.segment_id we can stop scanning.
            if run_gc {
                let mut reclaim_error = None;
                for seg_id in segment::list_segments(dir)? {
                    if seg_id >= up_to.segment_id {
                        break;
                    }

                    let path = segment::segment_path(dir, seg_id);
                    let bytes = match fs::metadata(&path) {
                        Ok(metadata) => metadata
                            .len()
                            .checked_add(SEGMENT_ALLOWANCE_BYTES)
                            .context("reclaimed journal segment size overflowed")?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => {
                            reclaim_error =
                                Some(anyhow::Error::new(error).context(format!(
                                    "reading WAL segment size {}",
                                    path.display()
                                )));
                            break;
                        }
                    };
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            maintenance.pending_deleted_bytes = maintenance
                                .pending_deleted_bytes
                                .checked_add(bytes)
                                .context("reclaimed journal capacity overflowed")?;
                        }

                        // A prior commit already removed it — commit is idempotent.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}

                        Err(e) => {
                            reclaim_error = Some(
                                anyhow::Error::new(e)
                                    .context(format!("reclaiming WAL segment {}", path.display())),
                            );
                            break;
                        }
                    }
                }
                if maintenance.pending_deleted_bytes != 0 {
                    #[cfg(test)]
                    if let Some(hook) = test_io_hook.as_ref() {
                        run_test_io_hook(hook, TestIoStep::ReclaimDirectorySync)?;
                    }
                    segment::sync_directory(dir).with_context(|| {
                        format!("syncing reclaimed WAL directory {}", dir.display())
                    })?;
                    capacity.release(std::mem::take(&mut maintenance.pending_deleted_bytes));
                }
                if let Some(error) = reclaim_error {
                    return Err(error);
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
        // written and synced, so move on and read the next segment.
        if head.segment_id > self.cur_segment_id {
            self.cur_segment_id += 1;
            self.cur_byte_offset = 0;
            self.cur_reader = None;
            return true;
        }
        if head.segment_id == self.cur_segment_id && head.byte_offset > self.cur_byte_offset {
            // Head has advanced within the current segment. This means durable
            // bytes are available now, so continue without waiting for another
            // notify edge that might already have raced.
            return true;
        }

        // Completion is published before the writer wakes waiting readers.
        if self.writer_state.stopped() {
            // The writer has finished its final sync, so `head` is now durable
            // and final. Re-load it and drain any records we haven't consumed
            // yet before terminating — this guarantees a graceful shutdown
            // delivers every committed record instead of dropping the tail when
            // the reader hasn't caught up to the writer's final offset.
            let head = self.head.load();
            if head.segment_id > self.cur_segment_id {
                self.cur_segment_id += 1;
                self.cur_byte_offset = 0;
                self.cur_reader = None;
                return true;
            }
            return head.byte_offset > self.cur_byte_offset;
        }

        // Otherwise wait for the next writer notify edge.
        notified.await;
        true
    }
}

/// Outcome of a single blocking read attempt performed off-runtime by
/// [`read_one`]. The async [`WalSubscription::next`] inspects this to drive its
/// control flow without ever touching the disk itself.
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
    /// Segment file is missing inside the durable range.
    NotFound,
    /// The segment couldn't be opened.
    OpenError(std::io::Error),
    /// Seeking to the read cursor failed.
    SeekError(std::io::Error),
    /// Reading the segment failed for an operational reason.
    IoError(anyhow::Error),
    /// Clean EOF at a sealed segment boundary.
    Eof,
    /// A record whose framing or payload failed to decode.
    Corrupt(anyhow::Error),
}

/// Performs one blocking read step: lazily (re)opens the segment at `byte_offset`
/// when `reader` is `None`, then decodes a single record. Ownership of the
/// `BufReader` and the payload scratch buffer is passed in and handed back so the
/// caller can cache them across calls. Runs entirely on a blocking thread.
///
/// The returned reader is retained across successful decodes and EOF so tail
/// polling can re-read without reopen churn. It is dropped only on read failures,
/// where forcing a reopen is safer.
fn read_one(
    dir: &Path,
    segment_id: u64,
    byte_offset: u64,
    max_bytes: Option<u64>,
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
                return (ReadOutcome::OpenError(e), None, payload);
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
    let decoded = match max_bytes {
        Some(max_bytes) => codec::decode_into(&mut r.take(max_bytes), &mut payload),
        None => codec::decode_into(r, &mut payload),
    };
    match decoded {
        Ok(Some((event, bytes_consumed))) => (
            ReadOutcome::Record {
                event,
                bytes_consumed,
            },
            reader,
            payload,
        ),
        Ok(None) => (ReadOutcome::Eof, reader, payload),
        Err(error) => (classify_decode_error(error), None, payload),
    }
}

fn classify_decode_error(error: anyhow::Error) -> ReadOutcome {
    if error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() != std::io::ErrorKind::UnexpectedEof)
    {
        ReadOutcome::IoError(error)
    } else {
        ReadOutcome::Corrupt(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::encode_into;
    use crate::infrastructure::wal::segment::segment_filename;
    use crate::infrastructure::wal::test_support::sample_event;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_read_failure_corrupt_record_stops_at_offending_offset() -> Result<()> {
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
        drop(f);

        let later_path = dir.path().join(segment_filename(2));
        let mut later_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(later_path)
            .unwrap();
        encode_into(&mut buf, &sample_event(3)).unwrap();
        later_file.write_all(&buf).unwrap();
        drop(later_file);

        let head = Arc::new(AtomicWalOffset::new(WalOffset {
            segment_id: 2,
            byte_offset: buf.len() as u64,
        }));
        let notify = Arc::new(Notify::new());
        let mut sub = WalSubscription::new(
            Arc::new(Journal::open(dir.path().to_path_buf()).unwrap()),
            Capacity::recover(dir.path(), 128 * 1024 * 1024).unwrap(),
            head,
            notify,
            Arc::new(WriterState::default()),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            },
        );

        let a = sub.next().await?.context("record A")?;
        assert_eq!(a.event.ts_ms, sample_event(1).ts_ms);

        let error = match sub.next().await {
            Err(error) => error,
            Ok(_) => anyhow::bail!("a corrupt durable frame must fail instead of being skipped"),
        };
        assert!(error.to_string().contains("corrupt"));
        assert_eq!(sub.cur_segment_id, a.offset_after.segment_id);
        assert_eq!(sub.cur_byte_offset, a.offset_after.byte_offset);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_read_failure_missing_segment_does_not_skip_to_later_segment() -> Result<()> {
        let dir = tempdir()?;
        let first = sample_event(1);
        let later = sample_event(3);
        let mut first_frame = Vec::new();
        let mut later_frame = Vec::new();
        encode_into(&mut first_frame, &first)?;
        encode_into(&mut later_frame, &later)?;
        fs::write(dir.path().join(segment_filename(1)), first_frame)?;
        fs::write(dir.path().join(segment_filename(3)), &later_frame)?;

        let mut sub = new_sub_with_head(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            },
            WalOffset {
                segment_id: 3,
                byte_offset: later_frame.len() as u64,
            },
        )?;
        let first_read = sub.next().await?.context("first segment record")?;
        assert_eq!(first_read.event.ts_ms, first.ts_ms);

        let error = match sub.next().await {
            Err(error) => error,
            Ok(_) => anyhow::bail!("reader must not roll over a missing durable segment"),
        };
        assert!(error.to_string().contains("missing"));
        assert_eq!(sub.cur_segment_id, 2);
        assert_eq!(sub.cur_byte_offset, 0);
        assert!(crate::infrastructure::wal::cursor::read_cursor(dir.path())?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_read_failure_retries_same_offset_after_restore() -> Result<()> {
        let dir = tempdir()?;
        let event = sample_event(1);
        let mut frame = Vec::new();
        encode_into(&mut frame, &event)?;
        let path = dir.path().join(segment_filename(1));
        fs::write(&path, &frame)?;
        let mut sub = new_sub_with_head(
            dir.path(),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            },
            WalOffset {
                segment_id: 1,
                byte_offset: frame.len() as u64,
            },
        )?;

        fs::remove_file(&path)?;
        fs::create_dir(&path)?;
        assert!(sub.next().await.is_err());
        assert_eq!(sub.cur_byte_offset, 0);

        fs::remove_dir(&path)?;
        fs::write(&path, frame)?;
        let replayed = sub.next().await?.context("replayed record")?;
        assert_eq!(replayed.offset.byte_offset, 0);
        assert_eq!(replayed.event.ts_ms, event.ts_ms);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_read_failure_cancelled_wait_replays_from_same_offset() -> Result<()> {
        let dir = tempdir()?;
        let event = sample_event(1);
        let mut frame = Vec::new();
        encode_into(&mut frame, &event)?;
        fs::write(dir.path().join(segment_filename(1)), &frame)?;

        let start = WalOffset {
            segment_id: 1,
            byte_offset: 0,
        };
        let head = Arc::new(AtomicWalOffset::new(start));
        let notify = Arc::new(Notify::new());
        let mut sub = WalSubscription::new(
            Arc::new(Journal::open(dir.path().to_path_buf())?),
            Capacity::recover(dir.path(), 128 * 1024 * 1024)?,
            head.clone(),
            notify.clone(),
            Arc::new(WriterState::default()),
            start,
        );

        {
            let next = sub.next();
            tokio::pin!(next);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut next)
                    .await
                    .is_err(),
                "bytes past the published head must not be read"
            );
        }

        head.store(WalOffset {
            segment_id: 1,
            byte_offset: frame.len() as u64,
        });
        notify.notify_waiters();
        let replayed = tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .context("waiting for the synced head")??
            .context("synced record")?;
        assert_eq!(replayed.offset.byte_offset, 0);
        assert_eq!(replayed.event.ts_ms, event.ts_ms);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_read_failure_waits_for_future_segment_until_head_advances() -> Result<()> {
        let dir = tempdir()?;
        let event = sample_event(1);
        let mut frame = Vec::new();
        encode_into(&mut frame, &event)?;
        make_segment(dir.path(), 1);
        fs::write(dir.path().join(segment_filename(2)), &frame)?;

        let start = WalOffset {
            segment_id: 1,
            byte_offset: 0,
        };
        let head = Arc::new(AtomicWalOffset::new(start));
        let notify = Arc::new(Notify::new());
        let mut sub = WalSubscription::new(
            Arc::new(Journal::open(dir.path().to_path_buf())?),
            Capacity::recover(dir.path(), 128 * 1024 * 1024)?,
            head.clone(),
            notify.clone(),
            Arc::new(WriterState::default()),
            start,
        );

        {
            let next = sub.next();
            tokio::pin!(next);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut next)
                    .await
                    .is_err(),
                "a segment beyond the durable head must not be read"
            );
        }

        head.store(WalOffset {
            segment_id: 2,
            byte_offset: frame.len() as u64,
        });
        notify.notify_waiters();
        let entry = tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .context("waiting for the next durable segment")??
            .context("future segment record")?;
        assert_eq!(entry.event.ts_ms, event.ts_ms);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_read_failure_short_record_stops_at_original_offset() -> Result<()> {
        let dir = tempdir()?;
        let mut frame = Vec::new();
        encode_into(&mut frame, &sample_event(1))?;
        let head_offset = frame.len() as u64;
        fs::write(
            dir.path().join(segment_filename(1)),
            &frame[..frame.len() - 1],
        )?;
        let start = WalOffset {
            segment_id: 1,
            byte_offset: 0,
        };
        let mut sub = new_sub_with_head(
            dir.path(),
            start,
            WalOffset {
                segment_id: 1,
                byte_offset: head_offset,
            },
        )?;

        assert!(sub.next().await.is_err());
        assert_eq!(sub.cur_segment_id, start.segment_id);
        assert_eq!(sub.cur_byte_offset, start.byte_offset);
        Ok(())
    }

    #[tokio::test]
    async fn test_wait_or_advance_same_segment_head_progress_returns_immediately() {
        let dir = tempdir().unwrap();
        let head = Arc::new(AtomicWalOffset::new(WalOffset {
            segment_id: 1,
            byte_offset: 128,
        }));
        let notify = Arc::new(Notify::new());
        let mut sub = WalSubscription::new(
            Arc::new(Journal::open(dir.path().to_path_buf()).unwrap()),
            Capacity::recover(dir.path(), 128 * 1024 * 1024).unwrap(),
            head,
            notify,
            Arc::new(WriterState::default()),
            WalOffset {
                segment_id: 1,
                byte_offset: 64,
            },
        );

        let progressed =
            tokio::time::timeout(Duration::from_millis(50), sub.wait_or_advance()).await;
        assert!(
            matches!(progressed, Ok(true)),
            "same-segment head progress should not block waiting for notify"
        );
    }

    #[test]
    fn test_read_one_eof_keeps_reader_open_for_tail_polling() {
        let dir = tempdir().unwrap();
        let seg = dir.path().join(segment_filename(1));
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(seg)
            .unwrap();

        let (outcome, reader, payload) = read_one(dir.path(), 1, 0, None, None, Vec::new());
        assert!(matches!(outcome, ReadOutcome::Eof));
        assert!(
            reader.is_some(),
            "EOF should retain descriptor to avoid reopen churn"
        );
        assert!(payload.is_empty());
    }

    #[test]
    fn test_read_failure_classifies_disk_errors_separately_from_corruption() {
        let io_error = classify_decode_error(std::io::Error::other("disk read failed").into());
        assert!(
            matches!(io_error, ReadOutcome::IoError(_)),
            "disk read failures must not be reported as record corruption"
        );

        let short_read =
            classify_decode_error(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        assert!(matches!(short_read, ReadOutcome::Corrupt(_)));

        let corrupt = classify_decode_error(anyhow::anyhow!("invalid WAL frame"));
        assert!(matches!(corrupt, ReadOutcome::Corrupt(_)));
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
        new_sub_with_head(dir, start, start).expect("create WAL subscription")
    }

    fn new_sub_with_head(dir: &Path, start: WalOffset, head: WalOffset) -> Result<WalSubscription> {
        Ok(WalSubscription::new(
            Arc::new(Journal::open(dir.to_path_buf())?),
            Capacity::recover(dir, 128 * 1024 * 1024)?,
            Arc::new(AtomicWalOffset::new(head)),
            Arc::new(Notify::new()),
            Arc::new(WriterState::default()),
            start,
        ))
    }

    fn remaining_segments(dir: &Path) -> Vec<u64> {
        crate::infrastructure::wal::segment::list_segments(dir).unwrap()
    }

    #[tokio::test]
    async fn test_wal_sync_commit_cannot_advance_past_durable_head() -> Result<()> {
        let dir = tempdir()?;
        make_segment(dir.path(), 1);
        make_segment(dir.path(), 2);
        let head = WalOffset {
            segment_id: 1,
            byte_offset: 10,
        };
        let mut sub = new_sub(dir.path(), head);

        let result = sub
            .commit(WalOffset {
                segment_id: 2,
                byte_offset: 0,
            })
            .await;

        assert!(result.is_err(), "unsynced records cannot be committed");
        assert!(cursor::read_cursor(dir.path())?.is_none());
        assert_eq!(remaining_segments(dir.path()), vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_failed_cursor_write_does_not_reclaim_segments() -> Result<()> {
        let dir = tempdir()?;
        make_segment(dir.path(), 1);
        make_segment(dir.path(), 2);
        fs::create_dir(dir.path().join("commit.cursor.tmp"))?;
        let mut sub = new_sub(
            dir.path(),
            WalOffset {
                segment_id: 2,
                byte_offset: 0,
            },
        );

        let result = sub
            .commit(WalOffset {
                segment_id: 2,
                byte_offset: 0,
            })
            .await;

        assert!(result.is_err());
        assert_eq!(remaining_segments(dir.path()), vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_sync_failed_cursor_directory_sync_is_retried_before_reclaim() -> Result<()> {
        use crate::infrastructure::wal::test_support::TestIoStep;

        let dir = tempdir()?;
        make_segment(dir.path(), 1);
        make_segment(dir.path(), 2);
        let offset = WalOffset {
            segment_id: 2,
            byte_offset: 0,
        };
        let mut sub = new_sub(dir.path(), offset);
        let hook: TestIoHookSlot = Arc::new(std::sync::Mutex::new(None));
        *hook.lock().unwrap() = Some(Arc::new(|step| {
            if step == TestIoStep::CursorDirectorySync {
                return Err(std::io::Error::other(
                    "injected cursor directory sync failure",
                ));
            }
            Ok(())
        }));
        sub.test_io_hook = Some(hook);

        assert!(sub.commit(offset).await.is_err());
        assert_eq!(remaining_segments(dir.path()), vec![1, 2]);

        sub.test_io_hook = None;
        sub.commit(offset).await?;
        assert_eq!(remaining_segments(dir.path()), vec![2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_failed_reclaim_retries_at_the_same_cursor() -> Result<()> {
        let dir = tempdir()?;
        let blocked = dir.path().join(segment_filename(1));
        fs::create_dir(&blocked)?;
        make_segment(dir.path(), 2);
        let offset = WalOffset {
            segment_id: 2,
            byte_offset: 0,
        };
        let mut sub = new_sub(dir.path(), offset);
        let accounted = sub.capacity.snapshot().0;

        assert!(sub.commit(offset).await.is_err());
        assert!(blocked.exists());
        assert_eq!(sub.capacity.snapshot().0, accounted);

        fs::remove_dir(&blocked)?;
        make_segment(dir.path(), 1);
        sub.commit(offset).await?;
        assert_eq!(remaining_segments(dir.path()), vec![2]);
        assert!(sub.capacity.snapshot().0 < accounted);
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_reclaim_sync_failure_keeps_bytes_until_retry() -> Result<()> {
        let dir = tempdir()?;
        make_segment(dir.path(), 1);
        make_segment(dir.path(), 2);
        let offset = WalOffset {
            segment_id: 2,
            byte_offset: 0,
        };
        let mut sub = new_sub(dir.path(), offset);
        let accounted = sub.capacity.snapshot().0;
        let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let hook_fail_once = Arc::clone(&fail_once);
        let hook: TestIoHookSlot = Arc::new(std::sync::Mutex::new(Some(Arc::new(move |step| {
            if step == TestIoStep::ReclaimDirectorySync
                && hook_fail_once.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other(
                    "injected reclaim directory sync failure",
                ));
            }
            Ok(())
        }))));
        sub.test_io_hook = Some(hook);

        assert!(sub.commit(offset).await.is_err());
        assert_eq!(remaining_segments(dir.path()), vec![2]);
        assert_eq!(sub.capacity.snapshot().0, accounted);
        sub.commit(offset).await?;
        assert!(sub.capacity.snapshot().0 < accounted);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_segments_below_committed_segment_reclaims_older_files() {
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
        sub.head.store(WalOffset {
            segment_id: 3,
            byte_offset: 0,
        });

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
    async fn test_commit_first_call_reclaims_crash_leftover_segments_below_start() {
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
        sub.head.store(WalOffset {
            segment_id: 2,
            byte_offset: 5,
        });

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
