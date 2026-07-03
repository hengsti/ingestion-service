use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use metrics::counter;
use tokio::sync::Notify;
use tracing::{error, warn};

use crate::infrastructure::wal::{
    codec,
    segment::segment_path,
    types::{WalEvent, WalOffset},
};

/// How long buffered records may sit in the `BufWriter` before the writer flushes
/// them from process memory into the OS file cache and signals the reader.
/// Bounds reader latency without flushing on every record.
const FLUSH_INTERVAL: Duration = Duration::from_millis(5);

pub(super) struct AtomicWalOffset {
    version: AtomicU64,
    segment_id: AtomicU64,
    byte_offset: AtomicU64,
}

impl AtomicWalOffset {
    pub fn new(offset: WalOffset) -> Self {
        Self {
            version: AtomicU64::new(0),
            segment_id: AtomicU64::new(offset.segment_id),
            byte_offset: AtomicU64::new(offset.byte_offset),
        }
    }

    pub fn load(&self) -> WalOffset {
        loop {
            // Seqlock read section: `segment_id` and `byte_offset` are separate atomics
            // and therefore not atomically loaded as a pair. We read both fields only
            // while `version` is even, then verify the same `version` afterwards.
            // If a writer overlaps, `version` changes (or is odd) and we retry,
            // ensuring readers observe a coherent offset snapshot.
            let start = self.version.load(Ordering::Acquire);
            if start & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }

            let segment_id = self.segment_id.load(Ordering::Relaxed);
            let byte_offset = self.byte_offset.load(Ordering::Relaxed);

            let end = self.version.load(Ordering::Acquire);
            if start == end {
                return WalOffset {
                    segment_id,
                    byte_offset,
                };
            }
            std::hint::spin_loop();
        }
    }

    pub fn store(&self, offset: WalOffset) {
        // Seqlock write section: readers retry while `version` is odd.
        // The WAL has one writer thread, so a single write section is active.
        let start = self.version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(start & 1, 0, "AtomicWalOffset assumes a single writer");

        self.segment_id.store(offset.segment_id, Ordering::Relaxed);
        self.byte_offset
            .store(offset.byte_offset, Ordering::Relaxed);

        self.version.fetch_add(1, Ordering::Release);
    }
}

pub(super) type DurableAck = tokio::sync::oneshot::Sender<Result<(), String>>;

pub(super) struct WriteRequest {
    pub event: WalEvent,
    pub durable_ack: Option<DurableAck>,
}

pub(super) struct WriterHandle {
    pub tx: SyncSender<WriteRequest>,
    pub notify: Arc<Notify>,
    pub head: Arc<AtomicWalOffset>,
}

#[derive(Clone, Copy)]
enum WriterFatalReason {
    FlushBeforeRotation,
    OpenNextSegment,
    WriteAll,
}

impl WriterFatalReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::FlushBeforeRotation => "flush_before_rotation",
            Self::OpenNextSegment => "open_next_segment",
            Self::WriteAll => "write_all",
        }
    }
}

fn record_writer_fatal(reason: WriterFatalReason) {
    counter!("wal_writer_fatal_total", "reason" => reason.as_str()).increment(1);
}

const WAL_SEGMENT_ROTATIONS_METRIC: &str = "wal_segment_rotations_total";

fn record_segment_rotation() {
    counter!(WAL_SEGMENT_ROTATIONS_METRIC).increment(1);
}

pub(super) fn spawn_writer(
    dir: PathBuf,
    start_segment_id: u64,
    start_byte_offset: u64,
    segment_bytes: u64,
    queue_capacity: usize,
) -> Result<WriterHandle> {
    let path = segment_path(&dir, start_segment_id);
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .with_context(|| format!("opening WAL segment {}", path.display()))?;
    let writer = BufWriter::with_capacity(64 * 1024, file);

    let (tx, rx) = mpsc::sync_channel::<WriteRequest>(queue_capacity);
    let notify = Arc::new(Notify::new());
    let head = Arc::new(AtomicWalOffset::new(WalOffset {
        segment_id: start_segment_id,
        byte_offset: start_byte_offset,
    }));

    let task_notify = notify.clone();
    let task_head = head.clone();

    // The writer runs on a dedicated OS thread, not a tokio task: its file I/O is
    // blocking and would otherwise stall a runtime worker under disk pressure.
    // `head.store` and `Notify::notify_waiters` are both thread-safe and callable
    // from outside the runtime, so the reader still observes progress.
    thread::Builder::new()
        .name("wal-writer".to_string())
        .spawn(move || {
            writer_loop(
                dir,
                start_segment_id,
                start_byte_offset,
                segment_bytes,
                writer,
                rx,
                task_head,
                task_notify,
            );
        })
        .context("spawning WAL writer thread")?;

    Ok(WriterHandle { tx, notify, head })
}

#[allow(clippy::too_many_arguments)]
fn writer_loop(
    dir: PathBuf,
    mut current_segment_id: u64,
    mut current_byte_offset: u64,
    segment_bytes: u64,
    mut writer: BufWriter<File>,
    rx: Receiver<WriteRequest>,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
) {
    let mut encode_buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut pending_durable_acks: Vec<DurableAck> = Vec::new();

    // `dirty` means the BufWriter may hold bytes for records not yet flushed to
    // disk and therefore not yet reflected in `head`. Set after each successful
    // `write_all`, cleared after a flush. While clean the loop blocks on `recv`,
    // so an idle writer makes no syscalls and no spurious wakeups.
    let mut dirty = false;
    // Instant at which buffered bytes must be flushed to bound reader latency.
    // Armed on the clean→dirty transition (oldest unflushed record), never pushed
    // by later writes, so sustained load can't starve the reader.
    let mut flush_deadline = Instant::now();

    loop {
        // Flush at the top of the loop: under sustained load `recv_timeout` keeps
        // returning `Ok` and never reports a timeout, so the periodic flush must
        // be driven by this explicit deadline check rather than a timer branch.
        if dirty && Instant::now() >= flush_deadline {
            if let Err(e) = writer.flush() {
                warn!(error = %e, segment_id = current_segment_id, "WAL writer: periodic flush failed");
                // Back off so a failing disk doesn't spin this loop.
                flush_deadline = Instant::now() + FLUSH_INTERVAL;
            } else {
                dirty = false;
                head.store(WalOffset {
                    segment_id: current_segment_id,
                    byte_offset: current_byte_offset,
                });
                notify.notify_waiters();
                acknowledge_pending_durable_ok(&mut pending_durable_acks);
            }
        }

        // Wait for the next event. While clean, block indefinitely; while dirty,
        // block only until the flush deadline. `saturating_duration_since` yields
        // a non-negative timeout even if the deadline just elapsed (the loop top
        // will flush on the next iteration).
        let mut req = if dirty {
            match rx.recv_timeout(flush_deadline.saturating_duration_since(Instant::now())) {
                Ok(req) => req,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match rx.recv() {
                Ok(req) => req,
                Err(_) => break,
            }
        };

        if let Err(e) = codec::encode_into(&mut encode_buf, &req.event) {
            error!(error = %e, "WAL writer: failed to encode event; dropping event");
            if let Some(ack) = req.durable_ack.take() {
                let _ = ack.send(Err(format!("WAL writer encode failed: {e}")));
            }
            continue;
        }

        let record_len = encode_buf.len() as u64;

        if current_byte_offset > 0 && current_byte_offset + record_len > segment_bytes {
            if let Err(e) = writer.flush() {
                error!(error = %e, segment_id = current_segment_id, "WAL writer: flush failed before rotation; exiting");
                record_writer_fatal(WriterFatalReason::FlushBeforeRotation);
                if let Some(ack) = req.durable_ack.take() {
                    let _ = ack.send(Err(format!("WAL writer flush failed before rotation: {e}")));
                }
                fail_pending_durable_acks(
                    &mut pending_durable_acks,
                    format!("WAL writer flush failed before rotation: {e}"),
                );
                return;
            }
            head.store(WalOffset {
                segment_id: current_segment_id,
                byte_offset: current_byte_offset,
            });
            notify.notify_waiters();
            acknowledge_pending_durable_ok(&mut pending_durable_acks);
            drop(writer);

            current_segment_id += 1;
            current_byte_offset = 0;

            let path = segment_path(&dir, current_segment_id);
            let file = match OpenOptions::new().append(true).create(true).open(&path) {
                Ok(f) => f,
                Err(e) => {
                    error!(error = %e, path = %path.display(), "WAL writer: failed to open next segment; exiting");
                    record_writer_fatal(WriterFatalReason::OpenNextSegment);
                    if let Some(ack) = req.durable_ack.take() {
                        let _ =
                            ack.send(Err(format!("WAL writer failed to open next segment: {e}")));
                    }
                    fail_pending_durable_acks(
                        &mut pending_durable_acks,
                        format!("WAL writer failed to open next segment: {e}"),
                    );
                    return;
                }
            };
            writer = BufWriter::with_capacity(64 * 1024, file);
            record_segment_rotation();
        }

        if let Err(e) = writer.write_all(&encode_buf) {
            error!(error = %e, segment_id = current_segment_id, "WAL writer: write_all failed; exiting");
            record_writer_fatal(WriterFatalReason::WriteAll);
            if let Some(ack) = req.durable_ack.take() {
                let _ = ack.send(Err(format!("WAL writer write_all failed: {e}")));
            }
            fail_pending_durable_acks(
                &mut pending_durable_acks,
                format!("WAL writer write_all failed: {e}"),
            );
            return;
        }
        current_byte_offset += record_len;
        if let Some(ack) = req.durable_ack.take() {
            pending_durable_acks.push(ack);
        }

        // `head` (and the reader wake-up) is advanced only after a flush pushes
        // bytes out of this process buffer — see the flush at the top of the loop.
        // This is a page-cache durability boundary (`BufWriter::flush`), not a
        // storage-media fsync boundary. Advancing it here would expose
        // buffered-but-unflushed bytes to the reader, which reads from disk and
        // would busy-spin at EOF.
        if !dirty {
            dirty = true;
            flush_deadline = Instant::now() + FLUSH_INTERVAL;
        }
    }

    // Shutdown: all senders dropped. Flush whatever remains, publish head, then
    // wake the reader. Returning drops this thread's `notify`/`head` Arc clones,
    // letting the subscription's `strong_count == 1` shutdown detection fire.
    if let Err(e) = writer.flush() {
        warn!(error = %e, "WAL writer: final flush failed on shutdown");
        fail_pending_durable_acks(
            &mut pending_durable_acks,
            format!("WAL writer final flush failed on shutdown: {e}"),
        );
    } else {
        head.store(WalOffset {
            segment_id: current_segment_id,
            byte_offset: current_byte_offset,
        });
        acknowledge_pending_durable_ok(&mut pending_durable_acks);
    }
    notify.notify_waiters();
}

fn acknowledge_pending_durable_ok(pending: &mut Vec<DurableAck>) {
    for ack in pending.drain(..) {
        let _ = ack.send(Ok(()));
    }
}

fn fail_pending_durable_acks(pending: &mut Vec<DurableAck>, reason: String) {
    for ack in pending.drain(..) {
        let _ = ack.send(Err(reason.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::decode_from;
    use crate::infrastructure::wal::segment::list_segments;
    use crate::infrastructure::wal::test_support::sample_event;
    use std::fs;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use tempfile::tempdir;

    fn req(event: WalEvent) -> WriteRequest {
        WriteRequest {
            event,
            durable_ack: None,
        }
    }

    #[test]
    fn test_writer_fatal_reason_labels_are_stable() {
        assert_eq!(
            WriterFatalReason::FlushBeforeRotation.as_str(),
            "flush_before_rotation"
        );
        assert_eq!(
            WriterFatalReason::OpenNextSegment.as_str(),
            "open_next_segment"
        );
        assert_eq!(WriterFatalReason::WriteAll.as_str(), "write_all");
    }

    #[test]
    fn test_writer_rotation_metric_name_is_stable() {
        assert_eq!(WAL_SEGMENT_ROTATIONS_METRIC, "wal_segment_rotations_total");
    }

    async fn wait_for_head(handle: &WriterHandle, expected: WalOffset) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if handle.head.load() == expected {
                return;
            }
            handle.notify.notified().await;
        }
        panic!(
            "timed out waiting for head; got {:?}, expected {:?}",
            handle.head.load(),
            expected
        );
    }

    #[tokio::test]
    async fn test_spawn_writer_sender_drop_persists_single_event() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        let event = sample_event(1);
        handle.tx.send(req(event.clone())).unwrap();

        drop(handle.tx);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let bytes = fs::read(segment_path(dir.path(), 1)).unwrap();
        assert!(!bytes.is_empty(), "segment file should contain the record");

        let mut cur = Cursor::new(bytes);
        let (decoded, _) = decode_from(&mut cur).unwrap().expect("one event present");
        assert_eq!(decoded.topic, event.topic);
        assert_eq!(decoded.ts_ms, event.ts_ms);

        let trailing = decode_from(&mut cur).unwrap();
        assert!(trailing.is_none(), "only one record expected");
    }

    #[tokio::test]
    async fn test_spawn_writer_each_record_updates_head() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        let mut encode_buf = Vec::new();
        let ev1 = sample_event(1);
        codec::encode_into(&mut encode_buf, &ev1).unwrap();
        let len1 = encode_buf.len() as u64;

        let ev2 = sample_event(2);
        codec::encode_into(&mut encode_buf, &ev2).unwrap();
        let len2 = encode_buf.len() as u64;

        handle.tx.send(req(ev1)).unwrap();
        wait_for_head(
            &handle,
            WalOffset {
                segment_id: 1,
                byte_offset: len1,
            },
        )
        .await;

        handle.tx.send(req(ev2)).unwrap();
        wait_for_head(
            &handle,
            WalOffset {
                segment_id: 1,
                byte_offset: len1 + len2,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_spawn_writer_segment_threshold_exceeded_rotates_segment() {
        let dir = tempdir().unwrap();
        // Tiny segment_bytes guarantees rotation after the first record.
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 64, 8).unwrap();

        handle.tx.send(req(sample_event(1))).unwrap();
        handle.tx.send(req(sample_event(2))).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if handle.head.load().segment_id >= 2 {
                break;
            }
            handle.notify.notified().await;
        }
        assert!(
            handle.head.load().segment_id >= 2,
            "writer should have rotated to a new segment"
        );

        drop(handle.tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let segments = list_segments(dir.path()).unwrap();
        assert!(
            segments.len() >= 2,
            "expected at least two segment files, got {segments:?}"
        );
        assert_eq!(segments[0], 1);
        assert_eq!(segments[1], 2);
    }

    #[tokio::test]
    async fn test_spawn_writer_existing_segment_with_start_offset_appends_without_overwrite() {
        let dir = tempdir().unwrap();

        // Pre-populate segment 1 with one record using a first writer.
        let first = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        first.tx.send(req(sample_event(1))).unwrap();
        let mut buf = Vec::new();
        codec::encode_into(&mut buf, &sample_event(1)).unwrap();
        let first_len = buf.len() as u64;
        wait_for_head(
            &first,
            WalOffset {
                segment_id: 1,
                byte_offset: first_len,
            },
        )
        .await;
        drop(first.tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Reopen at the recovered offset; the new writer should append, not overwrite.
        let second = spawn_writer(dir.path().to_path_buf(), 1, first_len, 1024 * 1024, 8).unwrap();
        second.tx.send(req(sample_event(2))).unwrap();
        codec::encode_into(&mut buf, &sample_event(2)).unwrap();
        let second_len = buf.len() as u64;
        wait_for_head(
            &second,
            WalOffset {
                segment_id: 1,
                byte_offset: first_len + second_len,
            },
        )
        .await;
        drop(second.tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let bytes = fs::read(segment_path(dir.path(), 1)).unwrap();
        let mut cur = Cursor::new(bytes);
        let (a, _) = decode_from(&mut cur).unwrap().expect("first record");
        let (b, _) = decode_from(&mut cur).unwrap().expect("second record");
        assert_eq!(a.ts_ms, sample_event(1).ts_ms);
        assert_eq!(b.ts_ms, sample_event(2).ts_ms);
    }

    #[tokio::test]
    async fn test_spawn_writer_idle_does_not_notify_without_new_records() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        // Write one record and wait until it's flushed (head advanced). This
        // consumes the single notify that flush produces.
        let ev = sample_event(1);
        let mut buf = Vec::new();
        codec::encode_into(&mut buf, &ev).unwrap();
        let len = buf.len() as u64;
        handle.tx.send(req(ev)).unwrap();
        wait_for_head(
            &handle,
            WalOffset {
                segment_id: 1,
                byte_offset: len,
            },
        )
        .await;

        // The writer is now idle. Spanning many 5 ms flush ticks, a freshly
        // registered waiter must NOT be woken — proving the dirty flag suppresses
        // the periodic flush + notify when nothing new was written. Before the
        // fix, `notify_waiters` fired every tick and this would resolve at once.
        let woken = tokio::time::timeout(Duration::from_millis(60), handle.notify.notified()).await;
        assert!(
            woken.is_err(),
            "idle writer must not notify when no new records were written"
        );

        // A subsequent write must still wake the waiter (notify path intact).
        // Register the waiter *before* sending so the flush's `notify_waiters`
        // can't fire in the gap and be missed.
        let notified = handle.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        handle.tx.send(req(sample_event(2))).unwrap();
        let woken = tokio::time::timeout(Duration::from_millis(500), notified).await;
        assert!(
            woken.is_ok(),
            "a new record must still flush and notify the waiter"
        );
    }

    #[test]
    fn test_atomic_wal_offset_load_never_observes_torn_snapshot() {
        let head = Arc::new(AtomicWalOffset::new(WalOffset {
            segment_id: 1,
            byte_offset: 1_000_000,
        }));
        let stop = Arc::new(AtomicBool::new(false));

        let writer_head = head.clone();
        let writer_stop = stop.clone();
        let writer = thread::spawn(move || {
            let mut segment_id = 1u64;
            while !writer_stop.load(Ordering::Relaxed) {
                writer_head.store(WalOffset {
                    segment_id,
                    byte_offset: segment_id * 1_000_000,
                });
                segment_id = if segment_id == 1 { 2 } else { 1 };
            }
        });

        let mut torn = None;
        let deadline = Instant::now() + Duration::from_millis(120);
        while Instant::now() < deadline {
            let observed = head.load();
            let expected = observed.segment_id * 1_000_000;
            if observed.byte_offset != expected {
                torn = Some(observed);
                break;
            }
            std::hint::spin_loop();
        }

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();

        assert!(torn.is_none(), "observed torn WAL head snapshot: {torn:?}");
    }
}
