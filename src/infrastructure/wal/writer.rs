use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use metrics::counter;
use tokio::sync::Notify;
use tracing::{error, warn};

#[cfg(test)]
use crate::infrastructure::wal::test_support::{run_test_io_hook, TestIoHookSlot, TestIoStep};
use crate::infrastructure::wal::{
    codec,
    segment::{segment_path, sync_directory},
    types::{WalEvent, WalOffset},
};

/// Maximum batching interval before the writer attempts a group sync.
/// Filesystem I/O and scheduling can add latency.
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
            // A single sequentially consistent order keeps the two fields and
            // both version reads coherent, including on weakly ordered CPUs.
            let start = self.version.load(Ordering::SeqCst);
            if start & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }

            let segment_id = self.segment_id.load(Ordering::SeqCst);
            let byte_offset = self.byte_offset.load(Ordering::SeqCst);

            let end = self.version.load(Ordering::SeqCst);
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
        let start = self.version.fetch_add(1, Ordering::SeqCst);
        debug_assert_eq!(start & 1, 0, "AtomicWalOffset assumes a single writer");

        self.segment_id.store(offset.segment_id, Ordering::SeqCst);
        self.byte_offset.store(offset.byte_offset, Ordering::SeqCst);

        self.version.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) type DurableAck = tokio::sync::oneshot::Sender<Result<(), String>>;

pub(super) struct WriteRequest {
    pub event: WalEvent,
    pub durable_ack: Option<DurableAck>,
}

#[derive(Default)]
pub(super) struct WriterState {
    failure: OnceLock<String>,
    stopped: AtomicBool,
}

impl WriterState {
    pub fn failure(&self) -> Option<&str> {
        self.failure.get().map(String::as_str)
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn fail(&self, reason: &str) {
        let _ = self.failure.set(reason.to_owned());
    }
}

struct WriterCompletion {
    state: Arc<WriterState>,
    notify: Arc<Notify>,
}

impl Drop for WriterCompletion {
    fn drop(&mut self) {
        if thread::panicking() {
            self.state.fail("WAL writer thread panicked");
        }
        self.state.stopped.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub(super) struct WriterHandle {
    pub tx: SyncSender<WriteRequest>,
    pub notify: Arc<Notify>,
    pub head: Arc<AtomicWalOffset>,
    pub state: Arc<WriterState>,
    #[cfg(test)]
    test_io_hook: TestIoHookSlot,
}

#[derive(Clone, Copy)]
enum WriterFatalReason {
    FlushOrSync,
    FlushBeforeRotation,
    OpenNextSegment,
    WriteAll,
}

impl WriterFatalReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::FlushOrSync => "flush_or_sync",
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
    let file = open_segment(
        &dir,
        start_segment_id,
        false,
        #[cfg(test)]
        None,
    )?;
    let writer = BufWriter::with_capacity(64 * 1024, file);

    let (tx, rx) = mpsc::sync_channel::<WriteRequest>(queue_capacity);
    let notify = Arc::new(Notify::new());
    let head = Arc::new(AtomicWalOffset::new(WalOffset {
        segment_id: start_segment_id,
        byte_offset: start_byte_offset,
    }));
    let state = Arc::new(WriterState::default());

    let task_notify = notify.clone();
    let task_head = head.clone();
    let task_state = state.clone();
    #[cfg(test)]
    let test_io_hook: TestIoHookSlot = Arc::new(std::sync::Mutex::new(None));
    #[cfg(test)]
    let task_test_io_hook = test_io_hook.clone();

    // The writer runs on a dedicated OS thread, not a tokio task: its file I/O is
    // blocking and would otherwise stall a runtime worker under disk pressure.
    // `head.store` and `Notify::notify_waiters` are both thread-safe and callable
    // from outside the runtime, so the reader still observes progress.
    thread::Builder::new()
        .name("wal-writer".to_string())
        .spawn(move || {
            let _completion = WriterCompletion {
                state: task_state.clone(),
                notify: task_notify.clone(),
            };
            writer_loop(
                dir,
                start_segment_id,
                start_byte_offset,
                segment_bytes,
                queue_capacity,
                writer,
                rx,
                task_head,
                task_notify,
                task_state,
                #[cfg(test)]
                task_test_io_hook,
            );
        })
        .context("spawning WAL writer thread")?;

    Ok(WriterHandle {
        tx,
        notify,
        head,
        state,
        #[cfg(test)]
        test_io_hook,
    })
}

fn open_segment(
    dir: &Path,
    segment_id: u64,
    new_segment: bool,
    #[cfg(test)] test_io_hook: Option<&TestIoHookSlot>,
) -> Result<File> {
    let path = segment_path(dir, segment_id);
    let mut options = OpenOptions::new();
    options.append(true);
    if new_segment {
        options.create_new(true);
    } else {
        options.create(true);
    }
    let file = options
        .open(&path)
        .with_context(|| format!("opening WAL segment {}", path.display()))?;
    #[cfg(test)]
    if let Some(hook) = test_io_hook {
        run_test_io_hook(hook, TestIoStep::SegmentFileSync)?;
    }
    file.sync_all()
        .with_context(|| format!("syncing WAL segment {}", path.display()))?;
    #[cfg(test)]
    if let Some(hook) = test_io_hook {
        run_test_io_hook(hook, TestIoStep::SegmentDirectorySync)?;
    }
    sync_directory(dir).with_context(|| format!("syncing WAL directory {}", dir.display()))?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn writer_loop(
    dir: PathBuf,
    mut current_segment_id: u64,
    mut current_byte_offset: u64,
    segment_bytes: u64,
    queue_capacity: usize,
    mut writer: BufWriter<File>,
    rx: Receiver<WriteRequest>,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
    state: Arc<WriterState>,
    #[cfg(test)] test_io_hook: TestIoHookSlot,
) {
    let mut encode_buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut pending_durable_acks: Vec<DurableAck> = Vec::new();

    // `dirty` means records have been written but not synced or published.
    // While clean, the loop blocks on `recv` without periodic syscalls.
    let mut dirty = false;
    // The oldest unsynced record arms the deadline; later writes cannot defer it.
    let mut flush_deadline = Instant::now();

    loop {
        // Flush at the top of the loop: under sustained load `recv_timeout` keeps
        // returning `Ok` and never reports a timeout, so the periodic flush must
        // be driven by this explicit deadline check rather than a timer branch.
        if dirty && Instant::now() >= flush_deadline {
            if let Err(e) = flush_and_sync(
                &mut writer,
                #[cfg(test)]
                &test_io_hook,
            ) {
                error!(error = %e, segment_id = current_segment_id, "WAL writer: flush or sync failed; exiting");
                record_writer_fatal(WriterFatalReason::FlushOrSync);
                fail_writer_requests(
                    &state,
                    None,
                    &mut pending_durable_acks,
                    &rx,
                    queue_capacity,
                    &format!("WAL writer flush or sync failed: {e}"),
                );
                return;
            }
            dirty = false;
            head.store(WalOffset {
                segment_id: current_segment_id,
                byte_offset: current_byte_offset,
            });
            notify.notify_waiters();
            acknowledge_pending_durable_ok(&mut pending_durable_acks);
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

        let exceeds_segment = current_byte_offset
            .checked_add(record_len)
            .is_none_or(|length| length > segment_bytes);
        if current_byte_offset > 0 && exceeds_segment {
            if dirty {
                if let Err(e) = flush_and_sync(
                    &mut writer,
                    #[cfg(test)]
                    &test_io_hook,
                ) {
                    error!(error = %e, segment_id = current_segment_id, "WAL writer: flush or sync failed before rotation; exiting");
                    record_writer_fatal(WriterFatalReason::FlushBeforeRotation);
                    fail_writer_requests(
                        &state,
                        req.durable_ack.take(),
                        &mut pending_durable_acks,
                        &rx,
                        queue_capacity,
                        &format!("WAL writer flush or sync failed before rotation: {e}"),
                    );
                    return;
                }
                head.store(WalOffset {
                    segment_id: current_segment_id,
                    byte_offset: current_byte_offset,
                });
                notify.notify_waiters();
                acknowledge_pending_durable_ok(&mut pending_durable_acks);
                dirty = false;
            }
            drop(writer);

            current_segment_id = match current_segment_id.checked_add(1) {
                Some(next) => next,
                None => {
                    let reason = "WAL segment identifier exhausted";
                    error!("{reason}");
                    record_writer_fatal(WriterFatalReason::OpenNextSegment);
                    fail_writer_requests(
                        &state,
                        req.durable_ack.take(),
                        &mut pending_durable_acks,
                        &rx,
                        queue_capacity,
                        reason,
                    );
                    return;
                }
            };
            current_byte_offset = 0;

            let path = segment_path(&dir, current_segment_id);
            let file = match open_segment(
                &dir,
                current_segment_id,
                true,
                #[cfg(test)]
                Some(&test_io_hook),
            ) {
                Ok(f) => f,
                Err(e) => {
                    error!(error = %e, path = %path.display(), "WAL writer: failed to open next segment; exiting");
                    record_writer_fatal(WriterFatalReason::OpenNextSegment);
                    fail_writer_requests(
                        &state,
                        req.durable_ack.take(),
                        &mut pending_durable_acks,
                        &rx,
                        queue_capacity,
                        &format!("WAL writer failed to open or sync next segment: {e:#}"),
                    );
                    return;
                }
            };
            writer = BufWriter::with_capacity(64 * 1024, file);
            record_segment_rotation();
        }

        if let Err(e) = write_record(
            &mut writer,
            &encode_buf,
            #[cfg(test)]
            &test_io_hook,
        ) {
            error!(error = %e, segment_id = current_segment_id, "WAL writer: write_all failed; exiting");
            record_writer_fatal(WriterFatalReason::WriteAll);
            fail_writer_requests(
                &state,
                req.durable_ack.take(),
                &mut pending_durable_acks,
                &rx,
                queue_capacity,
                &format!("WAL writer write_all failed: {e}"),
            );
            return;
        }
        current_byte_offset += record_len;
        if let Some(ack) = req.durable_ack.take() {
            pending_durable_acks.push(ack);
        }

        // The published head and receipts advance only after the group sync.
        if !dirty {
            dirty = true;
            flush_deadline = Instant::now() + FLUSH_INTERVAL;
        }
        // Bound receipts held outside the bounded input queue.
        if pending_durable_acks.len() >= queue_capacity.max(1) {
            flush_deadline = Instant::now();
        }
    }

    // Shutdown: all senders dropped. Sync the remaining group before publishing it.
    if dirty {
        if let Err(e) = flush_and_sync(
            &mut writer,
            #[cfg(test)]
            &test_io_hook,
        ) {
            warn!(error = %e, "WAL writer: final flush or sync failed on shutdown");
            record_writer_fatal(WriterFatalReason::FlushOrSync);
            let reason = format!("WAL writer final flush or sync failed on shutdown: {e}");
            state.fail(&reason);
            fail_pending_durable_acks(&mut pending_durable_acks, &reason);
        } else {
            head.store(WalOffset {
                segment_id: current_segment_id,
                byte_offset: current_byte_offset,
            });
            acknowledge_pending_durable_ok(&mut pending_durable_acks);
        }
    }
    notify.notify_waiters();
}

fn write_record(
    writer: &mut BufWriter<File>,
    bytes: &[u8],
    #[cfg(test)] test_io_hook: &TestIoHookSlot,
) -> std::io::Result<()> {
    #[cfg(test)]
    run_test_io_hook(test_io_hook, TestIoStep::Write)?;
    writer.write_all(bytes)
}

fn flush_and_sync(
    writer: &mut BufWriter<File>,
    #[cfg(test)] test_io_hook: &TestIoHookSlot,
) -> std::io::Result<()> {
    #[cfg(test)]
    run_test_io_hook(test_io_hook, TestIoStep::Flush)?;
    writer.flush()?;
    #[cfg(test)]
    run_test_io_hook(test_io_hook, TestIoStep::Sync)?;
    writer.get_ref().sync_data()
}

fn acknowledge_pending_durable_ok(pending: &mut Vec<DurableAck>) {
    for ack in pending.drain(..) {
        let _ = ack.send(Ok(()));
    }
}

fn fail_pending_durable_acks(pending: &mut Vec<DurableAck>, reason: &str) {
    for ack in pending.drain(..) {
        let _ = ack.send(Err(reason.to_owned()));
    }
}

fn fail_writer_requests(
    state: &WriterState,
    current: Option<DurableAck>,
    pending: &mut Vec<DurableAck>,
    rx: &Receiver<WriteRequest>,
    queue_capacity: usize,
    reason: &str,
) {
    state.fail(reason);
    if let Some(ack) = current {
        let _ = ack.send(Err(reason.to_owned()));
    }
    fail_pending_durable_acks(pending, reason);
    for mut request in rx.try_iter().take(queue_capacity) {
        if let Some(ack) = request.durable_ack.take() {
            let _ = ack.send(Err(reason.to_owned()));
        }
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

    #[tokio::test]
    async fn test_wal_sync_receipt_and_head_wait_for_storage_sync() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        let (write_entered_tx, write_entered_rx) = tokio::sync::oneshot::channel();
        let write_entered_tx = std::sync::Mutex::new(Some(write_entered_tx));
        let (write_release_tx, write_release_rx) = mpsc::channel();
        let write_release_rx = std::sync::Mutex::new(write_release_rx);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = std::sync::Mutex::new(Some(entered_tx));
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        *handle.test_io_hook.lock().unwrap() = Some(Arc::new(move |step| {
            if step == TestIoStep::Write {
                if let Some(tx) = write_entered_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                    write_release_rx
                        .lock()
                        .unwrap()
                        .recv()
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                }
            }
            if step == TestIoStep::Sync {
                if let Some(tx) = entered_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                    release_rx
                        .lock()
                        .unwrap()
                        .recv()
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                }
            }
            Ok(())
        }));
        let (ack_tx, mut ack_rx) = tokio::sync::oneshot::channel();

        handle
            .tx
            .send(WriteRequest {
                event: sample_event(1),
                durable_ack: Some(ack_tx),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), write_entered_rx)
            .await
            .unwrap()
            .unwrap();
        let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(WriteRequest {
                event: sample_event(2),
                durable_ack: Some(second_tx),
            })
            .unwrap();
        write_release_tx.send(()).unwrap();

        tokio::select! {
            entered = entered_rx => assert!(entered.is_ok(), "sync hook was dropped"),
            receipt = &mut ack_rx => panic!("receipt resolved before storage sync: {receipt:?}"),
            _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("writer never attempted storage sync"),
        }
        assert!(matches!(
            ack_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            handle.head.load(),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            }
        );

        release_tx.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .unwrap()
            .unwrap()
            .is_ok());
        assert!(tokio::time::timeout(Duration::from_secs(2), second_rx)
            .await
            .unwrap()
            .unwrap()
            .is_ok());
        assert!(handle.head.load().byte_offset > 0);
    }

    #[tokio::test]
    async fn test_wal_sync_write_flush_and_sync_failures_reject_receipts() {
        for failed_step in [TestIoStep::Write, TestIoStep::Flush, TestIoStep::Sync] {
            let dir = tempdir().unwrap();
            let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
            *handle.test_io_hook.lock().unwrap() = Some(Arc::new(move |step| {
                if step == failed_step {
                    return Err(std::io::Error::other("injected WAL I/O failure"));
                }
                Ok(())
            }));
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

            handle
                .tx
                .send(WriteRequest {
                    event: sample_event(1),
                    durable_ack: Some(ack_tx),
                })
                .unwrap();

            let result = tokio::time::timeout(Duration::from_secs(2), ack_rx)
                .await
                .expect("receipt must not hang")
                .expect("writer must explicitly fail the receipt");
            assert!(result.is_err(), "{failed_step:?} failure was acknowledged");
            assert_eq!(
                handle.head.load(),
                WalOffset {
                    segment_id: 1,
                    byte_offset: 0,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_wal_sync_failure_explicitly_fails_queued_receipts() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = std::sync::Mutex::new(Some(entered_tx));
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        *handle.test_io_hook.lock().unwrap() = Some(Arc::new(move |step| {
            if step == TestIoStep::Sync {
                if let Some(tx) = entered_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                release_rx.lock().unwrap().recv().unwrap();
                return Err(std::io::Error::other("injected WAL sync failure"));
            }
            Ok(())
        }));
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(WriteRequest {
                event: sample_event(1),
                durable_ack: Some(first_tx),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();

        let mut receipts = vec![first_rx];
        for sequence in 2..=3 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(WriteRequest {
                    event: sample_event(sequence),
                    durable_ack: Some(ack_tx),
                })
                .unwrap();
            receipts.push(ack_rx);
        }
        release_tx.send(()).unwrap();

        for receipt in receipts {
            let outcome = tokio::time::timeout(Duration::from_secs(2), receipt)
                .await
                .expect("receipt must not hang")
                .expect("writer must explicitly fail queued receipts");
            assert!(outcome.is_err());
        }
        assert!(handle.state.failure().unwrap().contains("sync"));
        assert_eq!(
            handle.head.load(),
            WalOffset {
                segment_id: 1,
                byte_offset: 0,
            }
        );
    }

    #[tokio::test]
    async fn test_wal_sync_rotation_metadata_failure_rejects_new_record() {
        for failed_step in [
            TestIoStep::SegmentFileSync,
            TestIoStep::SegmentDirectorySync,
        ] {
            let dir = tempdir().unwrap();
            let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1, 8).unwrap();
            let (first_tx, first_rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(WriteRequest {
                    event: sample_event(1),
                    durable_ack: Some(first_tx),
                })
                .unwrap();
            assert!(tokio::time::timeout(Duration::from_secs(2), first_rx)
                .await
                .unwrap()
                .unwrap()
                .is_ok());
            let first_head = handle.head.load();
            *handle.test_io_hook.lock().unwrap() = Some(Arc::new(move |step| {
                if step == failed_step {
                    return Err(std::io::Error::other(
                        "injected segment metadata sync failure",
                    ));
                }
                Ok(())
            }));
            let (second_tx, second_rx) = tokio::sync::oneshot::channel();

            handle
                .tx
                .send(WriteRequest {
                    event: sample_event(2),
                    durable_ack: Some(second_tx),
                })
                .unwrap();

            let result = tokio::time::timeout(Duration::from_secs(2), second_rx)
                .await
                .expect("receipt must not hang")
                .expect("writer must explicitly report the failure");
            assert!(result.is_err(), "{failed_step:?} failure was acknowledged");
            assert_eq!(handle.head.load(), first_head);
            assert!(handle.state.failure().is_some());
        }
    }

    #[tokio::test]
    async fn test_wal_sync_shutdown_drains_pending_receipts() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        let mut receipts = Vec::new();
        for sequence in 1..=3 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(WriteRequest {
                    event: sample_event(sequence),
                    durable_ack: Some(ack_tx),
                })
                .unwrap();
            receipts.push(ack_rx);
        }

        drop(handle.tx);
        for receipt in receipts {
            assert!(tokio::time::timeout(Duration::from_secs(2), receipt)
                .await
                .expect("shutdown receipt must not hang")
                .expect("writer must resolve the receipt")
                .is_ok());
        }
        assert!(handle.head.load().byte_offset > 0);
    }

    #[tokio::test]
    async fn test_wal_sync_shutdown_failure_fails_pending_receipts() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        *handle.test_io_hook.lock().unwrap() = Some(Arc::new(|step| {
            if step == TestIoStep::Sync {
                return Err(std::io::Error::other("injected shutdown sync failure"));
            }
            Ok(())
        }));
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(WriteRequest {
                event: sample_event(1),
                durable_ack: Some(ack_tx),
            })
            .unwrap();

        drop(handle.tx);
        let result = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("shutdown receipt must not hang")
            .expect("writer must explicitly fail the receipt");
        assert!(result.is_err());
        assert_eq!(handle.head.load().byte_offset, 0);
        assert!(handle.state.failure().is_some());
    }

    #[tokio::test]
    async fn test_wal_sync_writer_panic_wakes_waiters_and_reports_failure() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        *handle.test_io_hook.lock().unwrap() = Some(Arc::new(|step| {
            if step == TestIoStep::Write {
                panic!("injected writer panic");
            }
            Ok(())
        }));
        let notified = handle.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        handle
            .tx
            .send(WriteRequest {
                event: sample_event(1),
                durable_ack: Some(ack_tx),
            })
            .unwrap();

        assert!(tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("receipt must not hang")
            .is_err());
        tokio::time::timeout(Duration::from_secs(2), notified)
            .await
            .expect("subscription must be woken");
        assert!(handle.state.stopped());
        assert!(handle.state.failure().unwrap().contains("panicked"));
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
