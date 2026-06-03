use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, Notify},
    time::{interval, MissedTickBehavior},
};
use tracing::{error, warn};

use crate::infrastructure::wal::{
    codec,
    segment::segment_path,
    types::{WalEvent, WalOffset},
};

pub(super) struct AtomicWalOffset {
    segment_id: AtomicU64,
    byte_offset: AtomicU64,
}

impl AtomicWalOffset {
    pub fn new(offset: WalOffset) -> Self {
        Self {
            segment_id: AtomicU64::new(offset.segment_id),
            byte_offset: AtomicU64::new(offset.byte_offset),
        }
    }

    pub fn load(&self) -> WalOffset {
        let segment_id = self.segment_id.load(Ordering::Acquire);
        let byte_offset = self.byte_offset.load(Ordering::Acquire);
        WalOffset {
            segment_id,
            byte_offset,
        }
    }

    pub fn store(&self, offset: WalOffset) {
        self.segment_id.store(offset.segment_id, Ordering::Release);
        self.byte_offset
            .store(offset.byte_offset, Ordering::Release);
    }
}

pub(super) struct WriterHandle {
    pub tx: mpsc::Sender<WalEvent>,
    pub notify: Arc<Notify>,
    pub head: Arc<AtomicWalOffset>,
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

    let (tx, rx) = mpsc::channel::<WalEvent>(queue_capacity);
    let notify = Arc::new(Notify::new());
    let head = Arc::new(AtomicWalOffset::new(WalOffset {
        segment_id: start_segment_id,
        byte_offset: start_byte_offset,
    }));

    let task_notify = notify.clone();
    let task_head = head.clone();

    tokio::spawn(writer_loop(
        dir,
        start_segment_id,
        start_byte_offset,
        segment_bytes,
        writer,
        rx,
        task_head,
        task_notify,
    ));

    Ok(WriterHandle { tx, notify, head })
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    dir: PathBuf,
    mut current_segment_id: u64,
    mut current_byte_offset: u64,
    segment_bytes: u64,
    mut writer: BufWriter<std::fs::File>,
    mut rx: mpsc::Receiver<WalEvent>,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
) {
    let mut encode_buf: Vec<u8> = Vec::with_capacity(4 * 1024);

    // `dirty` means the BufWriter may hold bytes for records not yet flushed to
    // disk and therefore not yet reflected in `head`. Set after each successful
    // `write_all`, cleared after a flush. The periodic flush_tick skips the
    // flush + `notify_waiters` entirely when not dirty, so an idle writer makes
    // no syscalls and no spurious wakeups (the 5 ms tick fires 200×/s).
    let mut dirty = false;

    let mut flush_tick = interval(Duration::from_millis(5));
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    flush_tick.tick().await;

    loop {
        tokio::select! {
            biased;

            maybe_ev = rx.recv() => {
                let Some(ev) = maybe_ev else {
                    if let Err(e) = writer.flush() {
                        warn!(error = %e, "WAL writer: final flush failed on shutdown");
                    } else {
                        head.store(WalOffset { segment_id: current_segment_id, byte_offset: current_byte_offset });
                    }
                    notify.notify_waiters();
                    return;
                };

                if let Err(e) = codec::encode_into(&mut encode_buf, &ev) {
                    error!(error = %e, "WAL writer: failed to encode event; dropping event");
                    continue;
                }

                let record_len = encode_buf.len() as u64;

                if current_byte_offset > 0 && current_byte_offset + record_len > segment_bytes {
                    if let Err(e) = writer.flush() {
                        error!(error = %e, segment_id = current_segment_id, "WAL writer: flush failed before rotation; exiting");
                        return;
                    }
                    drop(writer);

                    current_segment_id += 1;
                    current_byte_offset = 0;

                    let path = segment_path(&dir, current_segment_id);
                    let file = match OpenOptions::new().append(true).create(true).open(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            error!(error = %e, path = %path.display(), "WAL writer: failed to open next segment; exiting");
                            return;
                        }
                    };
                    writer = BufWriter::with_capacity(64 * 1024, file);
                }

                if let Err(e) = writer.write_all(&encode_buf) {
                    error!(error = %e, segment_id = current_segment_id, "WAL writer: write_all failed; exiting");
                    return;
                }
                current_byte_offset += record_len;
                dirty = true;

                // `head` (and the reader wake-up) is advanced only after a flush
                // makes the bytes durable on disk — see the flush_tick branch.
                // Advancing it here would expose buffered-but-unflushed bytes to
                // the reader, which reads from disk and would busy-spin at EOF.
            }

            _ = flush_tick.tick() => {
                // Nothing buffered since the last flush — skip the flush syscall
                // and the wakeup so an idle writer stays quiet.
                if !dirty {
                    continue;
                }

                if let Err(e) = writer.flush() {
                    warn!(error = %e, segment_id = current_segment_id, "WAL writer: periodic flush failed");
                    continue;
                }
                dirty = false;

                head.store(WalOffset { segment_id: current_segment_id, byte_offset: current_byte_offset });
                notify.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::wal::codec::decode_from;
    use crate::infrastructure::wal::segment::list_segments;
    use crate::model::messages::message::HandledMessage;
    use crate::model::messages::status::StatusMessage;
    use std::fs;
    use std::io::Cursor;
    use std::time::Instant;
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
    async fn spawn_writer_persists_single_event_after_sender_drop() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        let event = sample_event(1);
        handle.tx.send(event.clone()).await.unwrap();

        // Drop sender so writer task flushes + exits.
        drop(handle.tx);

        // Give the task time to flush and close.
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
    async fn spawn_writer_updates_head_after_each_record() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        let mut encode_buf = Vec::new();
        let ev1 = sample_event(1);
        codec::encode_into(&mut encode_buf, &ev1).unwrap();
        let len1 = encode_buf.len() as u64;

        let ev2 = sample_event(2);
        codec::encode_into(&mut encode_buf, &ev2).unwrap();
        let len2 = encode_buf.len() as u64;

        handle.tx.send(ev1).await.unwrap();
        wait_for_head(
            &handle,
            WalOffset {
                segment_id: 1,
                byte_offset: len1,
            },
        )
        .await;

        handle.tx.send(ev2).await.unwrap();
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
    async fn spawn_writer_rotates_segment_when_threshold_exceeded() {
        let dir = tempdir().unwrap();
        // Tiny segment_bytes guarantees rotation after the first record.
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 64, 8).unwrap();

        handle.tx.send(sample_event(1)).await.unwrap();
        handle.tx.send(sample_event(2)).await.unwrap();

        // Wait until head has advanced into segment 2.
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
    async fn spawn_writer_appends_to_existing_segment_using_start_offset() {
        let dir = tempdir().unwrap();

        // Pre-populate segment 1 with one record using a first writer.
        let first = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();
        first.tx.send(sample_event(1)).await.unwrap();
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
        second.tx.send(sample_event(2)).await.unwrap();
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
    async fn idle_writer_does_not_notify_when_no_new_records() {
        let dir = tempdir().unwrap();
        let handle = spawn_writer(dir.path().to_path_buf(), 1, 0, 1024 * 1024, 8).unwrap();

        // Write one record and wait until it's flushed (head advanced). This
        // consumes the single notify that flush produces.
        let ev = sample_event(1);
        let mut buf = Vec::new();
        codec::encode_into(&mut buf, &ev).unwrap();
        let len = buf.len() as u64;
        handle.tx.send(ev).await.unwrap();
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
        handle.tx.send(sample_event(2)).await.unwrap();
        let woken = tokio::time::timeout(Duration::from_millis(500), notified).await;
        assert!(
            woken.is_ok(),
            "a new record must still flush and notify the waiter"
        );
    }
}
