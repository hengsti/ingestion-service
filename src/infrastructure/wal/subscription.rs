use std::fs;
use std::sync::Arc;
use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    path::PathBuf,
};

use anyhow::{Context, Result};
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
            //       Err      – garbage payload: treat as torn (defensive).
            match codec::decode_from(reader) {
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

                // Treat decode errors the same as torn tail: don't propagate;
                // the writer will overwrite the bad bytes on next rotation/append.
                Err(e) => {
                    warn!(error = %e, "WAL subscription: decode error; treating as torn tail");
                    self.cur_reader = None;
                    if !self.wait_or_advance().await {
                        return None;
                    }
                }
            }
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
        //     `up_to.byte_offset`. `list_segments` returns ids ascending, so
        //     once we hit one >= up_to.segment_id we can stop scanning.
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

        Ok(())
    }

    async fn wait_or_advance(&mut self) -> bool {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let head = self.head.load();

        if head.segment_id > self.cur_segment_id {
            self.cur_segment_id += 1;
            self.cur_byte_offset = 0;
            return true;
        }

        if head.segment_id == self.cur_segment_id && head.byte_offset > self.cur_byte_offset {
            return true;
        }

        if Arc::strong_count(&self.notify) == 1 {
            return false;
        }

        notified.await;
        true
    }
}
