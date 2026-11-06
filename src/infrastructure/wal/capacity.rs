use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context, Result};
use tokio::sync::Notify;

use crate::infrastructure::wal::{codec::MAX_WAL_RECORD_PAYLOAD_BYTES, segment};

pub(crate) const METADATA_RESERVE_BYTES: u64 = 64 * 1024;
pub(crate) const SEGMENT_ALLOWANCE_BYTES: u64 = 4 * 1024;
const EXISTING_METADATA_INCLUDED_BYTES: u64 = METADATA_RESERVE_BYTES / 2;
const MAX_FRAME_BYTES: u64 = MAX_WAL_RECORD_PAYLOAD_BYTES as u64 + 4;

pub(crate) fn minimum_capacity(segment_bytes: u64) -> Result<u64> {
    if segment_bytes == 0 {
        bail!("WAL_SEGMENT_BYTES must be greater than zero");
    }
    // Keep room for an empty post-rotation segment and one possible new segment.
    segment_bytes
        .max(MAX_FRAME_BYTES)
        .checked_add(SEGMENT_ALLOWANCE_BYTES)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(METADATA_RESERVE_BYTES))
        .and_then(|bytes| bytes.checked_add(SEGMENT_ALLOWANCE_BYTES))
        .context("journal minimum capacity calculation overflowed")
}

pub(crate) fn validate_limit(name: &str, max_bytes: u64, segment_bytes: u64) -> Result<()> {
    let minimum = minimum_capacity(segment_bytes)?;
    if max_bytes < minimum {
        bail!("{name} must be at least {minimum} bytes for the configured segment size");
    }
    Ok(())
}

/// Lock-free snapshot and admission accounting for one journal.
pub struct Capacity {
    max_bytes: u64,
    used_bytes: AtomicU64,
    excess_metadata_bytes: u64,
    notify: Notify,
}

impl Capacity {
    pub(crate) fn recover(dir: &Path, max_bytes: u64) -> Result<Arc<Self>> {
        let segments = segment::list_segments(dir)?;
        let mut used = METADATA_RESERVE_BYTES;
        if segments.is_empty() {
            used = used
                .checked_add(SEGMENT_ALLOWANCE_BYTES)
                .context("journal capacity calculation overflowed")?;
        }
        for id in segments {
            let path = segment::segment_path(dir, id);
            let bytes = fs::metadata(&path)
                .with_context(|| format!("reading journal segment size {}", path.display()))?
                .len();
            used = used
                .checked_add(bytes)
                .and_then(|sum| sum.checked_add(SEGMENT_ALLOWANCE_BYTES))
                .context("journal capacity calculation overflowed")?;
        }
        let mut metadata_bytes = 0u64;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if matches!(
                name,
                ".journal.lock" | "commit.cursor" | "commit.cursor.tmp"
            ) || name.starts_with(".journal-probe-")
            {
                let path = entry.path();
                let bytes = fs::metadata(&path)
                    .with_context(|| format!("reading journal metadata size {}", path.display()))?
                    .len();
                metadata_bytes = metadata_bytes
                    .checked_add(bytes)
                    .context("journal metadata capacity calculation overflowed")?;
            }
        }
        // Keep half the reserve free for future cursor and manifest updates.
        let excess_metadata_bytes = metadata_bytes.saturating_sub(EXISTING_METADATA_INCLUDED_BYTES);
        used = used
            .checked_add(excess_metadata_bytes)
            .context("journal capacity calculation overflowed")?;
        if used > max_bytes {
            bail!("journal uses {used} accounted bytes, exceeding its {max_bytes}-byte limit");
        }
        Ok(Arc::new(Self {
            max_bytes,
            used_bytes: AtomicU64::new(used),
            excess_metadata_bytes,
            notify: Notify::new(),
        }))
    }

    pub(super) fn validate_recovered_active(
        &self,
        active_bytes: u64,
        retained_previous_bytes: Option<u64>,
    ) -> Result<()> {
        let retained_previous = match retained_previous_bytes {
            Some(bytes) => bytes
                .checked_add(SEGMENT_ALLOWANCE_BYTES)
                .context("recovered journal minimum capacity calculation overflowed")?,
            None => 0,
        };
        let minimum = METADATA_RESERVE_BYTES
            .checked_add(self.excess_metadata_bytes)
            .and_then(|bytes| bytes.checked_add(SEGMENT_ALLOWANCE_BYTES))
            .and_then(|bytes| bytes.checked_add(active_bytes))
            .and_then(|bytes| bytes.checked_add(retained_previous))
            .and_then(|bytes| bytes.checked_add(SEGMENT_ALLOWANCE_BYTES))
            .and_then(|bytes| bytes.checked_add(MAX_FRAME_BYTES))
            .context("recovered journal minimum capacity calculation overflowed")?;
        if self.max_bytes < minimum {
            bail!(
                "WAL_MAX_BYTES must be at least {minimum} bytes for the recovered active segment and retained rotation headroom"
            );
        }
        Ok(())
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.max_bytes)
            })
            .ok()?;
        Some(Reservation {
            capacity: Arc::clone(self),
            bytes,
            io_started: false,
        })
    }

    pub(crate) fn release(&self, bytes: u64) {
        if bytes != 0 {
            if let Err(used) =
                self.used_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        used.checked_sub(bytes)
                    })
            {
                tracing::error!(used, bytes, "journal capacity accounting underflow");
                return;
            }
            self.notify.notify_waiters();
        }
    }

    /// Returns currently accounted bytes and the configured maximum.
    pub fn snapshot(&self) -> (u64, u64) {
        (self.used_bytes.load(Ordering::Acquire), self.max_bytes)
    }

    pub(crate) fn has_room_for_max_record(&self) -> bool {
        let used = self.used_bytes.load(Ordering::Acquire);
        self.max_bytes.saturating_sub(used) >= MAX_FRAME_BYTES + SEGMENT_ALLOWANCE_BYTES
    }

    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub(crate) fn wake_waiters(&self) {
        self.notify.notify_waiters();
    }
}

pub(super) struct Reservation {
    capacity: Arc<Capacity>,
    bytes: u64,
    io_started: bool,
}

impl Reservation {
    pub(super) fn mark_io_started(&mut self) {
        // A failed or interrupted write may still have reached the filesystem.
        self.io_started = true;
    }

    pub(super) fn retain(mut self, bytes: u64) {
        debug_assert!(bytes <= self.bytes);
        self.capacity.release(self.bytes - bytes);
        self.bytes = 0;
    }

    pub(super) fn retain_all(mut self) {
        self.bytes = 0;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.io_started {
            self.capacity.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Barrier};
    use std::thread;

    use super::*;

    #[test]
    fn test_wal_capacity_concurrent_reservations_cannot_over_admit() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let base = METADATA_RESERVE_BYTES + SEGMENT_ALLOWANCE_BYTES;
        let capacity = Capacity::recover(dir.path(), base + 3 * 4096)?;
        let barrier = Arc::new(Barrier::new(9));
        let (tx, rx) = mpsc::channel();
        let mut threads = Vec::new();

        for _ in 0..8 {
            let capacity = Arc::clone(&capacity);
            let barrier = Arc::clone(&barrier);
            let tx = tx.clone();
            threads.push(thread::spawn(move || {
                barrier.wait();
                let reservation = capacity.reserve(4096);
                tx.send(reservation.is_some()).expect("report reservation");
                barrier.wait();
                drop(reservation);
            }));
        }
        drop(tx);
        barrier.wait();
        let admitted = rx.iter().take(8).filter(|admitted| *admitted).count();
        let accounted = capacity.snapshot().0;
        barrier.wait();
        for thread in threads {
            thread.join().expect("reservation thread");
        }
        assert_eq!(admitted, 3);
        assert_eq!(accounted, base + 3 * 4096);
        assert_eq!(capacity.snapshot().0, base);
        Ok(())
    }

    #[test]
    fn test_wal_capacity_reservation_drop_preserves_possible_disk_writes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let capacity = Capacity::recover(dir.path(), 1024 * 1024)?;
        let initial = capacity.snapshot().0;

        drop(capacity.reserve(100).expect("pending reservation"));
        assert_eq!(capacity.snapshot().0, initial);

        let mut writing = capacity.reserve(100).expect("write reservation");
        writing.mark_io_started();
        drop(writing);
        assert_eq!(capacity.snapshot().0, initial + 100);
        Ok(())
    }

    #[test]
    fn test_wal_capacity_minimum_includes_rotation_headroom() -> Result<()> {
        let segment_bytes = 2 * 1024 * 1024;
        let minimum = minimum_capacity(segment_bytes)?;
        let error = validate_limit("WAL_MAX_BYTES", minimum - 1, segment_bytes)
            .expect_err("insufficient rotation capacity must fail");
        assert!(error.to_string().contains(&minimum.to_string()));
        assert!(validate_limit("WAL_MAX_BYTES", minimum, segment_bytes).is_ok());
        assert!(minimum_capacity(0).is_err());
        assert!(minimum_capacity(u64::MAX).is_err());
        Ok(())
    }

    #[test]
    fn test_wal_capacity_counts_existing_journal_metadata() -> Result<()> {
        let base = METADATA_RESERVE_BYTES + SEGMENT_ALLOWANCE_BYTES;
        for name in [
            ".journal.lock",
            "commit.cursor",
            "commit.cursor.tmp",
            ".journal-probe-stale.tmp",
        ] {
            let dir = tempfile::tempdir()?;
            fs::write(dir.path().join(name), vec![0; 128 * 1024])?;
            assert!(
                Capacity::recover(
                    dir.path(),
                    base + 128 * 1024 - EXISTING_METADATA_INCLUDED_BYTES - 1,
                )
                .is_err(),
                "existing journal metadata {name} must count against the limit"
            );
        }
        Ok(())
    }
}
