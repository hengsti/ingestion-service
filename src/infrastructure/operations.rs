use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
    Arc, OnceLock, Weak,
};

use serde::Serialize;
use tokio::sync::mpsc;

use super::source::{IngestDispatcher, IngestJob};
use super::wal::capacity::Capacity;

/// Fixed operational states of the output sink.
#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum SinkState {
    /// No failed write is outstanding.
    Healthy,
    /// A recoverable sink write is being retried.
    Retrying,
    /// A permanent sink failure requires operator repair.
    Blocked,
}

impl SinkState {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Retrying => "retrying",
            Self::Blocked => "blocked",
        }
    }
}

/// Shared forwarder progress; contains no payloads or error strings.
#[derive(Default)]
pub struct ForwarderState {
    pub(crate) alive: Arc<AtomicBool>,
    pub(crate) read_failed: AtomicBool,
    pub(crate) commit_retrying: AtomicBool,
    pub(crate) batch_pending: AtomicBool,
    sink: AtomicU8,
    oldest_pending_ms: AtomicI64,
}

impl ForwarderState {
    /// Shares task liveness independently of sink retry state.
    pub fn liveness_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.alive)
    }

    pub(crate) fn set_sink(&self, state: SinkState) {
        self.sink.store(state as u8, Ordering::Relaxed);
    }

    pub(crate) fn sink(&self) -> SinkState {
        match self.sink.load(Ordering::Relaxed) {
            1 => SinkState::Retrying,
            2 => SinkState::Blocked,
            _ => SinkState::Healthy,
        }
    }

    pub(crate) fn set_oldest_pending(&self, timestamp_ms: i64) {
        self.oldest_pending_ms
            .store(timestamp_ms, Ordering::Relaxed);
    }

    pub(crate) fn oldest_pending_age_seconds(&self, now_ms: i64) -> f64 {
        let timestamp = self.oldest_pending_ms.load(Ordering::Relaxed);
        if timestamp == 0 {
            0.0
        } else {
            now_ms.saturating_sub(timestamp).max(0) as f64 / 1000.0
        }
    }
}

/// Cheap shared intake, storage and forwarder state for HTTP and metrics.
pub struct Operations {
    pub(crate) source_ready: Arc<AtomicBool>,
    pub(crate) workers_alive: Arc<AtomicBool>,
    pub(crate) accepting: AtomicBool,
    pub(crate) admission_paused: AtomicBool,
    worker_queues: OnceLock<Weak<[mpsc::Sender<IngestJob>]>>,
    pub(crate) pending_acks: AtomicUsize,
    pub(crate) queue_available: AtomicUsize,
    pub(crate) queue_capacity: AtomicUsize,
    pub(crate) forwarder: Arc<ForwarderState>,
    pub(crate) wal: Arc<Capacity>,
    pub(crate) quarantine: Arc<Capacity>,
}

impl Operations {
    /// Combines existing journal and forwarder snapshots without owning their tasks.
    pub fn new(
        source_ready: Arc<AtomicBool>,
        forwarder: Arc<ForwarderState>,
        wal: Arc<Capacity>,
        quarantine: Arc<Capacity>,
    ) -> Self {
        Self {
            source_ready,
            workers_alive: Arc::new(AtomicBool::new(true)),
            accepting: AtomicBool::new(true),
            admission_paused: AtomicBool::new(false),
            worker_queues: OnceLock::new(),
            pending_acks: AtomicUsize::new(0),
            queue_available: AtomicUsize::new(0),
            queue_capacity: AtomicUsize::new(0),
            forwarder,
            wal,
            quarantine,
        }
    }

    /// Publishes bounded worker admission and current-generation pending ACK counts.
    pub fn update_admission(&self, available: usize, capacity: usize, pending_acks: usize) {
        self.queue_available.store(available, Ordering::Relaxed);
        self.queue_capacity.store(capacity, Ordering::Relaxed);
        self.pending_acks.store(pending_acks, Ordering::Relaxed);
    }

    pub(crate) fn bind_dispatcher(&self, dispatcher: &IngestDispatcher) {
        if self.worker_queues.get().is_none() {
            let _ = self.worker_queues.set(dispatcher.queue_handle());
        }
    }

    pub(crate) fn queue_snapshot(&self) -> (usize, usize) {
        match self.worker_queues.get() {
            Some(queues) => match queues.upgrade() {
                Some(queues) => queues.iter().fold((0, 0), |(available, total), sender| {
                    (
                        available
                            + if sender.is_closed() {
                                0
                            } else {
                                sender.capacity()
                            },
                        total + sender.max_capacity(),
                    )
                }),
                None => (0, 0),
            },
            None => (
                self.queue_available.load(Ordering::Relaxed),
                self.queue_capacity.load(Ordering::Relaxed),
            ),
        }
    }

    pub(crate) fn admission_available(&self) -> bool {
        !self.admission_paused.load(Ordering::Relaxed) && self.queue_snapshot().0 > 0
    }

    /// Returns whether durable intake can currently admit bounded work.
    pub fn ready(&self) -> bool {
        self.accepting.load(Ordering::Relaxed)
            && self.source_ready.load(Ordering::Relaxed)
            && self.workers_alive.load(Ordering::Relaxed)
            && self.admission_available()
            && self.wal.is_writable()
            && self.quarantine.is_writable()
            && self.wal.has_room_for_max_record()
            && self.quarantine.has_room_for_max_record()
            && self.forwarder.alive.load(Ordering::Relaxed)
            && !self.forwarder.read_failed.load(Ordering::Relaxed)
            && !self.forwarder.commit_retrying.load(Ordering::Relaxed)
    }

    pub(crate) fn snapshot(&self) -> OperationsSnapshot {
        let (wal_bytes, wal_budget_bytes) = self.wal.snapshot();
        let (quarantine_bytes, quarantine_budget_bytes) = self.quarantine.snapshot();
        let (queue_available, queue_capacity) = self.queue_snapshot();
        OperationsSnapshot {
            ready: self.ready(),
            source_ready: self.source_ready.load(Ordering::Relaxed),
            workers_alive: self.workers_alive.load(Ordering::Relaxed),
            accepting: self.accepting.load(Ordering::Relaxed),
            admission_available: self.admission_available(),
            wal_writable: self.wal.is_writable(),
            quarantine_writable: self.quarantine.is_writable(),
            forwarder_alive: self.forwarder.alive.load(Ordering::Relaxed),
            read_failed: self.forwarder.read_failed.load(Ordering::Relaxed),
            commit_retrying: self.forwarder.commit_retrying.load(Ordering::Relaxed),
            sink: self.forwarder.sink().label(),
            wal_bytes,
            wal_budget_bytes,
            quarantine_bytes,
            quarantine_budget_bytes,
            pending_acks: self.pending_acks.load(Ordering::Relaxed),
            queue_available,
            queue_capacity,
            oldest_pending_output_age_seconds: self
                .forwarder
                .oldest_pending_age_seconds(chrono::Utc::now().timestamp_millis()),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct OperationsSnapshot {
    ready: bool,
    source_ready: bool,
    workers_alive: bool,
    accepting: bool,
    admission_available: bool,
    wal_writable: bool,
    quarantine_writable: bool,
    forwarder_alive: bool,
    read_failed: bool,
    commit_retrying: bool,
    sink: &'static str,
    wal_bytes: u64,
    wal_budget_bytes: u64,
    quarantine_bytes: u64,
    quarantine_budget_bytes: u64,
    pending_acks: usize,
    queue_available: usize,
    queue_capacity: usize,
    oldest_pending_output_age_seconds: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_durable_readiness_recovers_from_pressure_but_not_worker_exit() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let capacity = Capacity::recover(directory.path(), 128 * 1024 * 1024)?;
        let forwarder = Arc::new(ForwarderState::default());
        forwarder.alive.store(true, Ordering::Relaxed);
        let state = Operations::new(
            Arc::new(AtomicBool::new(true)),
            forwarder,
            capacity.clone(),
            capacity,
        );
        state.update_admission(1, 1, 0);
        assert!(state.ready());
        state.update_admission(0, 1, 1);
        assert!(!state.ready());
        state.update_admission(1, 1, 0);
        assert!(state.ready());
        state.workers_alive.store(false, Ordering::Relaxed);
        state.source_ready.store(true, Ordering::Relaxed);
        assert!(!state.ready(), "SubAck cannot resurrect a dead worker");
        Ok(())
    }

    #[test]
    fn test_durable_readiness_storage_reader_commit_and_shutdown_faults() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let capacity = Capacity::recover(directory.path(), 128 * 1024 * 1024)?;
        let forwarder = Arc::new(ForwarderState::default());
        forwarder.alive.store(true, Ordering::Relaxed);
        let state = Operations::new(
            Arc::new(AtomicBool::new(true)),
            forwarder,
            capacity.clone(),
            capacity,
        );
        state.update_admission(1, 1, 0);
        for flag in [
            &state.forwarder.read_failed,
            &state.forwarder.commit_retrying,
        ] {
            flag.store(true, Ordering::Relaxed);
            assert!(!state.ready());
            flag.store(false, Ordering::Relaxed);
            assert!(state.ready());
        }
        state.accepting.store(false, Ordering::Relaxed);
        assert!(!state.ready());
        state.accepting.store(true, Ordering::Relaxed);
        state.quarantine.mark_unwritable();
        assert!(!state.ready());
        Ok(())
    }
}
