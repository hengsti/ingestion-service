pub mod influx;

use std::future::Future;
use std::pin::Pin;

use crate::infrastructure::wal::types::WalEvent;

/// Classifies a sink write failure so the forwarder can decide whether to hold
/// the batch (and the WAL cursor) or drop it.
#[derive(Debug)]
pub enum SinkError {
    /// Transient failure (network error, timeout, HTTP 5xx, 408, 429). The
    /// forwarder must keep the batch and retry without advancing the WAL cursor,
    /// so the WAL buffers across the outage and a crash replays the batch.
    Retryable(anyhow::Error),
    /// Permanent failure (e.g. HTTP 4xx malformed line protocol). The batch can
    /// never succeed; it is dropped and the cursor advances so it can't stall
    /// the pipeline forever.
    Permanent(anyhow::Error),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::Retryable(e) => write!(f, "retryable sink error: {e}"),
            SinkError::Permanent(e) => write!(f, "permanent sink error: {e}"),
        }
    }
}

impl std::error::Error for SinkError {}

/// A terminal destination for WAL events.
///
/// Implementors receive batches of [`WalEvent`]s drained from the WAL and are
/// responsible for persisting them. The forwarder advances the WAL cursor based
/// on the result returned here.
///
/// `write` returns a boxed `Send` future so the trait is object-safe
/// (`Arc<dyn Sink>`) and usable from spawned tasks — mirroring the
/// `PipelineStage` convention.
pub trait Sink: Send + Sync {
    /// Writes a batch of events to the underlying store.
    ///
    /// # Errors
    /// Returns [`SinkError::Retryable`] for transient failures (the forwarder
    /// holds the batch and retries) or [`SinkError::Permanent`] for failures
    /// that can never succeed (the forwarder drops the batch).
    fn write<'a>(
        &'a self,
        batch: &'a [WalEvent],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;
}
