pub mod influx;

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;

use crate::infrastructure::wal::types::WalEvent;

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
    /// Returns an error if the batch could not be persisted after all internal
    /// retries are exhausted.
    fn write<'a>(
        &'a self,
        batch: &'a [WalEvent],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}
