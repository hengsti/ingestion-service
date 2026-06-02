use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use metrics::counter;
use tokio::time::MissedTickBehavior;
use tracing::error;

use crate::infrastructure::sink::Sink;
use crate::infrastructure::wal::subscription::WalSubscription;
use crate::infrastructure::wal::types::{WalEntry, WalEvent};

/// Drains the WAL subscription, batches entries, writes them to `sink`, and
/// advances the WAL cursor on each successful (or terminally failed) flush.
///
/// A flush is triggered when either the batch reaches `batch_size` or the
/// `flush_interval_ms` ticker fires with a non-empty batch. The loop exits
/// cleanly when the subscription is closed (`next()` returns `None`).
///
/// # Errors
/// Returns an error only if committing the WAL cursor fails — sink write
/// failures are non-fatal (the batch is dropped and the cursor still advances,
/// matching the previous drop-after-3-retries semantics).
pub async fn run_forwarder(
    mut sub: WalSubscription,
    sink: Arc<dyn Sink>,
    batch_size: usize,
    flush_interval_ms: u64,
) -> Result<()> {
    let mut batch: Vec<WalEntry> = Vec::with_capacity(batch_size);
    let mut ticker = tokio::time::interval(Duration::from_millis(flush_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&sink, &mut batch, &mut sub).await?;
                }
            }
            maybe = sub.next() => {
                match maybe {
                    Some(entry) => {
                        batch.push(entry);
                        if batch.len() >= batch_size {
                            flush(&sink, &mut batch, &mut sub).await?;
                        }
                    }
                    None => return Ok(()), // wal closed
                }
            }
        }
    }
}

/// Writes the buffered batch to the sink and commits the WAL cursor up to the
/// end of the last entry. The cursor is advanced even on sink failure so a
/// poison batch cannot stall the pipeline forever.
async fn flush(
    sink: &Arc<dyn Sink>,
    batch: &mut Vec<WalEntry>,
    sub: &mut WalSubscription,
) -> Result<()> {
    let events: Vec<WalEvent> = batch.iter().map(|e| e.event.clone()).collect();
    let highest = batch
        .last()
        .expect("flush called with a non-empty batch")
        .offset_after;
    let count = batch.len() as u64;

    match sink.write(&events).await {
        Ok(()) => {
            counter!("wal_forwarder_committed_total").increment(count);
        }
        Err(e) => {
            error!(error = %e, count, "sink write failed; dropping batch and advancing cursor");
            counter!("wal_forwarder_drop_total").increment(count);
        }
    }

    // Advance the cursor regardless of sink outcome (drop-on-failure semantics).
    sub.commit(highest).await?;
    batch.clear();
    Ok(())
}
