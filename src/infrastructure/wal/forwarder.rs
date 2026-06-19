use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use metrics::counter;
use tokio::time::MissedTickBehavior;
use tracing::{error, warn};

use crate::infrastructure::sink::{Sink, SinkError};
use crate::infrastructure::wal::subscription::WalSubscription;
use crate::infrastructure::wal::types::{WalEntry, WalEvent};

/// Upper bound on the exponential retry backoff while a retryable sink outage
/// persists. Keeps the hold-and-retry loop from sleeping unbounded.
const RETRY_BACKOFF_CAP_MS: u64 = 5_000;

/// Initial retry backoff is clamped to this so a large `flush_interval_ms`
/// doesn't delay recovery from a brief outage.
const RETRY_BACKOFF_START_CAP_MS: u64 = 1_000;

/// Drains the WAL subscription, batches entries, writes them to `sink`, and
/// advances the WAL cursor on each successful (or permanently failed) flush.
///
/// A flush is triggered when either the batch reaches `batch_size` or the
/// `flush_interval_ms` ticker fires with a non-empty batch. The loop exits
/// cleanly when the subscription is closed (`next()` returns `None`).
///
/// On a *retryable* sink failure the batch is held and retried with bounded
/// backoff without advancing the cursor, so the WAL buffers across the outage;
/// new entries accumulate on disk (not in RAM) because `next()` isn't polled
/// while a flush is in flight.
///
/// # Errors
/// This function only returns `Ok(())` on normal shutdown. Sink failures are
/// handled in-loop (retried when transient, dropped when permanent), and cursor
/// commit failures are retried in-loop until durable.
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
                    flush(&sink, &mut batch, &mut sub, flush_interval_ms).await?;
                }
            }
            maybe = sub.next() => {
                match maybe {
                    Some(entry) => {
                        batch.push(entry);
                        if batch.len() >= batch_size {
                            flush(&sink, &mut batch, &mut sub, flush_interval_ms).await?;
                        }
                    }
                    None => {
                        // WAL closed (writer gone): flush whatever is still
                        // buffered so the final partial batch is persisted and
                        // committed, then exit. Without this the last batch would
                        // be silently dropped on shutdown.
                        if !batch.is_empty() {
                            flush(&sink, &mut batch, &mut sub, flush_interval_ms).await?;
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Writes the buffered batch to the sink and commits the WAL cursor up to the
/// end of the last entry.
///
/// - On success the cursor advances and the batch clears.
/// - On a *permanent* sink error the batch is dropped, a metric is recorded, and
///   the cursor still advances so a poison batch can't stall the pipeline.
/// - On a *retryable* sink error the batch is held and retried with exponential
///   backoff (capped at [`RETRY_BACKOFF_CAP_MS`]) and the cursor is **not**
///   advanced, so the WAL buffers the outage and a crash replays the batch.
/// - After a terminal sink outcome (success or permanent drop), cursor commits
///   are retried with bounded backoff until durable before accepting more WAL.
async fn flush(
    sink: &Arc<dyn Sink>,
    batch: &mut Vec<WalEntry>,
    sub: &mut WalSubscription,
    flush_interval_ms: u64,
) -> Result<()> {
    let highest = batch
        .last()
        .expect("flush called with a non-empty batch")
        .offset_after;
    let count = batch.len() as u64;

    // Move events out of the batch instead of deep-cloning each one (String
    // topic + message). `events` is owned for the whole retry loop and
    // `highest`/`count` are captured above, so the drained `batch` is never
    // needed again. The Vec's capacity is retained for the next flush.
    let events: Vec<WalEvent> = batch.drain(..).map(|e| e.event).collect();

    let mut backoff_ms = flush_interval_ms.min(RETRY_BACKOFF_START_CAP_MS);
    loop {
        match sink.write(&events).await {
            Ok(()) => {
                counter!("wal_forwarder_committed_total").increment(count);
                break;
            }
            Err(SinkError::Permanent(e)) => {
                error!(error = %e, count, "permanent sink failure; dropping batch and advancing cursor");
                counter!("wal_forwarder_drop_total").increment(count);
                break;
            }
            Err(SinkError::Retryable(e)) => {
                warn!(error = %e, count, backoff_ms, "retryable sink failure; holding batch, will retry");
                counter!("wal_forwarder_retry_total").increment(1);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RETRY_BACKOFF_CAP_MS);
            }
        }
    }

    // Reached only after a successful or permanently-failed write.
    // Keep retrying commit until durable so the forwarder does not exit and the
    // sink write is not replayed.
    let mut commit_backoff_ms = flush_interval_ms.min(RETRY_BACKOFF_START_CAP_MS);
    loop {
        match sub.commit(highest).await {
            Ok(()) => break,
            Err(e) => {
                warn!(
                    error = %e,
                    count,
                    backoff_ms = commit_backoff_ms,
                    "WAL cursor commit failed after sink write; retrying"
                );
                counter!("wal_forwarder_commit_retry_total").increment(1);
                tokio::time::sleep(Duration::from_millis(commit_backoff_ms)).await;
                commit_backoff_ms = (commit_backoff_ms * 2).min(RETRY_BACKOFF_CAP_MS);
            }
        }
    }

    batch.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::infrastructure::wal::cursor::read_cursor;
    use crate::infrastructure::wal::types::WalOptions;
    use crate::infrastructure::wal::wal::Wal;

    enum Resp {
        Ok,
        Retry,
        Perm,
    }

    struct MockSink {
        responses: Mutex<VecDeque<Resp>>,
        calls: AtomicUsize,
    }

    impl MockSink {
        fn new(responses: Vec<Resp>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Sink for MockSink {
        fn write<'a>(
            &'a self,
            _batch: &'a [WalEvent],
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                match self.responses.lock().unwrap().pop_front() {
                    Some(Resp::Ok) | None => Ok(()),
                    Some(Resp::Retry) => Err(SinkError::Retryable(anyhow::anyhow!("transient"))),
                    Some(Resp::Perm) => Err(SinkError::Permanent(anyhow::anyhow!("poison"))),
                }
            })
        }
    }

    fn sample_event(seq: u64) -> WalEvent {
        WalEvent {
            topic: format!("smarthome/dev-{seq}/status"),
            ts_ms: 1_700_000_000_000 + seq as i64,
            line_protocol: format!(
                "device_status,device_id=dev-{seq},device_class=test rssi=-50i {}",
                1_700_000_000_000 + seq as i64
            ),
        }
    }

    fn opts(dir: &std::path::Path) -> WalOptions {
        WalOptions {
            dir: dir.to_path_buf(),
            segment_bytes: 1024 * 1024,
            queue_capacity: 32,
        }
    }

    async fn drain(sub: &mut WalSubscription, n: usize) -> Vec<WalEntry> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let entry = tokio::time::timeout(Duration::from_millis(500), sub.next())
                .await
                .expect("timed out draining")
                .expect("subscription closed early");
            out.push(entry);
        }
        out
    }

    #[tokio::test]
    async fn flush_retryable_then_success_commits_once_without_dropping() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = Wal::open(opts(dir.path())).await.unwrap();
        wal.try_append(sample_event(0)).unwrap();
        wal.try_append(sample_event(1)).unwrap();
        tokio::task::yield_now().await;

        let mut batch = drain(&mut sub, 2).await;
        let highest = batch.last().unwrap().offset_after;

        let mock = Arc::new(MockSink::new(vec![Resp::Retry, Resp::Ok]));
        let sink: Arc<dyn Sink> = mock.clone();

        // Small interval keeps the retry backoff sleep negligible.
        flush(&sink, &mut batch, &mut sub, 1).await.unwrap();

        assert_eq!(mock.call_count(), 2, "batch should be retried then succeed");
        assert!(batch.is_empty(), "batch cleared after success");
        assert_eq!(
            read_cursor(dir.path()).unwrap(),
            Some(highest),
            "cursor must advance only after the successful write"
        );
    }

    #[tokio::test]
    async fn flush_permanent_drops_batch_and_advances_cursor() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = Wal::open(opts(dir.path())).await.unwrap();
        wal.try_append(sample_event(0)).unwrap();
        wal.try_append(sample_event(1)).unwrap();
        tokio::task::yield_now().await;

        let mut batch = drain(&mut sub, 2).await;
        let highest = batch.last().unwrap().offset_after;

        let mock = Arc::new(MockSink::new(vec![Resp::Perm]));
        let sink: Arc<dyn Sink> = mock.clone();

        flush(&sink, &mut batch, &mut sub, 1).await.unwrap();

        assert_eq!(mock.call_count(), 1, "permanent error must not be retried");
        assert!(batch.is_empty(), "poison batch dropped");
        assert_eq!(
            read_cursor(dir.path()).unwrap(),
            Some(highest),
            "cursor advances past a dropped poison batch"
        );
    }

    #[tokio::test]
    async fn flush_success_commits_and_advances_cursor() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = Wal::open(opts(dir.path())).await.unwrap();
        wal.try_append(sample_event(0)).unwrap();
        tokio::task::yield_now().await;

        let mut batch = drain(&mut sub, 1).await;
        let highest = batch.last().unwrap().offset_after;

        let mock = Arc::new(MockSink::new(vec![Resp::Ok]));
        let sink: Arc<dyn Sink> = mock.clone();

        flush(&sink, &mut batch, &mut sub, 1).await.unwrap();

        assert_eq!(mock.call_count(), 1);
        assert_eq!(read_cursor(dir.path()).unwrap(), Some(highest));
    }

    #[tokio::test]
    async fn flush_retries_commit_until_cursor_becomes_writable_without_rewriting_sink() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let (wal, mut sub) = Wal::open(opts(&dir_path)).await.unwrap();
        wal.try_append(sample_event(0)).unwrap();
        tokio::task::yield_now().await;

        let mut batch = drain(&mut sub, 1).await;
        let highest = batch.last().unwrap().offset_after;

        let mock = Arc::new(MockSink::new(vec![Resp::Ok]));
        let sink: Arc<dyn Sink> = mock.clone();

        fs::remove_dir_all(&dir_path).unwrap();
        let recreate_dir = dir_path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            fs::create_dir_all(recreate_dir).unwrap();
        });

        flush(&sink, &mut batch, &mut sub, 5).await.unwrap();

        assert_eq!(
            mock.call_count(),
            1,
            "sink write must not be replayed while commit is retried"
        );
        assert_eq!(read_cursor(&dir_path).unwrap(), Some(highest));
    }
}
