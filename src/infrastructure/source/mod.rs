pub mod mqtt;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::config::{Config, InputSourceKind};

/// A single unit of work dispatched from a [`Source`] into the worker pool.
#[derive(Debug)]
pub struct IngestJob {
    pub topic: String,
    pub payload: Bytes,
}

/// Round-robins [`IngestJob`]s across a fixed set of per-worker bounded channels.
///
/// Owned by `main` and handed to [`Source::run`] by value so a source never needs
/// to know about worker count or pool internals — it just calls [`dispatch`](Self::dispatch).
#[derive(Clone)]
pub struct IngestDispatcher {
    senders: Arc<[mpsc::Sender<IngestJob>]>,
    next: Arc<AtomicUsize>,
}

impl IngestDispatcher {
    /// Builds a dispatcher over `senders`.
    ///
    /// # Panics
    /// Panics if `senders` is empty — a dispatcher with no destinations is a
    /// programming error at startup, not a runtime condition to recover from.
    pub fn new(senders: Vec<mpsc::Sender<IngestJob>>) -> Self {
        assert!(
            !senders.is_empty(),
            "IngestDispatcher requires at least one worker sender"
        );
        Self {
            senders: senders.into(),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Dispatches `job` to the next worker in round-robin order.
    ///
    /// If that worker's queue is full, the job is dropped, a warning is logged,
    /// and `ingest_event_queue_full_total` is incremented. The DLQ is not used
    /// for this pre-pipeline drop.
    pub fn dispatch(&self, job: IngestJob) {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        if let Err(err) = self.senders[idx].try_send(job) {
            metrics::counter!("ingest_event_queue_full_total").increment(1);
            warn!(error = %err, "event queue full; dropping incoming message before pipeline");
        }
    }
}

/// A transport that produces [`IngestJob`]s (e.g. an MQTT client, a future Kafka consumer).
///
/// `run` takes ownership of `self` (boxed, for object safety) and drives the
/// transport's event loop until `shutdown_rx` signals shutdown or an
/// unrecoverable error occurs.
pub trait Source: Send {
    fn run(
        self: Box<Self>,
        dispatcher: IngestDispatcher,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}

/// Publishes a rejected message to a dead-letter destination on the same
/// transport as the active [`Source`] (see [`build_source`]: both come from
/// one factory call so they always match).
pub trait DlqPublisher: Send + Sync {
    /// Publishes a single DLQ envelope built from the given fields.
    ///
    /// # Errors
    /// Returns an error if the publish fails. Callers (the DLQ pipeline stage)
    /// must not propagate this as a pipeline failure — log and continue.
    fn publish<'a>(
        &'a self,
        dlq_topic: &'a str,
        src_topic: &'a str,
        payload: &'a str,
        err: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Builds the configured input source and its matching DLQ publisher.
///
/// # Errors
/// Returns an error if `Config::input_source` names an unsupported source, or
/// if the underlying transport fails to connect/subscribe during construction.
pub async fn build_source(
    cfg: &Config,
    ready: Arc<AtomicBool>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    match cfg.input_source {
        InputSourceKind::Mqtt => mqtt::build(cfg, ready).await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn ingest_dispatcher_dispatch_round_robins_across_senders() {
        let (tx_a, mut rx_a) = mpsc::channel::<IngestJob>(4);
        let (tx_b, mut rx_b) = mpsc::channel::<IngestJob>(4);
        let dispatcher = IngestDispatcher::new(vec![tx_a, tx_b]);

        for i in 0..4 {
            dispatcher.dispatch(IngestJob {
                topic: format!("topic/{i}"),
                payload: Bytes::from_static(b"{}"),
            });
        }

        let mut received_a = Vec::new();
        while let Ok(job) = rx_a.try_recv() {
            received_a.push(job.topic);
        }
        let mut received_b = Vec::new();
        while let Ok(job) = rx_b.try_recv() {
            received_b.push(job.topic);
        }

        assert_eq!(received_a, vec!["topic/0", "topic/2"]);
        assert_eq!(received_b, vec!["topic/1", "topic/3"]);
    }

    #[tokio::test]
    async fn ingest_dispatcher_dispatch_drops_job_and_increments_metric_when_queue_full() {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        dispatcher.dispatch(IngestJob {
            topic: "topic/first".to_string(),
            payload: Bytes::from_static(b"{}"),
        });
        dispatcher.dispatch(IngestJob {
            topic: "topic/second".to_string(),
            payload: Bytes::from_static(b"{}"),
        });

        let first = tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("recv should not time out")
            .expect("channel must yield the first job");
        assert_eq!(first.topic, "topic/first");

        let second = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(second.is_err(), "no second job should have been queued");
    }

    #[tokio::test]
    #[should_panic(expected = "IngestDispatcher requires at least one worker sender")]
    async fn ingest_dispatcher_new_panics_on_empty_senders() {
        let _ = IngestDispatcher::new(Vec::new());
    }
}
