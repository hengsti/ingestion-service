pub mod mqtt;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::config::{Config, InputSourceKind};

/// A single unit of work dispatched from a [`Source`] into the worker pool.
#[derive(Debug)]
pub struct IngestJob {
    /// Concrete topic that delivered the message.
    pub topic: String,
    /// Raw MQTT payload bytes.
    pub payload: Bytes,
    /// Receive time captured once at MQTT intake, in epoch milliseconds.
    pub received_at_ms: i64,
    /// Monotonic intake order used to distinguish messages received in one millisecond.
    pub receive_sequence: u64,
    /// Whether the broker marked the message as retained.
    pub retained: bool,
    /// Transport QoS level of the received MQTT publication.
    pub qos: u8,
    /// Completion handle for a publication that needs a durable outcome before acknowledgement.
    pub acknowledgement: Option<AckHandle>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AckKey {
    pub generation: u64,
    pub packet_id: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AckReceipt {
    pub key: AckKey,
    pub durable: bool,
}

/// Reports a pipeline's durable completion back to its input source.
#[derive(Debug)]
pub struct AckHandle {
    key: AckKey,
    tx: mpsc::Sender<AckReceipt>,
}

impl AckHandle {
    pub(crate) fn new(key: AckKey, tx: mpsc::Sender<AckReceipt>) -> Self {
        Self { key, tx }
    }

    /// Sends the pipeline's completion status to the source.
    ///
    /// Returns `false` if the source is no longer receiving completion reports.
    pub async fn complete(self, durable: bool) -> bool {
        self.tx
            .send(AckReceipt {
                key: self.key,
                durable,
            })
            .await
            .is_ok()
    }
}

/// Round-robins [`IngestJob`]s across a fixed set of per-worker bounded channels.
///
/// Owned by `main` and handed to [`Source::run`] by value so a source never needs
/// to know about worker count or pool internals — it just calls [`dispatch`](Self::dispatch).
#[derive(Clone)]
pub struct IngestDispatcher {
    senders: Arc<[mpsc::Sender<IngestJob>]>,
    next: Arc<AtomicUsize>,
    receive_sequence: Arc<AtomicU64>,
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
            receive_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Dispatches `job` to the next worker in round-robin order.
    ///
    /// If that worker's queue is full, the job is dropped, a warning is logged,
    /// and `ingest_event_queue_full_total` is incremented. The DLQ is not used
    /// for this pre-pipeline drop.
    pub fn dispatch(&self, mut job: IngestJob) -> bool {
        job.receive_sequence = self.receive_sequence.fetch_add(1, Ordering::Relaxed);
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        if let Err(err) = self.senders[idx].try_send(job) {
            metrics::counter!("ingest_event_queue_full_total").increment(1);
            warn!(error = %err, "event queue full; dropping incoming message before pipeline");
            return false;
        }
        true
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
/// Returns an error if the configured source is unsupported or lacks the MQTT configuration.
pub async fn build_source(
    cfg: &Config,
    ready: Arc<AtomicBool>,
    routes: &crate::infrastructure::router::RouteSet,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    match cfg.input_source {
        InputSourceKind::Mqtt => mqtt::build(cfg, ready, routes).await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn ack_handle_reports_deferred_completion() {
        let (tx, mut rx) = mpsc::channel(1);
        let key = AckKey {
            generation: 7,
            packet_id: 42,
        };

        assert!(AckHandle::new(key, tx).complete(false).await);
        assert_eq!(
            rx.recv().await,
            Some(AckReceipt {
                key,
                durable: false,
            })
        );
    }

    #[tokio::test]
    async fn ingest_dispatcher_dispatch_round_robins_across_senders() {
        let (tx_a, mut rx_a) = mpsc::channel::<IngestJob>(4);
        let (tx_b, mut rx_b) = mpsc::channel::<IngestJob>(4);
        let dispatcher = IngestDispatcher::new(vec![tx_a, tx_b]);

        for i in 0..4 {
            assert!(dispatcher.dispatch(IngestJob {
                topic: format!("topic/{i}"),
                payload: Bytes::from_static(b"{}"),
                received_at_ms: i,
                receive_sequence: 0,
                retained: i % 2 == 0,
                qos: 1,
                acknowledgement: None,
            }));
        }

        let mut received_a = Vec::new();
        while let Ok(job) = rx_a.try_recv() {
            received_a.push((
                job.topic,
                job.received_at_ms,
                job.receive_sequence,
                job.retained,
            ));
        }
        let mut received_b = Vec::new();
        while let Ok(job) = rx_b.try_recv() {
            received_b.push((
                job.topic,
                job.received_at_ms,
                job.receive_sequence,
                job.retained,
            ));
        }

        assert_eq!(
            received_a,
            vec![
                ("topic/0".to_string(), 0, 0, true),
                ("topic/2".to_string(), 2, 2, true)
            ]
        );
        assert_eq!(
            received_b,
            vec![
                ("topic/1".to_string(), 1, 1, false),
                ("topic/3".to_string(), 3, 3, false)
            ]
        );
    }

    #[tokio::test]
    async fn ingest_dispatcher_dispatch_drops_job_and_increments_metric_when_queue_full() {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        assert!(dispatcher.dispatch(IngestJob {
            topic: "topic/first".to_string(),
            payload: Bytes::from_static(b"{}"),
            received_at_ms: 10,
            receive_sequence: 0,
            retained: false,
            qos: 1,
            acknowledgement: None,
        }));
        assert!(!dispatcher.dispatch(IngestJob {
            topic: "topic/second".to_string(),
            payload: Bytes::from_static(b"{}"),
            received_at_ms: 11,
            receive_sequence: 0,
            retained: true,
            qos: 1,
            acknowledgement: None,
        }));

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
