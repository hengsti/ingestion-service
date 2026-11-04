use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, Publish, QoS, SubscribeFilter,
    SubscribeReasonCode,
};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use super::{AckKey, AckReceipt, DlqPublisher, IngestDispatcher, IngestJob, Source};
use crate::config::{Config, MqttSourceConfig};
use crate::infrastructure::router::RouteSet;
use crate::infrastructure::wal::capacity::Capacity;

/// Delay between reconnect attempts after an event-loop poll error.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// MQTT wire-packet limit, including fixed header, topic, and payload.
const MAX_MQTT_PACKET_BYTES: usize = 128 * 1024;
const MAX_INFLIGHT_MESSAGES: usize = 100;
const MQTT_REQUEST_CAPACITY: usize = 10;

/// MQTT-backed [`Source`]. Rebuilds the client after connection errors and
/// re-subscribes to `filters` on every `ConnAck`. Readiness is only reported
/// once the broker acknowledges the subscription.
pub struct MqttSource {
    options: MqttOptions,
    eventloop: EventLoop,
    client: AsyncClient,
    publisher_client: Arc<Mutex<AsyncClient>>,
    filters: Vec<SubscribeFilter>,
    subscribe_pending: bool,
    expected_suback: Option<u16>,
    ready: Arc<AtomicBool>,
    ack_tx: mpsc::Sender<AckReceipt>,
    ack_rx: mpsc::Receiver<AckReceipt>,
    generation: u64,
    next_job_id: u64,
    pending_acks: HashMap<AckKey, Publish>,
    ready_acks: VecDeque<AckKey>,
    pending_qos0: Option<IngestJob>,
    wal_capacity: Arc<Capacity>,
    quarantine_capacity: Arc<Capacity>,
    reconnect_pending: bool,
}

impl MqttSource {
    /// Queues one batched subscription for all filters. Uses the non-blocking
    /// `try_` variant because the request channel is drained by this same
    /// task's `poll()`; if the channel is full, the request stays pending and
    /// is retried after the next poll.
    fn try_subscribe(&mut self) {
        match self.client.try_subscribe_many(self.filters.iter().cloned()) {
            Ok(()) => self.subscribe_pending = false,
            Err(err) => warn!(error = %err, "MQTT subscribe not queued; retrying"),
        }
    }

    fn invalidate_acknowledgements(&mut self) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .context("MQTT connection generation exhausted")?;
        self.pending_acks.clear();
        self.ready_acks.clear();
        Ok(())
    }

    fn try_ack_ready(&mut self) {
        while let Some(key) = self.ready_acks.front().copied() {
            if key.generation != self.generation {
                self.ready_acks.pop_front();
                self.pending_acks.remove(&key);
                continue;
            }

            match self
                .pending_acks
                .get(&key)
                .map(|publish| self.client.try_ack(publish))
            {
                Some(Ok(())) => {
                    self.ready_acks.pop_front();
                    self.pending_acks.retain(|pending_key, _| {
                        pending_key.generation != key.generation
                            || pending_key.packet_id != key.packet_id
                    });
                    self.ready_acks.retain(|ready_key| {
                        ready_key.generation != key.generation
                            || ready_key.packet_id != key.packet_id
                    });
                }
                Some(Err(err)) => {
                    warn!(error = %err, "MQTT acknowledgement not queued; retrying");
                    break;
                }
                None => {
                    self.ready_acks.pop_front();
                }
            }
        }
    }

    fn handle_ack_receipt(&mut self, receipt: AckReceipt) -> bool {
        if receipt.key.generation != self.generation
            || !self.pending_acks.contains_key(&receipt.key)
        {
            return false;
        }
        if receipt.outcome.is_ack_eligible() {
            if !self.ready_acks.contains(&receipt.key) {
                self.ready_acks.push_back(receipt.key);
            }
        } else {
            self.pending_acks.remove(&receipt.key);
            self.ready.store(false, Ordering::Relaxed);
            return true;
        }
        self.try_ack_ready();
        false
    }

    fn has_intake_capacity(&self, dispatcher: &IngestDispatcher) -> bool {
        dispatcher.all_workers_open()
            && dispatcher.has_available_worker()
            && self.has_storage_capacity()
    }

    fn has_storage_capacity(&self) -> bool {
        self.wal_capacity.has_room_for_max_record()
            && self.quarantine_capacity.has_room_for_max_record()
    }

    fn try_dispatch_pending_qos0(&mut self, dispatcher: &IngestDispatcher) -> bool {
        let Some(job) = self.pending_qos0.take() else {
            return true;
        };
        match dispatcher.dispatch(job) {
            Ok(()) => true,
            Err(job) => {
                self.pending_qos0 = Some(job);
                false
            }
        }
    }

    fn subscription_granted_qos1(
        &self,
        packet_id: u16,
        return_codes: &[SubscribeReasonCode],
    ) -> bool {
        self.expected_suback == Some(packet_id)
            && return_codes.len() == self.filters.len()
            && return_codes
                .iter()
                .all(|code| matches!(code, SubscribeReasonCode::Success(QoS::AtLeastOnce)))
    }

    fn reset_connection(&mut self) -> Result<()> {
        self.ready.store(false, Ordering::Relaxed);
        self.invalidate_acknowledgements()?;
        self.subscribe_pending = false;
        self.expected_suback = None;

        let (client, eventloop) = AsyncClient::new(self.options.clone(), MQTT_REQUEST_CAPACITY);
        *self
            .publisher_client
            .lock()
            .map_err(|_| anyhow::anyhow!("MQTT client handle was poisoned"))? = client.clone();
        self.client = client;
        self.eventloop = eventloop;
        self.reconnect_pending = true;
        Ok(())
    }

    fn dispatch_publish(&mut self, publish: Publish, dispatcher: &IngestDispatcher) -> Result<()> {
        if publish.qos == QoS::ExactlyOnce {
            metrics::counter!("mqtt_unsupported_qos2_total").increment(1);
            warn!("received unsupported MQTT QoS 2 publication; resetting connection");
            self.reset_connection()?;
            return Ok(());
        }

        if publish.qos == QoS::AtMostOnce {
            metrics::counter!("mqtt_qos0_policy_violations_total").increment(1);
        }

        let key = if publish.qos == QoS::AtLeastOnce {
            let job_id = self.next_job_id;
            self.next_job_id = self
                .next_job_id
                .checked_add(1)
                .context("MQTT publication identity exhausted")?;
            Some(AckKey {
                generation: self.generation,
                packet_id: publish.pkid,
                job_id,
            })
        } else {
            None
        };
        let job = IngestJob {
            topic: publish.topic.clone(),
            payload: publish.payload.clone(),
            received_at_ms: chrono::Utc::now().timestamp_millis(),
            receive_sequence: 0,
            retained: publish.retain,
            qos: match publish.qos {
                QoS::AtMostOnce => 0,
                QoS::AtLeastOnce => 1,
                QoS::ExactlyOnce => 2,
            },
            acknowledgement: key.map(|key| super::AckHandle::new(key, self.ack_tx.clone())),
        };

        if !self.has_storage_capacity() {
            if publish.qos == QoS::AtMostOnce {
                self.pending_qos0 = Some(job);
            }
            self.reset_connection()?;
            return Ok(());
        }

        match dispatcher.dispatch(job) {
            Ok(()) => {
                if let Some(key) = key {
                    self.pending_acks.insert(key, publish);
                }
            }
            Err(unadmitted) => {
                if publish.qos == QoS::AtMostOnce {
                    self.pending_qos0 = Some(unadmitted);
                }
                self.reset_connection()?;
            }
        }
        Ok(())
    }
}

impl Source for MqttSource {
    fn run(
        mut self: Box<Self>,
        dispatcher: IngestDispatcher,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            loop {
                if *shutdown_rx.borrow() {
                    if self.pending_qos0.is_some()
                        && (!self.has_intake_capacity(&dispatcher)
                            || !self.try_dispatch_pending_qos0(&dispatcher))
                    {
                        warn!("unadmitted MQTT QoS 0 message could not be quarantined before shutdown");
                    }
                    break;
                }

                if self.reconnect_pending {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        receipt = self.ack_rx.recv() => {
                            let Some(receipt) = receipt else {
                                break;
                            };
                            if self.handle_ack_receipt(receipt) {
                                self.reset_connection()?;
                            }
                        }
                        () = tokio::time::sleep(RECONNECT_DELAY) => {
                            if self.has_intake_capacity(&dispatcher)
                                && self.try_dispatch_pending_qos0(&dispatcher)
                            {
                                self.reconnect_pending = false;
                            }
                        }
                    }
                    continue;
                }

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        // Either the channel closed or shutdown was signalled — stop polling.
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    receipt = self.ack_rx.recv() => {
                        let Some(receipt) = receipt else {
                            break;
                        };
                        if self.handle_ack_receipt(receipt) {
                            self.reset_connection()?;
                        }
                    }
                    event = self.eventloop.poll() => {
                        let event = match event {
                            Ok(ev) => ev,
                            Err(err) => {
                                self.reset_connection()?;
                                warn!(error = %err, "MQTT connection error; reconnecting");
                                continue;
                            }
                        };

                        match &event {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                info!("MQTT connected");
                                self.invalidate_acknowledgements()?;
                                // Always re-subscribe: brokers may report
                                // `session_present` without restoring subscriptions.
                                self.subscribe_pending = true;
                            }
                            Event::Incoming(Incoming::SubAck(suback)) => {
                                if self.subscription_granted_qos1(
                                    suback.pkid,
                                    &suback.return_codes,
                                ) {
                                    if self.has_intake_capacity(&dispatcher) {
                                        self.ready.store(true, Ordering::Relaxed);
                                        info!(topics = self.filters.len(), "subscribed to MQTT topics");
                                    } else {
                                        self.reset_connection()?;
                                    }
                                } else {
                                    self.ready.store(false, Ordering::Relaxed);
                                    error!(
                                        packet_id = suback.pkid,
                                        codes = ?suback.return_codes,
                                        "MQTT subscription was rejected or granted below QoS 1"
                                    );
                                }
                            }
                            Event::Incoming(Incoming::Disconnect) => {
                                self.reset_connection()?;
                            }
                            Event::Outgoing(Outgoing::Subscribe(packet_id)) => {
                                self.expected_suback = Some(*packet_id);
                            }
                            _ => {}
                        }

                        if self.subscribe_pending {
                            self.try_subscribe();
                        }

                        if let Event::Incoming(Incoming::Publish(publish)) = event {
                            self.dispatch_publish(publish, &dispatcher)?;
                        }
                        self.try_ack_ready();
                    }
                }
            }
            Ok(())
        })
    }
}

/// MQTT-backed [`DlqPublisher`]. Publishes a JSON envelope with
/// `received_at`, `src_topic`, `error`, and `payload_raw` fields.
pub struct MqttDlqPublisher {
    client: Arc<Mutex<AsyncClient>>,
}

impl MqttDlqPublisher {
    /// Wraps an existing MQTT client for DLQ publishing.
    pub fn new(client: AsyncClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }
}

impl DlqPublisher for MqttDlqPublisher {
    fn publish<'a>(
        &'a self,
        dlq_topic: &'a str,
        src_topic: &'a str,
        payload: &'a str,
        err: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let dlq = json!({
                "received_at": chrono::Utc::now().to_rfc3339(),
                "src_topic": src_topic,
                "error": err,
                "payload_raw": payload,
            });

            info!(src_topic = %src_topic, "publishing message to DLQ topic");

            let bytes = serde_json::to_vec(&dlq)?;
            self.client
                .lock()
                .map_err(|_| anyhow::anyhow!("MQTT client handle was poisoned"))?
                .try_publish(dlq_topic, QoS::AtLeastOnce, false, bytes)?;

            Ok(())
        })
    }
}

/// Builds an [`MqttSource`] + [`MqttDlqPublisher`] pair and returns both handles.
/// The network connection is established once the source starts polling the
/// returned event loop; the source subscribes to the compiled route filters on each `ConnAck`.
///
/// # Errors
/// Returns an error if `cfg.mqtt` is `None` (should not happen — `Config::from_env`
/// guarantees it's populated when `input_source == InputSourceKind::Mqtt`).
pub async fn build(
    cfg: &Config,
    ready: Arc<AtomicBool>,
    routes: &RouteSet,
    wal_capacity: Arc<Capacity>,
    quarantine_capacity: Arc<Capacity>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    let mqtt_cfg = cfg
        .mqtt
        .as_ref()
        .context("INPUT_SOURCE=mqtt requires MQTT_* connection variables")?;

    let mut mqttoptions = mqtt_options(mqtt_cfg);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, eventloop) = AsyncClient::new(mqttoptions.clone(), MQTT_REQUEST_CAPACITY);
    let publisher = MqttDlqPublisher::new(client.clone());
    let publisher_client = publisher.client.clone();
    let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);

    let filters = routes
        .subscriptions()
        .into_iter()
        .map(|topic| SubscribeFilter::new(topic.to_owned(), QoS::AtLeastOnce))
        .collect();

    let source: Box<dyn Source> = Box::new(MqttSource {
        options: mqttoptions,
        eventloop,
        client: client.clone(),
        publisher_client: publisher_client.clone(),
        filters,
        subscribe_pending: false,
        expected_suback: None,
        ready,
        ack_tx,
        ack_rx,
        generation: 0,
        next_job_id: 0,
        pending_acks: HashMap::new(),
        ready_acks: VecDeque::new(),
        pending_qos0: None,
        wal_capacity,
        quarantine_capacity,
        reconnect_pending: false,
    });
    let publisher: Arc<dyn DlqPublisher> = Arc::new(publisher);

    Ok((source, publisher))
}

fn mqtt_options(mqtt_cfg: &MqttSourceConfig) -> MqttOptions {
    let mut options = MqttOptions::new(&mqtt_cfg.client_id, &mqtt_cfg.host, mqtt_cfg.port);
    options.set_clean_session(mqtt_cfg.clean_session);
    options.set_max_packet_size(MAX_MQTT_PACKET_BYTES, MAX_MQTT_PACKET_BYTES);
    options.set_manual_acks(true);
    if let (Some(username), Some(password)) = (&mqtt_cfg.username, &mqtt_cfg.password) {
        options.set_credentials(username, password);
    }
    options
}

#[cfg(test)]
mod tests {
    use rumqttc::MqttOptions as TestMqttOptions;

    use super::*;
    use crate::config::MqttSourceConfig;
    use crate::pipeline::outcome::{DeferredReason, PipelineOutcome};

    fn test_source(
        options: MqttOptions,
        client: AsyncClient,
        eventloop: EventLoop,
        ready: Arc<AtomicBool>,
        filters: Vec<SubscribeFilter>,
        capacity_limit: u64,
    ) -> (MqttSource, tempfile::TempDir) {
        let capacity_dir = tempfile::tempdir().expect("capacity directory");
        let capacity =
            Capacity::recover(capacity_dir.path(), capacity_limit).expect("capacity snapshot");
        let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);
        let publisher_client = Arc::new(Mutex::new(client.clone()));

        (
            MqttSource {
                options,
                eventloop,
                client,
                publisher_client,
                filters,
                subscribe_pending: false,
                expected_suback: None,
                ready,
                ack_tx,
                ack_rx,
                generation: 0,
                next_job_id: 0,
                pending_acks: HashMap::new(),
                ready_acks: VecDeque::new(),
                pending_qos0: None,
                wal_capacity: capacity.clone(),
                quarantine_capacity: capacity,
                reconnect_pending: false,
            },
            capacity_dir,
        )
    }

    fn test_publish(qos: QoS, packet_id: u16) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            topic: "topic".to_owned(),
            pkid: packet_id,
            payload: bytes::Bytes::from_static(b"{}"),
        }
    }

    #[test]
    fn mqtt_options_apply_the_documented_packet_limit() {
        let options = mqtt_options(&MqttSourceConfig {
            host: "localhost".to_owned(),
            port: 1883,
            username: None,
            password: None,
            client_id: "test".to_owned(),
            clean_session: true,
        });

        assert_eq!(options.max_packet_size(), MAX_MQTT_PACKET_BYTES);
    }

    #[test]
    fn mqtt_options_enable_manual_acknowledgements() {
        let options = mqtt_options(&MqttSourceConfig {
            host: "localhost".to_owned(),
            port: 1883,
            username: None,
            password: None,
            client_id: "test".to_owned(),
            clean_session: false,
        });

        assert!(options.manual_acks());
    }

    #[test]
    fn deferred_pipeline_result_does_not_queue_puback() {
        let mut options = TestMqttOptions::new("test-mqtt-deferred", "localhost", 1883);
        options.set_manual_acks(true);
        let (client, eventloop) = AsyncClient::new(options.clone(), 10);
        let ready = Arc::new(AtomicBool::new(true));
        let key = AckKey {
            generation: 1,
            packet_id: 7,
            job_id: 3,
        };
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            ready.clone(),
            Vec::new(),
            u64::MAX,
        );
        source.generation = key.generation;
        source.pending_acks.insert(
            key,
            Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "topic".to_owned(),
                pkid: key.packet_id,
                payload: bytes::Bytes::from_static(b"{}"),
            },
        );

        assert!(source.handle_ack_receipt(AckReceipt {
            key,
            outcome: PipelineOutcome::Deferred(DeferredReason::PersistenceFailure),
        }));

        assert!(source.ready_acks.is_empty());
        assert!(source.pending_acks.is_empty());
        assert!(!ready.load(Ordering::Relaxed));
    }

    /// Returns a client whose eventloop receiver is alive so that `publish` queues successfully.
    fn client_with_live_eventloop() -> (AsyncClient, rumqttc::EventLoop) {
        let opts = TestMqttOptions::new("test-mqtt-source-ok", "localhost", 1883);
        AsyncClient::new(opts, 10)
    }

    /// Returns a client whose eventloop has been dropped so that `publish` returns an error.
    fn client_with_dropped_eventloop() -> AsyncClient {
        let opts = TestMqttOptions::new("test-mqtt-source-err", "localhost", 1884);
        let (client, _eventloop) = AsyncClient::new(opts, 10);
        // Dropping the event loop closes the receiver, so `publish` fails.
        client
    }

    #[tokio::test]
    async fn mqtt_source_run_connection_error_keeps_running_until_shutdown() -> Result<()> {
        // Arrange: nothing listens on port 1, so every poll fails to connect.
        let opts = TestMqttOptions::new("test-mqtt-source-reconnect", "127.0.0.1", 1);
        let (client, eventloop) = AsyncClient::new(opts.clone(), MQTT_REQUEST_CAPACITY);
        let ready = Arc::new(AtomicBool::new(true));
        let (source, _capacity_dir) = test_source(
            opts,
            client,
            eventloop,
            ready.clone(),
            vec![SubscribeFilter::new(
                "smarthome/+/sensor".to_owned(),
                QoS::AtLeastOnce,
            )],
            u64::MAX,
        );
        let source = Box::new(source);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Act
        let handle = tokio::spawn(source.run(IngestDispatcher::new(vec![tx]), shutdown_rx));
        tokio::time::timeout(Duration::from_secs(10), async {
            while ready.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let still_running = !handle.is_finished();
        shutdown_tx.send(true)?;
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await??;

        // Assert
        assert!(still_running);
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn mqtt_source_run_stops_when_shutdown_is_already_signaled() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-shutdown", "127.0.0.1", 1);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(false)),
            Vec::new(),
            u64::MAX,
        );
        let (tx, _rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);

        tokio::time::timeout(
            Duration::from_secs(1),
            Box::new(source).run(IngestDispatcher::new(vec![tx]), shutdown_rx),
        )
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn mqtt_source_try_subscribe_full_channel_stays_pending() -> Result<()> {
        // Arrange: capacity-1 request channel already occupied by a publish.
        let opts = TestMqttOptions::new("test-mqtt-source-full", "127.0.0.1", 1);
        let (client, eventloop) = AsyncClient::new(opts.clone(), 1);
        client.try_publish("t", QoS::AtLeastOnce, false, "x")?;
        let (mut source, _capacity_dir) = test_source(
            opts,
            client,
            eventloop,
            Arc::new(AtomicBool::new(false)),
            vec![SubscribeFilter::new(
                "smarthome/+/sensor".to_owned(),
                QoS::AtLeastOnce,
            )],
            u64::MAX,
        );
        source.subscribe_pending = true;

        // Act
        source.try_subscribe();

        // Assert
        assert!(source.subscribe_pending);
        Ok(())
    }

    #[test]
    fn mqtt_source_rejects_intake_when_journal_headroom_is_insufficient() {
        let options = TestMqttOptions::new("test-mqtt-capacity", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let ready = Arc::new(AtomicBool::new(true));
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            ready.clone(),
            Vec::new(),
            100_000,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        source
            .dispatch_publish(test_publish(QoS::AtLeastOnce, 9), &dispatcher)
            .expect("rejecting intake should not fail the source");

        assert!(rx.try_recv().is_err());
        assert!(source.reconnect_pending);
        assert!(!ready.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn mqtt_source_unadmitted_qos1_publication_is_unacknowledged() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-full-worker", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let ready = Arc::new(AtomicBool::new(true));
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            ready.clone(),
            Vec::new(),
            u64::MAX,
        );
        let old_client = source.client.clone();
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);
        assert!(dispatcher
            .dispatch(IngestJob {
                topic: "occupied".to_string(),
                payload: bytes::Bytes::from_static(b"{}"),
                received_at_ms: 0,
                receive_sequence: 0,
                retained: false,
                qos: 1,
                acknowledgement: None,
            })
            .is_ok());

        source.dispatch_publish(test_publish(QoS::AtLeastOnce, 12), &dispatcher)?;

        assert!(source.pending_acks.is_empty());
        assert!(source.reconnect_pending);
        assert!(!ready.load(Ordering::Relaxed));
        assert!(old_client
            .try_ack(&test_publish(QoS::AtLeastOnce, 12))
            .is_err());
        assert_eq!(
            rx.recv().await.map(|job| job.topic),
            Some("occupied".to_string())
        );
        Ok(())
    }

    #[test]
    fn mqtt_source_rejects_qos2_without_dispatch_or_ack() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-qos2", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            u64::MAX,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        source.dispatch_publish(test_publish(QoS::ExactlyOnce, 13), &dispatcher)?;

        assert!(source.reconnect_pending);
        assert!(source.pending_acks.is_empty());
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn mqtt_source_admits_qos0_without_ack_handle() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-qos0", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            u64::MAX,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        source.dispatch_publish(test_publish(QoS::AtMostOnce, 0), &dispatcher)?;

        let job = rx.recv().await.expect("admitted QoS 0 job");
        assert_eq!(job.qos, 0);
        assert!(job.acknowledgement.is_none());
        assert!(source.pending_acks.is_empty());
        Ok(())
    }

    #[test]
    fn mqtt_source_retains_qos0_when_worker_queue_is_full() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-qos0-full", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            u64::MAX,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);
        dispatcher
            .dispatch(IngestJob {
                topic: "occupied".to_string(),
                payload: bytes::Bytes::from_static(b"{}"),
                received_at_ms: 0,
                receive_sequence: 0,
                retained: false,
                qos: 1,
                acknowledgement: None,
            })
            .expect("first job should occupy worker queue");

        source.dispatch_publish(test_publish(QoS::AtMostOnce, 0), &dispatcher)?;

        let pending = source
            .pending_qos0
            .as_ref()
            .expect("unadmitted QoS 0 must be retained for quarantine");
        assert_eq!(pending.qos, 0);
        assert!(pending.acknowledgement.is_none());
        assert!(source.reconnect_pending);

        assert_eq!(rx.try_recv()?.topic, "occupied");
        assert!(source.try_dispatch_pending_qos0(&dispatcher));
        assert_eq!(rx.try_recv()?.topic, "topic");
        Ok(())
    }

    #[test]
    fn mqtt_source_retains_qos0_when_journal_headroom_is_insufficient() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-qos0-capacity", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            100_000,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);

        source.dispatch_publish(test_publish(QoS::AtMostOnce, 0), &dispatcher)?;

        assert!(source.pending_qos0.is_some());
        assert!(source.reconnect_pending);
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn mqtt_source_only_accepts_current_qos1_suback_for_all_filters() {
        let options = TestMqttOptions::new("test-mqtt-suback", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let filters = vec![
            SubscribeFilter::new("one".to_owned(), QoS::AtLeastOnce),
            SubscribeFilter::new("two".to_owned(), QoS::AtLeastOnce),
        ];
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(false)),
            filters,
            u64::MAX,
        );
        source.expected_suback = Some(21);
        let grants = [
            SubscribeReasonCode::Success(QoS::AtLeastOnce),
            SubscribeReasonCode::Success(QoS::AtLeastOnce),
        ];
        let downgraded = [
            SubscribeReasonCode::Success(QoS::AtMostOnce),
            SubscribeReasonCode::Success(QoS::AtLeastOnce),
        ];

        assert!(source.subscription_granted_qos1(21, &grants));
        assert!(!source.subscription_granted_qos1(20, &grants));
        assert!(!source.subscription_granted_qos1(21, &downgraded));
        assert!(!source.subscription_granted_qos1(21, &grants[..1]));
    }

    #[test]
    fn mqtt_source_ignores_late_completion_after_packet_id_reuse() {
        let options = TestMqttOptions::new("test-mqtt-packet-reuse", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(false)),
            Vec::new(),
            u64::MAX,
        );
        source.generation = 4;
        let current_key = AckKey {
            generation: 4,
            packet_id: 17,
            job_id: 2,
        };
        source
            .pending_acks
            .insert(current_key, test_publish(QoS::AtLeastOnce, 17));

        assert!(!source.handle_ack_receipt(AckReceipt {
            key: AckKey {
                generation: 4,
                packet_id: 17,
                job_id: 1,
            },
            outcome: PipelineOutcome::OutputStored,
        }));
        assert!(source.ready_acks.is_empty());
        assert!(source.pending_acks.contains_key(&current_key));
    }

    #[test]
    fn mqtt_source_retries_ack_after_request_channel_is_full() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-ack-full", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), 1);
        client.try_publish("occupied", QoS::AtLeastOnce, false, "x")?;
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(false)),
            Vec::new(),
            u64::MAX,
        );
        source.generation = 5;
        let key = AckKey {
            generation: 5,
            packet_id: 18,
            job_id: 1,
        };
        source
            .pending_acks
            .insert(key, test_publish(QoS::AtLeastOnce, 18));
        source.ready_acks.push_back(key);

        source.try_ack_ready();

        assert_eq!(source.ready_acks.front(), Some(&key));
        assert!(source.pending_acks.contains_key(&key));
        Ok(())
    }

    #[tokio::test]
    async fn mqtt_source_reset_discards_queued_ack_and_refreshes_dlq_client() -> Result<()> {
        let options = TestMqttOptions::new("test-mqtt-reset-pair", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let (mut source, _capacity_dir) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            u64::MAX,
        );
        let stale_client = source.client.clone();
        let key = AckKey {
            generation: 1,
            packet_id: 19,
            job_id: 1,
        };
        source
            .pending_acks
            .insert(key, test_publish(QoS::AtLeastOnce, 19));
        source.ready_acks.push_back(key);
        source.try_ack_ready();
        let publisher = MqttDlqPublisher {
            client: source.publisher_client.clone(),
        };

        source.reset_connection()?;

        assert!(source.pending_acks.is_empty());
        assert!(source.ready_acks.is_empty());
        assert!(source.reconnect_pending);
        assert!(stale_client
            .try_publish("stale", QoS::AtLeastOnce, false, "x")
            .is_err());
        assert!(publisher
            .publish("dlq", "source", "payload", "reason")
            .await
            .is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn mqtt_dlq_publisher_publish_succeeds_with_live_eventloop() {
        let (client, _eventloop) = client_with_live_eventloop();
        let publisher = MqttDlqPublisher::new(client);

        let result = publisher
            .publish(
                "smarthome/_dlq/ingest",
                "smarthome/esp32-1/sensor",
                "raw",
                "boom",
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mqtt_dlq_publisher_publish_errors_with_dropped_eventloop() {
        let client = client_with_dropped_eventloop();
        let publisher = MqttDlqPublisher::new(client);

        let result = publisher
            .publish(
                "smarthome/_dlq/ingest",
                "smarthome/esp32-1/sensor",
                "raw",
                "boom",
            )
            .await;

        assert!(result.is_err());
    }
}
