use std::collections::{HashMap, HashSet, VecDeque};
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
mod subscriptions;

use crate::config::{Config, MqttSourceConfig};
use crate::infrastructure::operations::Operations;
use crate::infrastructure::router::RouteSet;
use crate::infrastructure::wal::capacity::Capacity;
use subscriptions::{filter_plan, BrokerIdentity, SubscriptionManifest};

/// Delay between reconnect attempts after an event-loop poll error.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// MQTT wire-packet limit, including fixed header, topic, and payload.
const MAX_MQTT_PACKET_BYTES: usize = 128 * 1024;
const MAX_INFLIGHT_MESSAGES: usize = 100;
const MQTT_REQUEST_CAPACITY: usize = 10;

/// MQTT-backed [`Source`]. Rebuilds the client after connection errors and
/// reconciles the desired filters on every `ConnAck`. Readiness is only reported
/// after current subscriptions and obsolete unsubscriptions are acknowledged.
pub struct MqttSource {
    options: MqttOptions,
    eventloop: EventLoop,
    client: AsyncClient,
    publisher_client: Arc<Mutex<AsyncClient>>,
    filters: Vec<SubscribeFilter>,
    manifest: SubscriptionManifest,
    reconciliation: Option<Reconciliation>,
    fresh_session_filters: Option<Vec<String>>,
    subscribe_pending: bool,
    expected_suback: Option<u16>,
    ready: Arc<AtomicBool>,
    operations: Arc<Operations>,
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

impl Drop for MqttSource {
    fn drop(&mut self) {
        self.ready.store(false, Ordering::Relaxed);
        self.operations.update_admission(0, 0, 0);
    }
}

struct Reconciliation {
    desired_filters: Vec<String>,
    subscription_acked: bool,
    unsubscribe_pending: VecDeque<String>,
    queued_unsubscribes: usize,
    expected_unsubacks: HashSet<u16>,
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
        self.wal_capacity.is_writable()
            && self.quarantine_capacity.is_writable()
            && self.wal_capacity.has_room_for_max_record()
            && self.quarantine_capacity.has_room_for_max_record()
    }

    fn publish_admission(&self, dispatcher: &IngestDispatcher) {
        self.operations.bind_dispatcher(dispatcher);
        self.operations
            .pending_acks
            .store(self.pending_acks.len(), Ordering::Relaxed);
        self.operations.admission_paused.store(
            self.reconnect_pending || self.pending_qos0.is_some(),
            Ordering::Relaxed,
        );
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

    async fn start_reconciliation(&mut self, plan: subscriptions::FilterPlan) -> Result<()> {
        self.manifest.persist(plan.pending).await?;
        self.reconciliation = Some(Reconciliation {
            desired_filters: plan.subscribe,
            subscription_acked: false,
            unsubscribe_pending: plan.unsubscribe.into(),
            queued_unsubscribes: 0,
            expected_unsubacks: HashSet::new(),
        });
        self.expected_suback = None;
        self.subscribe_pending = true;
        Ok(())
    }

    fn try_queue_unsubscribes(&mut self) {
        let Some(reconciliation) = self.reconciliation.as_mut() else {
            return;
        };
        if !reconciliation.subscription_acked {
            return;
        }

        while let Some(filter) = reconciliation.unsubscribe_pending.front() {
            match self.client.try_unsubscribe(filter.clone()) {
                Ok(()) => {
                    reconciliation.unsubscribe_pending.pop_front();
                    reconciliation.queued_unsubscribes += 1;
                }
                Err(err) => {
                    warn!(error = %err, "MQTT unsubscribe not queued; retrying");
                    break;
                }
            }
        }
    }

    async fn finish_reconciliation(&mut self, dispatcher: &IngestDispatcher) -> Result<()> {
        let complete = self.reconciliation.as_ref().is_some_and(|reconciliation| {
            reconciliation.subscription_acked
                && reconciliation.unsubscribe_pending.is_empty()
                && reconciliation.queued_unsubscribes == 0
                && reconciliation.expected_unsubacks.is_empty()
        });
        if !complete {
            return Ok(());
        }

        let Some(desired_filters) = self
            .reconciliation
            .as_ref()
            .map(|reconciliation| reconciliation.desired_filters.clone())
        else {
            return Ok(());
        };
        self.manifest.persist(desired_filters).await?;
        self.reconciliation = None;
        if self.has_intake_capacity(dispatcher) {
            self.ready.store(true, Ordering::Relaxed);
            self.publish_admission(dispatcher);
            info!(topics = self.filters.len(), "MQTT subscriptions reconciled");
        } else {
            self.reset_connection()?;
        }
        Ok(())
    }

    fn reset_connection(&mut self) -> Result<()> {
        self.ready.store(false, Ordering::Relaxed);
        self.operations
            .admission_paused
            .store(true, Ordering::Relaxed);
        self.operations.pending_acks.store(0, Ordering::Relaxed);
        self.invalidate_acknowledgements()?;
        self.reconciliation = None;
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
                self.publish_admission(&dispatcher);
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
                            Event::Incoming(Incoming::ConnAck(connack)) => {
                                info!("MQTT connected");
                                self.invalidate_acknowledgements()?;
                                self.ready.store(false, Ordering::Relaxed);
                                if !connack.session_present {
                                    self.fresh_session_filters = Some(Vec::new());
                                }
                                let desired_filters: Vec<_> = self
                                    .filters
                                    .iter()
                                    .map(|filter| filter.path.clone())
                                    .collect();
                                let known_filters = self
                                    .fresh_session_filters
                                    .as_deref()
                                    .or_else(|| self.manifest.known_filters());
                                let plan = filter_plan(
                                    known_filters,
                                    connack.session_present,
                                    &desired_filters,
                                )?;
                                if let Err(err) = self.start_reconciliation(plan).await {
                                    error!(
                                        error = %err,
                                        "could not sync MQTT subscription manifest; reconnecting"
                                    );
                                    self.reset_connection()?;
                                } else {
                                    self.fresh_session_filters = None;
                                }
                            }
                            Event::Incoming(Incoming::SubAck(suback)) => {
                                if self.expected_suback == Some(suback.pkid) {
                                    if self.subscription_granted_qos1(
                                        suback.pkid,
                                        &suback.return_codes,
                                    ) {
                                        self.expected_suback = None;
                                        if let Some(reconciliation) = self.reconciliation.as_mut()
                                        {
                                            reconciliation.subscription_acked = true;
                                        }
                                    } else {
                                        self.ready.store(false, Ordering::Relaxed);
                                        error!(
                                            packet_id = suback.pkid,
                                            codes = ?suback.return_codes,
                                            "MQTT subscription was rejected or granted below QoS 1"
                                        );
                                        self.reset_connection()?;
                                    }
                                }
                            }
                            Event::Incoming(Incoming::UnsubAck(unsuback)) => {
                                if let Some(reconciliation) = self.reconciliation.as_mut() {
                                    reconciliation.expected_unsubacks.remove(&unsuback.pkid);
                                }
                            }
                            Event::Incoming(Incoming::Disconnect) => {
                                self.reset_connection()?;
                            }
                            Event::Outgoing(Outgoing::Subscribe(packet_id)) => {
                                if self.reconciliation.is_some() {
                                    self.expected_suback = Some(*packet_id);
                                }
                            }
                            Event::Outgoing(Outgoing::Unsubscribe(packet_id)) => {
                                if let Some(reconciliation) = self.reconciliation.as_mut() {
                                    reconciliation.queued_unsubscribes = reconciliation
                                        .queued_unsubscribes
                                        .checked_sub(1)
                                        .context("unexpected MQTT unsubscribe request")?;
                                    reconciliation.expected_unsubacks.insert(*packet_id);
                                }
                            }
                            _ => {}
                        }

                        if self.subscribe_pending {
                            self.try_subscribe();
                        }
                        self.try_queue_unsubscribes();
                        if let Err(err) = self.finish_reconciliation(&dispatcher).await {
                            error!(
                                error = %err,
                                "could not commit MQTT subscription manifest; reconnecting"
                            );
                            self.reset_connection()?;
                        }

                        if let Event::Incoming(Incoming::Publish(publish)) = event {
                            self.dispatch_publish(publish, &dispatcher)?;
                        }
                        self.try_ack_ready();
                    }
                }
            }
            self.ready.store(false, Ordering::Relaxed);
            self.operations.update_admission(0, 0, 0);
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
/// returned event loop; the source reconciles the compiled route filters on each `ConnAck`.
///
/// # Errors
/// Returns an error if `cfg.mqtt` is `None` (should not happen — `Config::from_env`
/// guarantees it's populated when `input_source == InputSourceKind::Mqtt`).
pub async fn build(
    cfg: &Config,
    routes: &RouteSet,
    operations: Arc<Operations>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    let mqtt_cfg = cfg
        .mqtt
        .as_ref()
        .context("INPUT_SOURCE=mqtt requires MQTT_* connection variables")?;
    let desired_filters: Vec<_> = routes
        .subscriptions()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let mut manifest = SubscriptionManifest::open(
        &cfg.wal_dir,
        BrokerIdentity {
            host: mqtt_cfg.host.clone(),
            port: mqtt_cfg.port,
            client_id: mqtt_cfg.client_id.clone(),
        },
    )?;
    let known_filters = manifest.known_filters();
    let initial_plan = filter_plan(known_filters, known_filters.is_some(), &desired_filters)?;
    manifest.validate_filters(&initial_plan.pending)?;
    manifest.initialize_if_missing().await?;

    let mut mqttoptions = mqtt_options(mqtt_cfg);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, eventloop) = AsyncClient::new(mqttoptions.clone(), MQTT_REQUEST_CAPACITY);
    let publisher = MqttDlqPublisher::new(client.clone());
    let publisher_client = publisher.client.clone();
    let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);

    let filters = desired_filters
        .into_iter()
        .map(|topic| SubscribeFilter::new(topic, QoS::AtLeastOnce))
        .collect();

    let source: Box<dyn Source> = Box::new(MqttSource {
        options: mqttoptions,
        eventloop,
        client: client.clone(),
        publisher_client: publisher_client.clone(),
        filters,
        manifest,
        reconciliation: None,
        fresh_session_filters: None,
        subscribe_pending: false,
        expected_suback: None,
        ready: operations.source_ready.clone(),
        ack_tx,
        ack_rx,
        generation: 0,
        next_job_id: 0,
        pending_acks: HashMap::new(),
        ready_acks: VecDeque::new(),
        pending_qos0: None,
        wal_capacity: operations.wal.clone(),
        quarantine_capacity: operations.quarantine.clone(),
        operations,
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use rumqttc::MqttOptions as TestMqttOptions;

    use super::subscriptions::{filter_plan, BrokerIdentity, SubscriptionManifest};
    use super::*;
    use crate::config::MqttSourceConfig;
    use crate::pipeline::outcome::{DeferredReason, PipelineOutcome};

    #[test]
    fn test_subscription_reconcile_preserves_union_until_broker_ack() -> Result<()> {
        let known = ["route/a".to_owned(), "route/shared".to_owned()];
        let desired = ["route/b".to_owned(), "route/shared".to_owned()];

        let plan = filter_plan(Some(&known), true, &desired)?;

        assert_eq!(plan.pending, ["route/a", "route/b", "route/shared"]);
        assert_eq!(plan.subscribe, ["route/b", "route/shared"]);
        assert_eq!(plan.unsubscribe, ["route/a"]);
        Ok(())
    }

    #[test]
    fn test_subscription_reconcile_ignores_old_filters_for_fresh_session() -> Result<()> {
        let known = ["route/old".to_owned()];
        let desired = ["route/current".to_owned()];

        let plan = filter_plan(Some(&known), false, &desired)?;

        assert_eq!(plan.pending, desired);
        assert_eq!(plan.subscribe, desired);
        assert!(plan.unsubscribe.is_empty());
        Ok(())
    }

    #[test]
    fn test_subscription_reconcile_rejects_resumed_session_without_manifest() {
        let desired = ["route/current".to_owned()];

        assert!(filter_plan(None, true, &desired).is_err());
        assert!(filter_plan(None, false, &desired).is_ok());
    }

    #[tokio::test]
    async fn test_subscription_reconcile_untrusted_seed_requires_fresh_session() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut manifest = SubscriptionManifest::open(
            dir.path(),
            BrokerIdentity {
                host: "broker.example".to_owned(),
                port: 1883,
                client_id: "ingest".to_owned(),
            },
        )?;
        manifest.initialize_if_missing().await?;
        let desired = ["route/current".to_owned()];

        assert!(manifest.known_filters().is_none());
        assert!(filter_plan(manifest.known_filters(), true, &desired).is_err());
        assert!(filter_plan(manifest.known_filters(), false, &desired).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_subscription_reconcile_rejects_manifest_exceeding_metadata_budget() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let mut manifest = SubscriptionManifest::open(
            dir.path(),
            BrokerIdentity {
                host: "broker.example".to_owned(),
                port: 1883,
                client_id: "ingest".to_owned(),
            },
        )?;

        assert!(manifest.persist(vec!["x".repeat(16 * 1024)]).await.is_err());
        assert!(manifest.known_filters().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_subscription_reconcile_manifest_round_trips_filters_and_identity() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let identity = BrokerIdentity {
            host: "broker.example".to_owned(),
            port: 1883,
            client_id: "ingest".to_owned(),
        };
        let mut manifest = SubscriptionManifest::open(dir.path(), identity.clone())?;

        assert!(manifest.known_filters().is_none());
        manifest
            .persist(vec!["route/old".to_owned(), "route/current".to_owned()])
            .await?;

        let reopened = SubscriptionManifest::open(dir.path(), identity)?;

        assert_eq!(
            reopened.known_filters(),
            Some(["route/old".to_owned(), "route/current".to_owned()].as_slice())
        );
        assert!(SubscriptionManifest::open(
            dir.path(),
            BrokerIdentity {
                host: "other.example".to_owned(),
                port: 1883,
                client_id: "ingest".to_owned(),
            },
        )
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_subscription_reconcile_recovers_previous_slot_after_interrupted_write(
    ) -> Result<()> {
        let dir = tempfile::tempdir()?;
        let identity = BrokerIdentity {
            host: "broker.example".to_owned(),
            port: 1883,
            client_id: "ingest".to_owned(),
        };
        let mut manifest = SubscriptionManifest::open(dir.path(), identity.clone())?;

        manifest.persist(vec!["route/known".to_owned()]).await?;
        std::fs::write(dir.path().join(".mqtt-subscriptions.0.json"), b"{")?;

        let recovered = SubscriptionManifest::open(dir.path(), identity)?;

        assert_eq!(
            recovered.known_filters(),
            Some(["route/known".to_owned()].as_slice())
        );
        Ok(())
    }

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
        let manifest = SubscriptionManifest::open(
            capacity_dir.path(),
            BrokerIdentity {
                host: "localhost".to_owned(),
                port: 1883,
                client_id: "test".to_owned(),
            },
        )
        .expect("subscription manifest");
        let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);
        let publisher_client = Arc::new(Mutex::new(client.clone()));
        let operations = Arc::new(Operations::new(
            ready.clone(),
            Arc::new(crate::infrastructure::operations::ForwarderState::default()),
            capacity.clone(),
            capacity.clone(),
        ));

        (
            MqttSource {
                options,
                eventloop,
                client,
                publisher_client,
                filters,
                manifest,
                reconciliation: None,
                fresh_session_filters: None,
                subscribe_pending: false,
                expected_suback: None,
                ready,
                operations,
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

    async fn read_mqtt_packet(stream: &mut tokio::net::TcpStream) -> Result<(u8, Vec<u8>)> {
        let header = stream.read_u8().await?;
        let mut remaining = 0usize;
        let mut multiplier = 1usize;
        loop {
            let byte = stream.read_u8().await?;
            remaining += usize::from(byte & 0x7f) * multiplier;
            if byte & 0x80 == 0 {
                break;
            }
            multiplier *= 128;
        }
        let mut body = vec![0; remaining];
        stream.read_exact(&mut body).await?;
        Ok((header, body))
    }

    fn parse_filter_packet(body: &[u8], has_qos: bool) -> Result<(u16, Vec<String>)> {
        let packet_id = u16::from_be_bytes([body[0], body[1]]);
        let mut filters = Vec::new();
        let mut offset = 2;
        while offset < body.len() {
            let len = usize::from(u16::from_be_bytes([body[offset], body[offset + 1]]));
            offset += 2;
            filters.push(std::str::from_utf8(&body[offset..offset + len])?.to_owned());
            offset += len + usize::from(has_qos);
        }
        Ok((packet_id, filters))
    }

    async fn write_mqtt_packet(
        stream: &mut tokio::net::TcpStream,
        header: u8,
        body: &[u8],
    ) -> Result<()> {
        let len = u8::try_from(body.len())?;
        stream.write_all(&[header, len]).await?;
        stream.write_all(body).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_subscription_reconcile_subscribes_new_and_unsubscribes_obsolete() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (unsubscribed_tx, unsubscribed_rx) = tokio::sync::oneshot::channel();
        let (release_broker_tx, release_broker_rx) = tokio::sync::oneshot::channel();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await?;
            let (header, _) = read_mqtt_packet(&mut first).await?;
            anyhow::ensure!(header >> 4 == 1, "expected first MQTT CONNECT");
            first.write_all(&[0x20, 0x02, 0x01, 0x00]).await?;
            let (header, body) = read_mqtt_packet(&mut first).await?;
            anyhow::ensure!(header >> 4 == 8, "expected first MQTT SUBSCRIBE");
            let (packet_id, subscribed) = parse_filter_packet(&body, true)?;
            anyhow::ensure!(subscribed == ["route/b"], "unexpected first subscriptions");
            write_mqtt_packet(
                &mut first,
                0x90,
                &[(packet_id >> 8) as u8, packet_id as u8, 1],
            )
            .await?;
            let (header, body) = read_mqtt_packet(&mut first).await?;
            anyhow::ensure!(header >> 4 == 10, "expected first MQTT UNSUBSCRIBE");
            let (_, unsubscribed) = parse_filter_packet(&body, false)?;
            anyhow::ensure!(
                unsubscribed == ["route/a"],
                "unexpected first unsubscriptions"
            );
            drop(first);

            let (mut second, _) = listener.accept().await?;
            let (header, _) = read_mqtt_packet(&mut second).await?;
            anyhow::ensure!(header >> 4 == 1, "expected reconnect MQTT CONNECT");
            second.write_all(&[0x20, 0x02, 0x01, 0x00]).await?;
            let (header, body) = read_mqtt_packet(&mut second).await?;
            anyhow::ensure!(header >> 4 == 8, "expected reconnect MQTT SUBSCRIBE");
            let (packet_id, subscribed) = parse_filter_packet(&body, true)?;
            anyhow::ensure!(
                subscribed == ["route/b"],
                "unexpected reconnect subscriptions"
            );
            write_mqtt_packet(
                &mut second,
                0x90,
                &[(packet_id >> 8) as u8, packet_id as u8, 1],
            )
            .await?;
            let (header, body) = read_mqtt_packet(&mut second).await?;
            anyhow::ensure!(header >> 4 == 10, "expected reconnect MQTT UNSUBSCRIBE");
            let (packet_id, unsubscribed) = parse_filter_packet(&body, false)?;
            anyhow::ensure!(
                unsubscribed == ["route/a"],
                "unexpected reconnect unsubscriptions"
            );
            unsubscribed_tx
                .send(())
                .map_err(|_| anyhow::anyhow!("test receiver dropped"))?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            write_mqtt_packet(
                &mut second,
                0xb0,
                &[(packet_id >> 8) as u8, packet_id as u8],
            )
            .await?;
            release_broker_rx.await?;
            Ok::<_, anyhow::Error>(())
        });

        let mut options = TestMqttOptions::new("test-mqtt-reconcile", "127.0.0.1", address.port());
        options.set_clean_session(false);
        let (client, eventloop) = AsyncClient::new(options.clone(), MQTT_REQUEST_CAPACITY);
        let ready = Arc::new(AtomicBool::new(false));
        let client_id = options.client_id().to_owned();
        let (mut source, capacity_dir) = test_source(
            options,
            client,
            eventloop,
            ready.clone(),
            vec![SubscribeFilter::new("route/b".to_owned(), QoS::AtLeastOnce)],
            u64::MAX,
        );
        source.manifest = SubscriptionManifest::open(
            capacity_dir.path(),
            BrokerIdentity {
                host: "127.0.0.1".to_owned(),
                port: address.port(),
                client_id: client_id.clone(),
            },
        )?;
        source.manifest.persist(vec!["route/a".to_owned()]).await?;
        let (worker_tx, _worker_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let source_task =
            tokio::spawn(Box::new(source).run(IngestDispatcher::new(vec![worker_tx]), shutdown_rx));

        if !matches!(
            tokio::time::timeout(Duration::from_secs(5), unsubscribed_rx).await,
            Ok(Ok(()))
        ) {
            let source_state = if source_task.is_finished() {
                format!("{:?}", source_task.await)
            } else {
                "still running".to_owned()
            };
            let broker_state = if broker.is_finished() {
                format!("{:?}", broker.await)
            } else {
                "still running".to_owned()
            };
            anyhow::bail!(
                "broker did not observe UNSUBSCRIBE; source={source_state}, broker={broker_state}"
            );
        }
        assert!(!ready.load(Ordering::Relaxed));
        let became_ready = tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if became_ready.is_err() {
            let _ = release_broker_tx.send(());
            shutdown_tx.send(true)?;
            let source_result = tokio::time::timeout(Duration::from_secs(5), source_task).await??;
            let manifest = SubscriptionManifest::open(
                capacity_dir.path(),
                BrokerIdentity {
                    host: "localhost".to_owned(),
                    port: 1883,
                    client_id: "test".to_owned(),
                },
            )?;
            anyhow::bail!(
                "source did not become ready; result={source_result:?}, manifest={:?}",
                manifest.known_filters()
            );
        }
        shutdown_tx.send(true)?;
        tokio::time::timeout(Duration::from_secs(5), source_task).await???;
        release_broker_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("test broker already stopped"))?;
        broker.await??;
        let manifest = SubscriptionManifest::open(
            capacity_dir.path(),
            BrokerIdentity {
                host: "127.0.0.1".to_owned(),
                port: address.port(),
                client_id,
            },
        )?;
        assert_eq!(
            manifest.known_filters(),
            Some(["route/b".to_owned()].as_slice())
        );
        Ok(())
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
    fn test_durable_readiness_pending_ack_and_admission_reset_on_reconnect_and_exit() -> Result<()>
    {
        let options = TestMqttOptions::new("test-state", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(options.clone(), 10);
        let (mut source, _directory) = test_source(
            options,
            client,
            eventloop,
            Arc::new(AtomicBool::new(true)),
            Vec::new(),
            u64::MAX,
        );
        let (tx, mut rx) = mpsc::channel(1);
        let dispatcher = IngestDispatcher::new(vec![tx]);
        source.dispatch_publish(test_publish(QoS::AtLeastOnce, 7), &dispatcher)?;
        source.publish_admission(&dispatcher);
        let operations = source.operations.clone();
        assert_eq!(operations.pending_acks.load(Ordering::Relaxed), 1);
        assert!(!operations.admission_available());
        assert!(rx.try_recv().is_ok());
        assert!(operations.admission_available());

        source.reset_connection()?;
        assert_eq!(operations.pending_acks.load(Ordering::Relaxed), 0);
        assert!(!operations.admission_available());
        assert!(!operations.source_ready.load(Ordering::Relaxed));
        operations.source_ready.store(true, Ordering::Relaxed);
        drop(source);
        assert!(!operations.source_ready.load(Ordering::Relaxed));
        Ok(())
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
