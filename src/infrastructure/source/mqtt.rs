use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, Publish, QoS, SubscribeFilter,
    SubscribeReasonCode,
};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use super::{AckKey, AckReceipt, DlqPublisher, IngestDispatcher, IngestJob, Source};
use crate::config::{Config, MqttSourceConfig};
use crate::infrastructure::router::RouteSet;

/// Delay between reconnect attempts after an event-loop poll error.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// MQTT wire-packet limit, including fixed header, topic, and payload.
const MAX_MQTT_PACKET_BYTES: usize = 128 * 1024;
const MAX_INFLIGHT_MESSAGES: usize = 100;

/// MQTT-backed [`Source`]. Never exits on connection errors: it keeps polling
/// so `rumqttc` reconnects, and re-subscribes to `filters` on every `ConnAck`.
/// Readiness is only reported once the broker acknowledges the subscription.
pub struct MqttSource {
    eventloop: EventLoop,
    client: AsyncClient,
    filters: Vec<SubscribeFilter>,
    subscribe_pending: bool,
    ready: Arc<AtomicBool>,
    ack_tx: mpsc::Sender<AckReceipt>,
    ack_rx: mpsc::Receiver<AckReceipt>,
    generation: u64,
    pending_acks: HashMap<AckKey, Publish>,
    ready_acks: VecDeque<AckKey>,
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

    fn invalidate_acknowledgements(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.pending_acks.clear();
        self.ready_acks.clear();
    }

    fn try_ack_ready(&mut self) {
        while let Some(key) = self.ready_acks.front().copied() {
            if key.generation != self.generation {
                self.ready_acks.pop_front();
                self.pending_acks.remove(&key);
                continue;
            }

            let Some(publish) = self.pending_acks.get(&key) else {
                self.ready_acks.pop_front();
                continue;
            };
            match self.client.try_ack(publish) {
                Ok(()) => {
                    self.ready_acks.pop_front();
                    self.pending_acks.remove(&key);
                }
                Err(err) => {
                    warn!(error = %err, "MQTT acknowledgement not queued; retrying");
                    break;
                }
            }
        }
    }

    fn handle_ack_receipt(&mut self, receipt: AckReceipt) {
        if receipt.key.generation != self.generation {
            return;
        }
        if receipt.durable {
            self.ready_acks.push_back(receipt.key);
        } else {
            self.pending_acks.remove(&receipt.key);
            self.ready.store(false, Ordering::Relaxed);
        }
        self.try_ack_ready();
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
                        self.handle_ack_receipt(receipt);
                    }
                    event = self.eventloop.poll() => {
                        let event = match event {
                            Ok(ev) => ev,
                            Err(err) => {
                                self.ready.store(false, Ordering::Relaxed);
                                self.invalidate_acknowledgements();
                                warn!(error = %err, "MQTT connection error; reconnecting");
                                tokio::select! {
                                    changed = shutdown_rx.changed() => {
                                        if changed.is_err() || *shutdown_rx.borrow() {
                                            break;
                                        }
                                    }
                                    () = tokio::time::sleep(RECONNECT_DELAY) => {}
                                }
                                continue;
                            }
                        };

                        match &event {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                info!("MQTT connected");
                                self.invalidate_acknowledgements();
                                // Always re-subscribe: brokers may report
                                // `session_present` without restoring subscriptions.
                                self.subscribe_pending = true;
                            }
                            Event::Incoming(Incoming::SubAck(suback)) => {
                                let all_ok = suback
                                    .return_codes
                                    .iter()
                                    .all(|c| matches!(c, SubscribeReasonCode::Success(_)));
                                if all_ok {
                                    self.ready.store(true, Ordering::Relaxed);
                                    info!(topics = self.filters.len(), "subscribed to MQTT topics");
                                } else {
                                    error!(codes = ?suback.return_codes, "MQTT subscribe rejected by broker");
                                }
                            }
                            Event::Incoming(Incoming::Disconnect) => {
                                self.ready.store(false, Ordering::Relaxed);
                                self.invalidate_acknowledgements();
                            }
                            _ => {}
                        }

                        if self.subscribe_pending {
                            self.try_subscribe();
                        }

                        if let Event::Incoming(Incoming::Publish(publish)) = event {
                            let key = (publish.qos == QoS::AtLeastOnce).then_some(AckKey {
                                generation: self.generation,
                                packet_id: publish.pkid,
                            });
                            let accepted = dispatcher.dispatch(IngestJob {
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
                                acknowledgement: key.map(|key| {
                                    super::AckHandle::new(key, self.ack_tx.clone())
                                }),
                            });
                            if accepted {
                                if let Some(key) = key {
                                    self.pending_acks.insert(key, publish);
                                }
                            } else {
                                self.ready.store(false, Ordering::Relaxed);
                            }
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
    client: AsyncClient,
}

impl MqttDlqPublisher {
    /// Wraps an existing MQTT client for DLQ publishing.
    pub fn new(client: AsyncClient) -> Self {
        Self { client }
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
                .publish(dlq_topic, QoS::AtLeastOnce, false, bytes)
                .await?;

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
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    let mqtt_cfg = cfg
        .mqtt
        .as_ref()
        .context("INPUT_SOURCE=mqtt requires MQTT_* connection variables")?;

    let mut mqttoptions = mqtt_options(mqtt_cfg);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);

    let filters = routes
        .subscriptions()
        .into_iter()
        .map(|topic| SubscribeFilter::new(topic.to_owned(), QoS::AtLeastOnce))
        .collect();

    let source: Box<dyn Source> = Box::new(MqttSource {
        eventloop,
        client: client.clone(),
        filters,
        subscribe_pending: false,
        ready,
        ack_tx,
        ack_rx,
        generation: 0,
        pending_acks: HashMap::new(),
        ready_acks: VecDeque::new(),
    });
    let publisher: Arc<dyn DlqPublisher> = Arc::new(MqttDlqPublisher::new(client));

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
        let (client, _eventloop) = AsyncClient::new(options, 10);
        let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);
        let ready = Arc::new(AtomicBool::new(true));
        let key = AckKey {
            generation: 1,
            packet_id: 7,
        };
        let mut pending_acks = HashMap::new();
        pending_acks.insert(
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
        let mut source = MqttSource {
            eventloop: _eventloop,
            client,
            filters: Vec::new(),
            subscribe_pending: false,
            ready: ready.clone(),
            ack_tx,
            ack_rx,
            generation: key.generation,
            pending_acks,
            ready_acks: VecDeque::new(),
        };

        source.handle_ack_receipt(AckReceipt {
            key,
            durable: false,
        });

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
        let (client, eventloop) = AsyncClient::new(opts, 10);
        let ready = Arc::new(AtomicBool::new(true));
        let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);
        let source = Box::new(MqttSource {
            eventloop,
            client,
            filters: vec![SubscribeFilter::new(
                "smarthome/+/sensor".to_owned(),
                QoS::AtLeastOnce,
            )],
            subscribe_pending: false,
            ready: ready.clone(),
            ack_tx,
            ack_rx,
            generation: 0,
            pending_acks: HashMap::new(),
            ready_acks: VecDeque::new(),
        });
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
    async fn mqtt_source_try_subscribe_full_channel_stays_pending() -> Result<()> {
        // Arrange: capacity-1 request channel already occupied by a publish.
        let opts = TestMqttOptions::new("test-mqtt-source-full", "127.0.0.1", 1);
        let (client, eventloop) = AsyncClient::new(opts, 1);
        client.try_publish("t", QoS::AtLeastOnce, false, "x")?;
        let (ack_tx, ack_rx) = mpsc::channel(MAX_INFLIGHT_MESSAGES);
        let mut source = MqttSource {
            eventloop,
            client,
            filters: vec![SubscribeFilter::new(
                "smarthome/+/sensor".to_owned(),
                QoS::AtLeastOnce,
            )],
            subscribe_pending: true,
            ready: Arc::new(AtomicBool::new(false)),
            ack_tx,
            ack_rx,
            generation: 0,
            pending_acks: HashMap::new(),
            ready_acks: VecDeque::new(),
        };

        // Act
        source.try_subscribe();

        // Assert
        assert!(source.subscribe_pending);
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
