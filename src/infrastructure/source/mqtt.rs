use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS};
use serde_json::json;
use tokio::sync::watch;
use tracing::info;

use super::{DlqPublisher, IngestDispatcher, IngestJob, Source};
use crate::config::Config;

/// MQTT-backed [`Source`]. Holds only the event loop and readiness flag; the
/// `AsyncClient` used for subscribing is dropped after [`build`] returns since
/// the event loop owns the network connection independent of client handles.
pub struct MqttSource {
    eventloop: EventLoop,
    ready: Arc<AtomicBool>,
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
                    event = self.eventloop.poll() => {
                        let event = match event {
                            Ok(ev) => ev,
                            Err(err) => {
                                self.ready.store(false, Ordering::Relaxed);
                                return Err(err).context("MQTT poll failed");
                            }
                        };

                        match &event {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                self.ready.store(true, Ordering::Relaxed);
                                info!("MQTT connected");
                            }
                            Event::Incoming(Incoming::Disconnect) => {
                                self.ready.store(false, Ordering::Relaxed);
                            }
                            _ => {}
                        }

                        if let Event::Incoming(Incoming::Publish(publish)) = event {
                            dispatcher.dispatch(IngestJob {
                                topic: publish.topic,
                                payload: publish.payload,
                            });
                        }
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

            info!(src_topic = %src_topic, error = %err, "publishing message to DLQ topic");

            let bytes = serde_json::to_vec(&dlq)?;
            self.client
                .publish(dlq_topic, QoS::AtLeastOnce, false, bytes)
                .await?;

            Ok(())
        })
    }
}

/// Builds an [`MqttSource`] + [`MqttDlqPublisher`] pair: connects, subscribes
/// to every configured non-DLQ `MQTT_TOPIC_*` route, and returns both handles.
///
/// # Errors
/// Returns an error if `cfg.mqtt` is `None` (should not happen — `Config::from_env`
/// guarantees it's populated when `input_source == InputSourceKind::Mqtt`), or if
/// subscribing to any configured topic fails.
pub async fn build(
    cfg: &Config,
    ready: Arc<AtomicBool>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    let mqtt_cfg = cfg
        .mqtt
        .as_ref()
        .context("INPUT_SOURCE=mqtt requires MQTT_* connection variables")?;

    let mut mqttoptions = MqttOptions::new(&mqtt_cfg.client_id, &mqtt_cfg.host, mqtt_cfg.port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    if let (Some(username), Some(password)) = (&mqtt_cfg.username, &mqtt_cfg.password) {
        mqttoptions.set_credentials(username, password);
    }

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

    for (_, topic) in cfg.mqtt_topics.iter().filter(|(k, _)| !k.ends_with("DLQ")) {
        client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "subscribed to MQTT topic");
    }

    let source: Box<dyn Source> = Box::new(MqttSource { eventloop, ready });
    let publisher: Arc<dyn DlqPublisher> = Arc::new(MqttDlqPublisher::new(client));

    Ok((source, publisher))
}

#[cfg(test)]
mod tests {
    use rumqttc::MqttOptions as TestMqttOptions;

    use super::*;

    /// Returns a client whose eventloop receiver is alive so that `publish` queues successfully.
    fn client_with_live_eventloop() -> (AsyncClient, rumqttc::EventLoop) {
        let opts = TestMqttOptions::new("test-mqtt-source-ok", "localhost", 1883);
        AsyncClient::new(opts, 10)
    }

    /// Returns a client whose eventloop has been dropped so that `publish` returns an error.
    fn client_with_dropped_eventloop() -> AsyncClient {
        let opts = TestMqttOptions::new("test-mqtt-source-err", "localhost", 1884);
        let (client, _eventloop) = AsyncClient::new(opts, 10);
        // _eventloop is dropped here → receiver gone → publish will fail
        client
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
