mod config;
mod db;
mod dlq;
mod observation;
mod model;
mod validate;
mod cache;

use anyhow::{Context, Result};
use config::Config;
use db::influx::{InfluxWriter, sensor_to_point, status_to_point};
use observation::prometheus::MetricsServer;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use validate::{HandledMessage, MessageType, Route, Router};
use cache::state::CacheState;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let cfg = Config::from_env()?;
    info!(?cfg, "starting smarthome-ingest");

    // Cache
    let app_state = CacheState::new(cfg.cache_ttl_ms, cfg.cache_buffer);
    
    let http_state = app_state.clone();
    
    let _http_task = tokio::spawn(async move {
        let app = cache::http::router(http_state);
        let listener = tokio::net::TcpListener::bind(&cfg.cache_bind)
            .await
            .expect("failed to bind CACHE_BIND");
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    // Prometheus metrics server
    let _metrics_server = MetricsServer::start(&cfg.metrics_bind).await?;

    // Routes for MQTT topics with JSON schema validation
    let mut router = Router::new()
        .strict(true);

    for (k, v) in cfg.mqtt_topics.iter().filter(|(k, _)| k.starts_with("MQTT_TOPIC_")) {
            let message_type = k.strip_prefix("MQTT_TOPIC_").unwrap().to_string().to_uppercase();
            let msg_type = match message_type.as_str() {
                "SENSOR" => MessageType::Sensor,
                "STATUS" => MessageType::Status,
                "DLQ" => continue, // skip DLQ topic
                _ => {
                    warn!(topic=%k, "unknown MQTT topic config key; skipping");
                    continue;
                }
            };

            let schema_path = match message_type.as_str() {
                "SENSOR" => include_str!("../schema/sensor.schema.json"),
                "STATUS" => include_str!("../schema/status.schema.json"),
                _ => unreachable!(),
            };

            router = router.add_route(Route::new(msg_type, &schema_path, v)?);

            info!(message_type=%message_type, topic=%v, schema=%schema_path, "configured route for MQTT topic with schema validation");
        }
    
    let dlq_topic = cfg.mqtt_topics.iter()
        .find(|(k, _)| k.ends_with("DLQ"))
        .map(|(_, v)| v.as_str())
        .context("MQTT_TOPIC_DLQ not configured")?;

    // MQTT
    let mut mqttoptions = MqttOptions::new(&cfg.mqtt_client_id, &cfg.mqtt_host, cfg.mqtt_port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(30));
    
    if let (Some(u), Some(p)) = (&cfg.mqtt_username, &cfg.mqtt_password) {
        mqttoptions.set_credentials(u, p);
    }

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    
    for (_, v) in cfg.mqtt_topics.iter().filter(|(k, _)| !k.ends_with("DLQ")) {
        mqtt_client.subscribe(v, QoS::AtLeastOnce).await?;
        info!(topic=%v, "subscribed to MQTT topic");
    }

    // Influx batch channel
    let (tx, rx) = mpsc::channel::<String>(10_000);

    let influx = InfluxWriter::new(
        &cfg.influx_url,
        &cfg.influx_org,
        &cfg.influx_bucket,
        &cfg.influx_token,
    )?;

    let batch_size = cfg.batch_size;
    let flush_interval_ms = cfg.flush_interval_ms;

    let _influx_task = tokio::spawn(async move {
        if let Err(e) = influx.run_batcher(rx, batch_size, flush_interval_ms).await {
            error!(error=%e, "influx batcher failed");
        }
    });

    // Main consume loop
    loop {
        let event = eventloop.poll().await.context("MQTT poll failed")?;
        if let Event::Incoming(Incoming::Publish(p)) = event {
            metrics::counter!("mqtt_messages_received_total").increment(1);
            
            let topic = p.topic;
            
            // Handle non-UTF8 payloads immediately with DLQ
            let payload_str = match std::str::from_utf8(&p.payload) {
                Ok(s) => s,
                Err(e) => {
                    metrics::counter!("ingest_incoming_non_utf8_total").increment(1);
                    warn!(topic=%topic, error=%e, "payload not utf8; sending to DLQ");
                    
                    if let Err(e) = dlq::publish_dlq(
                        &mqtt_client,
                        dlq_topic,
                        &topic,
                        "<non-utf8>",
                        "payload not utf8",
                    )
                    .await {
                        metrics::counter!("dlq_publish_errors_total").increment(1);
                        warn!(topic=%topic, error=%e, "failed to publish to DLQ");
                    } else {
                        metrics::counter!("dlq_messages_published_total").increment(1);
                    }
                
                    continue;
                }
            };

            // Parse JSON
            let payload_value: serde_json::Value = match serde_json::from_str(payload_str) {
                Ok(v) => v,
                Err(e) => {
                    metrics::counter!("ingest_incoming_invalid_json_total").increment(1);
                    warn!(topic=%topic, error=%e, "invalid JSON; sending to DLQ");
                    
                    if let Err(e) = dlq::publish_dlq(
                        &mqtt_client,
                        dlq_topic,
                        &topic,
                        payload_str,
                        &format!("invalid JSON: {}", e),
                    )
                    .await {
                        metrics::counter!("dlq_publish_errors_total").increment(1);
                        warn!(topic=%topic, error=%e, "failed to publish to DLQ");
                    } else {
                        metrics::counter!("dlq_messages_published_total").increment(1);
                    }

                    continue;
                }
            };

            // JSON Schema validation
            let handled = match router.process(&topic, payload_value, cfg.enforce_topic_device_match) {
                Ok(Some(h)) => h,
                Ok(None) => continue, // No matching route, but router is in non-strict mode
                Err(e) => {
                    metrics::counter!("ingest_validation_failed_total").increment(1);
                    warn!(topic=%topic, error=%e, "validation failed; sending to DLQ");

                    if let Err(e2) = dlq::publish_dlq(
                        &mqtt_client,
                        dlq_topic,
                        &topic,
                        payload_str,
                        &format!("validation failed: {}", e),
                    )
                    .await {
                        metrics::counter!("dlq_publish_errors_total").increment(1);
                        warn!(topic=%topic, error=%e2, "failed to publish to DLQ");
                    } else {
                        metrics::counter!("dlq_messages_published_total").increment(1);
                    }

                    continue;
                }
            };

            // Update in-memory cache state
            app_state.update(&handled);

            match handled {
                HandledMessage::Sensor(_sensor_msg) => {
                    let point = sensor_to_point(&_sensor_msg);
                    let line = point.to_line_protocol();

                    if let Err(e) = tx.try_send(line) {
                        metrics::counter!("ingest_queue_full_total").increment(1);
                        warn!(topic=%topic, error=%e, "ingest queue full; sending to DLQ");
                        
                        if let Err(e2) = dlq::publish_dlq(
                            &mqtt_client,
                            dlq_topic,
                            &topic,
                            payload_str,
                            "ingest queue full",
                        )
                        .await {
                            metrics::counter!("dlq_publish_errors_total").increment(1);
                            warn!(topic=%topic, error=%e2, "failed to publish to DLQ");
                        } else {
                            metrics::counter!("dlq_messages_published_total").increment(1);
                        }
                    } else {
                        metrics::counter!("ingest_messages_enqueued_total").increment(1);
                    }
                },
                HandledMessage::Status(_status_msg) => {
                    let point = status_to_point(&_status_msg);
                    let line = point.to_line_protocol();
                    
                    if let Err(e) = tx.try_send(line) {
                        metrics::counter!("ingest_queue_full_total").increment(1);
                        warn!(topic=%topic, error=%e, "ingest queue full; sending to DLQ");
                        
                        if let Err(e2) = dlq::publish_dlq(
                            &mqtt_client,
                            dlq_topic,
                            &topic,
                            payload_str,
                            "ingest queue full",
                        )
                        .await {
                            metrics::counter!("dlq_publish_errors_total").increment(1);
                            warn!(topic=%topic, error=%e2, "failed to publish to DLQ");
                        } else {
                            metrics::counter!("dlq_messages_published_total").increment(1);
                        }
                    } else {
                        metrics::counter!("ingest_messages_enqueued_total").increment(1);
                    }
                },
            }

        }
    }

    #[allow(unreachable_code)]
    {
        _influx_task.await.ok();
        Ok(())
    }
}
