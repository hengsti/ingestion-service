use std::sync::Arc;
use std::{future::Future, pin::Pin, time::Instant};

use anyhow::Context;
use tracing::warn;

use metrics::{counter, histogram};

use crate::{
    infrastructure::sink::Encoder,
    infrastructure::wal::{
        codec::wal_record_frame_size,
        types::{AppendDurableError, WalEvent},
        wal::Wal,
    },
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct PersistStage {
    wal: Arc<Wal>,
    encoder: Arc<dyn Encoder>,
}

impl PersistStage {
    pub fn new(wal: Arc<Wal>, encoder: Arc<dyn Encoder>) -> Self {
        Self { wal, encoder }
    }
}

impl PipelineStage for PersistStage {
    fn name(&self) -> &'static str {
        "persist"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();

            let route_id = ctx.route()?.metric_label();
            let record = ctx.telemetry_record()?;
            let mut payload = String::new();
            self.encoder.encode(record, &mut payload);
            wal_record_frame_size(ctx.topic().len(), payload.len())
                .context("encoded telemetry exceeds the WAL reader record limit")?;

            let event = WalEvent {
                topic: ctx.topic().to_string(),
                ts_ms: chrono::Utc::now().timestamp_millis(),
                payload,
            };

            match self.wal.append_durable(event).await {
                Ok(()) => {
                    counter!("ingest_messages_enqueued_total", "route" => route_id.clone())
                        .increment(1);
                    histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Continue)
                }
                Err(AppendDurableError::Full(_)) => {
                    counter!("ingest_queue_full_total", "route" => route_id.clone()).increment(1);
                    warn!(topic = %ctx.topic(), "wal queue full; marking for DLQ");
                    ctx.mark_dlq("wal queue full");
                    histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "queue_full")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(AppendDurableError::Closed(_)) => {
                    counter!("ingest_queue_closed_total", "route" => route_id.clone()).increment(1);
                    warn!(topic = %ctx.topic(), "wal queue closed; marking for DLQ");
                    ctx.mark_dlq("wal queue closed");
                    histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "queue_closed")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(AppendDurableError::TooLarge(_)) => {
                    anyhow::bail!("encoded telemetry exceeds the WAL reader record limit")
                }
                Err(AppendDurableError::Durability(reason)) => {
                    counter!("ingest_durability_ack_failed_total", "route" => route_id.clone())
                        .increment(1);
                    warn!(topic = %ctx.topic(), reason = %reason, "wal durability ack failed; marking for DLQ");
                    ctx.mark_dlq("wal durability ack failed");
                    histogram!("ingest_persist_duration_seconds", "route" => route_id, "result" => "durability_failed")
                        .record(start.elapsed().as_secs_f64());
                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::OnceLock;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::routes,
        infrastructure::router::{CompiledRoute, RouteSet},
        infrastructure::wal::{
            segment::segment_path,
            subscription::WalSubscription,
            types::{WalEvent, WalOptions},
        },
        model::messages::telemetry::{TelemetryRecord, TelemetryValue},
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    fn telemetry_record() -> TelemetryRecord {
        TelemetryRecord {
            route_id: "firmware-sensor".to_owned(),
            device_id: "esp32-1".to_owned(),
            measurement: "test_measurement".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: std::collections::BTreeMap::from([
                ("device_id".to_owned(), "esp32-1".to_owned()),
                ("room".to_owned(), "living_room".to_owned()),
            ]),
            fields: std::collections::BTreeMap::from([
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
                ("temp_c".to_owned(), TelemetryValue::Float(22.5)),
            ]),
            source_topic: "smarthome/esp32-1/bme680".to_owned(),
            retained: false,
        }
    }

    fn ctx_with_record() -> PipelineContext {
        static ROUTE: OnceLock<Arc<CompiledRoute>> = OnceLock::new();
        let route = ROUTE.get_or_init(|| {
            let routes = RouteSet::compile(
                routes::load("config/routes.json").expect("load routes"),
                "smarthome/_dlq/ingest",
            )
            .expect("compile routes");
            routes
                .select("smarthome/esp32-1/bme680")
                .expect("select sensor route")
        });
        let mut ctx = PipelineContext::new("smarthome/esp32-1/bme680", vec![]);
        ctx.set_route(route.clone());
        ctx.set_telemetry_record(telemetry_record());
        ctx
    }

    fn expected_line_protocol() -> String {
        let mut out = String::new();
        crate::infrastructure::sink::influx::InfluxEncoder.encode(&telemetry_record(), &mut out);
        out
    }

    async fn open_wal(dir: &std::path::Path, queue_capacity: usize) -> (Arc<Wal>, WalSubscription) {
        let (wal, sub) = Wal::open(WalOptions {
            dir: dir.to_path_buf(),
            segment_bytes: 1024 * 1024,
            queue_capacity,
        })
        .await
        .expect("wal open");
        (Arc::new(wal), sub)
    }

    async fn recv_one(sub: &mut WalSubscription, ms: u64) -> Option<WalEvent> {
        tokio::time::timeout(Duration::from_millis(ms), sub.next())
            .await
            .ok()
            .flatten()
            .map(|entry| entry.event)
    }

    #[tokio::test]
    async fn run_on_telemetry_record_appends_to_wal_and_returns_continue() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);
        let mut ctx = ctx_with_record();

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());

        let event = recv_one(&mut sub, 500).await.expect("event should arrive");
        assert_eq!(event.topic, "smarthome/esp32-1/bme680");
        assert_eq!(event.payload, expected_line_protocol());
    }

    #[tokio::test]
    async fn test_persist_rejects_oversized_encoded_payload_before_wal_append() {
        struct OversizedEncoder;

        impl Encoder for OversizedEncoder {
            fn encoded_size_upper_bound(&self, _record: &TelemetryRecord) -> usize {
                0
            }

            fn encode(&self, _record: &TelemetryRecord, out: &mut String) {
                out.push_str(&"x".repeat(1024 * 1024));
            }
        }

        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let stage = PersistStage::new(wal, Arc::new(OversizedEncoder));
        let mut ctx = ctx_with_record();

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err(), "oversized encoded record must be rejected");
        assert!(recv_one(&mut sub, 50).await.is_none());
    }

    #[tokio::test]
    async fn run_marks_dlq_with_queue_full_when_wal_queue_is_saturated() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 1).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal.clone(), encoder);

        // Keep the WAL channel under pressure with best-effort non-durable appends
        // from a separate producer so this durable append can observe `Full`.
        let flood_wal = wal.clone();
        let _flood = std::thread::spawn(move || {
            for i in 0..100_000u64 {
                let _ = flood_wal.try_append(WalEvent {
                    topic: format!("smarthome/dev-{i}/status"),
                    ts_ms: i as i64,
                    payload: format!(
                        "device_status,device_id=dev-{i},device_class=test rssi=-50i {i}"
                    ),
                });
            }
        });

        let mut saturated = None;
        for _ in 0..1_000 {
            let mut ctx = ctx_with_record();
            let result = stage.run(&mut ctx).await;
            if ctx.should_publish_dlq() {
                saturated = Some((result, ctx));
                break;
            }
        }

        let (result, ctx) = saturated.expect("WAL queue should saturate under producer contention");
        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert_eq!(ctx.dlq_reason(), Some("wal queue full"));
    }

    #[tokio::test]
    async fn run_marks_dlq_with_queue_closed_when_wal_writer_has_exited() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = Wal::open(WalOptions {
            dir: dir.path().to_path_buf(),
            // Tiny segment limit guarantees rotation on the second record.
            segment_bytes: 1,
            queue_capacity: 64,
        })
        .await
        .expect("wal open");
        let wal = Arc::new(wal);
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);

        let mut first = ctx_with_record();
        assert!(matches!(
            stage.run(&mut first).await,
            Ok(StageFlow::Continue)
        ));

        // Force rotation-open to fail by pre-creating segment 2 path as a directory.
        let seg2 = segment_path(dir.path(), 2);
        fs::create_dir_all(&seg2).unwrap();

        // Second append is accepted into the queue; writer then hits fatal open
        // error while rotating and exits, disconnecting the channel.
        let mut second = ctx_with_record();
        let _ = stage.run(&mut second).await;

        // Wait until a subsequent append observes the closed sender.
        let mut closed = None;
        for _ in 0..200 {
            let mut ctx = ctx_with_record();
            let result = stage.run(&mut ctx).await;
            if ctx.dlq_reason() == Some("wal queue closed") {
                closed = Some((result, ctx));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let (result, ctx) = closed.expect("WAL channel should close after writer fatal exit");
        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert_eq!(ctx.dlq_reason(), Some("wal queue closed"));
    }

    #[tokio::test]
    async fn run_without_telemetry_record_returns_error() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 16).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/bme680", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
