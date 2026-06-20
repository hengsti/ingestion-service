use std::sync::Arc;
use std::{future::Future, pin::Pin, time::Instant};

use tracing::warn;

use metrics::{counter, histogram};

use crate::{
    infrastructure::database::influx::{sensor_to_point, status_to_point},
    infrastructure::wal::{
        types::{AppendDurableError, WalEvent},
        wal::Wal,
    },
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct PersistStage {
    wal: Arc<Wal>,
}

impl PersistStage {
    pub fn new(wal: Arc<Wal>) -> Self {
        Self { wal }
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

            let message = ctx.handled_message()?;
            let mut line_protocol = String::new();
            let kind = match message {
                HandledMessage::Sensor(sensor) => {
                    sensor_to_point(sensor).write_line_protocol(&mut line_protocol);
                    "sensor"
                }
                HandledMessage::Status(status) => {
                    status_to_point(status).write_line_protocol(&mut line_protocol);
                    "status"
                }
            };

            let event = WalEvent {
                topic: ctx.topic().to_string(),
                ts_ms: chrono::Utc::now().timestamp_millis(),
                line_protocol,
            };

            match self.wal.append_durable(event).await {
                Ok(()) => {
                    counter!("ingest_messages_enqueued_total", "kind" => kind).increment(1);
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Continue)
                }
                Err(AppendDurableError::Full(_)) => {
                    counter!("ingest_queue_full_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "wal queue full; marking for DLQ");
                    ctx.mark_dlq("wal queue full");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_full")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(AppendDurableError::Closed(_)) => {
                    counter!("ingest_queue_closed_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "wal queue closed; marking for DLQ");
                    ctx.mark_dlq("wal queue closed");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_closed")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(AppendDurableError::Durability(reason)) => {
                    counter!("ingest_durability_ack_failed_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), reason = %reason, "wal durability ack failed; marking for DLQ");
                    ctx.mark_dlq("wal durability ack failed");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "durability_failed")
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
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        infrastructure::database::influx::{sensor_to_point, status_to_point},
        infrastructure::wal::{
            segment::segment_path,
            subscription::WalSubscription,
            types::{WalEvent, WalOptions},
        },
        model::messages::{
            message::HandledMessage,
            sensor::{SensorData, SensorMessage},
            status::StatusMessage,
        },
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    // ── helpers ───────────────────────────────────────────────────────────────

    fn valid_sensor_msg() -> SensorMessage {
        SensorMessage {
            device_id: "esp32-1".to_string(),
            room: "living_room".to_string(),
            device_class: "esp32p4-bme680".to_string(),
            fw_version: "1.0.0".to_string(),
            time_ms: 1_700_000_000_000,
            time_iso: "2023-11-14T22:13:20Z".to_string(),
            time_valid: true,
            data: SensorData {
                temp_c: 22.5,
                rel_hum_perc: 45.0,
                pressure_hpa: 1013.25,
                gas_ohm: 50_000.0,
                iaq_score: 85.0,
                iaq_text: "Air quality is Good".to_string(),
                dew_point_c: 9.5,
                heat_index_c: 22.0,
                altitude_m: 500.0,
            },
        }
    }

    fn valid_status_msg() -> StatusMessage {
        StatusMessage {
            device_id: "esp32-1".to_string(),
            device_class: "esp32p4-bme680".to_string(),
            fw_version: "1.0.0".to_string(),
            ip: "192.168.1.42".to_string(),
            rssi: -65,
            time_ms: 1_700_000_000_000,
            time_iso: "2023-11-14T22:13:20Z".to_string(),
            time_valid: true,
            uptime: 3600,
            free_mem: 200_000,
            ssid: "HomeNet".to_string(),
        }
    }

    fn ctx_with_message(handled: HandledMessage) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);
        ctx.set_handled_message(handled);
        ctx
    }

    fn expected_line_protocol(msg: &HandledMessage) -> String {
        let mut out = String::new();
        match msg {
            HandledMessage::Sensor(sensor) => sensor_to_point(sensor).write_line_protocol(&mut out),
            HandledMessage::Status(status) => status_to_point(status).write_line_protocol(&mut out),
        }
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

    // ── run(): success paths ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_on_sensor_message_appends_to_wal_and_returns_continue() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let stage = PersistStage::new(wal);
        let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());

        let event = recv_one(&mut sub, 500).await.expect("event should arrive");
        assert_eq!(event.topic, "smarthome/esp32-1/sensor");
        assert_eq!(
            event.line_protocol,
            expected_line_protocol(&HandledMessage::Sensor(valid_sensor_msg()))
        );
    }

    #[tokio::test]
    async fn run_on_status_message_appends_to_wal_and_returns_continue() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let stage = PersistStage::new(wal);
        let mut ctx = ctx_with_message(HandledMessage::Status(valid_status_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());

        let event = recv_one(&mut sub, 500).await.expect("event should arrive");
        assert_eq!(
            event.line_protocol,
            expected_line_protocol(&HandledMessage::Status(valid_status_msg()))
        );
    }

    // ── run(): queue error paths ──────────────────────────────────────────────

    #[tokio::test]
    async fn run_marks_dlq_with_queue_full_when_wal_queue_is_saturated() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 1).await;
        let stage = PersistStage::new(wal.clone());

        // Keep the WAL channel under pressure with best-effort non-durable appends
        // from a separate producer so this durable append can observe `Full`.
        let flood_wal = wal.clone();
        let _flood = std::thread::spawn(move || {
            for i in 0..100_000u64 {
                let _ = flood_wal.try_append(WalEvent {
                    topic: format!("smarthome/dev-{i}/status"),
                    ts_ms: i as i64,
                    line_protocol: format!(
                        "device_status,device_id=dev-{i},device_class=test rssi=-50i {i}"
                    ),
                });
            }
        });

        let mut saturated = None;
        for _ in 0..1_000 {
            let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));
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
        let stage = PersistStage::new(wal);

        // First append writes segment 1.
        let mut first = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));
        assert!(matches!(
            stage.run(&mut first).await,
            Ok(StageFlow::Continue)
        ));

        // Force rotation-open to fail by pre-creating segment 2 path as a directory.
        let seg2 = segment_path(dir.path(), 2);
        fs::create_dir_all(&seg2).unwrap();

        // Second append is accepted into the queue; writer then hits fatal open
        // error while rotating and exits, disconnecting the channel.
        let mut second = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));
        let _ = stage.run(&mut second).await;

        // Wait until a subsequent append observes the closed sender.
        let mut closed = None;
        for _ in 0..200 {
            let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));
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

    // ── run(): missing handled_message ────────────────────────────────────────

    #[tokio::test]
    async fn run_without_handled_message_returns_error() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 16).await;
        let stage = PersistStage::new(wal);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
