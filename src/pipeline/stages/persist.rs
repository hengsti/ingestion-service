use std::{future::Future, pin::Pin, time::Instant};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

use metrics::{counter, histogram};

use crate::{
    infrastructure::database::influx::{sensor_to_point, status_to_point},
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct PersistStage {
    tx: mpsc::Sender<String>,
}

impl PersistStage {
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
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

            let (line, kind) = match ctx.handled_message()? {
                HandledMessage::Sensor(sensor_msg) => {
                    (sensor_to_point(sensor_msg).to_line_protocol(), "sensor")
                }
                HandledMessage::Status(status_msg) => {
                    (status_to_point(status_msg).to_line_protocol(), "status")
                }
            };

            ctx.set_line_protocol(line.clone());

            match self.tx.try_send(line) {
                Ok(()) => {
                    counter!("ingest_messages_enqueued_total", "kind" => kind).increment(1);
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Continue)
                }
                Err(TrySendError::Full(_)) => {
                    counter!("ingest_queue_full_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "ingest queue full; marking for DLQ");
                    ctx.mark_dlq("ingest queue full");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_full")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(TrySendError::Closed(_)) => {
                    counter!("ingest_queue_closed_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "ingest queue closed; marking for DLQ");
                    ctx.mark_dlq("ingest queue closed");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_closed")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
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

    // ── run(): success paths ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_on_sensor_message_enqueues_line_and_returns_continue() {
        let (tx, mut rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        // Message must have been sent to the channel.
        let received = rx.try_recv().expect("expected a line in the channel");
        assert!(!received.is_empty());
    }

    #[tokio::test]
    async fn run_on_status_message_enqueues_line_and_returns_continue() {
        let (tx, mut rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Status(valid_status_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        let received = rx.try_recv().expect("expected a line in the channel");
        assert!(!received.is_empty());
    }

    // ── run(): line_protocol written to context ───────────────────────────────

    #[tokio::test]
    async fn run_sets_line_protocol_on_context_for_sensor() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));

        stage.run(&mut ctx).await.unwrap();

        let line = ctx.line_protocol().expect("line_protocol must be set");
        assert!(
            line.starts_with("bme680"),
            "expected measurement 'bme680', got: {line}"
        );
        assert!(line.contains("device_id=esp32-1"), "tag missing: {line}");
        assert!(line.contains("room=living_room"), "tag missing: {line}");
    }

    #[tokio::test]
    async fn run_sets_line_protocol_on_context_for_status() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Status(valid_status_msg()));

        stage.run(&mut ctx).await.unwrap();

        let line = ctx.line_protocol().expect("line_protocol must be set");
        assert!(
            line.starts_with("device_status"),
            "expected measurement 'device_status', got: {line}"
        );
        assert!(line.contains("device_id=esp32-1"), "tag missing: {line}");
    }

    #[tokio::test]
    async fn run_includes_timestamp_when_time_valid_and_time_ms_positive() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut msg = valid_sensor_msg();
        msg.time_valid = true;
        msg.time_ms = 1_700_000_000_000;
        let mut ctx = ctx_with_message(HandledMessage::Sensor(msg));

        stage.run(&mut ctx).await.unwrap();

        let line = ctx.line_protocol().unwrap();
        assert!(
            line.ends_with("1700000000000"),
            "expected timestamp at end of line, got: {line}"
        );
    }

    #[tokio::test]
    async fn run_omits_timestamp_when_time_valid_is_false() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut msg = valid_sensor_msg();
        msg.time_valid = false;
        let mut ctx = ctx_with_message(HandledMessage::Sensor(msg));

        stage.run(&mut ctx).await.unwrap();

        let line = ctx.line_protocol().unwrap();
        // Without a timestamp the line ends with the last field value, not a number.
        assert!(
            !line.ends_with("1700000000000"),
            "timestamp must be absent when time_valid=false, got: {line}"
        );
    }

    #[tokio::test]
    async fn run_omits_timestamp_when_time_ms_is_zero() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut msg = valid_sensor_msg();
        msg.time_valid = true;
        msg.time_ms = 0;
        let mut ctx = ctx_with_message(HandledMessage::Sensor(msg));

        stage.run(&mut ctx).await.unwrap();

        let line = ctx.line_protocol().unwrap();
        assert!(
            !line.ends_with(" 0"),
            "timestamp must be absent when time_ms=0, got: {line}"
        );
    }

    // ── run(): queue error paths ──────────────────────────────────────────────

    #[tokio::test]
    async fn run_marks_dlq_with_queue_full_when_channel_is_at_capacity() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        // Pre-fill the single-slot channel so the next try_send returns Full.
        tx.try_send("filler".to_string()).unwrap();
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert_eq!(ctx.dlq_reason(), Some("ingest queue full"));
    }

    #[tokio::test]
    async fn run_marks_dlq_with_queue_closed_when_receiver_is_dropped() {
        let (tx, rx) = mpsc::channel::<String>(1);
        drop(rx);
        let stage = PersistStage::new(tx);
        let mut ctx = ctx_with_message(HandledMessage::Sensor(valid_sensor_msg()));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert_eq!(ctx.dlq_reason(), Some("ingest queue closed"));
    }

    // ── run(): missing handled_message ────────────────────────────────────────

    #[tokio::test]
    async fn run_without_handled_message_returns_error() {
        let (tx, _rx) = mpsc::channel::<String>(1);
        let stage = PersistStage::new(tx);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
