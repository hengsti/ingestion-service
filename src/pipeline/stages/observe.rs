use std::{future::Future, pin::Pin};

use metrics::{counter, histogram};

use crate::{
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Debug, Default, Clone, Copy)]
pub struct ObserveStage;

impl ObserveStage {
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for ObserveStage {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            counter!("ingest_messages_processed_total").increment(1);

            match ctx.handled_message()? {
                HandledMessage::Sensor(_) => {
                    counter!("ingest_sensor_messages_processed_total").increment(1);
                }
                HandledMessage::Status(_) => {
                    counter!("ingest_status_messages_processed_total").increment(1);
                }
            }

            histogram!("ingest_pipeline_duration_seconds")
                .record(ctx.started_at().elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::messages::{
            message::HandledMessage,
            sensor::{SensorData, SensorMessage},
            status::StatusMessage,
        },
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    fn sensor_message() -> HandledMessage {
        HandledMessage::Sensor(SensorMessage {
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
        })
    }

    fn status_message() -> HandledMessage {
        HandledMessage::Status(StatusMessage {
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
        })
    }

    fn ctx_with_message(handled: HandledMessage) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);
        ctx.set_handled_message(handled);
        ctx
    }

    #[tokio::test]
    async fn run_on_sensor_message_returns_continue() {
        let mut ctx = ctx_with_message(sensor_message());

        let result = ObserveStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_status_message_returns_continue() {
        let mut ctx = ctx_with_message(status_message());

        let result = ObserveStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_without_handled_message_returns_error() {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = ObserveStage::new().run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
