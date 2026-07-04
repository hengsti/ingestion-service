use std::{future::Future, pin::Pin, time::Instant};

use anyhow::{Context, Result};
use tracing::warn;

use metrics::{counter, histogram};

use crate::{
    infrastructure::schema::JsonSchema,
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

pub struct ValidateBusinessStage {
    sensor_schema: JsonSchema,
    status_schema: JsonSchema,
}

impl ValidateBusinessStage {
    pub fn new() -> Result<Self> {
        let sensor_schema =
            JsonSchema::new(include_str!("../../../schema/sensor.business.schema.json"))
                .context("failed to load sensor business schema")?;

        let status_schema =
            JsonSchema::new(include_str!("../../../schema/status.business.schema.json"))
                .context("failed to load status business schema")?;

        Ok(Self {
            sensor_schema,
            status_schema,
        })
    }

    fn validate_handled_message(&self, handled: &HandledMessage) -> Result<()> {
        match handled {
            HandledMessage::Sensor(sensor) => {
                let value = serde_json::to_value(sensor)
                    .context("failed to serialize SensorMessage for business validation")?;

                self.sensor_schema
                    .validate(&value)
                    .context("sensor business schema validation failed")?;

                Ok(())
            }
            HandledMessage::Status(status) => {
                let value = serde_json::to_value(status)
                    .context("failed to serialize StatusMessage for business validation")?;

                self.status_schema
                    .validate(&value)
                    .context("status business schema validation failed")?;

                Ok(())
            }
        }
    }
}

impl PipelineStage for ValidateBusinessStage {
    fn name(&self) -> &'static str {
        "validate_business"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();

            let handled = ctx.handled_message()?;
            let kind = match handled {
                HandledMessage::Sensor(_) => "sensor",
                HandledMessage::Status(_) => "status",
            };

            match self.validate_handled_message(handled) {
                Ok(()) => {
                    counter!("ingest_validate_business_success_total", "kind" => kind).increment(1);
                    histogram!("ingest_validate_business_duration_seconds", "kind" => kind, "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Continue)
                }
                Err(err) => {
                    counter!("ingest_validate_business_failed_total", "kind" => kind).increment(1);
                    warn!(
                        topic = %ctx.topic(),
                        error = %err,
                        "business validation failed; marking for DLQ"
                    );

                    ctx.mark_dlq(format!("business validation failed: {}", err));

                    histogram!("ingest_validate_business_duration_seconds", "kind" => kind, "result" => "failed")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
            }
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

    fn stage() -> ValidateBusinessStage {
        ValidateBusinessStage::new().unwrap()
    }

    fn valid_sensor() -> HandledMessage {
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

    fn valid_status() -> HandledMessage {
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

    #[test]
    fn validate_handled_message_accepts_valid_sensor() {
        assert!(stage().validate_handled_message(&valid_sensor()).is_ok());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_empty_device_id() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.device_id = String::new();
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_negative_time_ms() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.time_ms = -1;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_rel_hum_at_zero() {
        // exclusiveMinimum: 0 — zero is not allowed.
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.rel_hum_perc = 0.0;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_rel_hum_above_100() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.rel_hum_perc = 100.1;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_gas_ohm_at_zero() {
        // exclusiveMinimum: 0 — zero is not allowed.
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.gas_ohm = 0.0;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_iaq_score_above_100() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.iaq_score = 100.1;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_sensor_with_empty_iaq_text() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.iaq_text = String::new();
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_accepts_sensor_with_rel_hum_at_100() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.rel_hum_perc = 100.0;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_ok());
    }

    #[test]
    fn validate_handled_message_accepts_sensor_with_iaq_score_at_0() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.iaq_score = 0.0;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_ok());
    }

    #[test]
    fn validate_handled_message_accepts_sensor_with_iaq_score_at_100() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.data.iaq_score = 100.0;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Sensor(msg))
            .is_ok());
    }

    #[test]
    fn validate_handled_message_accepts_valid_status() {
        assert!(stage().validate_handled_message(&valid_status()).is_ok());
    }

    #[test]
    fn validate_handled_message_rejects_status_with_empty_device_id() {
        let HandledMessage::Status(mut msg) = valid_status() else {
            unreachable!()
        };
        msg.device_id = String::new();
        assert!(stage()
            .validate_handled_message(&HandledMessage::Status(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_status_with_empty_ip() {
        let HandledMessage::Status(mut msg) = valid_status() else {
            unreachable!()
        };
        msg.ip = String::new();
        assert!(stage()
            .validate_handled_message(&HandledMessage::Status(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_status_with_negative_uptime() {
        let HandledMessage::Status(mut msg) = valid_status() else {
            unreachable!()
        };
        msg.uptime = -1;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Status(msg))
            .is_err());
    }

    #[test]
    fn validate_handled_message_rejects_status_with_negative_free_mem() {
        let HandledMessage::Status(mut msg) = valid_status() else {
            unreachable!()
        };
        msg.free_mem = -1;
        assert!(stage()
            .validate_handled_message(&HandledMessage::Status(msg))
            .is_err());
    }

    #[tokio::test]
    async fn run_on_valid_sensor_returns_continue() {
        let mut ctx = ctx_with_message(valid_sensor());

        let result = stage().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_valid_status_returns_continue() {
        let mut ctx = ctx_with_message(valid_status());

        let result = stage().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_business_violation_marks_dlq_and_stops() {
        let HandledMessage::Sensor(mut msg) = valid_sensor() else {
            unreachable!()
        };
        msg.device_id = String::new(); // violates minLength: 1
        let mut ctx = ctx_with_message(HandledMessage::Sensor(msg));

        let result = stage().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx
            .dlq_reason()
            .unwrap()
            .contains("business validation failed"));
    }

    #[tokio::test]
    async fn run_without_handled_message_in_context_returns_error() {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage().run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
