use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use tracing::{debug, warn};

use metrics::{counter, histogram};

use crate::{
    infrastructure::router::Router,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

pub struct ValidateRawStage {
    router: Arc<Router>,
    enforce_topic_device_match: bool,
}

impl ValidateRawStage {
    pub fn new(router: Arc<Router>, enforce_topic_device_match: bool) -> Self {
        Self {
            router,
            enforce_topic_device_match,
        }
    }
}

impl PipelineStage for ValidateRawStage {
    fn name(&self) -> &'static str {
        "validate_raw"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();
            let payload = ctx.payload_json()?;

            let (result, result_label) = match self.router.validate_raw(
                ctx.topic(),
                payload,
                self.enforce_topic_device_match,
            ) {
                Ok(Some(_message_type)) => {
                    counter!("ingest_validate_raw_success_total").increment(1);

                    (StageFlow::Continue, "success")
                }
                Ok(None) => {
                    counter!("ingest_validate_raw_ignored_total").increment(1);
                    debug!(topic = %ctx.topic(), "no matching route; stopping pipeline without DLQ");

                    ctx.mark_ignored("no matching route");

                    (StageFlow::Stop, "no_matching_route")
                }
                Err(err) => {
                    counter!("ingest_validate_raw_failed_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "raw validation failed; marking for DLQ");

                    ctx.mark_dlq(format!("raw validation failed: {}", err));

                    (StageFlow::Stop, "failed")
                }
            };

            histogram!("ingest_validate_raw_duration_seconds", "result" => result_label)
                .record(start.elapsed().as_secs_f64());

            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{json, Value};

    use super::*;
    use crate::{
        infrastructure::router::{Route, Router},
        model::messages::message::MessageType,
        pipeline::stage::StageFlow,
    };

    const SENSOR_SCHEMA: &str = include_str!("../../../schema/sensor.schema.json");
    const STATUS_SCHEMA: &str = include_str!("../../../schema/status.schema.json");

    /// Router with a single sensor route on `smarthome/+/sensor`.
    fn sensor_router() -> Arc<Router> {
        let route = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
        Arc::new(Router::new().add_route(route))
    }

    /// Non-strict router (unknown topics are silently ignored, not DLQ'd).
    fn non_strict_sensor_router() -> Arc<Router> {
        let route = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
        Arc::new(Router::new().strict(false).add_route(route))
    }

    fn valid_sensor_payload() -> Value {
        json!({
            "device_id": "esp32-1",
            "room": "living_room",
            "device_class": "esp32p4-bme680",
            "fw_version": "1.0.0",
            "time_ms": 1_700_000_000_000_i64,
            "time_iso": "2023-11-14T22:13:20Z",
            "time_valid": true,
            "data": {
                "temp_c": 22.5,
                "rel_hum_perc": 45.0,
                "pressure_hpa": 1013.25,
                "gas_ohm": 50_000.0,
                "altitude_m": 500.0
            }
        })
    }

    /// Build a context with `payload_json` already set (simulates post-decode state).
    fn ctx_with_json(topic: &str, payload: Value) -> PipelineContext {
        let mut ctx = PipelineContext::new(topic, vec![]);
        ctx.set_payload_json(payload);
        ctx
    }

    #[tokio::test]
    async fn run_on_valid_sensor_payload_returns_continue() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", valid_sensor_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        assert!(ctx.ignored_reason().is_none());
    }

    #[tokio::test]
    async fn run_on_unknown_topic_with_non_strict_router_marks_ignored_and_stops() {
        let stage = ValidateRawStage::new(non_strict_sensor_router(), false);
        let mut ctx = ctx_with_json("home/unknown/topic", valid_sensor_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(!ctx.should_publish_dlq());
        assert_eq!(ctx.ignored_reason(), Some("no matching route"));
    }

    #[tokio::test]
    async fn run_on_unknown_topic_with_strict_router_marks_dlq_and_stops() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        let mut ctx = ctx_with_json("home/unknown/topic", valid_sensor_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("raw validation failed"));
    }

    #[tokio::test]
    async fn run_on_schema_invalid_payload_marks_dlq_and_stops() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", json!({"device_id": "esp32-1"}));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("raw validation failed"));
    }

    #[tokio::test]
    async fn run_on_invalid_time_iso_marks_dlq_and_stops() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        let mut payload = valid_sensor_payload();
        payload["time_iso"] = json!("not-a-timestamp");
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", payload);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("raw validation failed"));
    }

    #[tokio::test]
    async fn run_with_device_id_mismatch_when_enforced_marks_dlq_and_stops() {
        let stage = ValidateRawStage::new(sensor_router(), true);
        let mut payload = valid_sensor_payload();
        payload["device_id"] = json!("esp32-2");
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", payload);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("raw validation failed"));
    }

    #[tokio::test]
    async fn run_with_device_id_mismatch_when_not_enforced_returns_continue() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        let mut payload = valid_sensor_payload();
        payload["device_id"] = json!("esp32-2"); // mismatches topic "esp32-1"
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", payload);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_without_payload_json_in_context_returns_error() {
        let stage = ValidateRawStage::new(sensor_router(), false);
        // Context has no payload_json set — simulates a stage ordering bug.
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_valid_status_payload_returns_continue() {
        let route = Route::new(MessageType::Status, STATUS_SCHEMA, "smarthome/+/status").unwrap();
        let router = Arc::new(Router::new().add_route(route));
        let stage = ValidateRawStage::new(router, false);

        let payload = json!({
            "device_id": "esp32-1",
            "device_class": "esp32p4-bme680",
            "fw_version": "1.0.0",
            "ip": "192.168.1.42",
            "rssi": -65,
            "time_ms": 1_700_000_000_000_i64,
            "time_iso": "2023-11-14T22:13:20Z",
            "time_valid": true
        });
        let mut ctx = ctx_with_json("smarthome/esp32-1/status", payload);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }
}
