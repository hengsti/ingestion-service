use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use metrics::{counter, histogram};
use tracing::warn;

use crate::{
    infrastructure::router::RouteSet,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

/// Selects and validates the configured route for each raw payload.
pub struct ValidateRawStage {
    routes: Arc<RouteSet>,
}

impl ValidateRawStage {
    /// Creates a stage using the compiled route set.
    pub fn new(routes: Arc<RouteSet>) -> Self {
        Self { routes }
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
            let Some(route) = self.routes.select(ctx.topic()) else {
                counter!("ingest_validate_raw_failed_total").increment(1);
                ctx.mark_dlq("raw validation failed: no configured route for topic");
                histogram!("ingest_validate_raw_duration_seconds", "result" => "failed")
                    .record(start.elapsed().as_secs_f64());
                return Ok(StageFlow::Stop);
            };

            match route.validate_raw(ctx.payload_json()?) {
                Ok(()) => {
                    ctx.set_route(route);
                    counter!("ingest_validate_raw_success_total").increment(1);
                    histogram!("ingest_validate_raw_duration_seconds", "result" => "success")
                        .record(start.elapsed().as_secs_f64());
                    Ok(StageFlow::Continue)
                }
                Err(err) => {
                    counter!("ingest_validate_raw_failed_total").increment(1);
                    warn!(
                        topic = %ctx.topic(),
                        route = %route.id(),
                        error = %err,
                        "raw validation failed; marking for DLQ"
                    );
                    ctx.mark_dlq(format!("raw validation failed: {err}"));
                    histogram!("ingest_validate_raw_duration_seconds", "result" => "failed")
                        .record(start.elapsed().as_secs_f64());
                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::Result;
    use serde_json::{json, Value};

    use super::*;
    use crate::{
        config::routes,
        infrastructure::router::RouteSet,
        model::messages::telemetry::TelemetryValue,
        pipeline::{stage::StageFlow, stages::telemetry_mapping::TelemetryMappingStage},
    };

    fn configured_route_set() -> Result<Arc<RouteSet>> {
        let bundle_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/routes.json");
        let bundle = routes::load(bundle_path)?;
        Ok(Arc::new(RouteSet::compile(
            bundle,
            "smarthome/_dlq/ingest",
        )?))
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

    fn ctx_with_json(
        topic: &str,
        payload: Value,
        received_at_ms: i64,
        retained: bool,
    ) -> PipelineContext {
        let mut ctx = PipelineContext::new_with_metadata(topic, vec![], received_at_ms, retained);
        ctx.set_payload_json(payload);
        ctx
    }

    #[tokio::test]
    async fn test_configured_raw_and_mapping_preserve_intake_metadata() -> Result<()> {
        let raw_stage = ValidateRawStage::new(configured_route_set()?);
        let mapping_stage = TelemetryMappingStage::new();
        let mut payload = valid_sensor_payload();
        payload["time_valid"] = json!(false);
        let received_at_ms = 1_700_000_123_456;
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", payload, received_at_ms, true);

        assert!(matches!(
            raw_stage.run(&mut ctx).await?,
            StageFlow::Continue
        ));
        assert_eq!(ctx.route()?.id(), "firmware-sensor");
        assert!(matches!(
            mapping_stage.run(&mut ctx).await?,
            StageFlow::Continue
        ));

        let record = ctx.telemetry_record()?;
        assert_eq!(record.timestamp_ms, received_at_ms);
        assert_eq!(
            record.fields.get("mqtt_retained"),
            Some(&TelemetryValue::Boolean(true))
        );
        assert!(record.fields.contains_key("dew_point_c"));
        Ok(())
    }

    #[tokio::test]
    async fn test_configured_raw_rejects_unmatched_topic() -> Result<()> {
        let stage = ValidateRawStage::new(configured_route_set()?);
        let mut ctx = ctx_with_json(
            "smarthome/esp32-1/unknown",
            valid_sensor_payload(),
            1_700_000_000_000,
            false,
        );

        assert!(matches!(stage.run(&mut ctx).await?, StageFlow::Stop));
        assert!(ctx.should_publish_dlq());
        assert!(ctx
            .dlq_reason()
            .is_some_and(|reason| reason.contains("route")));
        Ok(())
    }

    #[tokio::test]
    async fn test_configured_raw_rejects_schema_invalid_payload() -> Result<()> {
        let stage = ValidateRawStage::new(configured_route_set()?);
        let mut ctx = ctx_with_json(
            "smarthome/esp32-1/sensor",
            json!({"device_id": "esp32-1"}),
            1_700_000_000_000,
            false,
        );

        assert!(matches!(stage.run(&mut ctx).await?, StageFlow::Stop));
        assert!(ctx.should_publish_dlq());
        Ok(())
    }
}
