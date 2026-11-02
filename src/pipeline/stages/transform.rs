use std::{future::Future, pin::Pin};

use metrics::counter;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
    stages::telemetry_mapping::map_payload,
};

/// Maps the selected route into a typed record and applies its compiled calculations.
#[derive(Debug, Default, Clone, Copy)]
pub struct TelemetryMappingStage;

impl TelemetryMappingStage {
    /// Creates the configured telemetry mapping stage.
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for TelemetryMappingStage {
    fn name(&self) -> &'static str {
        "transform"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let record = {
                let route = ctx.route()?;
                let mapped = map_payload(
                    route.config(),
                    ctx.payload_json()?,
                    ctx.topic(),
                    ctx.received_at_ms(),
                    ctx.retained(),
                )?;
                route.calculations().apply(mapped)?
            };
            ctx.set_telemetry_record(record);
            counter!("ingest_transform_success_total").increment(1);
            Ok(StageFlow::Continue)
        })
    }
}
