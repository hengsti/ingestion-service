use std::{future::Future, pin::Pin};

use anyhow::{Context, Result};
use tracing::warn;

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
            match self.validate_handled_message(ctx.handled_message()?) {
                Ok(()) => Ok(StageFlow::Continue),
                Err(err) => {
                    metrics::counter!("ingest_validate_business_failed_total").increment(1);
                    warn!(
                        topic = %ctx.topic(),
                        error = %err,
                        "business validation failed; marking for DLQ"
                    );
                    ctx.mark_dlq(format!("business validation failed: {}", err));
                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}
