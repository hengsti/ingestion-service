use std::{future::Future, pin::Pin, sync::Arc};

use tracing::{debug, warn};

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
            let payload = ctx.payload_json()?;

            match self
                .router
                .validate_raw(ctx.topic(), payload, self.enforce_topic_device_match)
            {
                Ok(Some(_message_type)) => Ok(StageFlow::Continue),
                Ok(None) => {
                    debug!(topic = %ctx.topic(), "no matching route; stopping pipeline without DLQ");
                    ctx.mark_ignored("no matching route");
                    Ok(StageFlow::Stop)
                }
                Err(err) => {
                    metrics::counter!("ingest_validate_raw_failed_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "raw validation failed; marking for DLQ");
                    ctx.mark_dlq(format!("raw validation failed: {}", err));
                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}
