use std::{future::Future, pin::Pin, sync::Arc};

use tracing::{debug, warn};

use crate::{
    infrastructure::router::Router,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

pub struct ValidateStage {
    router: Arc<Router>,
    enforce_topic_device_match: bool,
}

impl ValidateStage {
    pub fn new(router: Arc<Router>, enforce_topic_device_match: bool) -> Self {
        Self {
            router,
            enforce_topic_device_match,
        }
    }
}

impl PipelineStage for ValidateStage {
    fn name(&self) -> &'static str {
        "ValidateStage"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let payload = ctx.payload_json()?.clone();

            match self
                .router
                .process(ctx.topic(), payload, self.enforce_topic_device_match)
            {
                Ok(Some(msg)) => {
                    ctx.set_handled_message(msg);
                    Ok(StageFlow::Continue)
                }
                Ok(None) => {
                    debug!(topic = %ctx.topic(), "no matching route; stopping pipeline without DLQ");
                    ctx.mark_ignored("no matching route".to_string());
                    Ok(StageFlow::Stop)
                }
                Err(err) => {
                    metrics::counter!("ingest_validation_failed_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "validation failed; marking for DLQ");
                    ctx.mark_dlq(format!("validation failed: {}", err));
                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}
