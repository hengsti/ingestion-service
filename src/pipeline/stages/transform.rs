use std::{future::Future, pin::Pin};

use tracing::debug;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformStage;

impl TransformStage {
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for TransformStage {
    fn name(&self) -> &'static str {
        "transform"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let _msg = ctx.handled_message()?;

            debug!(
                topic = %ctx.topic(),
                "transform stage currently acts as a passthrough placeholder"
            );

            // TODO:
            // - normalize payloads
            // - enrich device metadata
            // - map future message versions to canonical internal models
            // - prepare alternative storage representations

            Ok(StageFlow::Continue)
        })
    }
}
