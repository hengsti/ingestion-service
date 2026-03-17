use tracing::{debug, warn};

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow},
};

pub struct PipelineRunner {
    stages: Vec<Box<dyn PipelineStage>>,
    failure_stage: Option<Box<dyn PipelineStage>>,
}

impl PipelineRunner {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            failure_stage: None,
        }
    }

    pub fn add_stage<S>(mut self, stage: S) -> Self
    where
        S: PipelineStage + 'static,
    {
        self.stages.push(Box::new(stage));
        self
    }

    pub fn with_failure_stage<S>(mut self, stage: S) -> Self
    where
        S: PipelineStage + 'static,
    {
        self.failure_stage = Some(Box::new(stage));
        self
    }

    pub async fn run(&self, ctx: &mut PipelineContext) {
        for stage in &self.stages {
            debug!(stage=%stage.name(), topic=%ctx.topic(), "running pipeline stage");

            let flow = match stage.run(ctx).await {
                Ok(flow) => flow,
                Err(err) => {
                    let msg = format!("stage '{}' failed: {}", stage.name(), err);
                    warn!(stage = stage.name(), topic = %ctx.topic(), error = %msg, "pipeline stage failed");
                    ctx.mark_dlq(msg);
                    StageFlow::Stop
                }
            };

            if matches!(flow, StageFlow::Stop) {
                break;
            }
        }

        if ctx.should_publish_dlq() {
            if let Some(stage) = &self.failure_stage {
                if let Err(err) = stage.run(ctx).await {
                    warn!(stage = stage.name(), topic = %ctx.topic(), error = %err, "failure stage failed to publish DLQ message");
                }
            } else {
                warn!(topic = %ctx.topic(), reason = ?ctx.dlq_reason(), "message marked for DLQ, but no failure stage is configured");
            }
        }
    }
}

impl Default for PipelineRunner {
    fn default() -> Self {
        Self::new()
    }
}
