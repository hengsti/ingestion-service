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

    /// Runs the pipeline and reports whether output or rejection storage completed.
    pub async fn run(&self, ctx: &mut PipelineContext) -> bool {
        let mut completed = true;
        for stage in &self.stages {
            debug!(stage=%stage.name(), topic=%ctx.topic(), "running pipeline stage");

            let flow = match stage.run(ctx).await {
                Ok(flow) => flow,
                Err(err) => {
                    let msg = format!("stage '{}' failed: {}", stage.name(), err);
                    warn!(stage = stage.name(), topic = %ctx.topic(), error = %msg, "pipeline stage failed");
                    ctx.mark_dlq(msg);
                    completed = false;
                    StageFlow::Stop
                }
            };

            if matches!(flow, StageFlow::Stop) {
                completed = false;
                break;
            }
        }

        if ctx.should_publish_dlq() {
            if let Some(stage) = &self.failure_stage {
                return match stage.run(ctx).await {
                    Ok(_) => true,
                    Err(err) => {
                        warn!(stage = stage.name(), topic = %ctx.topic(), error = %err, "failure stage failed to persist rejection");
                        false
                    }
                };
            } else {
                warn!(topic = %ctx.topic(), reason = ?ctx.dlq_reason(), "message marked for DLQ, but no failure stage is configured");
                return false;
            }
        }

        completed
    }
}

impl Default for PipelineRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use anyhow::bail;

    use super::*;
    use crate::pipeline::stage::{PipelineStage, StageResult};

    struct RejectStage;

    impl PipelineStage for RejectStage {
        fn name(&self) -> &'static str {
            "reject"
        }

        fn run<'a>(
            &'a self,
            ctx: &'a mut PipelineContext,
        ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
            Box::pin(async move {
                ctx.mark_dlq("rejected");
                Ok(StageFlow::Stop)
            })
        }
    }

    struct FailingFailureStage;

    impl PipelineStage for FailingFailureStage {
        fn name(&self) -> &'static str {
            "failing_failure"
        }

        fn run<'a>(
            &'a self,
            _ctx: &'a mut PipelineContext,
        ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
            Box::pin(async { bail!("quarantine unavailable") })
        }
    }

    #[tokio::test]
    async fn run_defers_acknowledgement_when_rejection_persistence_fails() {
        let runner = PipelineRunner::new()
            .add_stage(RejectStage)
            .with_failure_stage(FailingFailureStage);
        let mut ctx = PipelineContext::new("topic", b"invalid".to_vec());

        assert!(!runner.run(&mut ctx).await);
    }
}
