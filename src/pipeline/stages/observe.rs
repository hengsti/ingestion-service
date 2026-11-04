use std::{future::Future, pin::Pin};

use metrics::histogram;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct ObserveStage;

impl ObserveStage {
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for ObserveStage {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            histogram!("ingest_pipeline_duration_seconds")
                .record(ctx.started_at().elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{context::PipelineContext, stage::StageFlow};

    #[tokio::test]
    async fn run_records_duration_without_pipeline_state() {
        let mut ctx = PipelineContext::new("topic", vec![]);

        assert!(matches!(
            ObserveStage::new().run(&mut ctx).await,
            Ok(StageFlow::Continue)
        ));
        assert!(!ctx.should_quarantine());
    }
}
