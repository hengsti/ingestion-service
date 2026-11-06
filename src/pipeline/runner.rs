use metrics::counter;
use tracing::{debug, warn};

use crate::pipeline::{
    context::PipelineContext,
    outcome::{DeferredReason, PipelineOutcome},
    stage::{PipelineStage, StageFlow},
};

pub(crate) const STAGE_LABELS: &[&str] = &[
    "decode",
    "validate_raw",
    "transform",
    "validate_output",
    "cache_update",
    "persist",
    "observe",
    "dlq_publish",
    "other",
];
pub(crate) const STAGE_OUTCOMES: &[&str] = &["continue", "stop", "error"];

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

    /// Runs the pipeline and returns its single terminal storage outcome.
    pub async fn run(&self, ctx: &mut PipelineContext) -> PipelineOutcome {
        let mut stopped = ctx.should_quarantine();
        if !stopped {
            for stage in &self.stages {
                debug!(stage=%stage.name(), topic=%ctx.topic(), "running pipeline stage");

                let flow = match stage.run(ctx).await {
                    Ok(flow) => {
                        Self::record_stage(
                            ctx,
                            stage.name(),
                            match flow {
                                StageFlow::Continue => "continue",
                                StageFlow::Stop => "stop",
                            },
                        );
                        flow
                    }
                    Err(_) => {
                        Self::record_stage(ctx, stage.name(), "error");
                        warn!(
                            stage = stage.name(),
                            topic = %ctx.topic(),
                            "pipeline stage failed; deferring outcome"
                        );
                        return Self::record_outcome(
                            ctx,
                            PipelineOutcome::Deferred(DeferredReason::StageFailure),
                        );
                    }
                };

                if matches!(flow, StageFlow::Stop) {
                    stopped = true;
                    break;
                }
            }
        }

        if let Some(reason) = ctx.deferred_reason() {
            return Self::record_outcome(ctx, PipelineOutcome::Deferred(reason));
        }

        if ctx.should_quarantine() {
            if let Some(stage) = &self.failure_stage {
                return match stage.run(ctx).await {
                    Ok(_) => {
                        Self::record_stage(ctx, stage.name(), "stop");
                        Self::record_outcome(ctx, PipelineOutcome::RejectionStored)
                    }
                    Err(_) => {
                        Self::record_stage(ctx, stage.name(), "error");
                        warn!(
                            stage = stage.name(),
                            topic = %ctx.topic(),
                            "failure stage failed to persist rejection"
                        );
                        Self::record_outcome(
                            ctx,
                            PipelineOutcome::Deferred(DeferredReason::QuarantineFailure),
                        )
                    }
                };
            } else {
                warn!(
                    topic = %ctx.topic(),
                    "rejected message has no durable quarantine stage"
                );
                return Self::record_outcome(
                    ctx,
                    PipelineOutcome::Deferred(DeferredReason::QuarantineFailure),
                );
            }
        }

        if stopped {
            return Self::record_outcome(
                ctx,
                PipelineOutcome::Deferred(DeferredReason::UnexpectedStop),
            );
        }

        if ctx.output_stored() {
            Self::record_outcome(ctx, PipelineOutcome::OutputStored)
        } else {
            Self::record_outcome(
                ctx,
                PipelineOutcome::Deferred(DeferredReason::MissingPersistence),
            )
        }
    }

    fn record_stage(ctx: &PipelineContext, stage: &str, outcome: &'static str) {
        let stage = STAGE_LABELS
            .iter()
            .copied()
            .find(|label| *label == stage)
            .unwrap_or("other");
        let route = ctx
            .route()
            .map(|route| metrics::SharedString::from(route.metric_label()))
            .unwrap_or_else(|_| metrics::SharedString::from(""));
        counter!("ingest_stage_outcomes_total", "route" => route, "stage" => stage, "outcome" => outcome)
            .increment(1);
    }

    fn record_outcome(ctx: &PipelineContext, outcome: PipelineOutcome) -> PipelineOutcome {
        let route = ctx
            .route()
            .map(|route| metrics::SharedString::from(route.metric_label()))
            .unwrap_or_else(|_| metrics::SharedString::from(""));
        counter!("ingest_pipeline_outcomes_total", "route" => route, "outcome" => outcome.metric_label())
            .increment(1);
        outcome
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
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;
    use crate::pipeline::outcome::DeferredReason;
    use crate::pipeline::stage::{PipelineStage, StageResult};

    #[test]
    fn test_pipeline_outcome_ack_eligibility_requires_stored_data() {
        assert!(PipelineOutcome::OutputStored.is_ack_eligible());
        assert!(PipelineOutcome::RejectionStored.is_ack_eligible());
        assert!(!PipelineOutcome::Deferred(DeferredReason::PersistenceFailure).is_ack_eligible());
    }

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
                ctx.mark_rejection("rejected");
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

    struct StopStage;

    impl PipelineStage for StopStage {
        fn name(&self) -> &'static str {
            "stop"
        }

        fn run<'a>(
            &'a self,
            _ctx: &'a mut PipelineContext,
        ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
            Box::pin(async { Ok(StageFlow::Stop) })
        }
    }

    struct FailingStage;

    impl PipelineStage for FailingStage {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn run<'a>(
            &'a self,
            _ctx: &'a mut PipelineContext,
        ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
            Box::pin(async { bail!("operational failure") })
        }
    }

    struct DeferredStage;

    impl PipelineStage for DeferredStage {
        fn name(&self) -> &'static str {
            "deferred"
        }

        fn run<'a>(
            &'a self,
            ctx: &'a mut PipelineContext,
        ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
            Box::pin(async move {
                ctx.mark_deferred(DeferredReason::PersistenceFailure);
                Ok(StageFlow::Stop)
            })
        }
    }

    #[tokio::test]
    async fn test_pipeline_outcome_quarantine_failure_is_deferred() {
        let runner = PipelineRunner::new()
            .add_stage(RejectStage)
            .with_failure_stage(FailingFailureStage);
        let mut ctx = PipelineContext::new("topic", b"invalid".to_vec());

        assert_eq!(
            runner.run(&mut ctx).await,
            PipelineOutcome::Deferred(DeferredReason::QuarantineFailure)
        );
    }

    #[tokio::test]
    async fn test_pipeline_outcome_missing_persistence_is_deferred() {
        let runner = PipelineRunner::new();
        let mut ctx = PipelineContext::new("topic", b"valid".to_vec());

        assert_eq!(
            runner.run(&mut ctx).await,
            PipelineOutcome::Deferred(DeferredReason::MissingPersistence)
        );
    }

    #[tokio::test]
    async fn test_pipeline_outcome_early_stop_is_deferred() {
        let runner = PipelineRunner::new().add_stage(StopStage);
        let mut ctx = PipelineContext::new("topic", b"valid".to_vec());

        assert_eq!(
            runner.run(&mut ctx).await,
            PipelineOutcome::Deferred(DeferredReason::UnexpectedStop)
        );
    }

    #[tokio::test]
    async fn test_pipeline_outcome_stage_error_is_deferred() {
        let runner = PipelineRunner::new()
            .add_stage(FailingStage)
            .with_failure_stage(FailingFailureStage);
        let mut ctx = PipelineContext::new("topic", b"valid".to_vec());

        assert_eq!(
            runner.run(&mut ctx).await,
            PipelineOutcome::Deferred(DeferredReason::StageFailure)
        );
        assert!(!ctx.should_quarantine());
    }

    #[tokio::test]
    async fn test_pipeline_outcome_operational_failure_skips_quarantine() {
        let runner = PipelineRunner::new()
            .add_stage(DeferredStage)
            .with_failure_stage(FailingFailureStage);
        let mut ctx = PipelineContext::new("topic", b"valid".to_vec());

        assert_eq!(
            runner.run(&mut ctx).await,
            PipelineOutcome::Deferred(DeferredReason::PersistenceFailure)
        );
        assert!(!ctx.should_quarantine());
    }

    #[tokio::test]
    async fn test_pipeline_premarked_rejection_skips_normal_stages() {
        let runner = PipelineRunner::new()
            .add_stage(FailingStage)
            .with_failure_stage(RejectStage);
        let mut ctx = PipelineContext::new("topic", b"policy violation".to_vec());
        ctx.mark_rejection("QoS 0 is not recoverable");

        assert_eq!(runner.run(&mut ctx).await, PipelineOutcome::RejectionStored);
    }

    #[test]
    fn test_pipeline_outcome_metric_uses_terminal_label() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            PipelineRunner::record_outcome(
                &PipelineContext::new("topic", vec![]),
                PipelineOutcome::Deferred(DeferredReason::PersistenceFailure),
            );
        });

        assert!(handle
            .render()
            .contains("ingest_pipeline_outcomes_total{route=\"\",outcome=\"deferred\"} 1\n"));
    }
}
