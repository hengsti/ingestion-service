use std::{future::Future, pin::Pin};

use anyhow::Result;

use crate::pipeline::context::PipelineContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageFlow {
    Continue,
    Stop,
}

pub type StageResult = Result<StageFlow>;

pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &str;

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>>;
}
