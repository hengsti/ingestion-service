use std::{future::Future, pin::Pin};

use crate::{
    infrastructure::cache::state::CacheState,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct CacheUpdateStage {
    cache_state: CacheState,
}

impl CacheUpdateStage {
    pub fn new(cache_state: CacheState) -> Self {
        Self { cache_state }
    }
}

impl PipelineStage for CacheUpdateStage {
    fn name(&self) -> &'static str {
        "cache_update"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let msg = ctx.handled_message()?;
            self.cache_state.update(msg);
            Ok(StageFlow::Continue)
        })
    }
}
