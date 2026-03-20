use std::{future::Future, pin::Pin, time::Instant};

use metrics::{counter, histogram};

use crate::{
    infrastructure::cache::state::CacheState,
    model::messages::message::HandledMessage,
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
            let start = Instant::now();

            let msg = ctx.handled_message()?;

            let kind = match msg {
                HandledMessage::Sensor(msg) => {
                    self.cache_state
                        .update(&HandledMessage::Sensor(msg.clone()));
                    "sensor"
                }
                HandledMessage::Status(msg) => {
                    self.cache_state
                        .update(&HandledMessage::Status(msg.clone()));
                    "status"
                }
            };

            counter!("ingest_cache_updates_total", "kind" => kind).increment(1);
            histogram!("ingest_cache_update_duration_seconds", "kind" => kind)
                .record(start.elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}
