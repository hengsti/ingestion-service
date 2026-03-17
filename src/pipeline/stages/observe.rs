use std::{future::Future, pin::Pin};

use metrics::{counter, histogram};

use crate::{
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
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
            counter!("ingest_messages_processed_total").increment(1);

            match ctx.handled_message()? {
                HandledMessage::Sensor(_) => {
                    counter!("ingest_sensor_messages_processed_total").increment(1);
                }
                HandledMessage::Status(_) => {
                    counter!("ingest_status_messages_processed_total").increment(1);
                }
            }

            histogram!("ingest_pipeline_duration_seconds")
                .record(ctx.started_at().elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}
