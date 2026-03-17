use std::{future::Future, pin::Pin};

use tokio::sync::mpsc;
use tracing::warn;

use crate::{
    infrastructure::database::influx::{sensor_to_point, status_to_point},
    model::messages::message::HandledMessage,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct PersistStage {
    tx: mpsc::Sender<String>,
}

impl PersistStage {
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
    }
}

impl PipelineStage for PersistStage {
    fn name(&self) -> &'static str {
        "persist"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let line = match ctx.handled_message()? {
                HandledMessage::Sensor(sensor_msg) => {
                    sensor_to_point(sensor_msg).to_line_protocol()
                }
                HandledMessage::Status(status_msg) => {
                    status_to_point(status_msg).to_line_protocol()
                }
            };

            ctx.set_line_protocol(line.clone());

            if let Err(err) = self.tx.try_send(line) {
                metrics::counter!("ingest_queue_full_total").increment(1);
                warn!(topic = %ctx.topic(), error = %err, "ingest queue full; marking for DLQ");
                ctx.mark_dlq("ingest queue full");
                return Ok(StageFlow::Stop);
            }

            metrics::counter!("ingest_messages_enqueued_total").increment(1);
            Ok(StageFlow::Continue)
        })
    }
}
