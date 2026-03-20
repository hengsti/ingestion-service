use std::{future::Future, pin::Pin, time::Instant};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

use metrics::{counter, histogram};

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
            let start = Instant::now();

            let (line, kind) = match ctx.handled_message()? {
                HandledMessage::Sensor(sensor_msg) => {
                    (sensor_to_point(sensor_msg).to_line_protocol(), "sensor")
                }
                HandledMessage::Status(status_msg) => {
                    (status_to_point(status_msg).to_line_protocol(), "status")
                }
            };

            ctx.set_line_protocol(line.clone());

            match self.tx.try_send(line) {
                Ok(()) => {
                    counter!("ingest_messages_enqueued_total", "kind" => kind).increment(1);
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Continue)
                }
                Err(TrySendError::Full(_)) => {
                    counter!("ingest_queue_full_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "ingest queue full; marking for DLQ");
                    ctx.mark_dlq("ingest queue full");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_full")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
                Err(TrySendError::Closed(_)) => {
                    counter!("ingest_queue_closed_total", "kind" => kind).increment(1);
                    warn!(topic = %ctx.topic(), "ingest queue closed; marking for DLQ");
                    ctx.mark_dlq("ingest queue closed");
                    histogram!("ingest_persist_duration_seconds", "kind" => kind, "result" => "queue_closed")
                        .record(start.elapsed().as_secs_f64());

                    Ok(StageFlow::Stop)
                }
            }
        })
    }
}
