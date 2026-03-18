use std::{future::Future, pin::Pin, time::Instant};

use serde_json::Value;
use tracing::warn;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

use metrics::histogram;

#[derive(Debug, Default, Clone, Copy)]
pub struct DecodeStage;

impl DecodeStage {
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for DecodeStage {
    fn name(&self) -> &'static str {
        "decode"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            metrics::counter!("mqtt_messages_received_total").increment(1);

            let start = Instant::now();
            let payload_str = match std::str::from_utf8(ctx.raw_payload()) {
                Ok(s) => s.to_string(),
                Err(err) => {
                    metrics::counter!("ingest_incoming_non_utf8_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "payload not utf8; marking for DLQ");
                    ctx.mark_dlq("payload not utf8".to_string());
                    return Ok(StageFlow::Stop);
                }
            };

            let payload_json: Value = match serde_json::from_str(&payload_str) {
                Ok(json) => json,
                Err(err) => {
                    metrics::counter!("ingest_incoming_invalid_json_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "invalid JSON; marking for DLQ");
                    ctx.set_payload_utf8(payload_str);
                    ctx.mark_dlq("payload not valid JSON".to_string());
                    return Ok(StageFlow::Stop);
                }
            };

            ctx.set_payload_utf8(payload_str);
            ctx.set_payload_json(payload_json);

            histogram!("ingest_decode_duration_seconds").record(start.elapsed().as_secs_f64());
            Ok(StageFlow::Continue)
        })
    }
}
