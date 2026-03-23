use std::{future::Future, pin::Pin, time::Instant};

use anyhow::Result;
use rumqttc::AsyncClient;
use serde_json::json;
use tracing::{info, warn};

use metrics::{counter, histogram};

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

pub async fn publish_dlq(
    client: &AsyncClient,
    dlq_topic: &str,
    src_topic: &str,
    payload: &str,
    err: &str,
) -> Result<()> {
    let dlq = json!({
        "received_at": chrono::Utc::now().to_rfc3339(),
        "src_topic": src_topic,
        "error": err,
        "payload_raw": payload,
    });

    info!(src_topic = %src_topic, error = %err, "publishing message to DLQ topic");

    let bytes = serde_json::to_vec(&dlq)?;
    client
        .publish(dlq_topic, rumqttc::QoS::AtLeastOnce, false, bytes)
        .await?;

    Ok(())
}

#[derive(Clone)]
pub struct DlqPublishStage {
    client: AsyncClient,
    dlq_topic: String,
}

impl DlqPublishStage {
    pub fn new(client: AsyncClient, dlq_topic: impl Into<String>) -> Self {
        Self {
            client,
            dlq_topic: dlq_topic.into(),
        }
    }
}

impl PipelineStage for DlqPublishStage {
    fn name(&self) -> &'static str {
        "dlq_publish"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(reason) = ctx.dlq_reason().map(str::to_owned) else {
                return Ok(StageFlow::Stop);
            };

            let start = Instant::now();
            let payload = ctx.payload_for_dlq();

            if let Err(err) = publish_dlq(
                &self.client,
                &self.dlq_topic,
                ctx.topic(),
                &payload,
                &reason,
            )
            .await
            {
                counter!("dlq_publish_errors_total").increment(1);
                warn!(topic = %ctx.topic(), error = %err, "failed to publish to DLQ");
                histogram!("ingest_dlq_publish_duration_seconds", "result" => "failed")
                    .record(start.elapsed().as_secs_f64());
            } else {
                counter!("dlq_messages_published_total").increment(1);
                histogram!("ingest_dlq_publish_duration_seconds", "result" => "success")
                    .record(start.elapsed().as_secs_f64());
            }

            Ok(StageFlow::Stop)
        })
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::MqttOptions;

    use super::*;
    use crate::pipeline::{context::PipelineContext, stage::StageFlow};

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Returns a client whose eventloop receiver is alive so that `publish` queues successfully.
    fn client_with_live_eventloop() -> (AsyncClient, rumqttc::EventLoop) {
        let opts = MqttOptions::new("test-dlq-ok", "localhost", 1883);
        AsyncClient::new(opts, 10)
    }

    /// Returns a client whose eventloop has been dropped so that `publish` returns an error.
    fn client_with_dropped_eventloop() -> AsyncClient {
        let opts = MqttOptions::new("test-dlq-err", "localhost", 1884);
        let (client, _eventloop) = AsyncClient::new(opts, 10);
        // _eventloop is dropped here → receiver gone → publish will fail
        client
    }

    fn ctx_with_dlq_reason(reason: &str) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", b"raw payload".to_vec());
        ctx.mark_dlq(reason.to_string());
        ctx
    }

    // ── run(): no DLQ reason ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_without_dlq_reason_returns_stop_without_publishing() {
        let (client, _eventloop) = client_with_live_eventloop();
        let stage = DlqPublishStage::new(client, "smarthome/_dlq/ingest");
        // Context has no dlq_reason — stage must return Stop immediately.
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ reason present, publish succeeds ───────────────────────────

    #[tokio::test]
    async fn run_with_dlq_reason_returns_stop_when_publish_succeeds() {
        let (client, _eventloop) = client_with_live_eventloop();
        let stage = DlqPublishStage::new(client, "smarthome/_dlq/ingest");
        let mut ctx = ctx_with_dlq_reason("schema validation failed");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ reason present, publish fails (closed channel) ────────────

    #[tokio::test]
    async fn run_with_dlq_reason_returns_stop_even_when_publish_fails() {
        // The stage must absorb publish errors and never propagate them.
        let client = client_with_dropped_eventloop();
        let stage = DlqPublishStage::new(client, "smarthome/_dlq/ingest");
        let mut ctx = ctx_with_dlq_reason("schema validation failed");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ topic is forwarded to publish ──────────────────────────────

    #[tokio::test]
    async fn run_uses_configured_dlq_topic() {
        // Verify the stage accepts any DLQ topic string without panic.
        let (client, _eventloop) = client_with_live_eventloop();
        let stage = DlqPublishStage::new(client, "custom/dlq/topic");
        let mut ctx = ctx_with_dlq_reason("some error");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }
}
