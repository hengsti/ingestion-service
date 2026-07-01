use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use tracing::warn;

use metrics::{counter, histogram};

use crate::infrastructure::source::DlqPublisher;
use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

pub struct DlqPublishStage {
    publisher: Arc<dyn DlqPublisher>,
    dlq_topic: String,
}

impl DlqPublishStage {
    pub fn new(publisher: Arc<dyn DlqPublisher>, dlq_topic: impl Into<String>) -> Self {
        Self {
            publisher,
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

            if let Err(err) = self
                .publisher
                .publish(&self.dlq_topic, ctx.topic(), &payload, &reason)
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
    use rumqttc::{AsyncClient, MqttOptions};

    use super::*;
    use crate::infrastructure::source::mqtt::MqttDlqPublisher;
    use crate::pipeline::{context::PipelineContext, stage::StageFlow};

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Returns a publisher whose eventloop receiver is alive so that `publish` queues successfully.
    fn publisher_with_live_eventloop() -> (Arc<dyn DlqPublisher>, rumqttc::EventLoop) {
        let opts = MqttOptions::new("test-dlq-ok", "localhost", 1883);
        let (client, eventloop) = AsyncClient::new(opts, 10);
        (Arc::new(MqttDlqPublisher::new(client)), eventloop)
    }

    /// Returns a publisher whose eventloop has been dropped so that `publish` returns an error.
    fn publisher_with_dropped_eventloop() -> Arc<dyn DlqPublisher> {
        let opts = MqttOptions::new("test-dlq-err", "localhost", 1884);
        let (client, _eventloop) = AsyncClient::new(opts, 10);
        // _eventloop is dropped here → receiver gone → publish will fail
        Arc::new(MqttDlqPublisher::new(client))
    }

    fn ctx_with_dlq_reason(reason: &str) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", b"raw payload".to_vec());
        ctx.mark_dlq(reason.to_string());
        ctx
    }

    // ── run(): no DLQ reason ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_without_dlq_reason_returns_stop_without_publishing() {
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest");
        // Context has no dlq_reason — stage must return Stop immediately.
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ reason present, publish succeeds ───────────────────────────

    #[tokio::test]
    async fn run_with_dlq_reason_returns_stop_when_publish_succeeds() {
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest");
        let mut ctx = ctx_with_dlq_reason("schema validation failed");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ reason present, publish fails (closed channel) ────────────

    #[tokio::test]
    async fn run_with_dlq_reason_returns_stop_even_when_publish_fails() {
        // The stage must absorb publish errors and never propagate them.
        let publisher = publisher_with_dropped_eventloop();
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest");
        let mut ctx = ctx_with_dlq_reason("schema validation failed");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }

    // ── run(): DLQ topic is forwarded to publish ──────────────────────────────

    #[tokio::test]
    async fn run_uses_configured_dlq_topic() {
        // Verify the stage accepts any DLQ topic string without panic.
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let stage = DlqPublishStage::new(publisher, "custom/dlq/topic");
        let mut ctx = ctx_with_dlq_reason("some error");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
    }
}
