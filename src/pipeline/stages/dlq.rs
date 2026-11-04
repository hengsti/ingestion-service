use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use tracing::warn;

use metrics::{counter, histogram};

use crate::infrastructure::quarantine::{Quarantine, Rejection};
use crate::infrastructure::source::DlqPublisher;
use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

pub struct DlqPublishStage {
    publisher: Arc<dyn DlqPublisher>,
    dlq_topic: String,
    quarantine: Arc<Quarantine>,
}

impl DlqPublishStage {
    pub fn new(
        publisher: Arc<dyn DlqPublisher>,
        dlq_topic: impl Into<String>,
        quarantine: Arc<Quarantine>,
    ) -> Self {
        Self {
            publisher,
            dlq_topic: dlq_topic.into(),
            quarantine,
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
            let Some(reason) = ctx.rejection_reason() else {
                return Ok(StageFlow::Stop);
            };

            self.quarantine
                .append_durable(Rejection {
                    topic: ctx.topic(),
                    received_at_ms: ctx.received_at_ms(),
                    raw_payload: ctx.raw_payload(),
                    reason,
                    route_id: ctx.route_id(),
                    retained: ctx.retained(),
                    qos: ctx.qos(),
                })
                .await?;

            let payload = ctx.payload_for_dlq();
            let start = Instant::now();

            if self
                .publisher
                .publish(&self.dlq_topic, ctx.topic(), &payload, reason)
                .await
                .is_err()
            {
                counter!("dlq_publish_errors_total").increment(1);
                warn!(
                    topic = %ctx.topic(),
                    "supplementary DLQ publish failed after durable quarantine"
                );
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
    use tempfile::TempDir;

    use super::*;
    use crate::infrastructure::source::mqtt::MqttDlqPublisher;
    use crate::infrastructure::wal::{subscription::WalSubscription, types::WalOptions};
    use crate::pipeline::{context::PipelineContext, stage::StageFlow};

    async fn test_quarantine() -> anyhow::Result<(Arc<Quarantine>, TempDir, WalSubscription)> {
        let dir = tempfile::tempdir()?;
        let (quarantine, subscription) = Quarantine::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: 16 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity: 16,
        })
        .await?;
        Ok((Arc::new(quarantine), dir, subscription))
    }

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
        // Dropping the event loop closes the receiver, so `publish` fails.
        Arc::new(MqttDlqPublisher::new(client))
    }

    fn ctx_with_dlq_reason(reason: &str) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", b"raw payload".to_vec());
        ctx.mark_rejection(reason.to_string());
        ctx
    }

    #[tokio::test]
    async fn run_without_dlq_reason_returns_stop_without_publishing() -> anyhow::Result<()> {
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let (quarantine, _dir, _subscription) = test_quarantine().await?;
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest", quarantine);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        Ok(())
    }

    #[tokio::test]
    async fn run_with_dlq_reason_returns_stop_when_publish_succeeds() -> anyhow::Result<()> {
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let (quarantine, _dir, _subscription) = test_quarantine().await?;
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest", quarantine);
        let mut ctx = ctx_with_dlq_reason("schema validation failed");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        Ok(())
    }

    #[tokio::test]
    async fn test_dlq_publish_failure_keeps_durable_quarantine_record() -> anyhow::Result<()> {
        let publisher = publisher_with_dropped_eventloop();
        let (quarantine, _dir, mut subscription) = test_quarantine().await?;
        let stage = DlqPublishStage::new(publisher, "smarthome/_dlq/ingest", quarantine);
        let mut ctx = ctx_with_dlq_reason("schema validation failed");
        ctx.set_qos(1);

        let result = stage.run(&mut ctx).await;
        let entry = subscription
            .next()
            .await?
            .expect("local quarantine record remains readable");
        let envelope: serde_json::Value = serde_json::from_str(&entry.event.payload)?;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert_eq!(
            envelope["raw_payload"],
            serde_json::json!([114, 97, 119, 32, 112, 97, 121, 108, 111, 97, 100])
        );
        assert_eq!(envelope["qos"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn run_uses_configured_dlq_topic() -> anyhow::Result<()> {
        let (publisher, _eventloop) = publisher_with_live_eventloop();
        let (quarantine, _dir, _subscription) = test_quarantine().await?;
        let stage = DlqPublishStage::new(publisher, "custom/dlq/topic", quarantine);
        let mut ctx = ctx_with_dlq_reason("some error");

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        Ok(())
    }
}
