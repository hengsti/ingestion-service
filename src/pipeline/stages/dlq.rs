use std::{future::Future, pin::Pin};

use anyhow::Result;
use rumqttc::AsyncClient;
use serde_json::json;
use tracing::{info, warn};

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
                metrics::counter!("dlq_publish_errors_total").increment(1);
                warn!(topic = %ctx.topic(), error = %err, "failed to publish to DLQ");
            } else {
                metrics::counter!("dlq_messages_published_total").increment(1);
            }

            Ok(StageFlow::Stop)
        })
    }
}
