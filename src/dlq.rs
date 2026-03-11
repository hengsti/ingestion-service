use anyhow::Result;
use rumqttc::AsyncClient;
use serde_json::json;
use tracing::info;

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

    info!(src_topic=%src_topic, error=%err, "publishing message to DLQ topic");

    let bytes = serde_json::to_vec(&dlq)?;
    client
        .publish(dlq_topic, rumqttc::QoS::AtLeastOnce, false, bytes)
        .await?;
    Ok(())
}
