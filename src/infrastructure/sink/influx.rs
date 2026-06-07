use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result};
use metrics::{counter, histogram};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};

use crate::infrastructure::{
    sink::{Sink, SinkError},
    wal::types::WalEvent,
};

/// InfluxDB v2 sink: converts WAL events to line protocol and writes them with a bounded retry loop.
pub struct InfluxSink {
    client: Client,
    write_url: String,
    token: SecretString,
}

impl InfluxSink {
    /// Builds a new sink targeting an InfluxDB v2 write endpoint.
    ///
    /// The `token` is kept wrapped in [`SecretString`] and only exposed when
    /// building the `Authorization` header, so it is never logged or surfaced
    /// via `Debug`.
    ///
    /// # Errors
    /// Returns an error if the underlying HTTP client cannot be constructed.
    pub fn new(url: &str, org: &str, bucket: &str, token: SecretString) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build reqwest client")?;

        // InfluxDB v2 write endpoint
        let write_url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ms",
            url.trim_end_matches('/'),
            urlencoding::encode(org),
            urlencoding::encode(bucket)
        );

        Ok(Self {
            client,
            write_url,
            token,
        })
    }
}

/// Converts a batch of WAL events into a newline-delimited InfluxDB line-protocol body.
fn build_body(batch: &[WalEvent]) -> String {
    let mut body = String::new();
    for event in batch {
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&event.line_protocol);
    }
    body
}

impl Sink for InfluxSink {
    fn write<'a>(
        &'a self,
        batch: &'a [WalEvent],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            if batch.is_empty() {
                return Ok(());
            }

            let body = build_body(batch);
            let lines = batch.len();

            let start = Instant::now();
            let mut last_err = anyhow::anyhow!("no write attempts made");

            for attempt in 0u32..3 {
                if attempt > 0 {
                    tokio::time::sleep(Duration::from_secs(1u64 << (attempt - 1))).await;
                    tracing::warn!(attempt, "retrying influx write");
                }

                let send_result = self
                    .client
                    .post(&self.write_url)
                    .header(
                        "Authorization",
                        format!("Token {}", self.token.expose_secret()),
                    )
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(body.clone())
                    .send()
                    .await;

                match send_result {
                    // Network/timeout failures are always transient — retry.
                    Err(e) => {
                        last_err = anyhow::anyhow!("Influx write request failed: {}", e);
                        continue;
                    }
                    Ok(resp) if !resp.status().is_success() => {
                        let status = resp.status();
                        let text = resp.text().await.unwrap_or_default();
                        let err =
                            anyhow::anyhow!("Influx write failed: status={} body={}", status, text);

                        // 4xx are permanent (malformed line protocol) and must not
                        // be retried — except 408/429, which are transient.
                        if is_permanent_status(status) {
                            counter!("influx_write_errors_total").increment(1);
                            return Err(SinkError::Permanent(err));
                        }

                        last_err = err;
                        continue;
                    }
                    Ok(_) => {
                        histogram!("influx_write_duration_seconds")
                            .record(start.elapsed().as_secs_f64());
                        counter!("influx_lines_written_total").increment(lines as u64);
                        counter!("influx_write_success_total").increment(1);
                        return Ok(());
                    }
                }
            }

            counter!("influx_write_errors_total").increment(1);
            Err(SinkError::Retryable(last_err))
        })
    }
}

/// Returns `true` if a non-success HTTP status represents a permanent failure
/// that retrying cannot fix. Client errors (4xx) are permanent, with the
/// exception of `408 Request Timeout` and `429 Too Many Requests`, which are
/// transient and should be retried.
fn is_permanent_status(status: StatusCode) -> bool {
    status.is_client_error()
        && status != StatusCode::REQUEST_TIMEOUT
        && status != StatusCode::TOO_MANY_REQUESTS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_sensor() -> WalEvent {
        WalEvent {
            topic: "smarthome/dev-1/sensor".to_string(),
            ts_ms: 1_700_000_000_000,
            line_protocol: "bme680,device_id=dev-1 temp_c=21.5 1700000000000".to_string(),
        }
    }

    fn sample_status() -> WalEvent {
        WalEvent {
            topic: "smarthome/dev-1/status".to_string(),
            ts_ms: 1_700_000_000_000,
            line_protocol: "device_status,device_id=dev-1 rssi=-55i 1700000000000".to_string(),
        }
    }

    #[test]
    fn build_body_joins_mixed_batch_with_newlines() {
        let batch = vec![sample_sensor(), sample_status()];

        let body = build_body(&batch);

        let expected = format!("{}\n{}", batch[0].line_protocol, batch[1].line_protocol);

        assert_eq!(body, expected);
        assert_eq!(body.lines().count(), 2);
    }

    #[test]
    fn build_body_empty_batch_is_empty_string() {
        assert_eq!(build_body(&[]), "");
    }

    #[test]
    fn is_permanent_status_classifies_expected_http_codes() {
        assert!(is_permanent_status(StatusCode::BAD_REQUEST));
        assert!(is_permanent_status(StatusCode::UNAUTHORIZED));
        assert!(is_permanent_status(StatusCode::NOT_FOUND));

        assert!(!is_permanent_status(StatusCode::REQUEST_TIMEOUT));
        assert!(!is_permanent_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_permanent_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!is_permanent_status(StatusCode::SERVICE_UNAVAILABLE));
    }
}
