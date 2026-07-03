use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result};
use metrics::{counter, histogram};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};

use crate::infrastructure::{
    sink::{point::Point, Encoder, Sink, SinkError},
    wal::types::WalEvent,
};
use crate::model::messages::{
    message::HandledMessage, sensor::SensorMessage, status::StatusMessage,
};

/// Renders canonical messages as InfluxDB line protocol, pairing 1:1 with
/// [`InfluxSink`] (both are constructed together for a given output sink).
pub struct InfluxEncoder;

impl Encoder for InfluxEncoder {
    fn encode(&self, message: &HandledMessage, out: &mut String) {
        match message {
            HandledMessage::Sensor(s) => sensor_to_point(s).write_line_protocol(out),
            HandledMessage::Status(s) => status_to_point(s).write_line_protocol(out),
        }
    }
}

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
        body.push_str(&event.payload);
    }
    body
}

fn http_write_error(status: StatusCode, body: &str) -> anyhow::Error {
    anyhow::anyhow!("Influx write failed: status={} body={}", status, body)
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
                    tracing::warn!(attempt, error = %last_err, "retrying influx write");
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
                        let err = http_write_error(status, &text);

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

pub fn sensor_to_point(msg: &SensorMessage) -> Point {
    let mut b = Point::build("bme680")
        .tag("device_id", &msg.device_id)
        .tag("room", &msg.room)
        .tag("device_class", &msg.device_class)
        .tag("fw_version", &msg.fw_version)
        .field_f64("temp_c", msg.data.temp_c)
        .field_f64("rel_hum_perc", msg.data.rel_hum_perc)
        .field_f64("pressure_hpa", msg.data.pressure_hpa)
        .field_f64("gas_ohm", msg.data.gas_ohm)
        .field_f64("iaq_score", msg.data.iaq_score)
        .field_str("iaq_text", &msg.data.iaq_text)
        .field_f64("dew_point_c", msg.data.dew_point_c)
        .field_f64("heat_index_c", msg.data.heat_index_c)
        .field_f64("altitude_m", msg.data.altitude_m)
        .field_bool("time_valid", msg.time_valid);

    // Timestamp only used if the message is valid and non-zero, otherwise InfluxDB will use the server time.
    if msg.time_valid && msg.time_ms > 0 {
        b = b.timestamp_ms(msg.time_ms);
    }

    b.build()
}

pub fn status_to_point(msg: &StatusMessage) -> Point {
    let mut b = Point::build("device_status")
        .tag("device_id", &msg.device_id)
        .tag("device_class", &msg.device_class)
        .tag("fw_version", &msg.fw_version)
        .tag("ip", &msg.ip)
        .field_str("time_iso", &msg.time_iso)
        .field_bool("time_valid", msg.time_valid)
        .field_i64("uptime", msg.uptime)
        .field_i64("free_mem", msg.free_mem)
        .field_str("ssid", &msg.ssid)
        .field_i64("rssi", msg.rssi);

    if msg.time_valid && msg.time_ms > 0 {
        b = b.timestamp_ms(msg.time_ms);
    }

    b.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_sensor() -> WalEvent {
        WalEvent {
            topic: "smarthome/dev-1/sensor".to_string(),
            ts_ms: 1_700_000_000_000,
            payload: "bme680,device_id=dev-1 temp_c=21.5 1700000000000".to_string(),
        }
    }

    fn sample_status() -> WalEvent {
        WalEvent {
            topic: "smarthome/dev-1/status".to_string(),
            ts_ms: 1_700_000_000_000,
            payload: "device_status,device_id=dev-1 rssi=-55i 1700000000000".to_string(),
        }
    }

    #[test]
    fn build_body_joins_mixed_batch_with_newlines() {
        let batch = vec![sample_sensor(), sample_status()];

        let body = build_body(&batch);

        let expected = format!("{}\n{}", batch[0].payload, batch[1].payload);

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

    #[test]
    fn http_write_error_includes_status_and_body_context() {
        let err = http_write_error(StatusCode::SERVICE_UNAVAILABLE, "outage");
        let text = err.to_string();
        assert!(text.contains("status=503 Service Unavailable"), "{text}");
        assert!(text.contains("body=outage"), "{text}");
    }
}
