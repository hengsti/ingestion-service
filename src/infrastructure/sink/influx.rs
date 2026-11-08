use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result};
use metrics::{counter, histogram};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};

use crate::infrastructure::{
    sink::{
        point::{line_protocol_size_upper_bound, write_line_protocol},
        Encoder, Sink, SinkError,
    },
    wal::types::WalEvent,
};
use crate::model::messages::telemetry::TelemetryRecord;

/// Renders canonical messages as InfluxDB line protocol, pairing 1:1 with
/// [`InfluxSink`] (both are constructed together for a given output sink).
pub struct InfluxEncoder;

impl Encoder for InfluxEncoder {
    fn encoded_size_upper_bound(&self, record: &TelemetryRecord) -> usize {
        line_protocol_size_upper_bound(record)
    }

    fn encode(&self, record: &TelemetryRecord, out: &mut String) {
        write_line_protocol(record, out);
    }
}

/// Writes WAL payload batches to the InfluxDB v2 write API with bounded retries.
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

/// Joins WAL payloads into a newline-delimited Influx write body.
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

fn http_write_error(status: StatusCode) -> anyhow::Error {
    anyhow::anyhow!("Influx write failed: status={}", status)
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
                        let err = http_write_error(status);

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
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

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
    fn http_write_error_reports_status_only() {
        let err = http_write_error(StatusCode::SERVICE_UNAVAILABLE);
        let text = err.to_string();
        assert_eq!(text, "Influx write failed: status=503 Service Unavailable");
    }

    #[test]
    fn test_telemetry_encoding_renders_typed_fields_sorted_and_escaped_at_ms_precision() {
        let record = TelemetryRecord {
            route_id: "typed-test".to_owned(),
            device_id: "dev-1".to_owned(),
            measurement: "weather, station".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::from([
                ("room key".to_owned(), "lab,west".to_owned()),
                ("device_id".to_owned(), "dev-1".to_owned()),
            ]),
            fields: BTreeMap::from([
                ("uint".to_owned(), TelemetryValue::Unsigned(3)),
                (
                    "string".to_owned(),
                    TelemetryValue::String("a\\b\"c".to_owned()),
                ),
                ("int".to_owned(), TelemetryValue::Integer(-2)),
                ("float".to_owned(), TelemetryValue::Float(1.5)),
                ("bool".to_owned(), TelemetryValue::Boolean(false)),
            ]),
            source_topic: "home/dev-1/weather".to_owned(),
            retained: false,
        };
        let mut line = String::new();

        InfluxEncoder.encode(&record, &mut line);

        assert_eq!(
            line,
            r#"weather\,\ station,device_id=dev-1,room\ key=lab\,west bool=false,float=1.5,int=-2i,string="a\\b\"c",uint=3u 1700000000000"#
        );
        assert_eq!(line.lines().count(), 1);
    }

    #[test]
    fn test_telemetry_encoding_without_tags_preserves_zero_timestamp_and_empty_string() {
        let record = TelemetryRecord {
            route_id: "typed-test".to_owned(),
            device_id: "dev-1".to_owned(),
            measurement: "weather".to_owned(),
            timestamp_ms: 0,
            tags: BTreeMap::new(),
            fields: BTreeMap::from([
                ("string".to_owned(), TelemetryValue::String(String::new())),
                ("bool".to_owned(), TelemetryValue::Boolean(true)),
            ]),
            source_topic: "home/dev-1/weather".to_owned(),
            retained: false,
        };
        let mut line = String::new();

        InfluxEncoder.encode(&record, &mut line);

        assert_eq!(line, r#"weather bool=true,string="" 0"#);
    }

    #[test]
    fn test_telemetry_encoding_appends_unicode_and_escapes_all_delimiters() {
        let record = TelemetryRecord {
            route_id: "typed-test".to_owned(),
            device_id: "dev-1".to_owned(),
            measurement: r"μ\, =p".to_owned(),
            timestamp_ms: -12,
            tags: BTreeMap::from([(r"tag\, =k".to_owned(), r"été\, =v".to_owned())]),
            fields: BTreeMap::from([(
                r"f\, =x".to_owned(),
                TelemetryValue::String(r#"σ\"line"#.to_owned()),
            )]),
            source_topic: "home/dev-1/weather".to_owned(),
            retained: false,
        };
        let mut line = "prefix|".to_owned();

        InfluxEncoder.encode(&record, &mut line);

        assert_eq!(
            line,
            r#"prefix|μ\\\,\ =p,tag\\\,\ \=k=été\\\,\ \=v f\\\,\ \=x="σ\\\"line" -12"#
        );
    }

    #[test]
    fn test_telemetry_encoding_size_bound_covers_escapes_and_numeric_extremes() {
        let mut record = TelemetryRecord {
            route_id: "typed-test".to_owned(),
            device_id: "dev-1".to_owned(),
            measurement: "weather, \\station".to_owned(),
            timestamp_ms: i64::MIN,
            tags: BTreeMap::from([("tag, =\\".to_owned(), "value, =\\".to_owned())]),
            fields: BTreeMap::from([("field, =\\".to_owned(), TelemetryValue::Boolean(false))]),
            source_topic: "home/dev-1/weather".to_owned(),
            retained: false,
        };

        for timestamp in [i64::MIN, i64::MAX, -9_223_372_036_854, 0, 9_223_372_036_854] {
            record.timestamp_ms = timestamp;
            let mut out = "prefix".to_owned();
            InfluxEncoder.encode(&record, &mut out);
            assert!(
                out.len() - "prefix".len() <= InfluxEncoder.encoded_size_upper_bound(&record),
                "encoded size exceeded its bound for timestamp {timestamp}"
            );
        }

        record.fields.extend([
            ("float_min".to_owned(), TelemetryValue::Float(f64::MIN)),
            ("float_max".to_owned(), TelemetryValue::Float(f64::MAX)),
            (
                "float_small".to_owned(),
                TelemetryValue::Float(f64::from_bits(1)),
            ),
            ("int_min".to_owned(), TelemetryValue::Integer(i64::MIN)),
            ("uint_max".to_owned(), TelemetryValue::Unsigned(u64::MAX)),
            (
                "string".to_owned(),
                TelemetryValue::String("\\quoted\"".to_owned()),
            ),
        ]);
        let mut out = String::new();
        InfluxEncoder.encode(&record, &mut out);
        assert!(out.len() <= InfluxEncoder.encoded_size_upper_bound(&record));
    }
}
