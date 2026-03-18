use std::{future::Future, pin::Pin, str::Utf8Error, time::Instant};

use serde_json::{Error, Value};
use tracing::warn;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

use metrics::histogram;

#[derive(Debug)]
pub enum DecodeError {
    Utf8(Utf8Error),
    Json { payload_str: String, source: Error },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DecodeStage;

impl DecodeStage {
    pub fn new() -> Self {
        Self
    }

    pub fn decode_payload(payload: &[u8]) -> Result<(String, Value), DecodeError> {
        let payload_str = str::from_utf8(payload)
            .map_err(DecodeError::Utf8)?
            .to_owned();

        let payload_json =
            serde_json::from_str(&payload_str).map_err(|source| DecodeError::Json {
                payload_str: payload_str.clone(),
                source,
            })?;

        Ok((payload_str, payload_json))
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

            let result = match Self::decode_payload(ctx.raw_payload()) {
                Ok((payload_str, payload_json)) => {
                    ctx.set_payload_utf8(payload_str);
                    ctx.set_payload_json(payload_json);
                    StageFlow::Continue
                }
                Err(DecodeError::Utf8(e)) => {
                    metrics::counter!("ingest_incoming_non_utf8_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %e, "payload not utf8; marking for DLQ");
                    ctx.mark_dlq("payload not utf8".to_string());
                    StageFlow::Stop
                }
                Err(DecodeError::Json {
                    payload_str,
                    source,
                }) => {
                    metrics::counter!("ingest_incoming_invalid_json_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %source, "invalid JSON; marking for DLQ");
                    ctx.set_payload_utf8(payload_str);
                    ctx.mark_dlq("payload not valid JSON".to_string());
                    StageFlow::Stop
                }
            };

            histogram!("ingest_decode_duration_seconds").record(start.elapsed().as_secs_f64());
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decode_payload_returns_string_and_json_for_valid_payload() {
        let raw = br#"{"device_id":"esp32-1","temp_c":22.4}"#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Ok((payload_str, payload_json)) => {
                assert_eq!(payload_str, r#"{"device_id":"esp32-1","temp_c":22.4}"#);
                assert_eq!(
                    payload_json,
                    json!({
                        "device_id": "esp32-1",
                        "temp_c": 22.4
                    })
                );
            }
            Err(err) => panic!("expected Ok, got Err: {err:?}"),
        }
    }

    #[test]
    fn decode_payload_returns_utf8_error_for_non_utf8_payload() {
        let raw = &[0xff, 0xfe, 0xfd];

        let result = DecodeStage::decode_payload(raw);

        match result {
            Err(DecodeError::Utf8(_)) => {}
            Ok(value) => panic!("expected Utf8 error, got Ok: {value:?}"),
            Err(other) => panic!("expected Utf8 error, got different error: {other:?}"),
        }
    }

    #[test]
    fn decode_payload_returns_json_error_and_preserves_payload_string() {
        let raw = br#"{"device_id":"esp32-1""#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Err(DecodeError::Json {
                payload_str,
                source: _,
            }) => {
                assert_eq!(payload_str, r#"{"device_id":"esp32-1""#);
            }
            Ok(value) => panic!("expected Json error, got Ok: {value:?}"),
            Err(other) => panic!("expected Json error, got different error: {other:?}"),
        }
    }

    #[test]
    fn decode_payload_accepts_valid_json_array() {
        let raw = br#"[{"sensor":"a"},{"sensor":"b"}]"#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Ok((payload_str, payload_json)) => {
                assert_eq!(payload_str, r#"[{"sensor":"a"},{"sensor":"b"}]"#);
                assert_eq!(
                    payload_json,
                    json!([
                        {"sensor": "a"},
                        {"sensor": "b"}
                    ])
                );
            }
            Err(err) => panic!("expected Ok, got Err: {err:?}"),
        }
    }

    #[test]
    fn decode_payload_accepts_valid_json_scalar() {
        let raw = br#"42"#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Ok((payload_str, payload_json)) => {
                assert_eq!(payload_str, "42");
                assert_eq!(payload_json, json!(42));
            }
            Err(err) => panic!("expected Ok, got Err: {err:?}"),
        }
    }

    #[test]
    fn decode_payload_rejects_empty_payload_as_json_error() {
        let raw = b"";

        let result = DecodeStage::decode_payload(raw);

        match result {
            Err(DecodeError::Json {
                payload_str,
                source: _,
            }) => {
                assert_eq!(payload_str, "");
            }
            Ok(value) => panic!("expected Json error, got Ok: {value:?}"),
            Err(other) => panic!("expected Json error, got different error: {other:?}"),
        }
    }
}
