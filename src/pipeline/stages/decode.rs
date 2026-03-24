use std::{future::Future, pin::Pin, str::Utf8Error, time::Instant};

use serde_json::{Error, Value};
use tracing::warn;

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

use metrics::{counter, histogram};

const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub enum DecodeError {
    TooLarge(usize),
    Utf8(Utf8Error),
    Json { payload_str: String, source: Error },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DecodeStage;

impl DecodeStage {
    pub fn new() -> Self {
        Self
    }

    pub fn decode_payload(payload: &[u8]) -> Result<Value, DecodeError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(DecodeError::TooLarge(payload.len()));
        }

        let payload_str = str::from_utf8(payload).map_err(DecodeError::Utf8)?;

        serde_json::from_str(payload_str).map_err(|source| DecodeError::Json {
            payload_str: payload_str.to_owned(),
            source,
        })
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
            counter!("mqtt_messages_received_total").increment(1);

            let start = Instant::now();
            let raw_payload = ctx.raw_payload();

            histogram!("ingest_decode_payload_bytes").record(raw_payload.len() as f64);

            let (result, result_label) = match Self::decode_payload(raw_payload) {
                Ok(payload_json) => {
                    counter!("ingest_decode_success_total").increment(1);

                    ctx.set_payload_json(payload_json);

                    (StageFlow::Continue, "success")
                }
                Err(DecodeError::TooLarge(size)) => {
                    counter!("ingest_incoming_oversized_total").increment(1);
                    warn!(topic = %ctx.topic(), size, "payload too large; marking for DLQ");

                    ctx.mark_dlq(format!("payload too large: {size} bytes"));

                    (StageFlow::Stop, "too_large")
                }
                Err(DecodeError::Utf8(e)) => {
                    counter!("ingest_incoming_non_utf8_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %e, "payload not utf8; marking for DLQ");

                    ctx.mark_dlq("payload not utf8".to_string());

                    (StageFlow::Stop, "non_utf8")
                }
                Err(DecodeError::Json {
                    payload_str,
                    source,
                }) => {
                    counter!("ingest_incoming_invalid_json_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %source, "invalid JSON; marking for DLQ");

                    ctx.set_payload_utf8(payload_str);
                    ctx.mark_dlq("payload not valid JSON".to_string());

                    (StageFlow::Stop, "invalid_json")
                }
            };

            histogram!("ingest_decode_duration_seconds", "result" => result_label)
                .record(start.elapsed().as_secs_f64());
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::stage::StageFlow;
    use serde_json::json;

    // --- decode_payload: success paths ---

    #[test]
    fn decode_payload_returns_string_and_json_for_valid_payload() {
        let raw = br#"{"device_id":"esp32-1","temp_c":22.4}"#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Ok(payload_json) => {
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
    fn decode_payload_accepts_valid_json_array() {
        let raw = br#"[{"sensor":"a"},{"sensor":"b"}]"#;

        let result = DecodeStage::decode_payload(raw);

        match result {
            Ok(payload_json) => {
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
            Ok(payload_json) => {
                assert_eq!(payload_json, json!(42));
            }
            Err(err) => panic!("expected Ok, got Err: {err:?}"),
        }
    }

    // --- decode_payload: error paths ---

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

    #[test]
    fn decode_payload_returns_json_error_for_whitespace_only_payload() {
        let raw = b"   ";

        let result = DecodeStage::decode_payload(raw);

        match result {
            Err(DecodeError::Json {
                payload_str,
                source: _,
            }) => {
                assert_eq!(payload_str, "   ");
            }
            Ok(value) => panic!("expected Json error, got Ok: {value:?}"),
            Err(other) => panic!("expected Json error, got different error: {other:?}"),
        }
    }

    // --- decode_payload: size guard ---

    #[test]
    fn decode_payload_rejects_oversized_payload_with_actual_size() {
        let size = MAX_PAYLOAD_BYTES + 1;
        let raw = vec![b'x'; size];

        let result = DecodeStage::decode_payload(&raw);

        assert!(matches!(result, Err(DecodeError::TooLarge(n)) if n == size));
    }

    #[test]
    fn decode_payload_accepts_payload_at_size_limit() {
        let raw = vec![b'x'; MAX_PAYLOAD_BYTES]; // valid size, invalid JSON

        let result = DecodeStage::decode_payload(&raw);

        assert!(matches!(result, Err(DecodeError::Json { .. })));
    }

    // --- run(): context state after each path ---

    #[tokio::test]
    async fn run_on_valid_payload_sets_context_and_returns_continue() {
        let raw = br#"{"device_id":"esp32-1","temp_c":22.4}"#.to_vec();
        let mut ctx = PipelineContext::new("home/sensor/esp32-1", raw);

        let result = DecodeStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        assert_eq!(
            ctx.payload_json().unwrap(),
            &json!({"device_id": "esp32-1", "temp_c": 22.4})
        );
    }

    #[tokio::test]
    async fn run_on_oversized_payload_marks_dlq_and_returns_stop() {
        let raw = vec![b'x'; MAX_PAYLOAD_BYTES + 1];
        let mut ctx = PipelineContext::new("home/sensor/esp32-1", raw);

        let result = DecodeStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("too large"));
    }

    #[tokio::test]
    async fn run_on_non_utf8_payload_marks_dlq_and_returns_stop() {
        let raw = vec![0xff, 0xfe, 0xfd];
        let mut ctx = PipelineContext::new("home/sensor/esp32-1", raw);

        let result = DecodeStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert_eq!(ctx.dlq_reason().unwrap(), "payload not utf8");
    }

    #[tokio::test]
    async fn run_on_invalid_json_sets_utf8_marks_dlq_and_returns_stop() {
        let raw = br#"{"device_id":"esp32-1""#.to_vec();
        let mut ctx = PipelineContext::new("home/sensor/esp32-1", raw);

        let result = DecodeStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert_eq!(ctx.dlq_reason().unwrap(), "payload not valid JSON");
        // utf8 string must be set even on json failure (used by DLQ stage)
        assert_eq!(ctx.payload_utf8().unwrap(), r#"{"device_id":"esp32-1""#);
    }
}
