mod common;

use std::{sync::atomic::AtomicBool, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::Value;
use smarthome_ingest::{
    infrastructure::{
        cache::{http as cache_http, state::CacheState},
        sink::influx::{sensor_to_point, status_to_point},
    },
    model::messages::{
        message::HandledMessage,
        sensor::{SensorData, SensorMessage},
    },
    pipeline::context::PipelineContext,
};
use tower::ServiceExt;

#[derive(Deserialize)]
struct GoldenCase {
    id: String,
    topic: String,
    retained: bool,
    received_at_ms: u64,
    payload: Value,
    legacy_line_protocol: String,
    unified_line_protocol: Option<String>,
    unified_rejection: Option<String>,
}

fn point_line(point: smarthome_ingest::infrastructure::sink::point::Point) -> String {
    let mut line = String::new();
    point.write_line_protocol(&mut line);
    line
}

async fn assert_legacy_cases(path: &str) -> Result<()> {
    let cases: Vec<GoldenCase> = serde_json::from_str(path)?;
    let (pipeline, _subscription, _cache, _wal_dir) = common::build_pipeline().await;
    let mut actual_lines = Vec::with_capacity(cases.len());

    for case in &cases {
        assert!(case.received_at_ms > 0);
        let valid_time = case.payload["time_valid"]
            .as_bool()
            .with_context(|| format!("{} missing time_valid", case.id))?;
        let payload_time_ms = case.payload["time_ms"]
            .as_i64()
            .with_context(|| format!("{} missing time_ms", case.id))?;
        match case.unified_line_protocol.as_deref() {
            Some(line) => {
                assert!(!line.is_empty(), "unified golden missing for {}", case.id);
                assert!(
                    line.contains(&format!("mqtt_retained={}", case.retained)),
                    "unified golden has wrong retained flag for {}",
                    case.id
                );
                let expected_time_ms = if valid_time {
                    payload_time_ms
                } else {
                    case.received_at_ms as i64
                };
                assert!(
                    line.ends_with(&format!(" {expected_time_ms}")),
                    "unified golden has wrong frozen timestamp for {}",
                    case.id
                );
                if case.topic.ends_with("/sensor") {
                    assert!(line.contains("altitude_m="), "{}", case.id);
                    match case.payload.get("replayed").and_then(Value::as_bool) {
                        Some(replayed) => {
                            assert!(
                                line.contains(&format!("replayed={replayed}")),
                                "{}",
                                case.id
                            );
                        }
                        None => assert!(!line.contains("replayed="), "{}", case.id),
                    }
                    if case.payload.get("warmed_up") == Some(&Value::Bool(false)) {
                        assert!(!line.contains("iaq_score="), "{}", case.id);
                        assert!(!line.contains("iaq_text="), "{}", case.id);
                    }
                }
            }
            None => {
                assert!(valid_time && payload_time_ms == 0, "{}", case.id);
                assert_eq!(
                    case.unified_rejection.as_deref(),
                    Some("timestamp_ms must be at least 1"),
                    "rejected unified golden must explain {}",
                    case.id
                );
            }
        }
        let payload = serde_json::to_vec(&case.payload)?;
        let mut context = PipelineContext::new(&case.topic, payload);
        pipeline.run(&mut context).await;
        if context.should_publish_dlq() {
            bail!(
                "legacy pipeline rejected {}: {:?}",
                case.id,
                context.dlq_reason()
            );
        }

        let line = match context.handled_message()? {
            HandledMessage::Sensor(message) => point_line(sensor_to_point(message)),
            HandledMessage::Status(message) => point_line(status_to_point(message)),
        };
        actual_lines.push((case.id.as_str(), line));
    }

    for (case, (id, actual)) in cases.iter().zip(actual_lines) {
        assert_eq!(case.id, id);
        assert_eq!(
            case.legacy_line_protocol, actual,
            "legacy golden mismatch for {} (retained={})",
            case.id, case.retained
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_golden_firmware_sensor_lines_match_legacy_encoder() -> Result<()> {
    assert_legacy_cases(include_str!("fixtures/golden/firmware-sensor.json")).await
}

#[tokio::test]
async fn test_golden_firmware_status_lines_match_legacy_encoder() -> Result<()> {
    assert_legacy_cases(include_str!("fixtures/golden/firmware-status.json")).await
}

fn cache_sensor() -> SensorMessage {
    SensorMessage {
        device_id: "esp32-1".to_string(),
        room: "living_room".to_string(),
        device_class: "esp32p4-bme680".to_string(),
        fw_version: "1.0.0".to_string(),
        time_ms: 1_700_000_000_000,
        time_iso: "2023-11-14T22:13:20Z".to_string(),
        time_valid: true,
        boot_id: None,
        seq: None,
        warmed_up: None,
        replayed: false,
        data: SensorData {
            temp_c: 22.5,
            rel_hum_perc: 45.0,
            pressure_hpa: 1013.25,
            gas_ohm: 50_000.0,
            iaq_score: 97.91666666666666,
            iaq_text: "Air quality is Good".to_string(),
            dew_point_c: 9.964857762639978,
            heat_index_c: 21.980555555555554,
            altitude_m: 500.0,
        },
    }
}

async fn response_json(response: axum::response::Response) -> Result<Value> {
    let bytes = response.into_body().collect().await?.to_bytes();
    Ok(serde_json::from_slice(&bytes)?)
}

async fn request_json(app: axum::Router, uri: &str) -> Result<Value> {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    assert!(response.status().is_success());
    response_json(response).await
}

fn normalize_cache_timestamp(value: &mut Value, timestamp_ms: u64) {
    match value {
        Value::Object(object) => {
            if object.contains_key("last_seen_ms") {
                object.insert("last_seen_ms".to_string(), timestamp_ms.into());
            }
            for value in object.values_mut() {
                normalize_cache_timestamp(value, timestamp_ms);
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_cache_timestamp(value, timestamp_ms);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn test_golden_cache_state_device_and_sse_envelopes() -> Result<()> {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/golden/firmware-cache.json"))?;
    let received_at_ms = fixture["received_at_ms"]
        .as_u64()
        .context("cache fixture must include fixed receive time")?;
    let device_id = fixture["device_id"]
        .as_str()
        .context("cache fixture must include device id")?;
    let expected_homekit_keys = [
        "device_id",
        "last_seen_ms",
        "value.temp_c",
        "value.rel_hum_perc",
        "value.iaq_score",
        "value.iaq_text",
    ];
    assert_eq!(
        fixture["homekit_keys"],
        serde_json::json!(expected_homekit_keys)
    );

    let cache = CacheState::new(60_000, 64);
    let sensor = cache_sensor();
    cache.update_sensor(&sensor);
    let ready = Arc::new(AtomicBool::new(false));
    let app = cache_http::router(cache.clone(), ready);

    let mut state_list = request_json(app.clone(), "/v1/state").await?;
    normalize_cache_timestamp(&mut state_list, received_at_ms);
    assert_eq!(state_list, fixture["state_list"]);

    let mut device_state = request_json(app.clone(), &format!("/v1/state/{device_id}")).await?;
    normalize_cache_timestamp(&mut device_state, received_at_ms);
    assert_eq!(device_state, fixture["device_state"]);

    let response = app
        .oneshot(Request::builder().uri("/v1/stream").body(Body::empty())?)
        .await?;
    assert!(response.status().is_success());
    cache.update_sensor(&sensor);
    let frame = tokio::time::timeout(Duration::from_secs(1), response.into_body().frame())
        .await?
        .context("cache SSE stream produced no event")??;
    let data = frame
        .into_data()
        .map_err(|_| anyhow::anyhow!("cache SSE stream returned a non-data frame"))?;
    let text = std::str::from_utf8(&data)?;
    let event = text
        .lines()
        .find_map(|line| line.strip_prefix("event: "))
        .context("cache SSE event name is missing")?;
    let json = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .context("cache SSE JSON payload is missing")?;
    let mut sse_data: Value = serde_json::from_str(json)?;
    normalize_cache_timestamp(&mut sse_data, received_at_ms);
    assert_eq!(event, fixture["sse"]["event"].as_str().unwrap());
    assert_eq!(sse_data, fixture["sse"]["data"]);
    Ok(())
}
