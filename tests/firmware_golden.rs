mod common;

use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::Value;
use smarthome_ingest::{
    infrastructure::cache::{http as cache_http, state::CacheState},
    model::messages::telemetry::{TelemetryRecord, TelemetryValue},
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
    unified_line_protocol: Option<String>,
    unified_rejection: Option<String>,
}

async fn assert_configured_cases(path: &str) -> Result<()> {
    let cases: Vec<GoldenCase> = serde_json::from_str(path)?;
    let (pipeline, mut subscription, _cache, _wal_dir) = common::build_pipeline().await;

    for case in cases {
        let payload = serde_json::to_vec(&case.payload)?;
        let mut context = PipelineContext::new_with_metadata(
            &case.topic,
            payload,
            i64::try_from(case.received_at_ms)?,
            case.retained,
        );
        pipeline.run(&mut context).await;

        match case.unified_line_protocol {
            Some(expected) => {
                assert!(
                    !context.should_quarantine(),
                    "{} unexpectedly rejected: {:?}",
                    case.id,
                    context.rejection_reason()
                );
                let event = common::recv_event(&mut subscription, 500)
                    .await
                    .with_context(|| format!("{} should persist to WAL", case.id))?;
                assert_eq!(
                    event.payload, expected,
                    "configured firmware golden mismatch for {}",
                    case.id
                );
            }
            None => {
                assert!(
                    context.should_quarantine(),
                    "{} should be rejected: {:?}",
                    case.id,
                    case.unified_rejection
                );
                assert!(
                    common::recv_event(&mut subscription, 100).await.is_none(),
                    "{} must not reach WAL",
                    case.id
                );
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_firmware_sensor_lines_match_configured_encoder() -> Result<()> {
    assert_configured_cases(include_str!("fixtures/golden/firmware-sensor.json")).await
}

#[tokio::test]
async fn test_golden_firmware_status_lines_match_configured_encoder() -> Result<()> {
    assert_configured_cases(include_str!("fixtures/golden/firmware-status.json")).await
}

fn cache_record() -> TelemetryRecord {
    TelemetryRecord {
        route_id: "firmware-sensor".to_owned(),
        device_id: "esp32-1".to_owned(),
        measurement: "bme680".to_owned(),
        timestamp_ms: 1_700_000_000_000,
        tags: BTreeMap::new(),
        fields: BTreeMap::from([
            ("temp_c".to_owned(), TelemetryValue::Float(22.5)),
            ("rel_hum_perc".to_owned(), TelemetryValue::Float(45.0)),
            ("pressure_hpa".to_owned(), TelemetryValue::Float(1013.25)),
            ("gas_ohm".to_owned(), TelemetryValue::Float(50_000.0)),
            (
                "dew_point_c".to_owned(),
                TelemetryValue::Float(9.964857762639978),
            ),
            (
                "heat_index_c".to_owned(),
                TelemetryValue::Float(21.980555555555554),
            ),
            (
                "iaq_score".to_owned(),
                TelemetryValue::Float(97.91666666666666),
            ),
            (
                "iaq_text".to_owned(),
                TelemetryValue::String("Air quality is Good".to_owned()),
            ),
            ("altitude_m".to_owned(), TelemetryValue::Float(500.0)),
            ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
        ]),
        source_topic: "smarthome/esp32-1/bme680".to_owned(),
        retained: false,
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
    cache.update_record(&cache_record(), received_at_ms, 1);
    let ready = Arc::new(AtomicBool::new(false));
    let (wal, _subscription, _wal_dir) = common::open_temp_wal().await;
    let app = cache_http::router(
        cache.clone(),
        ready,
        Arc::new(AtomicBool::new(true)),
        wal.capacity_handle(),
    );

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
    cache.update_record(&cache_record(), received_at_ms, 2);
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
