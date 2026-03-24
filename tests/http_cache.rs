mod common;

use std::sync::{atomic::AtomicBool, Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tower::ServiceExt;

use smarthome_ingest::infrastructure::cache::{http as cache_http, state::CacheState};

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_app(mqtt_ready: bool) -> axum::Router {
    let cache = CacheState::new(60_000, 64);
    let ready = Arc::new(AtomicBool::new(mqtt_ready));
    cache_http::router(cache, ready)
}

fn make_app_with_cache(cache: CacheState, mqtt_ready: bool) -> axum::Router {
    let ready = Arc::new(AtomicBool::new(mqtt_ready));
    cache_http::router(cache, ready)
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ── /healthz ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn healthz_returns_200() {
    let app = make_app(false);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ── /readyz ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn readyz_returns_503_when_mqtt_not_ready() {
    let app = make_app(false);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn readyz_returns_200_when_mqtt_ready() {
    let app = make_app(true);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ── /v1/state ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_state_returns_empty_when_cache_is_empty() {
    let app = make_app(false);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    assert_eq!(
        json["sensors"].as_array().unwrap().len(),
        0,
        "expected empty sensors array"
    );
}

#[tokio::test]
async fn list_state_returns_cached_sensor() {
    let cache = CacheState::new(60_000, 64);
    cache.update_sensor(&smarthome_ingest::model::messages::sensor::SensorMessage {
        device_id: "esp32-1".to_string(),
        room: "living_room".to_string(),
        device_class: "esp32p4-bme680".to_string(),
        fw_version: "1.0.0".to_string(),
        time_ms: 1_700_000_000_000,
        time_iso: "2023-11-14T22:13:20Z".to_string(),
        time_valid: true,
        data: smarthome_ingest::model::messages::sensor::SensorData {
            temp_c: 22.5,
            rel_hum_perc: 45.0,
            pressure_hpa: 1013.25,
            gas_ohm: 50_000.0,
            iaq_score: 85.0,
            iaq_text: "Good".to_string(),
            dew_point_c: 9.5,
            heat_index_c: 22.0,
            altitude_m: 500.0,
        },
    });

    let app = make_app_with_cache(cache, false);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    let sensors = json["sensors"].as_array().unwrap();
    assert_eq!(sensors.len(), 1);
    assert_eq!(sensors[0]["device_id"], "esp32-1");
}

// ── /v1/state/{device_id} ────────────────────────────────────────────────────

#[tokio::test]
async fn get_state_returns_null_sensor_for_unknown_device() {
    let app = make_app(false);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/state/unknown-device")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = response_json(response).await;
    assert!(
        json["sensor"].is_null(),
        "expected null sensor for unknown device, got: {}",
        json["sensor"]
    );
}
