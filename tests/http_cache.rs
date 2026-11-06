mod common;

use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tower::ServiceExt;

use smarthome_ingest::infrastructure::cache::{http as cache_http, state::CacheState};
use smarthome_ingest::infrastructure::wal::{subscription::WalSubscription, wal::Wal};
use smarthome_ingest::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

// ── helpers ──────────────────────────────────────────────────────────────────

async fn make_app(
    source_ready: bool,
) -> (axum::Router, Arc<Wal>, WalSubscription, tempfile::TempDir) {
    let cache = CacheState::new(60_000, 64);
    let ready = Arc::new(AtomicBool::new(source_ready));
    let (wal, subscription, directory) = common::open_temp_wal().await;
    let wal_capacity = wal.capacity_handle();
    let app = cache_http::router(
        cache,
        ready,
        Arc::new(AtomicBool::new(true)),
        wal_capacity.clone(),
        wal_capacity,
    );
    (app, wal, subscription, directory)
}

async fn make_app_with_cache(
    cache: CacheState,
    source_ready: bool,
) -> (axum::Router, Arc<Wal>, WalSubscription, tempfile::TempDir) {
    let ready = Arc::new(AtomicBool::new(source_ready));
    let (wal, subscription, directory) = common::open_temp_wal().await;
    let wal_capacity = wal.capacity_handle();
    let app = cache_http::router(
        cache,
        ready,
        Arc::new(AtomicBool::new(true)),
        wal_capacity.clone(),
        wal_capacity,
    );
    (app, wal, subscription, directory)
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ── /healthz ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn healthz_returns_200() {
    let (app, _wal, _subscription, _directory) = make_app(false).await;

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
async fn readyz_returns_503_when_source_not_ready() {
    let (app, _wal, _subscription, _directory) = make_app(false).await;

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
async fn readyz_returns_200_when_source_ready() {
    let (app, _wal, _subscription, _directory) = make_app(true).await;

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

#[tokio::test]
async fn readyz_returns_503_when_forwarder_stopped() {
    let cache = CacheState::new(60_000, 64);
    let source_ready = Arc::new(AtomicBool::new(true));
    let forwarder_alive = Arc::new(AtomicBool::new(false));
    let (wal, _subscription, _directory) = common::open_temp_wal().await;
    let wal_capacity = wal.capacity_handle();
    let app = cache_http::router(
        cache,
        source_ready,
        forwarder_alive,
        wal_capacity.clone(),
        wal_capacity,
    );

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

// ── /v1/state ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_state_returns_empty_when_cache_is_empty() {
    let (app, _wal, _subscription, _directory) = make_app(false).await;

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
async fn list_state_returns_cached_typed_telemetry() {
    let cache = CacheState::new(60_000, 64);
    cache.update_record(
        &TelemetryRecord {
            route_id: "test-route".to_owned(),
            device_id: "ESP32-1".to_owned(),
            measurement: "sensor".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::new(),
            fields: BTreeMap::from([
                ("enabled".to_owned(), TelemetryValue::Boolean(true)),
                ("temp_c".to_owned(), TelemetryValue::Float(22.5)),
            ]),
            source_topic: "home/esp32-1/sensor".to_owned(),
            retained: false,
        },
        1_700_000_000_456,
        1,
    );

    let (app, _wal, _subscription, _directory) = make_app_with_cache(cache, false).await;

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
    assert_eq!(sensors[0]["last_seen_ms"], 1_700_000_000_456_u64);
    assert_eq!(sensors[0]["value"]["temp_c"], 22.5);
    assert_eq!(sensors[0]["value"]["enabled"], true);
}

// ── /v1/state/{device_id} ────────────────────────────────────────────────────

#[tokio::test]
async fn get_state_returns_null_sensor_for_unknown_device() {
    let (app, _wal, _subscription, _directory) = make_app(false).await;

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
