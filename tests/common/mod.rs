#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions};
use tempfile::TempDir;

use smarthome_ingest::{
    infrastructure::{
        cache::state::CacheState,
        router::{Route, Router},
        source::{mqtt::MqttDlqPublisher, DlqPublisher},
        wal::{
            subscription::WalSubscription,
            types::{WalEvent, WalOptions},
            wal::Wal,
        },
    },
    model::messages::message::MessageType,
    pipeline::{
        runner::PipelineRunner,
        stages::{
            cache_update::CacheUpdateStage, decode::DecodeStage, dlq::DlqPublishStage,
            observe::ObserveStage, persist::PersistStage, transform::TransformStage,
            validate_business::ValidateBusinessStage, validate_raw::ValidateRawStage,
        },
    },
};

const SENSOR_SCHEMA: &str = include_str!("../../schema/sensor.schema.json");
const STATUS_SCHEMA: &str = include_str!("../../schema/status.schema.json");

pub fn build_router() -> Arc<Router> {
    let sensor = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
    let status = Route::new(MessageType::Status, STATUS_SCHEMA, "smarthome/+/status").unwrap();
    Arc::new(Router::new().add_route(sensor).add_route(status))
}

pub fn build_non_strict_router() -> Arc<Router> {
    let sensor = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
    let status = Route::new(MessageType::Status, STATUS_SCHEMA, "smarthome/+/status").unwrap();
    Arc::new(
        Router::new()
            .strict(false)
            .add_route(sensor)
            .add_route(status),
    )
}

/// A client whose internal eventloop is dropped so that publish fails gracefully.
/// The DLQ stage handles the send error with a warning log and returns Ok(Stop).
pub fn dlq_client() -> AsyncClient {
    let opts = MqttOptions::new("test-ingest-dlq", "localhost", 1883);
    let (client, _ev) = AsyncClient::new(opts, 10);
    client
}

/// An `Arc<dyn DlqPublisher>` wrapping [`dlq_client`], for building pipelines
/// against the trait-based DLQ seam instead of a raw MQTT client.
pub fn dlq_publisher() -> Arc<dyn DlqPublisher> {
    Arc::new(MqttDlqPublisher::new(dlq_client()))
}

/// Opens a fresh WAL backed by a temporary directory.
///
/// The returned `TempDir` must be kept alive for the lifetime of the test —
/// dropping it deletes the WAL segments on disk.
pub async fn open_temp_wal() -> (Arc<Wal>, WalSubscription, TempDir) {
    let tmp = tempfile::tempdir().expect("create temp WAL dir");
    let (wal, sub) = Wal::open(WalOptions {
        dir: tmp.path().to_path_buf(),
        segment_bytes: 1024 * 1024,
        queue_capacity: 1_000,
    })
    .await
    .expect("open WAL");
    (Arc::new(wal), sub, tmp)
}

/// Builds the full ingest pipeline persisting to a temporary WAL.
///
/// Returns the runner, the WAL subscription (to assert on persisted events),
/// the cache state, and the backing `TempDir` (keep alive for the test).
pub async fn build_pipeline() -> (PipelineRunner, WalSubscription, CacheState, TempDir) {
    let router = build_router();
    let cache = CacheState::new(60_000, 64);
    let (wal, sub, tmp) = open_temp_wal().await;

    let pipeline = PipelineRunner::new()
        .add_stage(DecodeStage::new())
        .add_stage(ValidateRawStage::new(router.clone(), false))
        .add_stage(TransformStage::new(router.clone()))
        .add_stage(ValidateBusinessStage::new().unwrap())
        .add_stage(CacheUpdateStage::new(cache.clone()))
        .add_stage(PersistStage::new(wal))
        .add_stage(ObserveStage::new())
        .with_failure_stage(DlqPublishStage::new(
            dlq_publisher(),
            "smarthome/_dlq/ingest",
        ));

    (pipeline, sub, cache, tmp)
}

/// Pulls the next persisted event from the WAL, returning `None` if none
/// arrives within `ms` milliseconds (used to assert "nothing was persisted").
pub async fn recv_event(sub: &mut WalSubscription, ms: u64) -> Option<WalEvent> {
    tokio::time::timeout(Duration::from_millis(ms), sub.next())
        .await
        .ok()
        .flatten()
        .map(|entry| entry.event)
}

pub fn valid_sensor_payload(device_id: &str) -> Vec<u8> {
    serde_json::json!({
        "device_id": device_id,
        "room": "living_room",
        "device_class": "esp32p4-bme680",
        "fw_version": "1.0.0",
        "time_ms": 1_700_000_000_000_i64,
        "time_iso": "2023-11-14T22:13:20Z",
        "time_valid": true,
        "data": {
            "temp_c": 22.5,
            "rel_hum_perc": 45.0,
            "pressure_hpa": 1013.25,
            "gas_ohm": 50_000.0,
            "altitude_m": 500.0
        }
    })
    .to_string()
    .into_bytes()
}

pub fn valid_status_payload(device_id: &str) -> Vec<u8> {
    serde_json::json!({
        "device_id": device_id,
        "device_class": "esp32p4-bme680",
        "fw_version": "1.0.0",
        "ip": "192.168.1.42",
        "rssi": -65_i64,
        "time_ms": 1_700_000_000_000_i64,
        "time_iso": "2023-11-14T22:13:20Z",
        "time_valid": true,
        "uptime": 3600_i64,
        "free_mem": 200_000_i64,
        "ssid": "HomeNet"
    })
    .to_string()
    .into_bytes()
}
