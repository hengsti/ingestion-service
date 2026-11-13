#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions};
use tempfile::TempDir;

use smarthome_ingest::{
    config::routes,
    infrastructure::{
        cache::state::CacheState,
        operations::{ForwarderState, Operations},
        quarantine::Quarantine,
        router::RouteSet,
        sink::{influx::InfluxEncoder, Encoder},
        source::{mqtt::MqttDlqPublisher, DlqPublisher},
        wal::{
            subscription::WalSubscription,
            types::{WalEvent, WalOptions},
            wal::Wal,
        },
    },
    pipeline::{
        runner::PipelineRunner,
        stages::{
            cache_update::CacheUpdateStage, decode::DecodeStage, dlq::DlqPublishStage,
            observe::ObserveStage, persist::PersistStage, telemetry_mapping::TelemetryMappingStage,
            validate_output::ValidateOutputStage, validate_raw::ValidateRawStage,
        },
    },
};

/// Builds operational snapshots for HTTP fixtures without starting external services.
pub fn http_operations(
    ready: Arc<std::sync::atomic::AtomicBool>,
    forwarder_alive: bool,
    wal: Arc<smarthome_ingest::infrastructure::wal::capacity::Capacity>,
    quarantine: Arc<smarthome_ingest::infrastructure::wal::capacity::Capacity>,
) -> Arc<Operations> {
    let forwarder = Arc::new(ForwarderState::default());
    forwarder
        .liveness_handle()
        .store(forwarder_alive, std::sync::atomic::Ordering::Relaxed);
    let operations = Arc::new(Operations::new(ready, forwarder, wal, quarantine));
    operations.update_admission(1, 1, 0);
    operations
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
        max_bytes: 128 * 1024 * 1024,
        segment_bytes: 1024 * 1024,
        queue_capacity: 1_000,
    })
    .await
    .expect("open WAL");
    (Arc::new(wal), sub, tmp)
}

/// An `Arc<dyn Encoder>` wrapping [`InfluxEncoder`], for building pipelines
/// against the trait-based encoding seam instead of a raw sink-specific call.
pub fn influx_encoder() -> Arc<dyn Encoder> {
    Arc::new(InfluxEncoder)
}

/// Writes the shipped routes' v1 counterpart with its schemas for compatibility tests.
pub fn write_v1_shipped_bundle(directory: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let schemas = directory.join("schemas");
    std::fs::create_dir_all(&schemas)?;
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/schemas");
    for (family, filenames) in [
        (
            "esp32-bme680",
            &[
                "base.schema.json",
                "sensor.schema.json",
                "sensor.output.schema.json",
                "status.schema.json",
                "status.output.schema.json",
            ][..],
        ),
        (
            "aqara",
            &["aqara-p100.schema.json", "aqara-p100.output.schema.json"][..],
        ),
        (
            "shelly",
            &["shelly-plug.schema.json", "shelly-plug.output.schema.json"][..],
        ),
    ] {
        for filename in filenames {
            std::fs::copy(source.join(family).join(filename), schemas.join(filename))?;
        }
    }
    let path = directory.join("routes.json");
    std::fs::write(&path, include_str!("../fixtures/routes/shipped-v1.json"))?;
    Ok(path)
}

/// Builds the full ingest pipeline persisting to a temporary WAL.
///
/// Returns the runner, the WAL subscription (to assert on persisted events),
/// the cache state, and the backing `TempDir` (keep alive for the test).
pub async fn build_pipeline() -> (PipelineRunner, WalSubscription, CacheState, TempDir) {
    let routes_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/routes.json");
    let routes = Arc::new(
        RouteSet::compile(
            routes::load(routes_path).expect("load configured routes"),
            "smarthome/_dlq/ingest",
            "test_bucket",
        )
        .expect("compile configured routes"),
    );
    build_pipeline_with_routes(routes).await
}

pub async fn build_pipeline_with_routes(
    routes: Arc<RouteSet>,
) -> (PipelineRunner, WalSubscription, CacheState, TempDir) {
    build_pipeline_with_routes_and_encoder(routes, influx_encoder()).await
}

pub async fn build_pipeline_with_routes_and_encoder(
    routes: Arc<RouteSet>,
    encoder: Arc<dyn Encoder>,
) -> (PipelineRunner, WalSubscription, CacheState, TempDir) {
    let cache = CacheState::new(60_000, 64);
    let (wal, sub, tmp) = open_temp_wal().await;
    let (quarantine, quarantine_subscription) = Quarantine::open(WalOptions {
        dir: tmp.path().join("quarantine"),
        max_bytes: 16 * 1024 * 1024,
        segment_bytes: 1024 * 1024,
        queue_capacity: 1_000,
    })
    .await
    .expect("open quarantine journal");
    drop(quarantine_subscription);

    let pipeline = PipelineRunner::new()
        .add_stage(DecodeStage::new())
        .add_stage(ValidateRawStage::new(routes))
        .add_stage(TelemetryMappingStage::new())
        .add_stage(ValidateOutputStage::new(encoder.clone()))
        .add_stage(CacheUpdateStage::new(cache.clone()))
        .add_stage(PersistStage::new(wal, encoder))
        .add_stage(ObserveStage::new())
        .with_failure_stage(DlqPublishStage::new(
            dlq_publisher(),
            "smarthome/_dlq/ingest",
            Arc::new(quarantine),
        ));

    (pipeline, sub, cache, tmp)
}

/// Pulls the next persisted event from the WAL, returning `None` if none
/// arrives within `ms` milliseconds (used to assert "nothing was persisted").
pub async fn recv_event(sub: &mut WalSubscription, ms: u64) -> Option<WalEvent> {
    match tokio::time::timeout(Duration::from_millis(ms), sub.next()).await {
        Ok(Ok(Some(entry))) => Some(entry.event),
        Ok(Ok(None)) | Err(_) => None,
        Ok(Err(error)) => panic!("WAL subscription failed: {error:#}"),
    }
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
