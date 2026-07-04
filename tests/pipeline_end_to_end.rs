mod common;

use smarthome_ingest::{
    infrastructure::cache::state::CacheState,
    pipeline::{
        context::PipelineContext,
        runner::PipelineRunner,
        stages::{
            cache_update::CacheUpdateStage, decode::DecodeStage, dlq::DlqPublishStage,
            observe::ObserveStage, persist::PersistStage, transform::TransformStage,
            validate_business::ValidateBusinessStage, validate_raw::ValidateRawStage,
        },
    },
};

const SENSOR_SCHEMA: &str = include_str!("../schema/sensor.schema.json");
const STATUS_SCHEMA: &str = include_str!("../schema/status.schema.json");

// ── happy-path tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn valid_sensor_message_processes_end_to_end() {
    let (pipeline, mut sub, cache, _tmp) = common::build_pipeline().await;
    let mut ctx = PipelineContext::new(
        "smarthome/esp32-1/sensor",
        common::valid_sensor_payload("esp32-1"),
    );

    pipeline.run(&mut ctx).await;

    assert!(
        !ctx.should_publish_dlq(),
        "expected no DLQ, got: {:?}",
        ctx.dlq_reason()
    );
    assert!(ctx.ignored_reason().is_none(), "expected not ignored");

    let event = common::recv_event(&mut sub, 500)
        .await
        .expect("expected a sensor event in the WAL");
    assert_eq!(event.topic, "smarthome/esp32-1/sensor");
    assert!(
        event.payload.contains("bme680"),
        "expected a sensor line protocol, got: {:?}",
        event.payload
    );

    assert!(
        cache.snapshot_sensor("esp32-1").is_some(),
        "sensor must be present in cache after processing"
    );
}

#[tokio::test]
async fn valid_status_message_processes_end_to_end() {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let mut ctx = PipelineContext::new(
        "smarthome/esp32-1/status",
        common::valid_status_payload("esp32-1"),
    );

    pipeline.run(&mut ctx).await;

    assert!(
        !ctx.should_publish_dlq(),
        "expected no DLQ, got: {:?}",
        ctx.dlq_reason()
    );

    let event = common::recv_event(&mut sub, 500)
        .await
        .expect("expected a status event in the WAL");
    assert!(
        event.payload.contains("device_status"),
        "expected a status line protocol, got: {:?}",
        event.payload
    );
}

// ── failure paths → DLQ ──────────────────────────────────────────────────────

#[tokio::test]
async fn schema_invalid_payload_goes_to_dlq() {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    // Valid envelope but missing required `data` field → raw schema validation fails.
    let payload = serde_json::json!({
        "device_id": "esp32-1",
        "room": "living_room",
        "device_class": "cls",
        "fw_version": "1.0",
        "time_ms": 0,
        "time_iso": "2024-01-01T00:00:00Z",
        "time_valid": true
    })
    .to_string()
    .into_bytes();
    let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", payload);

    pipeline.run(&mut ctx).await;

    assert!(
        ctx.should_publish_dlq(),
        "expected DLQ for schema-invalid payload"
    );
    assert!(
        common::recv_event(&mut sub, 150).await.is_none(),
        "WAL must be empty when pipeline fails"
    );
}

#[tokio::test]
async fn transform_failure_goes_to_dlq() {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    // rel_hum_perc: 0 violates the transform stage's (0, 100] bounds check.
    let payload = serde_json::json!({
        "device_id": "esp32-1",
        "room": "living_room",
        "device_class": "esp32p4-bme680",
        "fw_version": "1.0.0",
        "time_ms": 1_700_000_000_000_i64,
        "time_iso": "2023-11-14T22:13:20Z",
        "time_valid": true,
        "data": {
            "temp_c": 22.5,
            "rel_hum_perc": 0.0,
            "pressure_hpa": 1013.25,
            "gas_ohm": 50_000.0,
            "altitude_m": 500.0
        }
    })
    .to_string()
    .into_bytes();
    let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", payload);

    pipeline.run(&mut ctx).await;

    assert!(
        ctx.should_publish_dlq(),
        "expected DLQ when transform bounds check fails"
    );
    assert!(
        common::recv_event(&mut sub, 150).await.is_none(),
        "WAL must be empty when pipeline fails"
    );
}

#[tokio::test]
async fn oversized_payload_goes_to_dlq() {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let payload = vec![b'x'; 64 * 1024 + 1];
    let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", payload);

    pipeline.run(&mut ctx).await;

    assert!(
        ctx.should_publish_dlq(),
        "expected DLQ for oversized payload"
    );
    assert!(
        ctx.dlq_reason().unwrap_or("").contains("too large"),
        "DLQ reason should mention size: {:?}",
        ctx.dlq_reason()
    );
    assert!(
        common::recv_event(&mut sub, 150).await.is_none(),
        "WAL must be empty when pipeline fails"
    );
}

// ── routing: strict vs. non-strict ───────────────────────────────────────────

#[tokio::test]
async fn unknown_topic_strict_mode_goes_to_dlq() {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let mut ctx = PipelineContext::new(
        "smarthome/unknown/foo",
        common::valid_sensor_payload("unknown"),
    );

    pipeline.run(&mut ctx).await;

    assert!(
        ctx.should_publish_dlq(),
        "expected DLQ for unknown topic in strict mode"
    );
    assert!(
        common::recv_event(&mut sub, 150).await.is_none(),
        "WAL must be empty"
    );
}

#[tokio::test]
async fn unknown_topic_non_strict_mode_is_ignored() {
    let router = common::build_non_strict_router();
    let cache = CacheState::new(60_000, 64);
    let (wal, mut sub, _tmp) = common::open_temp_wal().await;
    let non_strict_router_clone = router.clone();

    let pipeline = PipelineRunner::new()
        .add_stage(DecodeStage::new())
        .add_stage(ValidateRawStage::new(router, false))
        .add_stage(TransformStage::new(non_strict_router_clone))
        .add_stage(ValidateBusinessStage::new().unwrap())
        .add_stage(CacheUpdateStage::new(cache))
        .add_stage(PersistStage::new(wal, common::influx_encoder()))
        .add_stage(ObserveStage::new())
        .with_failure_stage(DlqPublishStage::new(
            common::dlq_publisher(),
            "smarthome/_dlq/ingest",
        ));

    let mut ctx = PipelineContext::new(
        "smarthome/unknown/foo",
        common::valid_sensor_payload("unknown"),
    );

    pipeline.run(&mut ctx).await;

    assert!(
        ctx.ignored_reason().is_some(),
        "expected ignored for unknown topic in non-strict mode"
    );
    assert!(
        !ctx.should_publish_dlq(),
        "expected no DLQ in non-strict mode"
    );
    assert!(
        common::recv_event(&mut sub, 150).await.is_none(),
        "WAL must be empty for ignored message"
    );
}

// Helper to verify schemas are accessible from the test binary —
// these consts are used in build_pipeline (via common), so include them to
// confirm the path is correct at compile time.
#[allow(dead_code)]
fn _assert_schemas_compile() {
    let _: &str = SENSOR_SCHEMA;
    let _: &str = STATUS_SCHEMA;
}
