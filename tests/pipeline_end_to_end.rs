mod common;

use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use smarthome_ingest::{
    config::routes::{self, RouteBundle},
    infrastructure::{router::RouteSet, sink::Encoder},
    model::messages::telemetry::{TelemetryRecord, TelemetryValue},
    pipeline::{context::PipelineContext, outcome::PipelineOutcome},
};

const DRAFT_7: &str = "http://json-schema.org/draft-07/schema#";

#[tokio::test]
async fn test_pipeline_outcome_output_stored_for_configured_firmware_sensor() -> Result<()> {
    let (pipeline, mut sub, cache, _tmp) = common::build_pipeline().await;
    let topic = "smarthome/esp32-1/bme680";
    let received_at_ms = 1_700_000_000_456;
    let mut ctx = PipelineContext::new_with_metadata(
        topic,
        common::valid_sensor_payload("esp32-1"),
        received_at_ms,
        true,
    );

    assert_eq!(pipeline.run(&mut ctx).await, PipelineOutcome::OutputStored);

    assert!(!ctx.should_quarantine(), "{:?}", ctx.rejection_reason());
    let event = common::recv_event(&mut sub, 500)
        .await
        .expect("configured route should persist to WAL");
    assert!(event.payload.starts_with("bme680,"), "{}", event.payload);
    assert!(event.payload.contains("altitude_m="), "{}", event.payload);
    assert!(
        event.payload.contains("mqtt_retained=true"),
        "{}",
        event.payload
    );
    assert!(
        event.payload.ends_with(" 1700000000000"),
        "{}",
        event.payload
    );
    let (state, _) = cache
        .snapshot_device("esp32-1")
        .expect("cache-enabled route should update state");
    assert_eq!(state.last_seen_ms, received_at_ms as u64);
    assert_eq!(
        state.value.get("temp_c"),
        Some(&TelemetryValue::Float(22.5))
    );
    Ok(())
}

#[tokio::test]
async fn test_pipeline_outcome_unrouted_qos_one_is_durably_rejected() -> Result<()> {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let mut ctx = PipelineContext::new_with_metadata(
        "smarthome/esp32-1/unconfigured",
        b"{}".to_vec(),
        1_700_000_000_456,
        false,
    );
    ctx.set_qos(1);

    assert_eq!(
        pipeline.run(&mut ctx).await,
        PipelineOutcome::RejectionStored
    );
    assert!(ctx
        .rejection_reason()
        .is_some_and(|reason| reason.contains("no configured route")));
    assert!(common::recv_event(&mut sub, 100).await.is_none());
    Ok(())
}

fn synthetic_zigbee_routes(
    directory: &std::path::Path,
    alternate_config: bool,
) -> Result<Arc<RouteSet>> {
    let raw_schema_path = directory.join("zigbee-raw.json");
    let output_schema_path = directory.join("zigbee-output.json");
    let (raw_schema, output_schema, route) = if alternate_config {
        (
            json!({
                "$schema": DRAFT_7,
                "type": "object",
                "required": ["device_id", "temperature", "humidity", "online"],
                "properties": {
                    "device_id": {"type": "string"},
                    "temperature": {"type": "number"},
                    "humidity": {"type": "number"},
                    "battery_pct": {"type": "number"},
                    "online": {"type": "boolean"}
                }
            }),
            json!({
                "$schema": DRAFT_7,
                "type": "object",
                "properties": {
                    "fields": {
                        "type": "object",
                        "required": ["battery_pct", "heat_index_c", "mqtt_retained"]
                    }
                }
            }),
            json!({
                "id": "zigbee-room-alternate",
                "topic": "zigbee/+/telemetry",
                "schema": raw_schema_path,
                "output_schema": output_schema_path,
                "device_id": {"pointer": "/device_id", "topic_segment": 1},
                "measurement": "zigbee_room_alternate",
                "tags": {"source": "zigbee"},
                "fields": {
                    "temp_c": {"pointer": "/temperature", "type": "float"},
                    "rel_hum_perc": {"pointer": "/humidity", "type": "float"},
                    "battery_pct": {
                        "pointer": "/battery_pct",
                        "type": "float",
                        "optional": true
                    },
                    "online": {"pointer": "/online", "type": "boolean"}
                },
                "timestamp": {"source": "received_at"},
                "calculations": ["heat_index"],
                "cache": false
            }),
        )
    } else {
        (
            json!({
            "$schema": DRAFT_7,
            "type": "object",
            "required": ["device_id", "temp_c", "rel_hum_perc", "online"],
            "properties": {
                "device_id": {"type": "string"},
                "temp_c": {"type": "number"},
                "rel_hum_perc": {"type": "number"},
                "online": {"type": "boolean"}
            }
            }),
            json!({
            "$schema": DRAFT_7,
            "type": "object",
            "properties": {
                "fields": {
                    "type": "object",
                    "required": ["dew_point_c", "mqtt_retained", "online"]
                }
            }
            }),
            json!({
                "id": "zigbee-room",
                "topic": "zigbee/+/telemetry",
                "schema": raw_schema_path,
                "output_schema": output_schema_path,
                "device_id": {"pointer": "/device_id", "topic_segment": 1},
                "measurement": "zigbee_room",
                "tags": {"source": "zigbee"},
                "fields": {
                    "temp_c": {"pointer": "/temp_c", "type": "float"},
                    "rel_hum_perc": {"pointer": "/rel_hum_perc", "type": "float"},
                    "online": {"pointer": "/online", "type": "boolean"}
                },
                "timestamp": {"source": "received_at"},
                "calculations": ["dew_point"],
                "cache": false
            }),
        )
    };
    fs::write(&raw_schema_path, serde_json::to_vec(&raw_schema)?)?;
    fs::write(&output_schema_path, serde_json::to_vec(&output_schema)?)?;
    let bundle: RouteBundle = serde_json::from_value(json!({
        "version": 1,
        "schema_resources": {},
        "routes": [route]
    }))?;
    Ok(Arc::new(RouteSet::compile(bundle, "zigbee/dlq")?))
}

#[tokio::test]
async fn arbitrary_topic_uses_configured_mapping_calculation_and_output_schema() -> Result<()> {
    let config_dir = tempfile::tempdir()?;
    let routes = synthetic_zigbee_routes(config_dir.path(), false)?;
    let (pipeline, mut sub, cache, _wal_dir) = common::build_pipeline_with_routes(routes).await;
    let topic = "zigbee/room-1/telemetry";
    let received_at_ms = 1_700_000_000_456;
    let payload: Value = json!({
        "device_id": "room-1",
        "temp_c": 20.0,
        "rel_hum_perc": 50.0,
        "online": true
    });
    let mut ctx = PipelineContext::new_with_metadata(
        topic,
        serde_json::to_vec(&payload)?,
        received_at_ms,
        true,
    );

    assert_eq!(pipeline.run(&mut ctx).await, PipelineOutcome::OutputStored);

    assert!(!ctx.should_quarantine(), "{:?}", ctx.rejection_reason());
    let event = common::recv_event(&mut sub, 500)
        .await
        .expect("arbitrary-topic configured route should persist to WAL");
    assert!(
        event
            .payload
            .starts_with("zigbee_room,device_id=room-1,source=zigbee "),
        "{}",
        event.payload
    );
    assert!(
        event.payload.contains("dew_point_c=9.254294282076941"),
        "{}",
        event.payload
    );
    assert!(
        event.payload.contains("mqtt_retained=true"),
        "{}",
        event.payload
    );
    assert!(
        event.payload.ends_with(" 1700000000456"),
        "{}",
        event.payload
    );
    assert!(
        cache.snapshot_all_devices().is_empty(),
        "cache-disabled route must not update state"
    );
    Ok(())
}

#[tokio::test]
async fn alternate_route_config_changes_schema_mapping_calculation_and_output() -> Result<()> {
    let config_dir = tempfile::tempdir()?;
    let routes = synthetic_zigbee_routes(config_dir.path(), true)?;
    let (pipeline, mut sub, _cache, _wal_dir) = common::build_pipeline_with_routes(routes).await;
    let topic = "zigbee/room-1/telemetry";

    let old_payload: Value = json!({
        "device_id": "room-1",
        "temp_c": 20.0,
        "rel_hum_perc": 50.0,
        "online": true
    });
    let mut invalid_ctx = PipelineContext::new_with_metadata(
        topic,
        serde_json::to_vec(&old_payload)?,
        1_700_000_000_456,
        false,
    );
    assert_eq!(
        pipeline.run(&mut invalid_ctx).await,
        PipelineOutcome::RejectionStored
    );
    assert!(
        invalid_ctx.should_quarantine(),
        "alternate raw schema must apply"
    );
    assert!(common::recv_event(&mut sub, 100).await.is_none());

    let missing_field_payload: Value = json!({
        "device_id": "room-1",
        "temperature": 20.0,
        "humidity": 50.0,
        "online": true
    });
    let mut invalid_output_ctx = PipelineContext::new_with_metadata(
        topic,
        serde_json::to_vec(&missing_field_payload)?,
        1_700_000_000_456,
        false,
    );
    assert_eq!(
        pipeline.run(&mut invalid_output_ctx).await,
        PipelineOutcome::RejectionStored
    );
    assert!(
        invalid_output_ctx.should_quarantine(),
        "alternate output schema must require battery_pct"
    );
    assert!(common::recv_event(&mut sub, 100).await.is_none());

    let payload: Value = json!({
        "device_id": "room-1",
        "temperature": 20.0,
        "humidity": 50.0,
        "battery_pct": 85.0,
        "online": true
    });
    let mut ctx = PipelineContext::new_with_metadata(
        topic,
        serde_json::to_vec(&payload)?,
        1_700_000_000_456,
        false,
    );
    assert_eq!(pipeline.run(&mut ctx).await, PipelineOutcome::OutputStored);
    assert!(!ctx.should_quarantine(), "{:?}", ctx.rejection_reason());
    let event = common::recv_event(&mut sub, 500)
        .await
        .context("alternate configured route should persist to WAL")?;
    assert!(
        event
            .payload
            .starts_with("zigbee_room_alternate,device_id=room-1,source=zigbee "),
        "{}",
        event.payload
    );
    assert!(
        event.payload.contains("battery_pct=85"),
        "{}",
        event.payload
    );
    assert!(event.payload.contains("heat_index_c="), "{}", event.payload);
    assert!(!event.payload.contains("dew_point_c="), "{}", event.payload);
    assert!(
        event.payload.ends_with(" 1700000000456"),
        "{}",
        event.payload
    );
    Ok(())
}

#[tokio::test]
async fn valid_timestamp_zero_is_rejected_before_wal_persistence() -> Result<()> {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let mut payload: Value = serde_json::from_slice(&common::valid_sensor_payload("esp32-1"))?;
    payload["time_ms"] = json!(0);
    payload["time_valid"] = json!(true);
    let mut ctx = PipelineContext::new_with_metadata(
        "smarthome/esp32-1/bme680",
        serde_json::to_vec(&payload)?,
        1_700_000_000_456,
        false,
    );

    assert_eq!(
        pipeline.run(&mut ctx).await,
        PipelineOutcome::RejectionStored
    );

    assert!(ctx.should_quarantine());
    assert!(
        common::recv_event(&mut sub, 100).await.is_none(),
        "invalid output must not reach the WAL"
    );
    Ok(())
}

#[tokio::test]
async fn oversized_payload_is_rejected_before_json_decoding() -> Result<()> {
    let (pipeline, mut sub, _cache, _tmp) = common::build_pipeline().await;
    let mut ctx = PipelineContext::new("smarthome/esp32-1/bme680", vec![b'x'; 64 * 1024 + 1]);

    assert_eq!(
        pipeline.run(&mut ctx).await,
        PipelineOutcome::RejectionStored
    );

    assert!(ctx.should_quarantine());
    assert!(ctx.rejection_reason().unwrap_or("").contains("too large"));
    assert!(common::recv_event(&mut sub, 100).await.is_none());
    Ok(())
}

#[tokio::test]
async fn oversized_encoded_output_is_rejected_before_cache_update() -> Result<()> {
    struct OversizedEncoder(Arc<AtomicBool>);

    impl Encoder for OversizedEncoder {
        fn encoded_size_upper_bound(&self, _record: &TelemetryRecord) -> usize {
            1024 * 1024
        }

        fn encode(&self, _record: &TelemetryRecord, out: &mut String) {
            self.0.store(true, Ordering::Relaxed);
            out.push_str(&"x".repeat(1024 * 1024));
        }
    }

    let routes = Arc::new(RouteSet::compile(
        routes::load("config/routes.json")?,
        "smarthome/_dlq/ingest",
    )?);
    let called = Arc::new(AtomicBool::new(false));
    let encoder = Arc::new(OversizedEncoder(called.clone()));
    let (pipeline, mut sub, cache, _tmp) =
        common::build_pipeline_with_routes_and_encoder(routes, encoder).await;
    let mut ctx = PipelineContext::new_with_metadata(
        "smarthome/esp32-1/bme680",
        common::valid_sensor_payload("esp32-1"),
        1_700_000_000_456,
        false,
    );

    assert_eq!(
        pipeline.run(&mut ctx).await,
        PipelineOutcome::RejectionStored
    );

    assert!(ctx.should_quarantine(), "oversized output must be rejected");
    assert!(
        !called.load(Ordering::Relaxed),
        "must reject before encoding"
    );
    assert!(cache.snapshot_device("esp32-1").is_none());
    assert!(common::recv_event(&mut sub, 100).await.is_none());
    Ok(())
}
