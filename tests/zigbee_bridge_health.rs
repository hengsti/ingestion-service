mod common;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use smarthome_ingest::{
    config::routes,
    infrastructure::router::RouteSet,
    pipeline::{context::PipelineContext, outcome::PipelineOutcome},
};

const TOPIC: &str = "zigbee2mqtt/bridge/health";
const PAYLOAD: &str = include_str!("fixtures/zigbee-bridge-health.json");

#[tokio::test]
async fn bridge_health_persists_typed_metrics_with_publisher_time_without_caching() -> Result<()> {
    let (pipeline, mut sub, cache, _wal) = common::build_pipeline().await;
    let mut context = PipelineContext::new_with_metadata(
        TOPIC,
        PAYLOAD.as_bytes().to_vec(),
        1_794_382_999_999,
        true,
    );

    assert_eq!(
        pipeline.run(&mut context).await,
        PipelineOutcome::OutputStored
    );
    let event = common::recv_event(&mut sub, 1000).await.context("output")?;
    assert_eq!(event.sink_target.as_deref(), Some("metrics"));
    assert_eq!(
        event.payload,
        concat!(
            "zigbee2mqtt_bridge_health,device_class=zigbee2mqtt-bridge,device_id=zigbee2mqtt-bridge ",
            "device_0x00124b0038a8eae2_leave_count=0i,",
            "device_0x00124b0038a8eae2_messages=1i,",
            "device_0x00124b0038a8eae2_messages_per_sec=0,",
            "device_0x00124b0038a8eae2_network_address_changes=0i,",
            "device_0x54ef44100171342e_leave_count=0i,",
            "device_0x54ef44100171342e_messages=1217i,",
            "device_0x54ef44100171342e_messages_per_sec=0.0012,",
            "device_0x54ef44100171342e_network_address_changes=0i,",
            "mqtt_connected=true,mqtt_published=4340i,mqtt_queued=0i,mqtt_received=13i,",
            "mqtt_retained=true,os_load_15m=6.14,os_load_1m=1.99,os_load_5m=4.43,",
            "os_memory_perc=48.5224,os_memory_used_mib=3794.32,",
            "process_memory_perc=1.6423,process_memory_rss_mib=128.43,",
            "process_uptime_sec=1039858i 1794382773699"
        )
    );
    let record = context.telemetry_record()?;
    assert_eq!(record.timestamp_ms, 1_794_382_773_699);
    assert!(cache.snapshot_device(&record.device_id).is_none());
    let routes = RouteSet::compile(routes::load("config/routes.json")?, "test/dlq", "fallback")?;
    assert!(routes.select("zigbee2mqtt/bridge/info").is_none());
    assert!(routes.select("zigbee2mqtt/bridge/health/extra").is_none());
    let schema = routes.select(TOPIC).context("health route")?;
    let schema = schema.output_schema().context("output schema")?;
    let mut invalid = serde_json::to_value(record)?;
    invalid["fields"]["process_memory_perc"] = json!(101);
    assert!(schema.validate(&invalid).is_err());
    Ok(())
}

#[tokio::test]
async fn missing_devices_are_omitted_and_zero_and_false_bridge_values_are_preserved() -> Result<()>
{
    let (pipeline, mut sub, _cache, _wal) = common::build_pipeline().await;
    let mut payload: Value = serde_json::from_str(PAYLOAD)?;
    payload["devices"] = json!({});
    payload["mqtt"] = json!({"connected": false, "queued": 0, "published": 0, "received": 0});
    let mut context = PipelineContext::new_with_metadata(
        TOPIC,
        serde_json::to_vec(&payload)?,
        1_794_382_999_999,
        false,
    );

    assert_eq!(
        pipeline.run(&mut context).await,
        PipelineOutcome::OutputStored
    );
    let event = common::recv_event(&mut sub, 1000).await.context("output")?;
    assert_eq!(event.sink_target.as_deref(), Some("metrics"));
    assert!(event
        .payload
        .contains("mqtt_connected=false,mqtt_published=0i,mqtt_queued=0i,mqtt_received=0i"));
    assert!(!event.payload.contains("device_0x"));
    Ok(())
}

#[test]
fn bridge_health_schema_rejects_malformed_metrics_and_accepts_changing_device_sets() -> Result<()> {
    let routes = RouteSet::compile(routes::load("config/routes.json")?, "test/dlq", "fallback")?;
    let route = routes.select(TOPIC).context("health route")?;
    assert_eq!(route.sink_target(), "metrics");
    let payload: Value = serde_json::from_str(PAYLOAD)?;
    for (pointer, value) in [
        ("/response_time", json!(0)),
        ("/response_time", json!(9_223_372_036_855i64)),
        ("/os/load_average", json!([1, 2])),
        ("/os/load_average", json!([1, 2, 3, 4])),
        ("/os/load_average/0", json!(-1)),
        ("/os/memory_percent", json!(101)),
        ("/process/memory_used_mb", json!(-1)),
        ("/mqtt/connected", json!("true")),
        ("/mqtt/queued", json!(-1)),
        ("/mqtt/received", json!(1.5)),
        ("/devices", json!(null)),
        ("/devices/0x54ef44100171342e/messages", json!(-1)),
        ("/devices/0x54ef44100171342e/messages_per_sec", json!("0.1")),
        ("/devices/0x54ef44100171342e/leave_count", json!(null)),
    ] {
        let mut invalid = payload.clone();
        *invalid.pointer_mut(pointer).context("fixture pointer")? = value;
        assert!(route.validate_raw(&invalid).is_err(), "accepted {pointer}");
    }
    let mut changed = payload;
    changed["devices"] = json!({"0x1234567890abcdef": {
        "messages": 0, "messages_per_sec": 0, "leave_count": 0, "network_address_changes": 0
    }});
    route.validate_raw(&changed)?;
    Ok(())
}
