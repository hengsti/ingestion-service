use anyhow::Context;
use serde_json::json;

use smarthome_ingest::{
    config::routes::{self, CalculationName, DeviceIdSource, TimestampFallback},
    infrastructure::router::RouteSet,
};

#[test]
fn test_firmware_routes_loads_and_compiles_two_routes() -> anyhow::Result<()> {
    let bundle = routes::load("config/routes.json")?;
    assert_eq!(bundle.version, 1);
    assert_eq!(bundle.routes.len(), 2);
    let sensor_has_topic_identity = matches!(
        &bundle.routes[0].device_id,
        DeviceIdSource::PointerAndTopicSegment(_)
    );
    let sensor_has_expected_calculations = bundle.routes[0].calculations
        == [
            CalculationName::DewPoint,
            CalculationName::HeatIndex,
            CalculationName::Iaq,
        ];
    let sensor_has_receive_time_fallback = matches!(
        &bundle.routes[0].timestamp.fallback,
        Some(TimestampFallback::ReceivedAt)
    );
    let sensor_is_cached = bundle.routes[0].cache;
    let status_is_uncached = !bundle.routes[1].cache;
    let status_has_no_calculations = bundle.routes[1].calculations.is_empty();

    let route_set = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
    assert_eq!(
        route_set.subscriptions(),
        ["smarthome/+/sensor", "smarthome/+/status"]
    );

    let sensor = route_set
        .select("smarthome/test-sensor/sensor")
        .context("sensor route was not selected")?;
    assert_eq!(sensor.id(), "firmware-sensor");
    assert!(sensor_has_topic_identity);
    assert!(sensor_has_expected_calculations);
    assert!(sensor_has_receive_time_fallback);
    assert!(sensor_is_cached);
    sensor.validate_raw(&json!({
        "device_id": "test-sensor",
        "device_class": "test-class",
        "fw_version": "1.0.0",
        "time_ms": 1_700_000_000_000i64,
        "time_iso": "2023-11-14T22:13:20Z",
        "time_valid": true,
        "room": "test-room",
        "data": {
            "temp_c": 22.5,
            "rel_hum_perc": 45.0,
            "pressure_hpa": 1013.25,
            "gas_ohm": 50_000.0,
            "altitude_m": 500.0
        }
    }))?;

    let status = route_set
        .select("smarthome/test-sensor/status")
        .context("status route was not selected")?;
    assert_eq!(status.id(), "firmware-status");
    assert!(status_is_uncached);
    assert!(status_has_no_calculations);
    status.validate_raw(&json!({
        "device_id": "test-sensor",
        "device_class": "test-class",
        "fw_version": "1.0.0",
        "time_ms": 1_700_000_000_000i64,
        "time_iso": "2023-11-14T22:13:20Z",
        "time_valid": true,
        "ip": "192.0.2.1",
        "rssi": -65
    }))?;
    Ok(())
}

#[test]
fn test_firmware_routes_sensor_output_schema_enforces_warmup_and_bounds() -> anyhow::Result<()> {
    let bundle = routes::load("config/routes.json")?;
    let route_set = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
    let sensor = route_set
        .select("smarthome/test-sensor/sensor")
        .context("sensor route was not selected")?;
    let schema = sensor
        .output_schema()
        .context("sensor route must have an output schema")?;
    let mut record = json!({
        "route_id": "firmware-sensor",
        "device_id": "test-sensor",
        "measurement": "bme680",
        "timestamp_ms": 1_700_000_000_000i64,
        "tags": {
            "device_id": "test-sensor",
            "room": "test-room",
            "device_class": "test-class"
        },
        "fields": {
            "firmware_version": "1.0.0",
            "temp_c": 22.5,
            "rel_hum_perc": 45.0,
            "pressure_hpa": 1013.25,
            "gas_ohm": 50_000.0,
            "altitude_m": 500.0,
            "time_valid": true,
            "dew_point_c": 9.5,
            "heat_index_c": 22.5,
            "iaq_score": 85.0,
            "iaq_text": "Good",
            "mqtt_retained": false
        },
        "source_topic": "smarthome/test-sensor/sensor",
        "retained": false
    });

    schema.validate(&record)?;
    record["fields"]["battery"] = json!(90);
    schema.validate(&record)?;
    record["fields"]["battery"] = json!(90.5);
    schema.validate(&record)?;
    record["fields"].as_object_mut().unwrap().remove("battery");
    record["fields"]["warmed_up"] = json!(false);
    record["fields"]
        .as_object_mut()
        .unwrap()
        .remove("iaq_score");
    record["fields"].as_object_mut().unwrap().remove("iaq_text");
    schema.validate(&record)?;

    record["fields"]["warmed_up"] = json!(true);
    assert!(schema.validate(&record).is_err());
    record["fields"]["warmed_up"] = json!(false);
    record["fields"]["rel_hum_perc"] = json!(0.0);
    assert!(schema.validate(&record).is_err());
    record["fields"]["rel_hum_perc"] = json!(45.0);
    record["timestamp_ms"] = json!(0);
    assert!(schema.validate(&record).is_err());
    Ok(())
}

#[test]
fn test_firmware_routes_status_output_schema_requires_nonempty_ip_and_valid_time(
) -> anyhow::Result<()> {
    let bundle = routes::load("config/routes.json")?;
    let route_set = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
    let status = route_set
        .select("smarthome/test-sensor/status")
        .context("status route was not selected")?;
    let schema = status
        .output_schema()
        .context("status route must have an output schema")?;
    let mut record = json!({
        "route_id": "firmware-status",
        "device_id": "test-sensor",
        "measurement": "device_status",
        "timestamp_ms": 1_700_000_000_000i64,
        "tags": {
            "device_id": "test-sensor",
            "device_class": "test-class"
        },
        "fields": {
            "firmware_version": "1.0.0",
            "ip_address": "192.0.2.1",
            "time_valid": true,
            "rssi": -65,
            "uptime": 0,
            "free_mem": 0,
            "ssid": "",
            "mqtt_retained": false
        },
        "source_topic": "smarthome/test-sensor/status",
        "retained": false
    });
    schema.validate(&record)?;
    record["fields"]["battery"] = json!(90);
    schema.validate(&record)?;

    let mut invalid = record.clone();
    invalid["fields"]["ip_address"] = json!("");
    assert!(schema.validate(&invalid).is_err());
    invalid = record;
    invalid["fields"]["uptime"] = json!(-1);
    assert!(schema.validate(&invalid).is_err());
    invalid["fields"]["uptime"] = json!(0);
    invalid["timestamp_ms"] = json!(0);
    assert!(schema.validate(&invalid).is_err());
    Ok(())
}

#[test]
fn test_firmware_routes_raw_schema_rejects_malformed_rfc3339() -> anyhow::Result<()> {
    let bundle = routes::load("config/routes.json")?;
    let route_set = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
    let sensor = route_set
        .select("smarthome/test-sensor/sensor")
        .context("sensor route was not selected")?;
    let payload = json!({
        "device_id": "test-sensor",
        "device_class": "test-class",
        "fw_version": "1.0.0",
        "time_ms": 1_700_000_000_000i64,
        "time_iso": "not-a-date-time",
        "time_valid": true,
        "room": "test-room",
        "data": {
            "temp_c": 22.5,
            "rel_hum_perc": 45.0,
            "pressure_hpa": 1013.25,
            "gas_ohm": 50_000.0,
            "altitude_m": 500.0
        }
    });

    assert!(sensor.validate_raw(&payload).is_err());
    Ok(())
}
