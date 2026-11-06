use std::process::Command;

use anyhow::{Context, Result};
use smarthome_ingest::{config::routes, infrastructure::router::RouteSet};

#[path = "common/broker.rs"]
mod broker;

#[test]
fn test_synthetic_routes_preserve_scalar_oracle_and_distinct_point_identities() -> Result<()> {
    let dir = tempfile::tempdir()?;
    broker::write_routes(dir.path())?;
    let routes = RouteSet::compile(
        routes::load(dir.path().join("routes.json"))?,
        "ingest-it/dlq",
    )?;
    for (topic, payload) in [
        broker::sensor_payload("fixture-sensor", 1)?,
        broker::status_payload("fixture-status", 2)?,
        broker::zigbee_payload("fixture-zigbee", 3)?,
    ] {
        let payload: serde_json::Value = serde_json::from_slice(&payload)?;
        routes
            .select(&topic)
            .context("fixture route")?
            .validate_raw(&payload)?;
        assert!(payload["test_id"].is_string());
    }
    let bundle: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("routes.json"))?)?;
    for route in bundle["routes"].as_array().context("fixture routes")? {
        assert_eq!(route["fields"]["test_id"]["type"], "string");
        assert!(route["tags"].get("test_id").is_none());
    }
    let (_, first) = broker::sensor_payload("fixture-sensor", 1)?;
    let (_, second) = broker::sensor_payload("fixture-sensor", 2)?;
    let first: serde_json::Value = serde_json::from_slice(&first)?;
    let second: serde_json::Value = serde_json::from_slice(&second)?;
    assert_ne!(first["test_id"], second["test_id"]);
    assert_ne!(first["time_ms"], second["time_ms"]);
    Ok(())
}
mod common;

#[test]
#[ignore = "requires the Docker Compose CLI"]
fn test_compose_stack_is_isolated_and_uses_deployment_images() -> Result<()> {
    let mut command = Command::new("docker");
    command
        .env("INGEST_IT_MQTT_PORT", "0")
        .env("INGEST_IT_EMQX_API_PORT", "0")
        .env("INGEST_IT_INFLUX_PORT", "0")
        .args([
            "compose",
            "-f",
            "tests/compose.integration.yaml",
            "-p",
            "ingest-it-render-test",
            "config",
            "--format",
            "json",
        ]);
    let output = broker::run_command(command)?;
    let stack: serde_json::Value =
        serde_json::from_str(&output).context("parse integration Compose")?;
    assert_eq!(stack["services"]["emqx"]["image"], "emqx/emqx:6.3.1");
    assert_eq!(stack["services"]["influxdb"]["image"], "influxdb:2.7");
    for service in ["emqx", "influxdb"] {
        assert!(stack["services"][service].get("container_name").is_none());
        for port in stack["services"][service]["ports"]
            .as_array()
            .context("published ports")?
        {
            assert_eq!(port["host_ip"], "127.0.0.1");
            assert_eq!(port["published"], "0");
        }
    }
    for volume in stack["volumes"]
        .as_object()
        .context("named volumes")?
        .values()
    {
        assert_eq!(
            volume["name"]
                .as_str()
                .context("volume name")?
                .split('_')
                .next(),
            Some("ingest-it-render-test")
        );
        assert!(volume.get("external").is_none());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_firmware_sensor_happy_path_observes_subscriber_ack_and_written_fields() -> Result<()>
{
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.start(None)?;
        harness.wait_ready().await?;
        let device = harness.device_id("sensor-baseline");
        let test_id = format!("{device}-1");
        let (topic, payload) = broker::sensor_payload(&device, 1)?;
        harness.publish(&topic, payload).await?;
        harness.wait_subscriber_ack(1).await?;
        let fields = harness.query_fields(&test_id).await?;
        // This synthetic baseline has no CSV-escaped commas; values match the #42 default golden.
        let headers: Vec<_> = fields
            .lines()
            .find(|line| line.starts_with(",result,table,"))
            .context("query CSV header")?
            .split(',')
            .collect();
        let types: Vec<_> = fields
            .lines()
            .find(|line| line.starts_with("#datatype,"))
            .context("query CSV types")?
            .split(',')
            .collect();
        let row: Vec<_> = fields
            .lines()
            .find(|line| !line.starts_with('#') && line.contains(&test_id))
            .context("queried point")?
            .split(',')
            .collect();
        for (name, value) in [
            ("temp_c", 22.5),
            ("rel_hum_perc", 45.0),
            ("pressure_hpa", 1013.25),
            ("gas_ohm", 50_000.0),
            ("altitude_m", 500.0),
            ("dew_point_c", 9.964857762639978),
            ("heat_index_c", 21.980555555555554),
            ("iaq_score", 97.91666666666666),
        ] {
            let index = headers
                .iter()
                .position(|header| *header == name)
                .context("expected numeric field")?;
            assert_eq!(types[index], "double", "{name} field type");
            assert_eq!(row[index].parse::<f64>()?, value, "{name} field value");
        }
        for (name, value, datatype) in [
            ("_measurement", "bme680", "string"),
            ("device_id", device.as_str(), "string"),
            ("room", "living_room", "string"),
            ("device_class", "esp32p4-bme680", "string"),
            ("firmware_version", "1.0.0", "string"),
            ("iaq_text", "Air quality is Good", "string"),
            ("mqtt_retained", "false", "boolean"),
            ("time_valid", "true", "boolean"),
            ("test_id", test_id.as_str(), "string"),
            ("_time", "2023-11-14T22:13:20.001Z", "dateTime:RFC3339"),
        ] {
            let index = headers
                .iter()
                .position(|header| *header == name)
                .context("expected scalar field")?;
            assert_eq!(types[index], datatype, "{name} field type");
            assert_eq!(row[index], value, "{name} field value");
        }
        harness.wait_committed(1).await?;
        harness.stop()?;
        assert!(harness.dir.join("wal").join("commit.cursor").is_file());
        assert!(harness.journal_evidence()?.contains("wal/commit.cursor:"));
        Ok(())
    }
    .await;
    harness.finish(result)
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_process_driver_barriers_hold_then_release_delivery() -> Result<()> {
    for barrier in ["before_append", "after_sync", "before_sink"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            harness.start(Some(barrier))?;
            harness.wait_ready().await?;
            let device = harness.device_id("barrier");
            let (topic, payload) = broker::sensor_payload(&device, 1)?;
            harness.publish(&topic, payload).await?;
            harness.wait_barrier(barrier).await?;
            if barrier == "before_sink" {
                harness.wait_subscriber_ack(1).await?;
            } else {
                let trace = std::fs::read_to_string(harness.dir.join("source.trace"))?;
                assert!(!trace.lines().any(|line| line.starts_with("puback ")));
            }
            harness.release(barrier)?;
            harness.wait_subscriber_ack(1).await?;
            harness.query_fields(&format!("{device}-1")).await?;
            Ok(())
        }
        .await;
        harness.finish(result)?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_driver_status_zigbee_and_broker_restart_preserve_session() -> Result<()> {
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.start(None)?;
        harness.wait_ready().await?;
        let status = harness.device_id("status");
        let zigbee = harness.device_id("zigbee");
        let (topic, payload) = broker::status_payload(&status, 1)?;
        harness.publish(&topic, payload).await?;
        harness.wait_subscriber_ack(1).await?;
        let (topic, payload) = broker::zigbee_payload(&zigbee, 2)?;
        harness.publish(&topic, payload).await?;
        harness.wait_subscriber_ack(2).await?;
        harness.query_fields(&format!("{status}-1")).await?;
        harness.query_fields(&format!("{zigbee}-2")).await?;
        let trace_offset = std::fs::metadata(harness.dir.join("source.trace"))?.len() as usize;
        harness.stop()?;
        harness.restart_broker()?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let trace = std::fs::read_to_string(harness.dir.join("source.trace"))?;
        assert!(trace
            .lines()
            .any(|line| line.starts_with("connack ") && line.ends_with(" false")));
        let resumed = trace.get(trace_offset..).context("post-restart trace")?;
        let connack = resumed
            .lines()
            .find(|line| line.starts_with("connack "))
            .context("post-restart CONNACK")?;
        assert!(
            connack.ends_with(" true"),
            "broker lost the durable session: {connack}"
        );
        Ok(())
    }
    .await;
    harness.finish(result)
}
