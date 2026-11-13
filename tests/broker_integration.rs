use std::collections::BTreeSet;
use std::process::Command;

use anyhow::{ensure, Context, Result};
use smarthome_ingest::{config::routes, infrastructure::router::RouteSet};

#[path = "common/broker.rs"]
mod broker;

fn assert_field(csv: &str, name: &str, value: &str, datatype: &str) -> Result<()> {
    let headers: Vec<_> = csv
        .lines()
        .find(|line| line.starts_with(",result,table,"))
        .context("CSV header")?
        .split(',')
        .collect();
    let types: Vec<_> = csv
        .lines()
        .find(|line| line.starts_with("#datatype,"))
        .context("CSV types")?
        .split(',')
        .collect();
    let row: Vec<_> = csv
        .lines()
        .find(|line| line.starts_with(",,") || line.starts_with(",_result,"))
        .context("CSV point")?
        .split(',')
        .collect();
    let index = headers
        .iter()
        .position(|header| *header == name)
        .context("expected field")?;
    ensure!(
        types[index] == datatype,
        "{name} type: {} != {datatype}",
        types[index]
    );
    ensure!(
        row[index] == value,
        "{name} value: {} != {value}",
        row[index]
    );
    Ok(())
}

#[test]
fn test_field_oracle_mismatch_returns_error_without_panicking() {
    let csv = "#datatype,string,long,double\n,result,table,temp_c\n,,0,22.5";
    assert!(assert_field(csv, "temp_c", "24", "double").is_err());
    assert!(assert_field(csv, "temp_c", "22.5", "long").is_err());
}

#[test]
fn test_publication_oracle_rejects_omissions_and_unexpected_ids() -> Result<()> {
    let expected = BTreeSet::from(["run-1".to_owned(), "run-2".to_owned()]);
    assert!(broker::assert_publication_ids(&expected, &expected, &BTreeSet::new()).is_ok());
    assert!(broker::assert_publication_ids(
        &expected,
        &BTreeSet::from(["run-1".to_owned()]),
        &BTreeSet::new()
    )
    .is_err());
    assert!(broker::assert_publication_ids(
        &expected,
        &expected,
        &BTreeSet::from(["unexpected-3".to_owned()])
    )
    .is_err());
    assert!(broker::assert_publication_ids(
        &expected,
        &BTreeSet::from(["run-1".to_owned()]),
        &BTreeSet::from(["run-2".to_owned()])
    )
    .is_ok());
    Ok(())
}

#[test]
fn test_synthetic_routes_use_the_scenario_bucket() -> Result<()> {
    let directory = tempfile::tempdir()?;
    broker::write_routes(directory.path())?;
    for bucket in ["scenario-a", "scenario-b"] {
        let routes = RouteSet::compile(
            routes::load(directory.path().join("routes.json"))?,
            "ingest-it/dlq",
            bucket,
        )?;
        for filter in routes.subscriptions() {
            let topic = filter.replace('+', "fixture-device");
            let route = routes.select(&topic).context("synthetic route")?;
            assert_eq!(route.sink_target(), bucket, "route {}", route.id());
        }
    }
    Ok(())
}

#[test]
fn test_synthetic_routes_preserve_scalar_oracle_and_distinct_point_identities() -> Result<()> {
    let dir = tempfile::tempdir()?;
    broker::write_routes(dir.path())?;
    let routes = RouteSet::compile(
        routes::load(dir.path().join("routes.json"))?,
        "ingest-it/dlq",
        "test_bucket",
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
        let stream = harness.stream().await?;
        harness.publish(&topic, payload).await?;
        let event = broker::Harness::sensor_event(stream).await?;
        assert_eq!(event["device_id"], device);
        assert_eq!(event["kind"], "sensor");
        assert!(event["last_seen_ms"].as_i64().is_some());
        for (name, value) in [
            ("temp_c", 22.5),
            ("rel_hum_perc", 45.0),
            ("iaq_score", 97.91666666666666),
        ] {
            assert_eq!(event["value"][name].as_f64(), Some(value));
        }
        assert_eq!(event["value"]["iaq_text"], "Air quality is Good");
        for (name, expected) in [
            ("absolute_humidity_g_m3", 8.98653),
            ("humidity_ratio_g_kg", 7.61830),
            ("surface_temp_at_80rh_c", 13.36327),
            ("dew_point_depression_c", 12.53514),
        ] {
            let value = event["value"][name].as_f64().context("SSE climate field")?;
            assert!((value - expected).abs() < 0.00001, "{name}={value}");
        }
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
        for (name, expected) in [
            ("absolute_humidity_g_m3", 8.98653),
            ("humidity_ratio_g_kg", 7.61830),
            ("surface_temp_at_80rh_c", 13.36327),
            ("dew_point_depression_c", 12.53514),
        ] {
            let index = headers
                .iter()
                .position(|header| *header == name)
                .context("persisted climate field")?;
            assert_eq!(types[index], "double", "{name} must be a float field");
            let value = row[index].parse::<f64>()?;
            assert!((value - expected).abs() < 0.00001, "{name}={value}");
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
        broker::assert_publication_ids(
            &BTreeSet::from([test_id]),
            &harness.output_ids(&device).await?,
            &BTreeSet::new(),
        )?;
        harness.stop()?;
        assert!(harness.dir.join("wal").join("commit.cursor").is_file());
        assert!(harness.journal_evidence()?.contains("wal/commit.cursor:"));
        Ok(())
    }
    .await;
    harness.finish(result).await
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
            broker::assert_publication_ids(
                &BTreeSet::from([format!("{device}-1")]),
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            Ok(())
        }
        .await;
        harness.finish(result).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_crash_windows_preserve_publication_ids_with_expected_replay() -> Result<()> {
    for barrier in ["before_append", "after_sync", "before_sink"] {
        for repetition in 1..=3 {
            let mut harness = broker::Harness::connect().await?;
            let result = async {
                harness.start(Some(barrier))?;
                harness.wait_ready().await?;
                let device = harness.device_id("crash");
                let id = format!("{device}-{repetition}");
                let expected = BTreeSet::from([id.clone()]);
                let (topic, payload) = broker::sensor_payload(&device, repetition)?;
                harness.publish(&topic, payload).await?;
                harness.wait_barrier(barrier).await?;
                if barrier == "before_sink" {
                    harness.wait_subscriber_ack(1).await?;
                } else {
                    harness.wait_inflight(1).await?;
                    assert!(!harness
                        .trace()?
                        .lines()
                        .any(|line| line.starts_with("puback ")));
                }
                if barrier != "after_sync" {
                    assert!(harness.output_ids(&device).await?.is_empty());
                    assert!(!harness.dir.join("wal").join("commit.cursor").exists());
                }
                harness.stop()?;
                let records = harness.journal_records("wal")?;
                if barrier == "before_append" {
                    assert!(
                        records.is_empty(),
                        "pre-append crash must require broker replay"
                    );
                } else {
                    assert_eq!(records.len(), 1);
                    assert!(records[0].payload.contains(&id));
                }
                let trace_offset = harness.trace()?.len();
                harness.start(None)?;
                harness.wait_ready().await?;
                harness.query_fields(&id).await?;
                harness.wait_committed(1).await?;
                let trace = harness.trace()?;
                let resumed = trace.get(trace_offset..).context("resumed source trace")?;
                assert!(resumed
                    .lines()
                    .any(|line| line.starts_with("connack ") && line.ends_with(" true")));
                if barrier == "before_sink" {
                    assert!(
                        !resumed.lines().any(|line| line.starts_with("publish ")),
                        "ACKed input must recover from WAL alone"
                    );
                } else {
                    harness.wait_subscriber_ack(1).await?;
                    assert!(
                        resumed.lines().any(|line| line.starts_with("publish ")),
                        "unacknowledged input must be redelivered"
                    );
                }
                broker::assert_publication_ids(
                    &expected,
                    &harness.output_ids(&device).await?,
                    &BTreeSet::new(),
                )?;
                harness.stop()?;
                assert!(harness.journal_records("quarantine")?.is_empty());
                Ok(())
            }
            .await;
            harness
                .finish(result)
                .await
                .with_context(|| format!("{barrier}, repetition {repetition}"))?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_arbitrary_topic_contract_changes_without_rebuilding_binary() -> Result<()> {
    use serde_json::{json, Value};
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        let device = harness.device_id("configured");
        let topic = format!("laboratory/{device}/telemetry");
        let path = harness.dir.join("routes.json");
        let mut bundle: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        bundle["routes"][0]["topic"] = json!(topic);
        bundle["routes"][0]["measurement"] = json!("contract_a");
        bundle["routes"][0]
            .as_object_mut()
            .context("route")?
            .remove("output_schema");
        std::fs::write(&path, serde_json::to_vec(&bundle)?)?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let (_, payload) = broker::sensor_payload(&device, 1)?;
        harness.publish(&topic, payload).await?;
        harness.wait_subscriber_ack(1).await?;
        let fields = harness.query_fields(&format!("{device}-1")).await?;
        assert_field(&fields, "_measurement", "contract_a", "string")?;
        assert_field(&fields, "temp_c", "22.5", "double")?;
        harness.stop()?;

        bundle["routes"][0]["measurement"] = json!("contract_b");
        bundle["routes"][0]["fields"]["temp_c"]["pointer"] = json!("/data/rel_hum_perc");
        bundle["routes"][0]["calculations"] = json!(["dew_point"]);
        bundle["routes"][0]["output_schema"] = json!("contract.output.json");
        std::fs::write(
            harness.dir.join("contract.output.json"),
            serde_json::to_vec(&json!({
                "$schema":"http://json-schema.org/draft-07/schema#",
                "type":"object", "properties":{"measurement":{"const":"contract_b"}},
                "required":["measurement"]
            }))?,
        )?;
        let schema_path = harness.dir.join("schemas").join("sensor.schema.json");
        let mut schema: Value = serde_json::from_slice(&std::fs::read(&schema_path)?)?;
        schema["allOf"][1]["properties"]["data"]["properties"]["temp_c"]["minimum"] = json!(30);
        std::fs::write(&schema_path, serde_json::to_vec(&schema)?)?;
        std::fs::write(&path, serde_json::to_vec(&bundle)?)?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let (_, invalid) = broker::sensor_payload(&device, 2)?;
        harness.publish(&topic, invalid.clone()).await?;
        harness.wait_subscriber_ack(2).await?;
        let (_, payload) = broker::sensor_payload(&device, 3)?;
        let mut payload: Value = serde_json::from_slice(&payload)?;
        payload["data"]["temp_c"] = json!(35);
        harness
            .publish(&topic, serde_json::to_vec(&payload)?)
            .await?;
        harness.wait_subscriber_ack(3).await?;
        let fields = harness.query_fields(&format!("{device}-3")).await?;
        assert_field(&fields, "_measurement", "contract_b", "string")?;
        assert_field(&fields, "temp_c", "45", "double")?;
        assert!(!fields.lines().any(|line| line.contains("iaq_score")));
        let output = harness.output_ids(&device).await?;
        assert!(!output.contains(&format!("{device}-2")));
        harness.stop()?;
        let records = harness.journal_records("quarantine")?;
        assert_eq!(records.len(), 1);
        let envelope: Value = serde_json::from_str(&records[0].payload)?;
        assert_eq!(envelope["raw_payload"], json!(invalid));
        assert!(envelope["reason"]
            .as_str()
            .context("rejection reason")?
            .contains("raw validation"));
        broker::assert_publication_ids(
            &(1..=3).map(|i| format!("{device}-{i}")).collect(),
            &output,
            &BTreeSet::from([format!("{device}-2")]),
        )?;
        Ok(())
    }
    .await;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_reconnect_discards_stale_receipts_and_locally_queued_acknowledgements() -> Result<()>
{
    for barrier in ["after_sync", "ack_queued"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            harness.start(Some(barrier))?;
            harness.wait_ready().await?;
            let device = harness.device_id("stale");
            let (topic, payload) = broker::sensor_payload(&device, 1)?;
            harness.publish(&topic, payload).await?;
            harness.wait_barrier(barrier).await?;
            harness.wait_inflight(1).await?;
            let first = harness
                .trace()?
                .lines()
                .find(|line| line.starts_with("publish "))
                .context("first packet")?
                .to_owned();
            std::fs::write(harness.dir.join("before_append.arm"), [])?;
            harness.disconnect().await?;
            if barrier == "ack_queued" {
                harness.release(barrier)?;
            }
            harness.wait_barrier("before_append").await?;
            let trace = harness.trace()?;
            let last = trace
                .lines()
                .rev()
                .find(|line| line.starts_with("publish "))
                .context("reused packet")?;
            let first: Vec<_> = first.split_whitespace().collect();
            let last: Vec<_> = last.split_whitespace().collect();
            assert_ne!(first[1], last[1], "connection generation must change");
            assert_eq!(first[2], last[2], "exercise actual broker packet-ID reuse");
            if barrier == "after_sync" {
                harness.release(barrier)?;
                harness.wait_trace("receipt_stale ").await?;
            }
            harness.wait_inflight(1).await?;
            let new_ack = format!("puback {} {}", last[1], last[2]);
            assert!(
                !harness.trace()?.lines().any(|line| line == new_ack),
                "a stale receipt/request must not ACK the new unsynced delivery"
            );
            harness.release("before_append")?;
            harness.wait_subscriber_ack(1).await?;
            harness.query_fields(&format!("{device}-1")).await?;
            broker::assert_publication_ids(
                &BTreeSet::from([format!("{device}-1")]),
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            Ok(())
        }
        .await;
        harness
            .finish(result)
            .await
            .with_context(|| format!("stale {barrier}"))?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_tiny_input_and_writer_queues_recover_every_publication_id() -> Result<()> {
    for barrier in ["before_append", "wal_write"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            harness.start_with_env(
                Some(barrier),
                &[
                    (
                        "INPUT_QUEUE_CAPACITY",
                        if barrier == "before_append" {
                            "1"
                        } else {
                            "16"
                        },
                    ),
                    ("WAL_QUEUE_CAPACITY", "1"),
                ],
            )?;
            harness.wait_ready().await?;
            let device = harness.device_id("queues");
            let expected: BTreeSet<_> = (1..=12).map(|i| format!("{device}-{i}")).collect();
            for sequence in 1..=12 {
                let (topic, raw) = broker::sensor_payload(&device, sequence)?;
                harness.publish(&topic, raw).await?;
                if sequence == 1 {
                    harness.wait_barrier(barrier).await?;
                }
            }
            harness
                .wait_metric(
                    if barrier == "before_append" {
                        "ingest_admission_pressure"
                    } else {
                        "ingest_queue_full_total{route=\"firmware-sensor\"}"
                    },
                    1.0,
                )
                .await?;
            harness.release(barrier)?;
            harness.wait_ready().await?;
            for id in &expected {
                harness.query_fields(id).await?;
            }
            let metrics = harness.metrics().await?;
            if barrier == "before_append" {
                // queue_full counts retained admission attempts, not discarded IDs.
                assert!(metrics.lines().any(|line| line
                    .strip_prefix("ingest_event_queue_full_total ")
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|value| value > 0)));
            }
            harness.wait_subscriber_ack(12).await?;
            broker::assert_publication_ids(
                &expected,
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            harness.stop()?;
            assert!(harness.journal_records("quarantine")?.is_empty());
            Ok(())
        }
        .await;
        harness
            .finish(result)
            .await
            .with_context(|| format!("saturated {barrier}"))?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_invalid_inputs_are_quarantined_before_ack_and_survive_restart() -> Result<()> {
    use serde_json::{json, Value};
    for invalid in ["utf8", "schema", "calculation"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            harness.start(Some("after_quarantine"))?;
            harness.wait_ready().await?;
            let device = harness.device_id(invalid);
            let (topic, mut raw) = broker::sensor_payload(&device, 1)?;
            match invalid {
                "utf8" => raw.push(0xff),
                "schema" => raw = serde_json::to_vec(&json!({"test_id":format!("{device}-1")}))?,
                "calculation" => {
                    let mut value: Value = serde_json::from_slice(&raw)?;
                    value["data"]["rel_hum_perc"] = json!(0);
                    raw = serde_json::to_vec(&value)?;
                }
                _ => unreachable!(),
            }
            harness.publish(&topic, raw.clone()).await?;
            harness.wait_barrier("after_quarantine").await?;
            harness.wait_inflight(1).await?;
            assert!(!harness
                .trace()?
                .lines()
                .any(|line| line.starts_with("puback ")));
            harness.stop()?;
            let records = harness.journal_records("quarantine")?;
            assert_eq!(records.len(), 1);
            let envelope: Value = serde_json::from_str(&records[0].payload)?;
            assert_eq!(envelope["raw_payload"], json!(raw));
            assert_eq!(envelope["qos"], 1);
            assert!(envelope["reason"]
                .as_str()
                .context("quarantine reason")?
                .contains(match invalid {
                    "utf8" => "utf8",
                    "schema" => "raw validation",
                    _ => "telemetry mapping",
                }));
            std::fs::write(harness.dir.join("dlq.fail"), [])?;
            harness.start(None)?;
            harness.wait_ready().await?;
            harness.wait_subscriber_ack(1).await?;
            harness.wait_metric("dlq_publish_errors_total", 1.0).await?;
            assert!(harness.output_ids(&device).await?.is_empty());
            harness.stop()?;
            let records = harness.journal_records("quarantine")?;
            assert!(!records.is_empty());
            let mut rejected = BTreeSet::new();
            for record in records {
                let rejection: Value = serde_json::from_str(&record.payload)?;
                assert_eq!(rejection["raw_payload"], json!(raw));
                let bytes: Vec<u8> = serde_json::from_value(rejection["raw_payload"].clone())?;
                let payload: Value = serde_json::from_slice(if invalid == "utf8" {
                    &bytes[..bytes.len() - 1]
                } else {
                    &bytes
                })?;
                rejected.insert(
                    payload["test_id"]
                        .as_str()
                        .context("actual rejection ID")?
                        .to_owned(),
                );
            }
            broker::assert_publication_ids(
                &BTreeSet::from([format!("{device}-1")]),
                &BTreeSet::new(),
                &rejected,
            )?;
            Ok(())
        }
        .await;
        harness
            .finish(result)
            .await
            .with_context(|| format!("invalid {invalid}"))?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_journal_write_and_sync_failures_leave_input_unacknowledged() -> Result<()> {
    use serde_json::{json, Value};
    for journal in ["wal", "quarantine"] {
        for step in ["write", "sync"] {
            let mut harness = broker::Harness::connect().await?;
            let result = async {
                let barrier = if journal == "wal" {
                    "before_append"
                } else {
                    "before_quarantine"
                };
                harness.start(Some(barrier))?;
                harness.wait_ready().await?;
                let device = harness.device_id("io-failure");
                let (topic, mut raw) = broker::sensor_payload(&device, 1)?;
                if journal == "quarantine" {
                    raw = serde_json::to_vec(&json!({"test_id":format!("{device}-1")}))?;
                }
                harness.publish(&topic, raw.clone()).await?;
                harness.wait_barrier(barrier).await?;
                let fault = harness.dir.join(format!("{journal}_{step}.fail"));
                std::fs::write(&fault, [])?;
                harness.release(barrier)?;
                harness.wait_unready().await?;
                assert!(!harness
                    .trace()?
                    .lines()
                    .any(|line| line.starts_with("puback ")));
                assert!(!harness.dir.join("wal").join("commit.cursor").exists());
                harness.stop()?;
                if journal == "wal" {
                    assert!(
                        harness.journal_records("quarantine")?.is_empty(),
                        "operational I/O failures must not become input rejections"
                    );
                }
                std::fs::remove_file(fault)?;
                harness.start(None)?;
                harness.wait_ready().await?;
                harness.wait_subscriber_ack(1).await?;
                let mut rejected = BTreeSet::new();
                if journal == "wal" {
                    harness.query_fields(&format!("{device}-1")).await?;
                }
                let output = harness.output_ids(&device).await?;
                harness.stop()?;
                for record in harness.journal_records("quarantine")? {
                    let envelope: Value = serde_json::from_str(&record.payload)?;
                    assert_eq!(envelope["raw_payload"], json!(raw));
                    rejected.insert(format!("{device}-1"));
                }
                broker::assert_publication_ids(
                    &BTreeSet::from([format!("{device}-1")]),
                    &output,
                    &rejected,
                )?;
                Ok(())
            }
            .await;
            harness
                .finish(result)
                .await
                .with_context(|| format!("{journal} {step} failure"))?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_output_capacity_reclaims_without_new_input_and_retries_failed_deletion() -> Result<()>
{
    use serde_json::{json, Value};
    use smarthome_ingest::infrastructure::wal::cursor::read_cursor;
    for failed_deletion in [false, true] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            let path = harness.dir.join("routes.json");
            let mut bundle: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
            bundle["routes"][0]
                .as_object_mut()
                .context("route")?
                .remove("output_schema");
            bundle["routes"][0]["measurement"] = json!("synthetic_pressure");
            bundle["routes"][0]["fields"]["padding"] =
                json!({"pointer":"/padding","type":"string"});
            std::fs::write(path, serde_json::to_vec(&bundle)?)?;
            harness.start_with_env(
                Some("before_sink"),
                &[("WAL_MAX_BYTES", "2228224"), ("WAL_SEGMENT_BYTES", "65536")],
            )?;
            harness.wait_ready().await?;
            let device = harness.device_id("capacity");
            let expected: BTreeSet<_> = (1..=24).map(|i| format!("{device}-{i}")).collect();
            for sequence in 1..=24 {
                let (topic, payload) = broker::sensor_payload(&device, sequence)?;
                let mut payload: Value = serde_json::from_slice(&payload)?;
                payload["padding"] = json!("x".repeat(60_000));
                harness
                    .publish(&topic, serde_json::to_vec(&payload)?)
                    .await?;
                if sequence == 1 {
                    harness.wait_barrier("before_sink").await?;
                }
            }
            harness.wait_unready().await?;
            assert!(read_cursor(&harness.dir.join("wal"))?.is_none());
            let fault = harness.dir.join("wal_delete.fail");
            if failed_deletion {
                std::fs::write(&fault, [])?;
            }
            harness.release("before_sink")?;
            if failed_deletion {
                harness
                    .wait_metric("wal_forwarder_commit_retry_total", 1.0)
                    .await?;
                let cursor = read_cursor(&harness.dir.join("wal"))?;
                let metrics = harness.metrics().await?;
                let delivered = metrics
                    .lines()
                    .find(|line| line.starts_with("wal_forwarder_delivered_total "))
                    .context("delivered metric")?
                    .to_owned();
                harness
                    .wait_metric("wal_forwarder_commit_retry_total", 2.0)
                    .await?;
                assert_eq!(read_cursor(&harness.dir.join("wal"))?, cursor);
                assert!(
                    harness
                        .metrics()
                        .await?
                        .lines()
                        .any(|line| line == delivered),
                    "commit retry must not repeat a completed sink write"
                );
                std::fs::remove_file(fault)?;
            }
            harness.wait_ready().await?;
            for id in &expected {
                harness.query_fields(id).await?;
            }
            harness.wait_subscriber_ack(24).await?;
            broker::assert_publication_ids(
                &expected,
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            harness.stop()?;
            assert!(harness.journal_records("quarantine")?.is_empty());
            Ok(())
        }
        .await;
        harness
            .finish(result)
            .await
            .with_context(|| format!("capacity, delete fault {failed_deletion}"))?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_quarantine_capacity_preserves_unfinished_input_until_budget_recovery() -> Result<()> {
    use serde_json::json;
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.start_with_env(
            None,
            &[
                ("QUARANTINE_MAX_BYTES", "2228224"),
                ("WAL_SEGMENT_BYTES", "65536"),
            ],
        )?;
        harness.wait_ready().await?;
        let device = harness.device_id("quarantine-capacity");
        let topic = format!("smarthome/{device}/bme680");
        let expected: BTreeSet<_> = (1..=10).map(|i| format!("{device}-{i}")).collect();
        for sequence in 1..=10 {
            harness
                .publish(
                    &topic,
                    serde_json::to_vec(&json!({
                        "test_id":format!("{device}-{sequence}"), "padding":"x".repeat(60_000)
                    }))?,
                )
                .await?;
        }
        harness.wait_unready().await?;
        harness.stop()?;
        let retained = harness.quarantine_ids()?;
        assert!(!retained.is_empty() && retained.len() < expected.len());
        assert!(retained.is_subset(&expected));
        harness.start_with_env(None, &[("WAL_SEGMENT_BYTES", "65536")])?;
        harness.wait_ready().await?;
        harness.wait_quarantine_ids(&expected).await?;
        harness.wait_subscriber_ack(10).await?;
        let output = harness.output_ids(&device).await?;
        harness.stop()?;
        broker::assert_publication_ids(&expected, &output, &harness.quarantine_ids()?)?;
        assert!(output.is_empty());
        Ok(())
    }
    .await;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_durable_reader_gaps_fail_closed_before_later_records() -> Result<()> {
    use smarthome_ingest::infrastructure::wal::{cursor::read_cursor, segment};
    for failure in ["missing", "unreadable", "corrupt"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            harness.start(Some("before_read"))?;
            harness.wait_ready().await?;
            let device = harness.device_id("reader");
            let expected: BTreeSet<_> = (1..=2).map(|i| format!("{device}-{i}")).collect();
            for sequence in 1..=2 {
                let (topic, raw) = broker::sensor_payload(&device, sequence)?;
                harness.publish(&topic, raw).await?;
            }
            harness.wait_barrier("before_read").await?;
            harness.wait_subscriber_ack(2).await?;
            let dir = harness.dir.join("wal");
            let path = segment::segment_path(&dir, segment::list_segments(&dir)?[0]);
            let saved = std::fs::read(&path)?;
            let cursor = read_cursor(&dir)?;
            match failure {
                "missing" => std::fs::remove_file(&path)?,
                "unreadable" => {
                    std::fs::remove_file(&path)?;
                    std::fs::create_dir(&path)?;
                }
                "corrupt" => {
                    let mut corrupt = saved.clone();
                    corrupt[4..12].fill(0xff);
                    std::fs::write(&path, corrupt)?;
                }
                _ => unreachable!(),
            }
            harness.release("before_read")?;
            harness.wait_unready().await?;
            assert_eq!(read_cursor(&dir)?, cursor);
            assert!(
                harness.output_ids(&device).await?.is_empty(),
                "a later valid record must not hide a durable gap"
            );
            harness.stop()?;
            if failure == "unreadable" {
                std::fs::remove_dir(&path)?;
            }
            std::fs::write(path, saved)?;
            harness.start(None)?;
            harness.wait_ready().await?;
            for id in &expected {
                harness.query_fields(id).await?;
            }
            harness.wait_committed(2).await?;
            broker::assert_publication_ids(
                &expected,
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            Ok(())
        }
        .await;
        harness
            .finish(result)
            .await
            .with_context(|| format!("reader {failure}"))?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_resumed_session_reconciles_routes_and_quarantines_queued_removed_topics() -> Result<()>
{
    use serde_json::{json, Value};
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        let device = harness.device_id("route-change");
        let a = format!("laboratory/{device}/a");
        let b = format!("laboratory/{device}/b");
        let path = harness.dir.join("routes.json");
        let mut bundle: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        let mut route = bundle["routes"][0].clone();
        route["topic"] = json!(a);
        bundle["routes"] = json!([route]);
        std::fs::write(&path, serde_json::to_vec(&bundle)?)?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let (_, raw) = broker::sensor_payload(&device, 1)?;
        harness.publish(&a, raw).await?;
        harness.wait_subscriber_ack(1).await?;
        harness.query_fields(&format!("{device}-1")).await?;
        harness.stop()?;
        let (_, raw) = broker::sensor_payload(&device, 2)?;
        harness.publish(&a, raw).await?;
        bundle["routes"][0]["topic"] = json!(b);
        std::fs::write(path, serde_json::to_vec(&bundle)?)?;
        harness.start(Some("before_quarantine"))?;
        harness.wait_ready().await?;
        harness.wait_barrier("before_quarantine").await?;
        harness.wait_inflight(1).await?;
        harness.release("before_quarantine")?;
        harness
            .wait_quarantine_ids(&BTreeSet::from([format!("{device}-2")]))
            .await?;
        harness.wait_subscriber_ack(2).await?;
        let subscriptions = harness
            .api(&format!("/clients/{}/subscriptions", harness.client_id()))
            .await?;
        let subscriptions: Value = serde_json::from_str(&subscriptions)?;
        assert!(!subscriptions
            .as_array()
            .context("broker subscriptions")?
            .iter()
            .any(|subscription| subscription["topic"] == a));
        assert!(subscriptions
            .as_array()
            .context("broker subscriptions")?
            .iter()
            .any(|subscription| subscription["topic"] == b));
        // This post-UNSUBACK publication is outside the subscriber's intake oracle.
        let (_, raw) = broker::sensor_payload(&device, 3)?;
        harness.publish(&a, raw).await?;
        let (_, raw) = broker::sensor_payload(&device, 4)?;
        harness.publish(&b, raw).await?;
        harness.query_fields(&format!("{device}-4")).await?;
        harness.wait_subscriber_ack(3).await?;
        let output = harness.output_ids(&device).await?;
        harness.stop()?;
        let quarantine = harness.quarantine_ids()?;
        assert_eq!(quarantine, BTreeSet::from([format!("{device}-2")]));
        broker::assert_publication_ids(
            &BTreeSet::from([
                format!("{device}-1"),
                format!("{device}-2"),
                format!("{device}-4"),
            ]),
            &output,
            &quarantine,
        )?;
        Ok(())
    }
    .await;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_broker_restart_preserves_queued_and_unacknowledged_qos1_ids() -> Result<()> {
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.start(Some("after_sync"))?;
        harness.wait_ready().await?;
        let device = harness.device_id("broker-restart");
        for sequence in 1..=2 {
            let (topic, raw) = broker::sensor_payload(&device, sequence)?;
            harness.publish(&topic, raw).await?;
        }
        harness.wait_barrier("after_sync").await?;
        harness.wait_inflight(2).await?;
        harness.stop()?;
        let (topic, raw) = broker::sensor_payload(&device, 3)?;
        harness.publish(&topic, raw).await?;
        let offset = harness.trace()?.len();
        harness.restart_broker()?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let expected: BTreeSet<_> = (1..=3).map(|i| format!("{device}-{i}")).collect();
        for id in &expected {
            harness.query_fields(id).await?;
        }
        harness.wait_subscriber_ack(3).await?;
        let trace = harness.trace()?;
        assert!(trace[offset..]
            .lines()
            .any(|line| line.starts_with("connack ") && line.ends_with(" true")));
        broker::assert_publication_ids(
            &expected,
            &harness.output_ids(&device).await?,
            &BTreeSet::new(),
        )?;
        Ok(())
    }
    .await;
    harness.finish(result).await
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
        let fields = harness.query_fields(&format!("{status}-1")).await?;
        for (name, value, datatype) in [
            ("_measurement", "device_status", "string"),
            ("rssi", "-65", "long"),
            ("uptime", "3600", "long"),
            ("free_mem", "200000", "long"),
            ("ssid", "HomeNet", "string"),
            ("firmware_version", "1.0.0", "string"),
            ("device_class", "esp32p4-bme680", "string"),
            ("ip_address", "192.168.1.42", "string"),
            ("time_valid", "true", "boolean"),
            ("mqtt_retained", "false", "boolean"),
            ("_time", "2023-11-14T22:13:20.001Z", "dateTime:RFC3339"),
        ] {
            assert_field(&fields, name, value, datatype)?;
        }
        let fields = harness.query_fields(&format!("{zigbee}-2")).await?;
        for (name, value, datatype) in [
            ("_measurement", "zigbee_climate", "string"),
            ("temp_c", "22.5", "double"),
            ("rel_hum_perc", "45", "double"),
            ("dew_point_c", "9.964857762639978", "double"),
            ("mqtt_retained", "false", "boolean"),
            ("_time", "2023-11-14T22:13:20.002Z", "dateTime:RFC3339"),
        ] {
            assert_field(&fields, name, value, datatype)?;
        }
        for (device, sequence) in [(&status, 1), (&zigbee, 2)] {
            broker::assert_publication_ids(
                &BTreeSet::from([format!("{device}-{sequence}")]),
                &harness.output_ids(device).await?,
                &BTreeSet::new(),
            )?;
        }
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
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_sink_outage_auth_and_missing_bucket_retain_output_until_repair() -> Result<()> {
    for failure in ["outage", "auth", "missing-bucket"] {
        let mut harness = broker::Harness::connect().await?;
        let result = async {
            if failure == "missing-bucket" {
                harness.use_owned_bucket("missing");
            }
            let overrides = if failure == "auth" {
                vec![("INFLUX_TOKEN", "synthetic-invalid-token")]
            } else {
                vec![]
            };
            harness.start_with_env(None, &overrides)?;
            harness.wait_ready().await?;
            if failure == "outage" {
                harness.stop_influx()?;
            }
            let device = harness.device_id(failure);
            let (topic, raw) = broker::sensor_payload(&device, 1)?;
            harness.publish(&topic, raw).await?;
            harness.wait_subscriber_ack(1).await?;
            harness
                .wait_metric("wal_forwarder_retry_total", 1.0)
                .await?;
            ensure!(
                !harness.dir.join("wal").join("commit.cursor").exists(),
                "failed sink advanced cursor"
            );
            let retained = harness.journal_records("wal")?;
            ensure!(retained.len() == 1, "failed sink did not retain one record");
            ensure!(
                retained[0].payload.contains(&format!("{device}-1")),
                "retained payload lost test ID"
            );
            harness.stop()?;
            if failure == "missing-bucket" {
                harness.create_bucket().await?;
            }
            // The outage explicitly includes ingest recovery while Influx is still down.
            harness.start(None)?;
            harness.wait_ready().await?;
            if failure == "outage" {
                harness
                    .wait_metric("wal_forwarder_retry_total", 1.0)
                    .await?;
                harness.restore_influx()?;
            }
            harness.query_fields(&format!("{device}-1")).await?;
            harness.wait_committed(1).await?;
            broker::assert_publication_ids(
                &BTreeSet::from([format!("{device}-1")]),
                &harness.output_ids(&device).await?,
                &BTreeSet::new(),
            )?;
            Ok(())
        }
        .await;
        // Restore the shared disposable DB even if an assertion failed.
        if failure == "outage" {
            harness.restore_influx()?;
        }
        harness
            .finish(result)
            .await
            .with_context(|| failure.to_owned())?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_partial_influx_acceptance_keeps_whole_mixed_batch_replayable() -> Result<()> {
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.use_owned_bucket("poison");
        harness.create_bucket().await?;
        let sensor = harness.device_id("mixed-sensor");
        let zigbee = harness.device_id("mixed-zigbee");
        harness
            .write_line(&format!(
                "zigbee_climate,device_id={zigbee} temp_c=\"historically-string\" 1700000000000000000"
            ))
            .await?;
        harness.start_with_env(Some("before_sink"), &[("BATCH_SIZE", "2"), ("FLUSH_INTERVAL_MS", "1000")])?;
        harness.wait_ready().await?;
        let (topic, raw) = broker::sensor_payload(&sensor, 1)?;
        harness.publish(&topic, raw).await?;
        let (topic, raw) = broker::zigbee_payload(&zigbee, 2)?;
        harness.publish(&topic, raw).await?;
        harness.wait_subscriber_ack(2).await?;
        harness.wait_barrier("before_sink").await?;
        let retained = harness.journal_records("wal")?;
        assert_eq!(retained.len(), 2);
        harness.release("before_sink")?;
        harness.wait_metric("wal_forwarder_retry_total", 1.0).await?;
        // Influx's 422 accepts the good point, but the WAL must retain both.
        harness.query_fields(&format!("{sensor}-1")).await?;
        assert_eq!(harness.output_ids(&zigbee).await?, BTreeSet::new());
        assert!(!harness.dir.join("wal").join("commit.cursor").exists());
        assert_eq!(
            harness.journal_records("wal")?.iter().map(|e| &e.payload).collect::<Vec<_>>(),
            retained.iter().map(|e| &e.payload).collect::<Vec<_>>()
        );
        harness.stop()?;
        // Changing the fallback cannot retarget records already persisted in the WAL.
        harness.use_owned_bucket("new-default");
        harness.create_bucket().await?;
        harness.start_with_env(None, &[("BATCH_SIZE", "2"), ("FLUSH_INTERVAL_MS", "1000")])?;
        harness.wait_ready().await?;
        harness.wait_metric("wal_forwarder_retry_total", 1.0).await?;
        assert_eq!(harness.output_ids(&sensor).await?, BTreeSet::new());
        assert_eq!(harness.output_ids(&zigbee).await?, BTreeSet::new());
        assert!(!harness.dir.join("wal").join("commit.cursor").exists());
        assert_eq!(
            harness.journal_records("wal")?.iter().map(|e| (&e.sink_target, &e.payload)).collect::<Vec<_>>(),
            retained.iter().map(|e| (&e.sink_target, &e.payload)).collect::<Vec<_>>()
        );
        harness.use_owned_bucket("poison");
        harness.query_fields(&format!("{sensor}-1")).await?;
        assert_eq!(harness.output_ids(&zigbee).await?, BTreeSet::new());
        Ok(())
    }.await;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_qos0_rejection_and_retained_snapshot_have_explicit_outcomes() -> Result<()> {
    let mut harness = broker::Harness::connect().await?;
    let device = harness.device_id("retained");
    let (topic, raw) = broker::sensor_payload(&device, 1)?;
    let result = async {
        harness
            .publish_with(&topic, raw, rumqttc::QoS::AtLeastOnce, true)
            .await?;
        harness.start(None)?;
        harness.wait_ready().await?;
        harness.wait_subscriber_ack(1).await?;
        let fields = harness.query_fields(&format!("{device}-1")).await?;
        assert_field(&fields, "mqtt_retained", "true", "boolean")?;
        let (qos0_topic, raw) = broker::sensor_payload(&device, 2)?;
        harness
            .publish_with(&qos0_topic, raw, rumqttc::QoS::AtMostOnce, false)
            .await?;
        harness
            .wait_quarantine_ids(&BTreeSet::from([format!("{device}-2")]))
            .await?;
        harness.stop()?;
        let records = harness.journal_records("quarantine")?;
        ensure!(
            records
                .first()
                .context("QoS 0 quarantine record")?
                .payload
                .contains("QoS 0 is not recoverable"),
            "missing QoS 0 policy classification"
        );
        broker::assert_publication_ids(
            &BTreeSet::from([format!("{device}-1"), format!("{device}-2")]),
            &harness.output_ids(&device).await?,
            &harness.quarantine_ids()?,
        )?;
        // QoS 0 is observed and quarantined here, not asserted replayable after a crash.
        Ok(())
    }
    .await;
    harness.stop()?;
    harness
        .publish_with(&topic, Vec::new(), rumqttc::QoS::AtLeastOnce, true)
        .await?;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated EMQX/InfluxDB Compose stack"]
async fn test_colliding_influx_point_identity_merges_fields_and_overwrites_test_id() -> Result<()> {
    use serde_json::{json, Value};
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        let path = harness.dir.join("routes.json");
        let mut bundle: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        bundle["routes"][0]["fields"]["first_only"] =
            json!({"pointer":"/first_only","type":"string","optional":true});
        std::fs::write(path, serde_json::to_vec(&bundle)?)?;
        harness.start(None)?;
        harness.wait_ready().await?;
        let device = harness.device_id("collision");
        let (topic, raw) = broker::sensor_payload(&device, 1)?;
        let mut first: Value = serde_json::from_slice(&raw)?;
        first["first_only"] = json!("merged");
        harness.publish(&topic, serde_json::to_vec(&first)?).await?;
        harness.wait_committed(1).await?;
        first
            .as_object_mut()
            .context("payload")?
            .remove("first_only");
        first["test_id"] = json!(format!("{device}-2"));
        first["data"]["temp_c"] = json!(24.0);
        harness.publish(&topic, serde_json::to_vec(&first)?).await?;
        harness.wait_committed(2).await?;
        harness.wait_subscriber_ack(2).await?;
        let fields = harness.query_fields(&format!("{device}-2")).await?;
        assert_field(&fields, "first_only", "merged", "string")?;
        assert_field(&fields, "temp_c", "24", "double")?;
        assert_eq!(
            harness.output_ids(&device).await?,
            BTreeSet::from([format!("{device}-2")])
        );
        // Unlike the recovery fixtures, both publications deliberately share a point identity.
        Ok(())
    }
    .await;
    harness.finish(result).await
}

#[tokio::test]
#[ignore = "requires the isolated stack and debug integration-test-hooks build"]
async fn test_shutdown_with_full_dlq_requests_and_waiting_acks_is_bounded_and_replayable(
) -> Result<()> {
    use serde_json::Value;
    let mut harness = broker::Harness::connect().await?;
    let result = async {
        harness.start(None)?;
        harness.wait_ready().await?;
        harness.stop()?;
        let device = harness.device_id("shutdown-pressure");
        let expected: BTreeSet<_> = (1..=5).map(|i| format!("{device}-{i}")).collect();
        for sequence in 1..=5 {
            let (topic, raw) = broker::sensor_payload(&device, sequence)?;
            let mut payload: Value = serde_json::from_slice(&raw)?;
            payload["data"]["temp_c"] = serde_json::json!("invalid");
            harness
                .publish(&topic, serde_json::to_vec(&payload)?)
                .await?;
        }
        // Queue the complete burst before consuming it, rather than assuming
        // live durable-stream prefetch will advance while all PUBACKs are held.
        harness.start_with_env(
            Some("before_quarantine,after_dispatch,ack_queued"),
            &[
                ("INGEST_TEST_DISPATCH_BARRIER_AT", "5"),
                ("INGEST_TEST_MQTT_REQUEST_CAPACITY", "2"),
            ],
        )?;
        harness.wait_barrier("after_dispatch").await?;
        harness.release("before_quarantine")?;
        harness.wait_quarantine_ids(&expected).await?;
        harness
            .wait_log("supplementary DLQ publish failed after durable quarantine")
            .await?;
        harness.release("after_dispatch")?;
        harness.wait_barrier("ack_queued").await?;
        harness.wait_trace("ack_backpressure ").await?;
        harness.wait_log("MQTT acknowledgement not queued").await?;
        harness.wait_inflight(5).await?;
        harness.request_shutdown()?;
        harness.release("ack_queued")?;
        harness.wait_exit().await?;
        harness.start(None)?;
        harness.wait_ready().await?;
        harness.wait_subscriber_ack(5).await?;
        harness.stop()?;
        broker::assert_publication_ids(
            &expected,
            &harness.output_ids(&device).await?,
            &harness.quarantine_ids()?,
        )?;
        Ok(())
    }
    .await;
    harness.finish(result).await
}
