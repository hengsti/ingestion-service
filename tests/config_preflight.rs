use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::Result;
use serde_json::{json, Value};
use tempfile::TempDir;

fn preflight_command(directory: &TempDir, routes_file: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_smarthome-ingest"));
    command
        .env_clear()
        .env("RUST_LOG", "warn")
        .env("INPUT_SOURCE", "mqtt")
        .env("MQTT_HOST", "localhost")
        .env("MQTT_PORT", "1883")
        .env("MQTT_CLIENT_ID", "preflight-test")
        .env("MQTT_TOPIC_DLQ", "test/dlq")
        .env("INGEST_ROUTES_FILE", routes_file)
        .env("OUTPUT_SINK", "influx")
        .env("INFLUX_URL", "http://localhost:8086")
        .env("INFLUX_ORG", "test")
        .env("INFLUX_BUCKET", "test")
        .env("INFLUX_TOKEN", "synthetic-preflight-token")
        .env("BATCH_SIZE", "1")
        .env("FLUSH_INTERVAL_MS", "1")
        .env("WAL_DIR", directory.path().join("wal"))
        .env("WAL_MAX_BYTES", "268435456")
        .env("QUARANTINE_DIR", directory.path().join("quarantine"))
        .env("QUARANTINE_MAX_BYTES", "268435456")
        .env("METRICS_BIND", "127.0.0.1:19090")
        .env("CACHE_TTL_MS", "1000")
        .env("CACHE_BIND", "127.0.0.1:18080")
        .env("CACHE_BUFFER", "1");
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    command
}

fn production_routes() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join("routes.json")
}

fn climate_routes() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join("examples")
        .join("climate")
        .join("routes.json")
}

fn write_route_bundle(directory: &TempDir, bundle: &Value, raw_schema: &str) -> Result<PathBuf> {
    let config_dir = directory.path().join("config");
    let schemas_dir = config_dir.join("schemas");
    fs::create_dir_all(&schemas_dir)?;
    fs::write(config_dir.join("routes.json"), serde_json::to_vec(bundle)?)?;
    fs::write(schemas_dir.join("climate.schema.json"), raw_schema)?;
    fs::write(
        schemas_dir.join("climate.output.schema.json"),
        include_str!("../config/examples/climate/schemas/climate.output.schema.json"),
    )?;
    Ok(config_dir.join("routes.json"))
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn copy_fixture_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_fixture_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[test]
fn test_check_config_v2_validates_complete_nested_bundle_from_another_cwd() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("config");
    copy_fixture_tree(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/examples/families"),
        &config,
    )?;
    let path = config.join("routes.json");
    let output = preflight_command(&directory, &path)
        .current_dir(directory.path())
        .arg("--check-config")
        .output()?;
    assert!(output.status.success(), "{}", combined_output(&output));
    assert!(combined_output(&output).contains("3 routes"));

    let family_path = config.join("routes/health.json");
    let family: Value = serde_json::from_slice(&fs::read(&family_path)?)?;
    for (pointer, value, expected) in [
        (
            "/routes/0/id",
            json!("example-climate-a"),
            "duplicate route id",
        ),
        (
            "/routes/0/topic",
            json!("example/climate-a/+"),
            "overlapping",
        ),
        ("/routes/0/topic", json!("test/dlq"), "DLQ"),
        (
            "/routes/0/sink_target",
            json!({"token": "sentinel-secret"}),
            "sink_target",
        ),
        (
            "/mapping/schema",
            json!("schemas/missing.json"),
            "missing.json",
        ),
        (
            "/mapping/calculations",
            json!(["missing_calculation"]),
            "missing_calculation",
        ),
    ] {
        let mut invalid = family.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        invalid.pointer_mut(parent).unwrap()[key] = value;
        if expected == "DLQ" {
            invalid["mapping"]["device_id"] = json!("dlq-fixture");
        }
        fs::write(&family_path, serde_json::to_vec(&invalid)?)?;
        let output = preflight_command(&directory, &path)
            .current_dir(directory.path())
            .arg("--check-config")
            .output()?;
        let message = combined_output(&output);
        assert!(!output.status.success(), "{pointer} accepted");
        assert!(message.contains(expected), "{pointer}: {message}");
        assert!(message.len() < 4096);
        assert!(!message.contains("sentinel-secret"));
        assert!(!message.contains("synthetic-preflight-token"));
    }
    let mut collision = family.clone();
    collision["routes"][0]["sink_target"] = json!("example-sensors");
    collision["mapping"]["measurement"] = json!("climate");
    collision["mapping"]["fields"] = json!({"temp_c": {"pointer": "/code", "type": "boolean"}});
    fs::write(&family_path, serde_json::to_vec(&collision)?)?;
    let output = preflight_command(&directory, &path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("type"));
    fs::write(&family_path, "{ invalid JSON")?;
    let output = preflight_command(&directory, &path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("health.json"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_accepts_production_bundle_without_journal_side_effects() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = preflight_command(&directory, &production_routes())
        .arg("--check-config")
        .output()?;

    assert!(
        output.status.success(),
        "preflight failed: {}",
        combined_output(&output)
    );
    assert!(combined_output(&output).contains("valid"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_rejects_invalid_fallback_bucket_with_bounded_diagnostic() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = preflight_command(&directory, &climate_routes())
        .env("INFLUX_BUCKET", "x".repeat(10_000))
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    let message = combined_output(&output);
    assert!(message.contains("INFLUX_BUCKET"));
    assert!(message.len() < 4096);
    assert!(!message.contains("synthetic-preflight-token"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_accepts_explicit_sink_target_and_rejects_invalid_targets() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut bundle: Value =
        serde_json::from_str(include_str!("../config/examples/climate/routes.json"))?;
    bundle["routes"][0]["sink_target"] = json!("synthetic-metrics");
    let path = write_route_bundle(
        &directory,
        &bundle,
        include_str!("../config/examples/climate/schemas/climate.schema.json"),
    )?;
    let output = preflight_command(&directory, &path)
        .arg("--check-config")
        .output()?;
    assert!(output.status.success(), "{}", combined_output(&output));

    for target in [
        json!(null),
        json!(""),
        json!("bad\nbucket"),
        json!("x".repeat(256)),
        json!({"token": "secret"}),
    ] {
        bundle["routes"][0]["sink_target"] = target;
        fs::write(&path, serde_json::to_vec(&bundle)?)?;
        let output = preflight_command(&directory, &path)
            .arg("--check-config")
            .output()?;
        assert!(!output.status.success());
        assert!(combined_output(&output).contains("sink_target"));
        assert!(!combined_output(&output).contains("secret"));
        assert!(combined_output(&output).len() < 4096);
    }
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_accepts_synthetic_climate_bundle() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = preflight_command(&directory, &climate_routes())
        .arg("--check-config")
        .output()?;

    assert!(
        output.status.success(),
        "preflight failed: {}",
        combined_output(&output)
    );
    assert!(combined_output(&output).contains("valid"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_rejects_missing_setting_without_leaking_token() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut command = preflight_command(&directory, &production_routes());
    command.env_remove("MQTT_HOST").arg("--check-config");
    let output = command.output()?;

    assert!(!output.status.success());
    assert!(combined_output(&output).contains("MQTT_HOST"));
    assert!(!combined_output(&output).contains("synthetic-preflight-token"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_rejects_invalid_startup_settings_without_side_effects() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut accepted = Vec::new();
    for (name, value) in [
        ("METRICS_BIND", "not-a-socket-address"),
        ("CACHE_BIND", "not-a-socket-address"),
        ("CACHE_BUFFER", "0"),
        ("FLUSH_INTERVAL_MS", "0"),
    ] {
        let output = preflight_command(&directory, &production_routes())
            .env(name, value)
            .arg("--check-config")
            .output()?;

        if output.status.success() {
            accepted.push(name);
        } else {
            assert!(combined_output(&output).contains(name));
        }
        assert!(!directory.path().join("wal").exists());
        assert!(!directory.path().join("quarantine").exists());
    }
    assert!(
        accepted.is_empty(),
        "preflight accepted invalid {accepted:?}"
    );
    Ok(())
}

#[test]
fn test_check_config_rejects_extra_and_unknown_arguments() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = preflight_command(&directory, &production_routes())
        .args(["--check-config", "unexpected"])
        .output()?;
    assert!(!output.status.success());

    let output = preflight_command(&directory, &production_routes())
        .arg("--unknown-option")
        .output()?;
    assert!(!output.status.success());
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_rejects_invalid_bundles_and_budget() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let invalid_json_path = directory.path().join("invalid-routes.json");
    fs::write(&invalid_json_path, "{")?;
    let output = preflight_command(&directory, &invalid_json_path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("parsing route bundle"));

    let mut bundle: Value =
        serde_json::from_str(include_str!("../config/examples/climate/routes.json"))?;
    let remote_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$ref": "https://schemas.invalid/climate.schema.json"
    });
    let route_path = write_route_bundle(&directory, &bundle, &remote_schema.to_string())?;
    let output = preflight_command(&directory, &route_path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("schema"));

    bundle["routes"][0]["calculations"] = json!(["unknown_calculation"]);
    let route_path = write_route_bundle(
        &directory,
        &bundle,
        include_str!("../config/examples/climate/schemas/climate.schema.json"),
    )?;
    let output = preflight_command(&directory, &route_path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("unknown_calculation"));

    bundle["routes"][0]["calculations"] = json!(["dew_point"]);
    bundle["routes"][0]["fields"]["dew_point_c"] = json!({"pointer": "/temp_c", "type": "string"});
    let route_path = write_route_bundle(
        &directory,
        &bundle,
        include_str!("../config/examples/climate/schemas/climate.schema.json"),
    )?;
    let output = preflight_command(&directory, &route_path)
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("dew_point_c"));

    let output = preflight_command(&directory, &climate_routes())
        .env("WAL_MAX_BYTES", "1")
        .arg("--check-config")
        .output()?;
    assert!(!output.status.success());
    assert!(combined_output(&output).contains("WAL_MAX_BYTES"));
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}

#[test]
fn test_check_config_bounds_invalid_setting_diagnostic() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let output = preflight_command(&directory, &production_routes())
        .env("INPUT_SOURCE", "x".repeat(4096))
        .arg("--check-config")
        .output()?;

    assert!(!output.status.success());
    assert!(combined_output(&output).contains("unsupported INPUT_SOURCE"));
    assert!(output.stderr.len() <= 2048);
    assert!(!directory.path().join("wal").exists());
    assert!(!directory.path().join("quarantine").exists());
    Ok(())
}
