mod common;

use std::{fs, path::Path, sync::Arc};

use anyhow::{Context, Result};
use serde_json::{json, Value};

use smarthome_ingest::{
    config::routes,
    infrastructure::router::RouteSet,
    pipeline::{context::PipelineContext, outcome::PipelineOutcome},
};

#[tokio::test]
async fn v1_and_v2_preserve_legacy_routes_golden_output_and_cache() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let legacy = common::write_v1_shipped_bundle(directory.path())?;
    let v1 = Arc::new(RouteSet::compile(
        routes::load(legacy)?,
        "smarthome/_dlq/ingest",
        "fallback",
    )?);
    let v2 = Arc::new(RouteSet::compile(
        routes::load("config/routes.json")?,
        "smarthome/_dlq/ingest",
        "fallback",
    )?);
    for topic in v1.subscriptions() {
        let topic = topic.replace('+', "fixture-device");
        let old = v1.select(&topic).context("v1 route")?;
        let new = v2.select(&topic).context("v2 route")?;
        assert_eq!(old.id(), new.id());
        assert_eq!(old.sink_target(), new.sink_target());
    }
    let (old_pipeline, mut old_sub, old_cache, _old_wal) =
        common::build_pipeline_with_routes(v1).await;
    let (new_pipeline, mut new_sub, new_cache, _new_wal) =
        common::build_pipeline_with_routes(v2).await;
    let mut old_events = old_cache.subscribe_events();
    let mut new_events = new_cache.subscribe_events();
    for fixture in [
        include_str!("fixtures/golden/firmware-sensor.json"),
        include_str!("fixtures/golden/firmware-status.json"),
    ] {
        let cases: Vec<Value> = serde_json::from_str(fixture)?;
        for (index, case) in cases.iter().enumerate() {
            let make_context = || {
                let mut context = PipelineContext::new_with_metadata(
                    case["topic"].as_str().unwrap(),
                    serde_json::to_vec(&case["payload"]).unwrap(),
                    case["received_at_ms"].as_i64().unwrap(),
                    case["retained"].as_bool().unwrap(),
                );
                context.set_receive_sequence(index as u64 + 1);
                context
            };
            let mut old = make_context();
            let mut new = make_context();
            let outcome = old_pipeline.run(&mut old).await;
            assert_eq!(outcome, new_pipeline.run(&mut new).await);
            if let Some(expected) = case["unified_line_protocol"].as_str() {
                assert_eq!(outcome, PipelineOutcome::OutputStored);
                let old_event = common::recv_event(&mut old_sub, 1000)
                    .await
                    .context("v1 output")?;
                let new_event = common::recv_event(&mut new_sub, 1000)
                    .await
                    .context("v2 output")?;
                assert_eq!(new_event.payload, expected);
                assert_eq!(new_event.payload, old_event.payload);
                assert_eq!(new_event.sink_target, old_event.sink_target);
                assert_eq!(
                    serde_json::to_value(old.telemetry_record()?)?,
                    serde_json::to_value(new.telemetry_record()?)?
                );
            } else {
                assert_eq!(outcome, PipelineOutcome::RejectionStored);
            }
            let device = case["payload"]["device_id"].as_str().unwrap();
            assert_eq!(
                serde_json::to_value(old_cache.snapshot_device(device))?,
                serde_json::to_value(new_cache.snapshot_device(device))?
            );
            assert_eq!(old_events.len(), new_events.len());
            while let Ok(event) = old_events.try_recv() {
                assert_eq!(
                    serde_json::to_value(event)?,
                    serde_json::to_value(new_events.try_recv()?)?
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn shared_family_bindings_keep_targets_fallback_and_mapping_policies() -> Result<()> {
    let routes = Arc::new(RouteSet::compile(
        routes::load("config/examples/families/routes.json")?,
        "example/dlq",
        "fallback",
    )?);
    assert_eq!(
        routes.subscriptions(),
        [
            "example/climate-a/+",
            "example/climate-b/+",
            "example/health/+"
        ]
    );
    let (pipeline, mut sub, cache, _wal) = common::build_pipeline_with_routes(routes).await;
    for (index, (topic, target, payload, expected_time)) in [
        (
            "example/climate-a/unit-a",
            "example-sensors",
            json!({"temp_c":22.5,"rel_hum_perc":45,"time_ms":1700000000000i64,"time_valid":true}),
            1700000000000i64,
        ),
        (
            "example/climate-b/unit-b",
            "example-metrics",
            json!({"temp_c":22.5,"rel_hum_perc":45,"time_ms":0,"time_valid":false}),
            1700000000456i64,
        ),
        (
            "example/health/unit-c",
            "fallback",
            json!({"code":-2}),
            1700000000456i64,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut context = PipelineContext::new_with_metadata(
            topic,
            serde_json::to_vec(&payload)?,
            1700000000456,
            false,
        );
        context.set_receive_sequence(index as u64 + 1);
        assert_eq!(
            pipeline.run(&mut context).await,
            PipelineOutcome::OutputStored,
            "{:?}",
            context.rejection_reason()
        );
        let event = common::recv_event(&mut sub, 1000).await.context("output")?;
        assert_eq!(event.sink_target.as_deref(), Some(target));
        let record = context.telemetry_record()?;
        assert_eq!(record.timestamp_ms, expected_time);
        if index < 2 {
            let fields = serde_json::to_value(&record.fields)?;
            assert_eq!(fields["site_label"], "unassigned");
            assert!(fields.get("sensor_label").is_none());
            assert!(fields["dew_point_c"].is_number());
            assert!(cache.snapshot_device(&record.device_id).is_some());
        } else {
            assert!(cache.snapshot_device(&record.device_id).is_none());
        }
    }
    Ok(())
}

#[test]
fn shipped_v2_matches_the_v1_counterpart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let legacy = common::write_v1_shipped_bundle(directory.path())?;
    let mut baseline: Value = serde_json::from_slice(&fs::read(legacy)?)?;
    let mut manifest: Value = serde_json::from_slice(&fs::read("config/routes.json")?)?;
    let mut expanded = Vec::new();
    for file in manifest["route_files"].as_array().context("family files")? {
        let family: Value =
            serde_json::from_slice(&fs::read(Path::new("config").join(file.as_str().unwrap()))?)?;
        for binding in family["routes"].as_array().context("bindings")? {
            let mut route = family["mapping"].as_object().unwrap().clone();
            route.extend(binding.as_object().unwrap().clone());
            expanded.push(Value::Object(route));
        }
    }
    manifest["routes"] = Value::Array(expanded);
    resolve_schema_contents(&mut manifest, Path::new("config"))?;
    resolve_schema_contents(&mut baseline, directory.path())?;
    let baseline_routes = baseline["routes"].as_array().context("legacy routes")?;
    // The v1 counterpart covers existing routes; new families have their own tests.
    manifest["routes"]
        .as_array_mut()
        .context("expanded routes")?
        .retain(|route| baseline_routes.iter().any(|old| old["id"] == route["id"]));
    assert_eq!(manifest["routes"], baseline["routes"]);
    assert_eq!(manifest["schema_resources"], baseline["schema_resources"]);
    Ok(())
}

fn resolve_schema_contents(bundle: &mut Value, root: &Path) -> Result<()> {
    for route in bundle["routes"].as_array_mut().context("routes")? {
        for key in ["schema", "output_schema"] {
            if let Some(reference) = route.get(key).and_then(Value::as_str) {
                route[key] = serde_json::from_slice(&fs::read(root.join(reference))?)?;
            }
        }
    }
    for reference in bundle["schema_resources"]
        .as_object_mut()
        .context("schema resources")?
        .values_mut()
    {
        let path = reference.as_str().context("schema resource path")?;
        *reference = serde_json::from_slice(&fs::read(root.join(path))?)?;
    }
    Ok(())
}

#[test]
fn schema_comparison_allows_relocation_but_detects_changed_contents() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bundle = |folder| {
        json!({
            "routes": [{"schema": format!("{folder}/input.json"), "output_schema": format!("{folder}/output.json")}],
            "schema_resources": {"https://schemas.test/shared": format!("{folder}/shared.json")}
        })
    };
    for folder in ["before", "after"] {
        fs::create_dir(directory.path().join(folder))?;
        for filename in ["input.json", "output.json", "shared.json"] {
            fs::write(
                directory.path().join(folder).join(filename),
                r#"{"type":"object"}"#,
            )?;
        }
    }
    let mut baseline = bundle("before");
    resolve_schema_contents(&mut baseline, directory.path())?;
    let mut relocated = bundle("after");
    resolve_schema_contents(&mut relocated, directory.path())?;
    assert_eq!(baseline, relocated);
    for filename in ["input.json", "output.json", "shared.json"] {
        let path = directory.path().join("after").join(filename);
        fs::write(&path, r#"{"type":"string"}"#)?;
        let mut changed = bundle("after");
        resolve_schema_contents(&mut changed, directory.path())?;
        assert_ne!(baseline, changed, "changed {filename} must remain visible");
        fs::write(path, r#"{"type":"object"}"#)?;
    }
    Ok(())
}
