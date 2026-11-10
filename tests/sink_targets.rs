mod common;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    routing::post,
    Router,
};
use bytes::Bytes;
use serde_json::json;
use smarthome_ingest::{
    config::routes,
    infrastructure::{
        router::RouteSet,
        sink::{influx::InfluxSink, Sink},
        wal::{
            cursor::read_cursor,
            forwarder::run_forwarder,
            types::{WalEvent, WalOptions},
            wal::Wal,
        },
    },
    pipeline::{context::PipelineContext, outcome::PipelineOutcome},
};

#[derive(Clone, Default)]
struct Writes {
    requests: Arc<Mutex<Vec<(String, String)>>>,
    fail_metrics: Arc<AtomicBool>,
}

async fn mock_influx(writes: Writes, failure: axum::http::StatusCode) -> Result<String> {
    let app = Router::new()
        .route(
            "/api/v2/write",
            post(
                move |State(writes): State<Writes>,
                      Query(query): Query<HashMap<String, String>>,
                      body: Bytes| async move {
                    assert_eq!(query.get("precision").map(String::as_str), Some("ms"));
                    let bucket = query["bucket"].clone();
                    writes
                        .requests
                        .lock()
                        .unwrap()
                        .push((bucket.clone(), String::from_utf8(body.to_vec()).unwrap()));
                    if bucket == "example-metrics" && writes.fail_metrics.load(Ordering::Relaxed) {
                        failure
                    } else {
                        axum::http::StatusCode::NO_CONTENT
                    }
                },
            ),
        )
        .with_state(writes);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(format!("http://{address}"))
}

fn sink(url: &str) -> Result<Arc<dyn Sink>> {
    Ok(Arc::new(InfluxSink::new(
        url,
        "test",
        "changed-default",
        "test-token".to_owned().into(),
    )?))
}

fn options(path: &std::path::Path) -> WalOptions {
    WalOptions {
        dir: path.to_owned(),
        max_bytes: 128 * 1024 * 1024,
        segment_bytes: 128,
        queue_capacity: 10,
    }
}

#[tokio::test]
async fn synthetic_routes_keep_types_timestamps_and_buckets_on_replay_after_route_edits(
) -> Result<()> {
    let bundle_path = "config/examples/sink-targets/routes.json";
    let routes = Arc::new(RouteSet::compile(
        routes::load(bundle_path)?,
        "example/dlq",
        "old-default",
    )?);
    let (pipeline, mut sub, _cache, directory) = common::build_pipeline_with_routes(routes).await;
    let mut expected = Vec::new();
    for kind in ["samples", "samples", "health", "samples", "health"] {
        let payload = if kind == "samples" {
            json!({"temperature":21.5,"count":7,"active":false,"label":"lab","time":1700000000123i64})
        } else {
            json!({"code":-2,"active":true,"time":1700000000124i64})
        };
        let mut context = PipelineContext::new_with_metadata(
            format!("example/{kind}/unit"),
            serde_json::to_vec(&payload)?,
            1700000099999,
            false,
        );
        assert_eq!(
            pipeline.run(&mut context).await,
            PipelineOutcome::OutputStored
        );
        let event = common::recv_event(&mut sub, 1000)
            .await
            .context("persisted output")?;
        let (target, line) = if kind == "samples" {
            ("example-sensors", "sample,device_id=unit active=false,count=7u,label=\"lab\",mqtt_retained=false,temperature=21.5 1700000000123")
        } else {
            (
                "example-metrics",
                "health,device_id=unit active=true,code=-2i,mqtt_retained=false 1700000000124",
            )
        };
        assert_eq!(event.sink_target.as_deref(), Some(target));
        assert_eq!(event.payload, line);
        expected.push((target.to_owned(), line.to_owned()));
    }
    drop(pipeline);
    drop(sub);

    // A different route bundle and fallback must never retarget already durable output.
    let mut changed = routes::load(bundle_path)?;
    changed.routes[0].sink_target = Some("edited-bucket".to_owned());
    changed.routes[1].sink_target = None;
    let changed = RouteSet::compile(changed, "example/dlq", "changed-default")?;
    assert_eq!(
        changed
            .select("example/samples/unit")
            .context("sample route")?
            .sink_target(),
        "edited-bucket"
    );
    assert_eq!(
        changed
            .select("example/health/unit")
            .context("health route")?
            .sink_target(),
        "changed-default"
    );
    let (wal, replay) = Wal::open_output(options(directory.path())).await?;
    let writes = Writes::default();
    let url = mock_influx(writes.clone(), axum::http::StatusCode::BAD_REQUEST).await?;
    let forwarder = tokio::spawn(run_forwarder(replay, sink(&url)?, 2, 60_000));
    drop(wal);
    tokio::time::timeout(Duration::from_secs(5), forwarder).await???;
    let requests = writes.requests.lock().unwrap();
    assert!(requests.iter().all(|(_, body)| body.lines().count() <= 2));
    let actual: Vec<_> = requests
        .iter()
        .flat_map(|(target, body)| body.lines().map(|line| (target.clone(), line.to_owned())))
        .collect();
    assert_eq!(actual, expected);
    assert!(read_cursor(directory.path())?.is_some());
    Ok(())
}

#[tokio::test]
async fn omitted_target_freezes_the_fallback_bucket_in_durable_output() -> Result<()> {
    let mut bundle = routes::load("config/examples/sink-targets/routes.json")?;
    bundle.routes[1].sink_target = None;
    let routes = Arc::new(RouteSet::compile(
        bundle,
        "example/dlq",
        "original-default",
    )?);
    let (pipeline, mut sub, _cache, directory) = common::build_pipeline_with_routes(routes).await;
    let mut context = PipelineContext::new_with_metadata(
        "example/health/unit",
        serde_json::to_vec(&json!({"code":-2,"active":false,"time":1700000000124i64}))?,
        1700000099999,
        false,
    );
    assert_eq!(
        pipeline.run(&mut context).await,
        PipelineOutcome::OutputStored
    );
    let original = sub.next().await?.context("durable output")?;
    drop(pipeline);
    drop(sub);
    let (_wal, mut replay) = Wal::open_output(options(directory.path())).await?;
    let replayed = replay.next().await?.context("replayed output")?;
    assert_eq!(
        replayed.event.sink_target.as_deref(),
        Some("original-default")
    );
    assert_eq!(replayed.event.payload, original.event.payload);
    Ok(())
}

#[tokio::test]
async fn failed_target_blocks_later_output_and_replays_without_advancing_cursor() -> Result<()> {
    for failure in [
        axum::http::StatusCode::BAD_REQUEST,
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let directory = tempfile::tempdir()?;
        let (wal, mut sub) = Wal::open_output(options(directory.path())).await?;
        let mut entries = Vec::new();
        for (index, bucket) in ["example-sensors", "example-metrics", "example-sensors"]
            .into_iter()
            .enumerate()
        {
            let event = WalEvent {
                topic: "example/topic".to_owned(),
                ts_ms: 1,
                payload: format!("sample value={index}i 1"),
                sink_target: Some(bucket.to_owned()),
            };
            assert!(wal.append_durable(event).await.is_ok());
            entries.push(sub.next().await?.context("durable entry")?);
        }
        drop(sub);
        drop(wal);
        let (wal, sub) = Wal::open_output(options(directory.path())).await?;
        let writes = Writes::default();
        writes.fail_metrics.store(true, Ordering::Relaxed);
        let url = mock_influx(writes.clone(), failure).await?;
        let handle = tokio::spawn(run_forwarder(sub, sink(&url)?, 1, 10));
        tokio::time::timeout(Duration::from_secs(6), async {
            let attempts = if failure.is_client_error() { 1 } else { 3 };
            while writes
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(bucket, _)| bucket == "example-metrics")
                .count()
                < attempts
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert_eq!(
            read_cursor(directory.path())?,
            Some(entries[0].offset_after)
        );
        assert!(writes
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(_, body)| !body.contains("value=2i")));

        // Model the bounded shutdown deadline expiring while a target is unavailable.
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        drop(wal);
        writes.fail_metrics.store(false, Ordering::Relaxed);
        let (wal, replay) = Wal::open_output(options(directory.path())).await?;
        let handle = tokio::spawn(run_forwarder(replay, sink(&url)?, 10, 10));
        drop(wal);
        tokio::time::timeout(Duration::from_secs(5), handle).await???;
        assert_eq!(
            read_cursor(directory.path())?,
            Some(entries[2].offset_after)
        );
        let requests = writes.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(_, body)| body.contains("value=0i"))
                .count(),
            1
        );
        assert_eq!(
            requests.last().context("last write")?.1,
            "sample value=2i 1"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sink_refuses_mixed_and_legacy_batches_without_sending_requests() -> Result<()> {
    let writes = Writes::default();
    let url = mock_influx(writes.clone(), axum::http::StatusCode::BAD_REQUEST).await?;
    let sink = sink(&url)?;
    let event = WalEvent {
        topic: "example/topic".to_owned(),
        ts_ms: 1,
        payload: "sample value=1i 1".to_owned(),
        sink_target: Some("example-sensors".to_owned()),
    };
    let mut other = event.clone();
    other.sink_target = Some("example-metrics".to_owned());
    assert!(sink.write(&[event, other.clone()]).await.is_err());
    other.sink_target = None;
    assert!(sink.write(&[other]).await.is_err());
    assert!(writes.requests.lock().unwrap().is_empty());
    Ok(())
}
