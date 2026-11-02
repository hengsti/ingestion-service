mod config;
mod infrastructure;
mod model;
mod pipeline;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Result;
use config::Config;
use infrastructure::cache::{http, state::CacheState};
use infrastructure::prometheus::MetricsServer;
use infrastructure::router::RouteSet;
use infrastructure::source::{build_source, IngestDispatcher, IngestJob};
use infrastructure::wal::forwarder::run_forwarder;
use infrastructure::wal::types::WalOptions;
use infrastructure::wal::wal::Wal;
use pipeline::{
    context::PipelineContext,
    runner::PipelineRunner,
    stages::{
        cache_update::CacheUpdateStage, decode::DecodeStage, dlq::DlqPublishStage,
        observe::ObserveStage, persist::PersistStage, transform::TelemetryMappingStage,
        validate_output::ValidateOutputStage, validate_raw::ConfiguredValidateRawStage,
    },
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};

use crate::infrastructure::sink::build_output;

fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

/// Waits for `SIGTERM`, the default signal Docker sends on `stop`/`down`.
///
/// # Errors
///
/// Returns an error if the OS signal handler cannot be installed.
#[cfg(unix)]
async fn wait_for_sigterm() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate())?;
    sigterm.recv().await;
    Ok(())
}

/// Non-Unix platforms have no `SIGTERM`; never resolves so `ctrl_c` remains
/// the sole shutdown trigger there.
#[cfg(not(unix))]
async fn wait_for_sigterm() -> std::io::Result<()> {
    std::future::pending().await
}

async fn recv_worker_job(
    rx: &mut mpsc::Receiver<IngestJob>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<IngestJob> {
    loop {
        if *shutdown_rx.borrow() {
            return rx.try_recv().ok();
        }

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() {
                    return rx.try_recv().ok();
                }
            }
            job = rx.recv() => return job,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let cfg = Config::from_env()?;
    info!(?cfg, "starting ingestion service");

    let route_bundle = config::routes::load(&cfg.routes_file)?;
    let dlq_topic = cfg.dlq_topic.clone();
    let routes = Arc::new(RouteSet::compile(route_bundle, &dlq_topic)?);

    // Cache / HTTP state.
    let app_state = CacheState::new(cfg.cache_ttl_ms, cfg.cache_buffer);
    let source_ready = Arc::new(AtomicBool::new(false));

    let http_state = app_state.clone();
    let cache_bind = cfg.cache_bind.clone();
    let source_ready_http = source_ready.clone();

    let mut _http_task = tokio::spawn(async move {
        let app = http::router(http_state, source_ready_http);
        let listener = tokio::net::TcpListener::bind(&cache_bind)
            .await
            .expect("failed to bind CACHE_BIND");

        if let Err(err) = axum::serve(listener, app).await {
            error!(error = %err, "HTTP server error");
        }
    });

    // Metrics server.
    let _metrics_server = MetricsServer::start(&cfg.metrics_bind).await?;

    // WAL, configured sink, and forwarder.
    let (wal, wal_sub) = Wal::open(WalOptions {
        dir: cfg.wal_dir.clone(),
        segment_bytes: cfg.wal_segment_bytes,
        queue_capacity: cfg.wal_queue_capacity,
    })
    .await?;
    let wal = Arc::new(wal);

    let (encoder, sink) = build_output(&cfg)?;

    let batch_size = cfg.batch_size;
    let flush_interval_ms = cfg.flush_interval_ms;

    let forwarder_task = tokio::spawn(async move {
        if let Err(err) = run_forwarder(wal_sub, sink, batch_size, flush_interval_ms).await {
            error!(error = %err, "wal forwarder failed");
        }
    });

    // Input source.
    let (source, dlq_publisher) = build_source(&cfg, source_ready.clone(), routes.as_ref()).await?;

    // Pipeline.
    let pipeline = Arc::new(
        PipelineRunner::new()
            .add_stage(DecodeStage::new())
            .add_stage(ConfiguredValidateRawStage::new(routes.clone()))
            .add_stage(TelemetryMappingStage::new())
            .add_stage(ValidateOutputStage::new())
            .add_stage(CacheUpdateStage::new(app_state.clone()))
            .add_stage(PersistStage::new(wal.clone(), encoder))
            .add_stage(ObserveStage::new())
            .with_failure_stage(DlqPublishStage::new(dlq_publisher, dlq_topic.clone())),
    );

    info!("pipeline initialized");

    // Worker queue: one channel per worker, round-robin dispatch.
    let worker_total = worker_count();
    let per_worker_cap = cfg.input_queue_capacity / worker_total;
    info!(workers = worker_total, "starting pipeline workers");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut workers = JoinSet::new();
    let mut job_txs: Vec<mpsc::Sender<IngestJob>> = Vec::with_capacity(worker_total);

    for worker_id in 0..worker_total {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(per_worker_cap);
        job_txs.push(tx);

        let pipeline = pipeline.clone();
        let mut shutdown_rx = shutdown_rx.clone();

        workers.spawn(async move {
            info!(worker_id, "pipeline worker started");

            loop {
                let job = match recv_worker_job(&mut rx, &mut shutdown_rx).await {
                    Some(j) => j,
                    None => break,
                };

                let mut ctx = PipelineContext::new_with_metadata(
                    job.topic.clone(),
                    job.payload,
                    job.received_at_ms,
                    job.retained,
                );
                ctx.set_receive_sequence(job.receive_sequence);
                pipeline.run(&mut ctx).await;
            }

            info!(worker_id, "pipeline worker stopped");
            Ok::<(), anyhow::Error>(())
        });
    }

    // Source task dispatches into the worker queue via `IngestDispatcher`.
    let dispatcher = IngestDispatcher::new(job_txs.clone());
    let mut source_task = tokio::spawn(source.run(dispatcher, shutdown_rx.clone()));

    // Wait for ctrl-c, SIGTERM, the HTTP task, or the input source, then signal shutdown.
    let mut fatal_source_err: Option<anyhow::Error> = None;
    let mut source_task_handled = false;

    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            match res {
                Ok(()) => info!("shutdown signal received"),
                Err(err) => error!(error = %err, "failed to listen for ctrl-c"),
            }
            let _ = shutdown_tx.send(true);
        }

        res = wait_for_sigterm() => {
            match res {
                Ok(()) => info!("SIGTERM received"),
                Err(err) => error!(error = %err, "failed to listen for sigterm"),
            }
            let _ = shutdown_tx.send(true);
        }

        res = &mut _http_task => {
            match res {
                Ok(()) => error!("HTTP server stopped unexpectedly"),
                Err(err) => error!(error = %err, "HTTP server task panicked"),
            }
            let _ = shutdown_tx.send(true);
        }

        res = &mut source_task => {
            source_task_handled = true;
            let _ = shutdown_tx.send(true);

            match res {
                Ok(Ok(())) => info!("input source stopped"),
                Ok(Err(err)) => {
                    source_ready.store(false, Ordering::Relaxed);
                    error!(error = %err, "input source failed");
                    fatal_source_err = Some(err);
                }
                Err(join_err) => {
                    source_ready.store(false, Ordering::Relaxed);
                    error!(error = %join_err, "input source task panicked");
                    fatal_source_err = Some(join_err.into());
                }
            }
        }
    }

    // Join the source task if shutdown was triggered by ctrl-c or the HTTP
    // task, so its senders drop before the worker channels close below.
    if !source_task_handled {
        match source_task.await {
            Ok(Ok(())) => info!("input source stopped"),
            Ok(Err(err)) => {
                source_ready.store(false, Ordering::Relaxed);
                error!(error = %err, "input source failed");
                fatal_source_err = Some(err);
            }
            Err(join_err) => {
                source_ready.store(false, Ordering::Relaxed);
                error!(error = %join_err, "input source task panicked");
                fatal_source_err = Some(join_err.into());
            }
        }
    }

    drop(job_txs);

    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(error = %err, "worker failed during shutdown"),
            Err(err) => error!(error = %err, "worker join failed during shutdown"),
        }
    }

    // Drop both `Arc<Wal>` handles (pipeline's and main's) so the writer
    // closes its channel, flushes, and lets the forwarder drain.
    drop(pipeline);
    drop(wal);

    let mut forwarder_task = forwarder_task;
    match tokio::time::timeout(std::time::Duration::from_secs(5), &mut forwarder_task).await {
        Ok(Ok(())) => info!("wal forwarder drained cleanly"),
        Ok(Err(err)) => error!(error = %err, "wal forwarder task panicked during drain"),
        Err(_) => {
            warn!("wal forwarder drain timed out after 5s; aborting and exiting");
            forwarder_task.abort();
        }
    }

    info!("ingestion service stopped");

    // A fatal input-source error still exits the process non-zero, but only
    // after workers and the WAL have drained cleanly above.
    match fatal_source_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn recv_worker_job_after_shutdown_drains_queued_messages_before_stopping() {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(4);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        tx.try_send(IngestJob {
            topic: "smarthome/dev-1/status".to_string(),
            payload: Bytes::from_static(b"{\"device_id\":\"dev-1\"}"),
            received_at_ms: 1,
            receive_sequence: 0,
            retained: false,
        })
        .unwrap();

        shutdown_tx.send(true).unwrap();

        let job = recv_worker_job(&mut rx, &mut shutdown_rx).await;
        assert!(
            job.is_some(),
            "queued message must be drained after shutdown"
        );
    }

    #[tokio::test]
    async fn recv_worker_job_after_shutdown_keeps_draining_until_queue_is_empty() {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(4);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        tx.try_send(IngestJob {
            topic: "smarthome/dev-1/status".to_string(),
            payload: Bytes::from_static(b"{\"device_id\":\"dev-1\"}"),
            received_at_ms: 1,
            receive_sequence: 0,
            retained: false,
        })
        .unwrap();
        tx.try_send(IngestJob {
            topic: "smarthome/dev-2/status".to_string(),
            payload: Bytes::from_static(b"{\"device_id\":\"dev-2\"}"),
            received_at_ms: 2,
            receive_sequence: 0,
            retained: true,
        })
        .unwrap();

        shutdown_tx.send(true).unwrap();

        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_some());
        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_some());
        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_none());
    }
}
