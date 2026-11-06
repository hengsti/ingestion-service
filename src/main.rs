mod calculations;
mod config;
mod infrastructure;
mod model;
mod pipeline;

#[cfg(all(feature = "integration-test-hooks", debug_assertions))]
mod test_hooks;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::{bail, Result};
use config::Config;
use infrastructure::cache::{http, state::CacheState};
use infrastructure::operations::Operations;
use infrastructure::prometheus::MetricsServer;
use infrastructure::quarantine::Quarantine;
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
        observe::ObserveStage, persist::PersistStage, telemetry_mapping::TelemetryMappingStage,
        validate_output::ValidateOutputStage, validate_raw::ValidateRawStage,
    },
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};

use crate::infrastructure::sink::build_output;

struct ForwarderLivenessGuard(Arc<AtomicBool>);

impl Drop for ForwarderLivenessGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

struct IngestWorkerLivenessGuard(Arc<AtomicBool>);

impl Drop for IngestWorkerLivenessGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

fn worker_queue_capacities(total_capacity: usize, requested_workers: usize) -> Vec<usize> {
    let worker_total = requested_workers.min(total_capacity);
    let base_capacity = total_capacity / worker_total;
    let remainder = total_capacity % worker_total;

    (0..worker_total)
        .map(|worker_id| base_capacity + usize::from(worker_id < remainder))
        .collect()
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
    if run_config_check_command()? {
        return Ok(());
    }

    if infrastructure::quarantine::run_maintenance_command().await? {
        return Ok(());
    }

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

    let (quarantine, quarantine_subscription) = Quarantine::open(WalOptions {
        dir: cfg.quarantine_dir.clone(),
        max_bytes: cfg.quarantine_max_bytes,
        segment_bytes: cfg.wal_segment_bytes,
        queue_capacity: cfg.wal_queue_capacity,
    })
    .await?;
    drop(quarantine_subscription);
    let quarantine = Arc::new(quarantine);
    let quarantine_capacity = quarantine.capacity_handle();

    // WAL, configured sink, and forwarder.
    let (wal, wal_sub) = Wal::open(WalOptions {
        dir: cfg.wal_dir.clone(),
        max_bytes: cfg.wal_max_bytes,
        segment_bytes: cfg.wal_segment_bytes,
        queue_capacity: cfg.wal_queue_capacity,
    })
    .await?;
    let wal = Arc::new(wal);
    let wal_capacity = wal.capacity_handle();
    let forwarder_state = wal_sub.forwarder_state();
    let forwarder_alive = forwarder_state.liveness_handle();
    let operations = Arc::new(Operations::new(
        source_ready.clone(),
        forwarder_state,
        wal_capacity.clone(),
        quarantine_capacity.clone(),
    ));
    let (wal_used_bytes, wal_max_bytes) = wal.capacity_bytes();
    info!(
        wal_used_bytes,
        wal_max_bytes, "journal capacity initialized"
    );

    let http_state = app_state.clone();
    let cache_bind = cfg.cache_bind.clone();
    let operations_http = operations.clone();
    let mut _http_task = tokio::spawn(async move {
        let app = http::router(http_state, operations_http);
        let listener = tokio::net::TcpListener::bind(&cache_bind)
            .await
            .expect("failed to bind CACHE_BIND");

        if let Err(err) = axum::serve(listener, app).await {
            error!(error = %err, "HTTP server error");
        }
    });

    // Metrics server.
    let _metrics_server =
        MetricsServer::start(&cfg.metrics_bind, &routes, operations.clone()).await?;

    let (encoder, sink) = build_output(&cfg)?;

    let batch_size = cfg.batch_size;
    let flush_interval_ms = cfg.flush_interval_ms;
    let forwarder_alive_task = forwarder_alive.clone();
    let (forwarder_stopped_tx, forwarder_stopped_rx) = watch::channel(false);

    let mut forwarder_task = tokio::spawn(async move {
        let _liveness = ForwarderLivenessGuard(forwarder_alive_task);
        let result = run_forwarder(wal_sub, sink, batch_size, flush_interval_ms).await;
        let _ = forwarder_stopped_tx.send(true);
        result
    });

    // Input source.
    let (source, dlq_publisher) = build_source(&cfg, routes.as_ref(), operations.clone()).await?;

    // Pipeline.
    let pipeline = Arc::new(
        PipelineRunner::new()
            .add_stage(DecodeStage::new())
            .add_stage(ValidateRawStage::new(routes.clone()))
            .add_stage(TelemetryMappingStage::new())
            .add_stage(ValidateOutputStage::new(encoder.clone()))
            .add_stage(CacheUpdateStage::new(app_state.clone()))
            .add_stage(
                PersistStage::new(wal.clone(), encoder)
                    .with_forwarder_stopped(forwarder_stopped_rx),
            )
            .add_stage(ObserveStage::new())
            .with_failure_stage(DlqPublishStage::new(
                dlq_publisher,
                dlq_topic.clone(),
                quarantine.clone(),
            )),
    );

    info!("pipeline initialized");

    // Worker queue: one channel per worker, round-robin dispatch.
    let worker_capacities = worker_queue_capacities(cfg.input_queue_capacity, worker_count());
    let worker_total = worker_capacities.len();
    info!(workers = worker_total, "starting pipeline workers");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut workers = JoinSet::new();
    let mut job_txs: Vec<mpsc::Sender<IngestJob>> = Vec::with_capacity(worker_total);

    for (worker_id, queue_capacity) in worker_capacities.into_iter().enumerate() {
        let (tx, mut rx) = mpsc::channel::<IngestJob>(queue_capacity);
        job_txs.push(tx);

        let pipeline = pipeline.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        let workers_alive = operations.workers_alive.clone();

        workers.spawn(async move {
            let _liveness = IngestWorkerLivenessGuard(workers_alive);
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
                ctx.set_qos(job.qos);
                if job.qos == 0 {
                    ctx.mark_rejection("MQTT QoS 0 is not recoverable by the broker");
                }
                let outcome = pipeline.run(&mut ctx).await;
                if let Some(acknowledgement) = job.acknowledgement {
                    if !acknowledgement.complete(outcome).await {
                        warn!(topic = %ctx.topic(), "MQTT acknowledgement result could not be returned to source");
                    }
                }
            }

            info!(worker_id, "pipeline worker stopped");
            Ok::<(), anyhow::Error>(())
        });
    }

    // Source task dispatches into the worker queue via `IngestDispatcher`.
    let dispatcher = IngestDispatcher::new(job_txs.clone());
    let source_future = source.run(dispatcher, shutdown_rx.clone());
    let source_ready_task = source_ready.clone();
    let mut source_task = tokio::spawn(async move {
        let _liveness = ForwarderLivenessGuard(source_ready_task);
        source_future.await
    });

    // Wait for ctrl-c, SIGTERM, the HTTP task, or the input source, then signal shutdown.
    let mut fatal_err: Option<anyhow::Error> = None;
    let mut source_task_handled = false;
    let mut forwarder_task_handled = false;

    tokio::select! {
        () = async {
            #[cfg(all(feature = "integration-test-hooks", debug_assertions))]
            if let Some(dir) = std::env::var_os("INGEST_TEST_HOOK_DIR").map(std::path::PathBuf::from) {
                while !dir.join("shutdown.request").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                return;
            }
            std::future::pending::<()>().await;
        } => {
            info!("test shutdown requested");
            let _ = shutdown_tx.send(true);
        }

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
                    fatal_err = Some(err);
                }
                Err(join_err) => {
                    source_ready.store(false, Ordering::Relaxed);
                    error!(error = %join_err, "input source task panicked");
                    fatal_err = Some(join_err.into());
                }
            }
        }

        res = &mut forwarder_task => {
            forwarder_task_handled = true;
            source_ready.store(false, Ordering::Relaxed);
            forwarder_alive.store(false, Ordering::Relaxed);
            let err = match res {
                Ok(Ok(())) => anyhow::anyhow!("WAL forwarder stopped unexpectedly"),
                Ok(Err(err)) => err,
                Err(join_err) => join_err.into(),
            };
            error!(error = %err, "WAL forwarder stopped unexpectedly");
            fatal_err = Some(err);
            let _ = shutdown_tx.send(true);
        }

        res = workers.join_next() => {
            let err = match res {
                Some(Ok(Err(err))) => err,
                Some(Err(err)) => err.into(),
                _ => anyhow::anyhow!("pipeline worker stopped unexpectedly"),
            };
            error!(error = %err, "pipeline worker stopped unexpectedly");
            fatal_err = Some(err);
            let _ = shutdown_tx.send(true);
        }
    }

    operations.accepting.store(false, Ordering::Relaxed);
    source_ready.store(false, Ordering::Relaxed);

    // Join the source task if shutdown was triggered by ctrl-c or the HTTP
    // task, so its senders drop before the worker channels close below.
    if !source_task_handled {
        match source_task.await {
            Ok(Ok(())) => info!("input source stopped"),
            Ok(Err(err)) => {
                source_ready.store(false, Ordering::Relaxed);
                error!(error = %err, "input source failed");
                fatal_err = Some(err);
            }
            Err(join_err) => {
                source_ready.store(false, Ordering::Relaxed);
                error!(error = %join_err, "input source task panicked");
                fatal_err = Some(join_err.into());
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
    drop(quarantine);
    drop(wal);

    if !forwarder_task_handled {
        match tokio::time::timeout(std::time::Duration::from_secs(5), &mut forwarder_task).await {
            Ok(Ok(Ok(()))) => info!("wal forwarder drained cleanly"),
            Ok(Ok(Err(err))) => {
                error!(error = %err, "wal forwarder failed during drain");
                fatal_err.get_or_insert(err);
            }
            Ok(Err(err)) => {
                error!(error = %err, "wal forwarder task failed during drain");
                fatal_err.get_or_insert(err.into());
            }
            Err(_) => {
                warn!("wal forwarder drain timed out after 5s; aborting and exiting");
                forwarder_task.abort();
            }
        }
    }

    info!("ingestion service stopped");

    // A fatal source or forwarder error exits non-zero after workers and the WAL
    // have drained.
    match fatal_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn run_config_check_command() -> Result<bool> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    match args.as_slice() {
        [flag] if flag == "--check-config" => {
            if let Err(error) = check_config() {
                let diagnostic: String = format!("{error:#}").chars().take(1024).collect();
                bail!("configuration check failed: {diagnostic}");
            }
            Ok(true)
        }
        [flag, ..] if flag == "--check-config" => {
            bail!("usage: --check-config");
        }
        _ => Ok(false),
    }
}

fn check_config() -> Result<()> {
    let cfg = Config::from_env()?;
    let route_bundle = config::routes::load(&cfg.routes_file)?;
    let routes = RouteSet::compile(route_bundle, &cfg.dlq_topic)?;
    println!(
        "Configuration is valid ({} routes).",
        routes.subscriptions().len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn forwarder_liveness_guard_marks_task_not_alive_when_dropped() {
        let alive = Arc::new(AtomicBool::new(true));

        drop(ForwarderLivenessGuard(alive.clone()));

        assert!(!alive.load(Ordering::Relaxed));
    }

    #[test]
    fn worker_queue_capacities_preserve_small_total_capacity() {
        assert_eq!(worker_queue_capacities(1, 4), vec![1]);
        assert_eq!(worker_queue_capacities(5, 3), vec![2, 2, 1]);
    }

    #[test]
    fn worker_queue_capacities_sum_to_configured_capacity() {
        let capacities = worker_queue_capacities(17, 8);

        assert_eq!(capacities.iter().sum::<usize>(), 17);
        assert!(capacities.iter().all(|capacity| *capacity > 0));
    }

    #[test]
    fn worker_liveness_guard_marks_workers_dead_when_worker_exits() {
        let ready = Arc::new(AtomicBool::new(true));

        drop(IngestWorkerLivenessGuard(ready.clone()));

        assert!(!ready.load(Ordering::Relaxed));
    }

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
            qos: 1,
            acknowledgement: None,
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
            qos: 1,
            acknowledgement: None,
        })
        .unwrap();
        tx.try_send(IngestJob {
            topic: "smarthome/dev-2/status".to_string(),
            payload: Bytes::from_static(b"{\"device_id\":\"dev-2\"}"),
            received_at_ms: 2,
            receive_sequence: 0,
            retained: true,
            qos: 1,
            acknowledgement: None,
        })
        .unwrap();

        shutdown_tx.send(true).unwrap();

        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_some());
        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_some());
        assert!(recv_worker_job(&mut rx, &mut shutdown_rx).await.is_none());
    }
}
