mod config;
mod infrastructure;
mod model;
mod pipeline;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use config::Config;
use infrastructure::cache::{http, state::CacheState};
use infrastructure::database::influx::InfluxWriter;
use infrastructure::prometheus::MetricsServer;
use infrastructure::router::{Route, Router};
use model::messages::message::MessageType;
use pipeline::{
    context::PipelineContext,
    runner::PipelineRunner,
    stages::{
        cache_update::CacheUpdateStage, decode::DecodeStage, dlq::DlqPublishStage,
        observe::ObserveStage, persist::PersistStage, transform::TransformStage,
        validate_business::ValidateBusinessStage, validate_raw::ValidateRawStage,
    },
};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};

const EVENT_QUEUE_CAPACITY: usize = 16_384;
const INFLUX_QUEUE_CAPACITY: usize = 10_000;

#[derive(Debug)]
struct IngestJob {
    topic: String,
    payload: Vec<u8>,
}

fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let cfg = Config::from_env()?;
    info!(?cfg, "starting ingestion service");

    // ------------------------------------------------------------
    // Cache / HTTP state
    // ------------------------------------------------------------
    let app_state = CacheState::new(cfg.cache_ttl_ms, cfg.cache_buffer);
    let mqtt_ready = Arc::new(AtomicBool::new(false));

    let http_state = app_state.clone();
    let cache_bind = cfg.cache_bind.clone();
    let mqtt_ready_http = mqtt_ready.clone();

    let mut _http_task = tokio::spawn(async move {
        let app = http::router(http_state, mqtt_ready_http);
        let listener = tokio::net::TcpListener::bind(&cache_bind)
            .await
            .expect("failed to bind CACHE_BIND");

        if let Err(err) = axum::serve(listener, app).await {
            error!(error = %err, "HTTP server error");
        }
    });

    // ------------------------------------------------------------
    // Metrics server
    // ------------------------------------------------------------
    let _metrics_server = MetricsServer::start(&cfg.metrics_bind).await?;

    // ------------------------------------------------------------
    // Router / schema routes
    // ------------------------------------------------------------
    let router = Arc::new(build_router(&cfg)?);

    // ------------------------------------------------------------
    // DLQ topic
    // ------------------------------------------------------------
    let dlq_topic = cfg
        .mqtt_topics
        .iter()
        .find(|(k, _)| k.ends_with("DLQ"))
        .map(|(_, v)| v.clone())
        .context("MQTT_TOPIC_DLQ not configured")?;

    // ------------------------------------------------------------
    // MQTT
    // ------------------------------------------------------------
    let mut mqttoptions = MqttOptions::new(&cfg.mqtt_client_id, &cfg.mqtt_host, cfg.mqtt_port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(30));

    if let (Some(username), Some(password)) = (&cfg.mqtt_username, &cfg.mqtt_password) {
        mqttoptions.set_credentials(username, password);
    }

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    for (_, topic) in cfg.mqtt_topics.iter().filter(|(k, _)| !k.ends_with("DLQ")) {
        mqtt_client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "subscribed to MQTT topic");
    }

    // ------------------------------------------------------------
    // Influx batcher
    // ------------------------------------------------------------
    let (influx_tx, influx_rx) = mpsc::channel::<String>(INFLUX_QUEUE_CAPACITY);

    let influx = InfluxWriter::new(
        &cfg.influx_url,
        &cfg.influx_org,
        &cfg.influx_bucket,
        &cfg.influx_token,
    )?;

    let batch_size = cfg.batch_size;
    let flush_interval_ms = cfg.flush_interval_ms;

    let _influx_task = tokio::spawn(async move {
        if let Err(err) = influx
            .run_batcher(influx_rx, batch_size, flush_interval_ms)
            .await
        {
            error!(error = %err, "influx batcher failed");
        }
    });

    // ------------------------------------------------------------
    // Pipeline
    // ------------------------------------------------------------
    let pipeline = Arc::new(
        PipelineRunner::new()
            .add_stage(DecodeStage::new())
            .add_stage(ValidateRawStage::new(
                router.clone(),
                cfg.enforce_topic_device_match,
            ))
            .add_stage(TransformStage::new(router.clone()))
            .add_stage(ValidateBusinessStage::new()?)
            .add_stage(CacheUpdateStage::new(app_state.clone()))
            .add_stage(PersistStage::new(influx_tx.clone()))
            .add_stage(ObserveStage::new())
            .with_failure_stage(DlqPublishStage::new(mqtt_client.clone(), dlq_topic.clone())),
    );

    info!("pipeline initialized");

    // ------------------------------------------------------------
    // Worker queue — one channel per worker, round-robin dispatch
    // ------------------------------------------------------------
    let worker_total = worker_count();
    let per_worker_cap = EVENT_QUEUE_CAPACITY / worker_total;
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
                let job = tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    job = rx.recv() => match job {
                        Some(j) => j,
                        None => break,
                    },
                };

                let mut ctx = PipelineContext::new(job.topic.clone(), job.payload);
                pipeline.run(&mut ctx).await;

                if let Some(reason) = ctx.ignored_reason() {
                    info!(worker_id, topic = %job.topic, reason = %reason, "message ignored by pipeline");
                }
            }

            info!(worker_id, "pipeline worker stopped");
            Ok::<(), anyhow::Error>(())
        });
    }

    // ------------------------------------------------------------
    // Main consume loop
    // ------------------------------------------------------------
    let mut dispatch_idx: usize = 0;
    loop {
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                match res {
                    Ok(()) => info!("shutdown signal received"),
                    Err(err) => error!(error = %err, "failed to listen for ctrl-c"),
                }
                let _ = shutdown_tx.send(true);
                break;
            }

            res = &mut _http_task => {
                match res {
                    Ok(()) => error!("HTTP server stopped unexpectedly"),
                    Err(err) => error!(error = %err, "HTTP server task panicked"),
                }
                let _ = shutdown_tx.send(true);
                break;
            }

            event = eventloop.poll() => {
                let event = match event {
                    Ok(ev) => ev,
                    Err(err) => {
                        mqtt_ready.store(false, Ordering::Relaxed);
                        return Err(err).context("MQTT poll failed");
                    }
                };

                match &event {
                    Event::Incoming(Incoming::ConnAck(_)) => {
                        mqtt_ready.store(true, Ordering::Relaxed);
                        info!("MQTT connected");
                    }
                    Event::Incoming(Incoming::Disconnect) => {
                        mqtt_ready.store(false, Ordering::Relaxed);
                    }
                    _ => {}
                }

                if let Event::Incoming(Incoming::Publish(publish)) = event {
                    let job = IngestJob {
                        topic: publish.topic,
                        payload: publish.payload.to_vec(),
                    };

                    let idx = dispatch_idx % worker_total;
                    dispatch_idx = dispatch_idx.wrapping_add(1);
                    if let Err(err) = job_txs[idx].try_send(job) {
                        metrics::counter!("ingest_event_queue_full_total").increment(1);
                        warn!(error = %err, "event queue full; dropping incoming MQTT message before pipeline");
                    }
                }
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

    info!("ingestion service stopped");
    Ok(())
}

fn build_router(cfg: &Config) -> Result<Router> {
    let mut router = Router::new().strict(true);

    for (key, topic) in cfg
        .mqtt_topics
        .iter()
        .filter(|(k, _)| k.starts_with("MQTT_TOPIC_"))
    {
        let message_type_name = key
            .strip_prefix("MQTT_TOPIC_")
            .expect("prefix already filtered")
            .to_uppercase();

        let message_type = match message_type_name.as_str() {
            "SENSOR" => MessageType::Sensor,
            "STATUS" => MessageType::Status,
            "DLQ" => continue,
            other => {
                warn!(config_key = %key, message_type = %other, "unknown MQTT topic config key; skipping");
                continue;
            }
        };

        let schema = match message_type_name.as_str() {
            "SENSOR" => include_str!("../schema/sensor.schema.json"),
            "STATUS" => include_str!("../schema/status.schema.json"),
            _ => unreachable!(),
        };

        router = router.add_route(Route::new(message_type, schema, topic)?);

        info!(
            config_key = %key,
            message_type = %message_type_name,
            topic = %topic,
            "configured route for MQTT topic"
        );
    }

    Ok(router)
}
