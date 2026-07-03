use anyhow::{Context, Result};
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::{net::SocketAddr, time::Duration};
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Background Prometheus exporter for Telegraf scraping.
///
/// Exposes `GET /metrics` and installs the global `metrics` recorder.
pub struct MetricsServer {
    _http_task: JoinHandle<()>,
    _upkeep_task: JoinHandle<()>,
}

impl MetricsServer {
    pub async fn start(bind: &str) -> Result<Self> {
        let handle: PrometheusHandle = PrometheusBuilder::new()
            .install_recorder()
            .context("failed to build Prometheus recorder")?;

        let upkeep_handle = handle.clone();
        let upkeep_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                upkeep_handle.run_upkeep();
            }
        });

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(handle);

        let address: SocketAddr = bind
            .parse()
            .context("METRICS_BIND must be a socket address like 0.0.0.0:9090")?;

        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("failed to bind metrics server to {}", address))?;

        info!(%address, "metrics endpoint listening");
        let http_task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                error!(%e, "metrics server failed");
            }
        });

        Ok(Self {
            _http_task: http_task,
            _upkeep_task: upkeep_task,
        })
    }
}

async fn metrics_handler(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    let body = handle.render();

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
