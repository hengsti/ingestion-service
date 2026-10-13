use anyhow::{Context, Result};
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use metrics::{counter, gauge};
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
        register_metrics();

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

const COUNTERS: &[&str] = &[
    "mqtt_messages_received_total",
    "ingest_event_queue_full_total",
    "ingest_decode_success_total",
    "ingest_incoming_oversized_total",
    "ingest_incoming_non_utf8_total",
    "ingest_incoming_invalid_json_total",
    "ingest_validate_raw_success_total",
    "ingest_validate_raw_ignored_total",
    "ingest_validate_raw_failed_total",
    "ingest_transform_attempt_total",
    "ingest_transform_success_total",
    "ingest_transform_failed_total",
    "ingest_transform_deserialize_failed_total",
    "ingest_transform_ignored_total",
    "dlq_messages_published_total",
    "dlq_publish_errors_total",
    "ingest_sensor_messages_processed_total",
    "ingest_status_messages_processed_total",
    "influx_lines_written_total",
    "influx_write_success_total",
    "influx_write_errors_total",
    "wal_forwarder_committed_total",
    "wal_forwarder_drop_total",
    "wal_forwarder_retry_total",
    "wal_forwarder_commit_retry_total",
    "wal_subscription_corrupt_skipped_total",
    "wal_segment_rotations_total",
];

const KIND_COUNTERS: &[&str] = &[
    "ingest_validate_business_success_total",
    "ingest_validate_business_failed_total",
    "ingest_cache_updates_total",
    "ingest_messages_enqueued_total",
    "ingest_queue_full_total",
    "ingest_queue_closed_total",
    "ingest_durability_ack_failed_total",
    "ingest_messages_processed_total",
];

const WAL_WRITER_FATAL_REASONS: &[&str] =
    &["flush_before_rotation", "open_next_segment", "write_all"];

fn register_metrics() {
    for &name in COUNTERS {
        counter!(name).absolute(0);
    }
    for &name in KIND_COUNTERS {
        for kind in ["sensor", "status"] {
            counter!(name, "kind" => kind).absolute(0);
        }
    }
    for &reason in WAL_WRITER_FATAL_REASONS {
        counter!("wal_writer_fatal_total", "reason" => reason).absolute(0);
    }
    gauge!("wal_forwarder_retry_outage_active").set(0.0);
}

async fn metrics_handler(State(handle): State<PrometheusHandle>) -> impl IntoResponse {
    let body = handle.render();

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_metrics_exports_full_inventory_with_zero_values() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, register_metrics);
        let body = handle.render();

        for &name in COUNTERS {
            assert!(body.contains(&format!("\n{name} 0\n")), "missing {name}");
        }
        for &name in KIND_COUNTERS {
            for kind in ["sensor", "status"] {
                assert!(
                    body.contains(&format!("{name}{{kind=\"{kind}\"}} 0\n")),
                    "missing {name}{{kind={kind}}}"
                );
            }
        }
        for &reason in WAL_WRITER_FATAL_REASONS {
            assert!(body.contains(&format!(
                "wal_writer_fatal_total{{reason=\"{reason}\"}} 0\n"
            )));
        }
        assert!(body.contains("\nwal_forwarder_retry_outage_active 0\n"));
    }
}
