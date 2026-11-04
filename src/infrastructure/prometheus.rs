use anyhow::{Context, Result};
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::infrastructure::{router::RouteSet, wal::capacity::Capacity};

#[derive(Clone)]
struct MetricsState {
    handle: PrometheusHandle,
    wal: Arc<Capacity>,
    quarantine: Arc<Capacity>,
}

/// Background Prometheus exporter for Telegraf scraping.
///
/// Exposes `GET /metrics` and installs the global `metrics` recorder.
pub struct MetricsServer {
    _http_task: JoinHandle<()>,
    _upkeep_task: JoinHandle<()>,
}

impl MetricsServer {
    /// Starts the metrics endpoint with live journal capacity snapshots.
    ///
    /// # Errors
    /// Returns an error if the metrics recorder or listening socket cannot be initialized.
    pub async fn start(
        bind: &str,
        routes: &RouteSet,
        wal: Arc<Capacity>,
        quarantine: Arc<Capacity>,
    ) -> Result<Self> {
        let handle: PrometheusHandle = PrometheusBuilder::new()
            .install_recorder()
            .context("failed to build Prometheus recorder")?;
        register_metrics(routes);

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
            .with_state(MetricsState {
                handle,
                wal,
                quarantine,
            });

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
    "ingest_validate_raw_failed_total",
    "ingest_transform_success_total",
    "dlq_messages_published_total",
    "dlq_publish_errors_total",
    "influx_lines_written_total",
    "influx_write_success_total",
    "influx_write_errors_total",
    "wal_forwarder_committed_total",
    "wal_forwarder_retry_total",
    "wal_forwarder_commit_retry_total",
    "wal_subscription_corrupt_skipped_total",
    "wal_segment_rotations_total",
    "wal_capacity_full_total",
];

const ROUTE_COUNTERS: &[&str] = &[
    "ingest_cache_updates_total",
    "ingest_messages_enqueued_total",
    "ingest_queue_full_total",
    "ingest_queue_closed_total",
    "ingest_durability_ack_failed_total",
    "ingest_messages_processed_total",
];

const WAL_WRITER_FATAL_REASONS: &[&str] =
    &["flush_before_rotation", "open_next_segment", "write_all"];

fn register_metrics(routes: &RouteSet) {
    for &name in COUNTERS {
        counter!(name).absolute(0);
    }
    for route in routes.metric_labels() {
        for &name in ROUTE_COUNTERS {
            counter!(name, "route" => route.clone()).absolute(0);
        }
    }
    for &reason in WAL_WRITER_FATAL_REASONS {
        counter!("wal_writer_fatal_total", "reason" => reason).absolute(0);
    }
    gauge!("wal_forwarder_retry_outage_active").set(0.0);
}

async fn metrics_handler(State(state): State<MetricsState>) -> impl IntoResponse {
    record_journal_capacity(&state.wal, &state.quarantine);
    let body = state.handle.render();

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn record_journal_capacity(wal: &Capacity, quarantine: &Capacity) {
    let (wal_used, wal_max) = wal.snapshot();
    let (quarantine_used, quarantine_max) = quarantine.snapshot();
    gauge!("wal_accounted_bytes").set(wal_used as f64);
    gauge!("wal_capacity_bytes").set(wal_max as f64);
    gauge!("quarantine_accounted_bytes").set(quarantine_used as f64);
    gauge!("quarantine_capacity_bytes").set(quarantine_max as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_capacity_metrics_export_live_journal_snapshots() -> Result<()> {
        let wal_dir = tempfile::tempdir()?;
        let quarantine_dir = tempfile::tempdir()?;
        let wal = Capacity::recover(wal_dir.path(), 1024 * 1024)?;
        let quarantine = Capacity::recover(quarantine_dir.path(), 2 * 1024 * 1024)?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || record_journal_capacity(&wal, &quarantine));
        let body = handle.render();
        assert!(body.contains(&format!("wal_accounted_bytes {}\n", wal.snapshot().0)));
        assert!(body.contains("wal_capacity_bytes 1048576\n"));
        assert!(body.contains(&format!(
            "quarantine_accounted_bytes {}\n",
            quarantine.snapshot().0
        )));
        assert!(body.contains("quarantine_capacity_bytes 2097152\n"));
        Ok(())
    }

    #[test]
    fn register_metrics_exports_full_inventory_with_zero_values() -> Result<()> {
        let routes = RouteSet::compile(
            crate::config::routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
        )?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || register_metrics(&routes));
        let body = handle.render();

        for &name in COUNTERS {
            assert!(body.contains(&format!("\n{name} 0\n")), "missing {name}");
        }
        assert!(!body.contains("kind="));
        for &reason in WAL_WRITER_FATAL_REASONS {
            assert!(body.contains(&format!(
                "wal_writer_fatal_total{{reason=\"{reason}\"}} 0\n"
            )));
        }
        assert!(body.contains("\nwal_forwarder_retry_outage_active 0\n"));
        Ok(())
    }

    #[test]
    fn test_register_metrics_exports_configured_route_counters_at_zero() -> Result<()> {
        let bundle = crate::config::routes::load("config/routes.json")?;
        let route_ids: Vec<_> = bundle.routes.iter().map(|route| route.id.clone()).collect();
        let routes = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || register_metrics(&routes));
        let body = handle.render();

        for route in &route_ids {
            for name in [
                "ingest_cache_updates_total",
                "ingest_messages_enqueued_total",
                "ingest_queue_full_total",
                "ingest_queue_closed_total",
                "ingest_durability_ack_failed_total",
                "ingest_messages_processed_total",
            ] {
                assert!(
                    body.contains(&format!("{name}{{route=\"{route}\"}} 0\n")),
                    "missing {name} for route {route}"
                );
            }
        }

        let route = route_ids.first().context("configured route is missing")?;
        metrics::with_local_recorder(&recorder, || {
            counter!("ingest_queue_full_total", "route" => route.clone()).increment(1);
        });
        assert!(handle
            .render()
            .contains(&format!("ingest_queue_full_total{{route=\"{route}\"}} 1\n")));
        Ok(())
    }

    #[test]
    fn test_route_metrics_preserve_distinct_ids_with_backslashes_and_quotes() -> Result<()> {
        for ids in [["sensor\\", "sensor\\\\"], ["sensor\"", "sensor\\\""]] {
            let mut bundle = crate::config::routes::load("config/routes.json")?;
            for (route, id) in bundle.routes.iter_mut().zip(ids) {
                route.id = id.to_owned();
            }
            let routes = RouteSet::compile(bundle, "smarthome/_dlq/ingest")?;
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();

            metrics::with_local_recorder(&recorder, || register_metrics(&routes));
            let sensor = routes
                .select("smarthome/device/bme680")
                .context("sensor route is missing")?;
            metrics::with_local_recorder(&recorder, || {
                counter!("ingest_messages_processed_total", "route" => sensor.metric_label())
                    .increment(1);
            });
            let body = handle.render();

            for (id, value) in ids.into_iter().zip([1, 0]) {
                let escaped = id.replace('\\', "\\\\").replace('"', "\\\"");
                assert!(
                    body.contains(&format!(
                        "ingest_messages_processed_total{{route=\"{escaped}\"}} {value}\n"
                    )),
                    "incorrect or colliding route label {id:?}: {body}"
                );
            }
        }
        Ok(())
    }
}
