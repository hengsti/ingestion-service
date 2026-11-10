use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::infrastructure::{operations::Operations, router::RouteSet, wal::capacity::Capacity};
use crate::pipeline::runner::{STAGE_LABELS, STAGE_OUTCOMES};

#[derive(Clone)]
struct MetricsState {
    handle: PrometheusHandle,
    operations: Arc<Operations>,
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
    pub async fn start(bind: &str, routes: &RouteSet, operations: Arc<Operations>) -> Result<Self> {
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
            .with_state(MetricsState { handle, operations });

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
    "mqtt_qos0_policy_violations_total",
    "mqtt_unsupported_qos2_total",
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
    "wal_forwarder_delivered_total",
    "quarantine_appends_total",
    "wal_forwarder_retry_total",
    "wal_forwarder_commit_retry_total",
    "wal_segment_rotations_total",
    "wal_capacity_full_total",
];

const ROUTE_COUNTERS: &[&str] = &[
    "ingest_cache_updates_total",
    "ingest_messages_enqueued_total",
    "ingest_queue_full_total",
    "ingest_queue_closed_total",
    "ingest_durability_ack_failed_total",
];

const PIPELINE_OUTCOMES: &[&str] = &["output_stored", "rejection_stored", "deferred"];

const WAL_WRITER_FATAL_REASONS: &[&str] = &[
    "flush_or_sync",
    "flush_before_rotation",
    "open_next_segment",
    "write_all",
];

fn register_metrics(routes: &RouteSet) {
    for &name in COUNTERS {
        counter!(name).absolute(0);
    }
    for route in routes.metric_labels() {
        for &name in ROUTE_COUNTERS {
            counter!(name, "route" => route.clone()).absolute(0);
        }
    }
    for route in routes
        .metric_labels()
        .chain(std::iter::once(Arc::<str>::from("")))
    {
        for &outcome in PIPELINE_OUTCOMES {
            counter!("ingest_pipeline_outcomes_total", "route" => route.clone(), "outcome" => outcome).absolute(0);
        }
        for &stage in STAGE_LABELS {
            for &outcome in STAGE_OUTCOMES {
                counter!("ingest_stage_outcomes_total", "route" => route.clone(), "stage" => stage, "outcome" => outcome).absolute(0);
            }
        }
    }
    for &reason in WAL_WRITER_FATAL_REASONS {
        counter!("wal_writer_fatal_total", "reason" => reason).absolute(0);
    }
    gauge!("wal_forwarder_retry_outage_active").set(0.0);
    for state in ["healthy", "retrying", "blocked"] {
        gauge!("ingest_sink_state", "state" => state).set(0.0);
    }
}

async fn metrics_handler(State(state): State<MetricsState>) -> impl IntoResponse {
    record_operational_state(&state.operations);
    let body = state.handle.render();

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn record_operational_state(state: &Operations) {
    record_journal_capacity(&state.wal, &state.quarantine);
    gauge!("ingest_ready").set(f64::from(state.ready()));
    gauge!("ingest_source_ready").set(f64::from(state.source_ready.load(Ordering::Relaxed)));
    gauge!("ingest_workers_alive").set(f64::from(state.workers_alive.load(Ordering::Relaxed)));
    gauge!("ingest_admission_pressure").set(f64::from(!state.admission_available()));
    let (available, capacity) = state.queue_snapshot();
    gauge!("ingest_queue_available").set(available as f64);
    gauge!("ingest_queue_capacity").set(capacity as f64);
    gauge!("mqtt_pending_acks").set(state.pending_acks.load(Ordering::Relaxed) as f64);
    gauge!("wal_writable").set(f64::from(state.wal.is_writable()));
    gauge!("quarantine_writable").set(f64::from(state.quarantine.is_writable()));
    gauge!("wal_forwarder_alive").set(f64::from(state.forwarder.alive.load(Ordering::Relaxed)));
    gauge!("wal_forwarder_read_failed").set(f64::from(
        state.forwarder.read_failed.load(Ordering::Relaxed),
    ));
    gauge!("wal_forwarder_commit_retrying").set(f64::from(
        state.forwarder.commit_retrying.load(Ordering::Relaxed),
    ));
    gauge!("wal_oldest_pending_output_age_seconds").set(
        state
            .forwarder
            .oldest_pending_age_seconds(chrono::Utc::now().timestamp_millis()),
    );
    let current = state.forwarder.sink().label();
    for sink in ["healthy", "retrying", "blocked"] {
        gauge!("ingest_sink_state", "state" => sink).set(f64::from(sink == current));
    }
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
    fn test_operational_metrics_recover_without_unbounded_labels() -> Result<()> {
        use crate::infrastructure::operations::{ForwarderState, SinkState};
        use std::sync::atomic::AtomicBool;

        let directory = tempfile::tempdir()?;
        let capacity = Capacity::recover(directory.path(), 128 * 1024 * 1024)?;
        let forwarder = Arc::new(ForwarderState::default());
        forwarder.alive.store(true, Ordering::Relaxed);
        let operations = Operations::new(
            Arc::new(AtomicBool::new(true)),
            forwarder.clone(),
            capacity.clone(),
            capacity,
        );
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        operations.update_admission(0, 8, 3);
        forwarder.set_sink(SinkState::Blocked);
        metrics::with_local_recorder(&recorder, || record_operational_state(&operations));
        let body = handle.render();
        assert!(body.contains("mqtt_pending_acks 3\n"));
        assert!(body.contains("ingest_admission_pressure 1\n"));
        assert!(body.contains("ingest_sink_state{state=\"blocked\"} 1\n"));
        assert!(body.contains("ingest_ready 0\n"));

        operations.update_admission(8, 8, 0);
        forwarder.set_sink(SinkState::Healthy);
        metrics::with_local_recorder(&recorder, || record_operational_state(&operations));
        let body = handle.render();
        assert!(body.contains("mqtt_pending_acks 0\n"));
        assert!(body.contains("ingest_admission_pressure 0\n"));
        assert!(body.contains("ingest_sink_state{state=\"blocked\"} 0\n"));
        assert!(body.contains("ingest_sink_state{state=\"healthy\"} 1\n"));
        assert!(body.contains("ingest_ready 1\n"));
        for forbidden in ["topic=", "device_id=", "error=", "token=", "timestamp="] {
            assert!(!body.contains(forbidden));
        }
        Ok(())
    }

    #[test]
    fn register_metrics_exports_full_inventory_with_zero_values() -> Result<()> {
        let routes = RouteSet::compile(
            crate::config::routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
            "test_bucket",
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
    fn test_operational_metrics_register_bounded_sink_and_stage_states() -> Result<()> {
        let routes = RouteSet::compile(
            crate::config::routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
            "test_bucket",
        )?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || register_metrics(&routes));
        let body = handle.render();

        assert!(body.contains("quarantine_appends_total 0\n"));
        assert!(body.contains("wal_forwarder_delivered_total 0\n"));
        for state in ["healthy", "retrying", "blocked"] {
            assert!(body.contains(&format!("ingest_sink_state{{state=\"{state}\"}}")));
        }
        assert!(body.contains("ingest_stage_outcomes_total{"));
        assert!(body.contains("route=\"firmware-sensor\""));
        assert!(!body.contains("kind="));
        Ok(())
    }

    #[test]
    fn register_metrics_exports_terminal_outcome_counters_at_zero() -> Result<()> {
        let routes = RouteSet::compile(
            crate::config::routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
            "test_bucket",
        )?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || register_metrics(&routes));
        let body = handle.render();

        for outcome in ["output_stored", "rejection_stored", "deferred"] {
            assert!(
                body.contains(&format!(
                    "ingest_pipeline_outcomes_total{{route=\"\",outcome=\"{outcome}\"}} 0\n"
                )),
                "missing terminal outcome metric {outcome}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_register_metrics_exports_configured_route_counters_at_zero() -> Result<()> {
        let bundle = crate::config::routes::load("config/routes.json")?;
        let route_ids: Vec<_> = bundle.routes.iter().map(|route| route.id.clone()).collect();
        let routes = RouteSet::compile(bundle, "smarthome/_dlq/ingest", "test_bucket")?;
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
            let routes = RouteSet::compile(bundle, "smarthome/_dlq/ingest", "test_bucket")?;
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();

            metrics::with_local_recorder(&recorder, || register_metrics(&routes));
            let sensor = routes
                .select("smarthome/device/bme680")
                .context("sensor route is missing")?;
            metrics::with_local_recorder(&recorder, || {
                counter!("ingest_queue_full_total", "route" => sensor.metric_label()).increment(1);
            });
            let body = handle.render();

            for (id, value) in ids.into_iter().zip([1, 0]) {
                let escaped = id.replace('\\', "\\\\").replace('"', "\\\"");
                assert!(
                    body.contains(&format!(
                        "ingest_queue_full_total{{route=\"{escaped}\"}} {value}\n"
                    )),
                    "incorrect or colliding route label {id:?}: {body}"
                );
            }
        }
        Ok(())
    }
}
