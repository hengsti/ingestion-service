use std::sync::Arc;
use std::{future::Future, pin::Pin, time::Instant};

use anyhow::Context;
use tokio::sync::watch;
use tracing::warn;

use metrics::{counter, histogram};

use crate::{
    infrastructure::sink::Encoder,
    infrastructure::wal::{
        codec::wal_record_frame_size,
        types::{AppendDurableError, WalEvent},
        wal::Wal,
    },
    pipeline::{
        context::PipelineContext,
        outcome::DeferredReason,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct PersistStage {
    wal: Arc<Wal>,
    encoder: Arc<dyn Encoder>,
    forwarder_stopped: Option<watch::Receiver<bool>>,
}

impl PersistStage {
    pub fn new(wal: Arc<Wal>, encoder: Arc<dyn Encoder>) -> Self {
        Self {
            wal,
            encoder,
            forwarder_stopped: None,
        }
    }

    /// Stops capacity waits when the WAL forwarder terminates.
    pub fn with_forwarder_stopped(mut self, stopped: watch::Receiver<bool>) -> Self {
        self.forwarder_stopped = Some(stopped);
        self
    }
}

async fn wait_for_progress(
    notification: impl Future<Output = ()>,
    forwarder_stopped: Option<&mut watch::Receiver<bool>>,
) -> bool {
    match forwarder_stopped {
        Some(receiver) if *receiver.borrow() => true,
        Some(receiver) => tokio::select! {
            _ = notification => false,
            changed = receiver.changed() => changed.is_err() || *receiver.borrow(),
        },
        None => {
            notification.await;
            false
        }
    }
}

impl PipelineStage for PersistStage {
    fn name(&self) -> &'static str {
        "persist"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();
            let mut forwarder_stopped = self.forwarder_stopped.clone();

            let route_id = ctx.route()?.metric_label();
            let record = ctx.telemetry_record()?;
            let mut payload = String::new();
            self.encoder.encode(record, &mut payload);
            wal_record_frame_size(ctx.topic().len(), payload.len())
                .context("encoded telemetry exceeds the WAL reader record limit")?;

            let mut event = WalEvent {
                topic: ctx.topic().to_string(),
                ts_ms: chrono::Utc::now().timestamp_millis(),
                payload,
            };

            let mut capacity_warned = false;
            #[cfg(all(feature = "integration-test-hooks", debug_assertions))]
            crate::test_hooks::barrier("before_append").await?;
            loop {
                let notified = self.wal.capacity_notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let writer_notified = self.wal.writer_notified();
                tokio::pin!(writer_notified);
                if capacity_warned {
                    writer_notified.as_mut().enable();
                }
                match self.wal.append_durable(event).await {
                    Ok(()) => {
                        #[cfg(all(feature = "integration-test-hooks", debug_assertions))]
                        crate::test_hooks::barrier("after_sync").await?;
                        ctx.mark_output_stored();
                        counter!("ingest_messages_enqueued_total", "route" => route_id.clone())
                            .increment(1);
                        histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "success")
                        .record(start.elapsed().as_secs_f64());

                        return Ok(StageFlow::Continue);
                    }
                    Err(AppendDurableError::Capacity(rejected)) => {
                        if !capacity_warned {
                            counter!("wal_capacity_full_total").increment(1);
                            warn!("wal capacity exhausted; waiting for committed segments to be reclaimed");
                            capacity_warned = true;
                        }
                        event = rejected;
                        if wait_for_progress(notified.as_mut(), forwarder_stopped.as_mut()).await {
                            warn!(topic = %ctx.topic(), "wal forwarder stopped; deferring message");
                            ctx.mark_deferred(DeferredReason::PersistenceFailure);
                            histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "forwarder_stopped")
                                .record(start.elapsed().as_secs_f64());
                            return Ok(StageFlow::Stop);
                        }
                    }
                    Err(AppendDurableError::Full(rejected)) if capacity_warned => {
                        event = rejected;
                        // Failed queue admission releases its reservation and wakes capacity waiters.
                        if wait_for_progress(writer_notified.as_mut(), forwarder_stopped.as_mut())
                            .await
                        {
                            warn!(topic = %ctx.topic(), "wal forwarder stopped; deferring message");
                            ctx.mark_deferred(DeferredReason::PersistenceFailure);
                            histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "forwarder_stopped")
                                .record(start.elapsed().as_secs_f64());
                            return Ok(StageFlow::Stop);
                        }
                    }
                    Err(AppendDurableError::Full(_)) => {
                        counter!("ingest_queue_full_total", "route" => route_id.clone())
                            .increment(1);
                        warn!(topic = %ctx.topic(), "wal queue full; deferring message");
                        ctx.mark_deferred(DeferredReason::PersistenceFailure);
                        histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "queue_full")
                        .record(start.elapsed().as_secs_f64());

                        return Ok(StageFlow::Stop);
                    }
                    Err(AppendDurableError::Closed(_)) => {
                        counter!("ingest_queue_closed_total", "route" => route_id.clone())
                            .increment(1);
                        warn!(topic = %ctx.topic(), "wal queue closed; deferring message");
                        ctx.mark_deferred(DeferredReason::PersistenceFailure);
                        histogram!("ingest_persist_duration_seconds", "route" => route_id.clone(), "result" => "queue_closed")
                        .record(start.elapsed().as_secs_f64());

                        return Ok(StageFlow::Stop);
                    }
                    Err(AppendDurableError::TooLarge(_)) => {
                        anyhow::bail!("encoded telemetry exceeds the WAL reader record limit")
                    }
                    Err(AppendDurableError::Durability(_)) => {
                        counter!("ingest_durability_ack_failed_total", "route" => route_id.clone())
                            .increment(1);
                        warn!(topic = %ctx.topic(), "wal durability ack failed; deferring message");
                        ctx.mark_deferred(DeferredReason::PersistenceFailure);
                        histogram!("ingest_persist_duration_seconds", "route" => route_id, "result" => "durability_failed")
                        .record(start.elapsed().as_secs_f64());
                        return Ok(StageFlow::Stop);
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{mpsc, Mutex, OnceLock};
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::routes,
        infrastructure::router::{CompiledRoute, RouteSet},
        infrastructure::wal::{
            segment::segment_path,
            subscription::WalSubscription,
            test_support::TestIoStep,
            types::{WalEvent, WalOptions},
        },
        model::messages::telemetry::{TelemetryRecord, TelemetryValue},
        pipeline::outcome::DeferredReason,
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    fn telemetry_record() -> TelemetryRecord {
        TelemetryRecord {
            route_id: "firmware-sensor".to_owned(),
            device_id: "esp32-1".to_owned(),
            measurement: "test_measurement".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: std::collections::BTreeMap::from([
                ("device_id".to_owned(), "esp32-1".to_owned()),
                ("room".to_owned(), "living_room".to_owned()),
            ]),
            fields: std::collections::BTreeMap::from([
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
                ("temp_c".to_owned(), TelemetryValue::Float(22.5)),
            ]),
            source_topic: "smarthome/esp32-1/bme680".to_owned(),
            retained: false,
        }
    }

    fn ctx_with_record() -> PipelineContext {
        static ROUTE: OnceLock<Arc<CompiledRoute>> = OnceLock::new();
        let route = ROUTE.get_or_init(|| {
            let routes = RouteSet::compile(
                routes::load("config/routes.json").expect("load routes"),
                "smarthome/_dlq/ingest",
            )
            .expect("compile routes");
            routes
                .select("smarthome/esp32-1/bme680")
                .expect("select sensor route")
        });
        let mut ctx = PipelineContext::new("smarthome/esp32-1/bme680", vec![]);
        ctx.set_route(route.clone());
        ctx.set_telemetry_record(telemetry_record());
        ctx
    }

    fn expected_line_protocol() -> String {
        let mut out = String::new();
        crate::infrastructure::sink::influx::InfluxEncoder.encode(&telemetry_record(), &mut out);
        out
    }

    async fn open_wal(dir: &std::path::Path, queue_capacity: usize) -> (Arc<Wal>, WalSubscription) {
        let (wal, sub) = Wal::open(WalOptions {
            dir: dir.to_path_buf(),
            max_bytes: 128 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity,
        })
        .await
        .expect("wal open");
        (Arc::new(wal), sub)
    }

    async fn recv_one(sub: &mut WalSubscription, ms: u64) -> Option<WalEvent> {
        match tokio::time::timeout(Duration::from_millis(ms), sub.next()).await {
            Ok(Ok(Some(entry))) => Some(entry.event),
            Ok(Ok(None)) | Err(_) => None,
            Ok(Err(error)) => panic!("WAL subscription failed: {error:#}"),
        }
    }

    #[tokio::test]
    async fn run_on_telemetry_record_appends_to_wal_and_returns_continue() {
        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);
        let mut ctx = ctx_with_record();

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_quarantine());
        assert!(ctx.output_stored());

        let event = recv_one(&mut sub, 500).await.expect("event should arrive");
        assert_eq!(event.topic, "smarthome/esp32-1/bme680");
        assert_eq!(event.payload, expected_line_protocol());
    }

    #[tokio::test]
    async fn test_wal_capacity_persist_waits_for_reclaim_without_routing_to_dlq(
    ) -> anyhow::Result<()> {
        let dir = tempdir()?;
        let (wal, mut sub) = Wal::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: 3 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity: 8,
        })
        .await?;
        let wal = Arc::new(wal);
        let event = WalEvent {
            topic: "t".to_string(),
            ts_ms: 1,
            payload: "x"
                .repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES - 25),
        };
        wal.append_durable(event.clone())
            .await
            .expect("first append");
        wal.append_durable(event).await.expect("second append");
        let (used, max) = wal.capacity_bytes();
        let frame_bytes =
            (max - used - crate::infrastructure::wal::capacity::SEGMENT_ALLOWANCE_BYTES) as usize;
        wal.append_durable(WalEvent {
            topic: "t".to_string(),
            ts_ms: 2,
            payload: "x".repeat(frame_bytes - 29),
        })
        .await
        .expect("fill remaining capacity");

        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(Arc::clone(&wal), encoder);
        let mut ctx = ctx_with_record();
        let mut run = stage.run(&mut ctx);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), run.as_mut())
                .await
                .is_err()
        );
        let first = sub
            .next()
            .await
            .context("reading first record")?
            .context("first record")?;
        let second = sub
            .next()
            .await
            .context("reading second record")?
            .context("second record")?;
        sub.commit(first.offset_after).await?;
        sub.commit(second.offset_after).await?;
        let result = tokio::time::timeout(Duration::from_secs(2), run.as_mut()).await?;
        assert!(matches!(result, Ok(StageFlow::Continue)));
        drop(run);
        assert!(!ctx.should_quarantine());
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_wait_stops_when_forwarder_exits() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let (wal, _sub) = Wal::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: 3 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity: 8,
        })
        .await?;
        let wal = Arc::new(wal);
        let event = WalEvent {
            topic: "t".to_owned(),
            ts_ms: 1,
            payload: "x"
                .repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES - 25),
        };
        wal.append_durable(event.clone())
            .await
            .expect("first append");
        wal.append_durable(event).await.expect("second append");
        let (used, max) = wal.capacity_bytes();
        let frame_bytes =
            (max - used - crate::infrastructure::wal::capacity::SEGMENT_ALLOWANCE_BYTES) as usize;
        wal.append_durable(WalEvent {
            topic: "t".to_owned(),
            ts_ms: 2,
            payload: "x".repeat(frame_bytes - 29),
        })
        .await
        .expect("fill remaining capacity");

        let (forwarder_stopped_tx, forwarder_stopped_rx) = tokio::sync::watch::channel(false);
        let stage = PersistStage::new(
            wal,
            Arc::new(crate::infrastructure::sink::influx::InfluxEncoder),
        )
        .with_forwarder_stopped(forwarder_stopped_rx);
        let mut ctx = ctx_with_record();
        let mut run = stage.run(&mut ctx);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), run.as_mut())
                .await
                .is_err()
        );

        forwarder_stopped_tx.send(true)?;
        let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut()).await?;
        drop(run);

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert_eq!(
            ctx.deferred_reason(),
            Some(DeferredReason::PersistenceFailure)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_wal_capacity_retried_event_survives_a_full_writer_queue() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let (wal, mut sub) = Wal::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: 3 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity: 1,
        })
        .await?;
        let wal = Arc::new(wal);
        let large = WalEvent {
            topic: "t".to_owned(),
            ts_ms: 1,
            payload: "x"
                .repeat(crate::infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES - 25),
        };
        wal.append_durable(large.clone())
            .await
            .expect("first append");
        wal.append_durable(large).await.expect("second append");
        let (used, max) = wal.capacity_bytes();
        let frame_bytes =
            (max - used - crate::infrastructure::wal::capacity::SEGMENT_ALLOWANCE_BYTES) as usize;
        wal.append_durable(WalEvent {
            topic: "t".to_owned(),
            ts_ms: 2,
            payload: "x".repeat(frame_bytes - 29),
        })
        .await
        .expect("fill remaining capacity");

        let stage = PersistStage::new(
            Arc::clone(&wal),
            Arc::new(crate::infrastructure::sink::influx::InfluxEncoder),
        );
        let mut ctx = ctx_with_record();
        let mut run = stage.run(&mut ctx);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), run.as_mut())
                .await
                .is_err()
        );

        let first = sub
            .next()
            .await
            .context("reading first record")?
            .context("first record")?;
        let second = sub
            .next()
            .await
            .context("reading second record")?
            .context("second record")?;
        sub.commit(first.offset_after).await?;
        sub.commit(second.offset_after).await?;

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = Mutex::new(Some(entered_tx));
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        *wal.test_io_hook.lock().expect("WAL hook lock") = Some(Arc::new(move |step| {
            if step == TestIoStep::Write {
                if let Some(tx) = entered_tx.lock().expect("entered lock").take() {
                    let _ = tx.send(());
                    release_rx
                        .lock()
                        .expect("release lock")
                        .recv_timeout(Duration::from_secs(5))
                        .expect("release stalled writer");
                }
            }
            Ok(())
        }));
        let small = WalEvent {
            topic: "queue".to_owned(),
            ts_ms: 3,
            payload: "value".to_owned(),
        };
        wal.try_append(small.clone()).expect("writer append");
        tokio::time::timeout(Duration::from_secs(2), entered_rx).await??;
        wal.try_append(small).expect("fill writer queue");

        let while_full = tokio::time::timeout(Duration::from_millis(100), run.as_mut()).await;
        release_tx.send(())?;
        assert!(
            while_full.is_err(),
            "an event deferred by capacity must not be sent to DLQ when the writer queue is full"
        );
        let result = tokio::time::timeout(Duration::from_secs(2), run.as_mut()).await?;
        assert!(matches!(result, Ok(StageFlow::Continue)));
        drop(run);
        assert!(!ctx.should_quarantine());
        Ok(())
    }

    #[tokio::test]
    async fn test_persist_rejects_oversized_encoded_payload_before_wal_append() {
        struct OversizedEncoder;

        impl Encoder for OversizedEncoder {
            fn encoded_size_upper_bound(&self, _record: &TelemetryRecord) -> usize {
                0
            }

            fn encode(&self, _record: &TelemetryRecord, out: &mut String) {
                out.push_str(&"x".repeat(1024 * 1024));
            }
        }

        let dir = tempdir().unwrap();
        let (wal, mut sub) = open_wal(dir.path(), 16).await;
        let stage = PersistStage::new(wal, Arc::new(OversizedEncoder));
        let mut ctx = ctx_with_record();

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err(), "oversized encoded record must be rejected");
        assert!(recv_one(&mut sub, 50).await.is_none());
    }

    #[tokio::test]
    async fn run_defers_when_wal_queue_is_saturated() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 1).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal.clone(), encoder);

        // Keep the WAL channel under pressure with best-effort non-durable appends
        // from a separate producer so this durable append can observe `Full`.
        let flood_wal = wal.clone();
        let _flood = std::thread::spawn(move || {
            for i in 0..100_000u64 {
                let _ = flood_wal.try_append(WalEvent {
                    topic: format!("smarthome/dev-{i}/status"),
                    ts_ms: i as i64,
                    payload: format!(
                        "device_status,device_id=dev-{i},device_class=test rssi=-50i {i}"
                    ),
                });
            }
        });

        let mut saturated = None;
        for _ in 0..1_000 {
            let mut ctx = ctx_with_record();
            let result = stage.run(&mut ctx).await;
            if ctx.deferred_reason() == Some(DeferredReason::PersistenceFailure) {
                saturated = Some((result, ctx));
                break;
            }
        }

        let (result, ctx) = saturated.expect("WAL queue should saturate under producer contention");
        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(!ctx.should_quarantine());
    }

    #[tokio::test]
    async fn run_defers_when_wal_writer_has_exited() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = Wal::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: 128 * 1024 * 1024,
            // Tiny segment limit guarantees rotation on the second record.
            segment_bytes: 1,
            queue_capacity: 64,
        })
        .await
        .expect("wal open");
        let wal = Arc::new(wal);
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);

        let mut first = ctx_with_record();
        assert!(matches!(
            stage.run(&mut first).await,
            Ok(StageFlow::Continue)
        ));

        // Force rotation-open to fail by pre-creating segment 2 path as a directory.
        let seg2 = segment_path(dir.path(), 2);
        fs::create_dir_all(&seg2).unwrap();

        // Second append is accepted into the queue; writer then hits fatal open
        // error while rotating and exits, disconnecting the channel.
        let mut second = ctx_with_record();
        let _ = stage.run(&mut second).await;

        // Wait until a subsequent append observes the closed sender.
        let mut closed = None;
        for _ in 0..200 {
            let mut ctx = ctx_with_record();
            let result = stage.run(&mut ctx).await;
            if ctx.deferred_reason() == Some(DeferredReason::PersistenceFailure) {
                closed = Some((result, ctx));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let (result, ctx) = closed.expect("WAL channel should close after writer fatal exit");
        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(!ctx.should_quarantine());
    }

    #[tokio::test]
    async fn run_defers_sync_failure_without_quarantining_input() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let (wal, _sub) = open_wal(dir.path(), 16).await;
        *wal.test_io_hook.lock().expect("WAL hook lock") = Some(Arc::new(|step| {
            if step == TestIoStep::Sync {
                Err(std::io::Error::other("sync failure"))
            } else {
                Ok(())
            }
        }));
        let stage = PersistStage::new(
            wal,
            Arc::new(crate::infrastructure::sink::influx::InfluxEncoder),
        );
        let mut ctx = ctx_with_record();

        assert!(matches!(stage.run(&mut ctx).await?, StageFlow::Stop));
        assert_eq!(
            ctx.deferred_reason(),
            Some(DeferredReason::PersistenceFailure)
        );
        assert!(!ctx.should_quarantine());
        Ok(())
    }

    #[tokio::test]
    async fn run_without_telemetry_record_returns_error() {
        let dir = tempdir().unwrap();
        let (wal, _sub) = open_wal(dir.path(), 16).await;
        let encoder = Arc::new(crate::infrastructure::sink::influx::InfluxEncoder);
        let stage = PersistStage::new(wal, encoder);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/bme680", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_quarantine());
    }
}
