use std::{future::Future, pin::Pin, time::Instant};

use anyhow::Context;
use metrics::{counter, histogram};

use crate::{
    infrastructure::cache::state::CacheState,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct CacheUpdateStage {
    cache_state: CacheState,
}

impl CacheUpdateStage {
    pub fn new(cache_state: CacheState) -> Self {
        Self { cache_state }
    }
}

impl PipelineStage for CacheUpdateStage {
    fn name(&self) -> &'static str {
        "cache_update"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();
            let route = ctx.route()?;
            let route_id = route.metric_label();
            if route.config().cache {
                let received_at_ms = u64::try_from(ctx.received_at_ms())
                    .context("cache receive time cannot be negative")?;
                if self.cache_state.update_record(
                    ctx.telemetry_record()?,
                    received_at_ms,
                    ctx.receive_sequence(),
                ) {
                    counter!("ingest_cache_updates_total", "route" => route_id.clone())
                        .increment(1);
                }
            }
            histogram!("ingest_cache_update_duration_seconds", "route" => route_id)
                .record(start.elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use anyhow::{Context, Result};

    use super::*;
    use crate::{
        config::routes,
        infrastructure::{
            cache::state::CacheEvent,
            router::{CompiledRoute, RouteSet},
        },
        model::messages::telemetry::{TelemetryRecord, TelemetryValue},
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    const TOPIC: &str = "smarthome/ESP32-1/sensor";
    const RECEIVED_AT_MS: i64 = 1_700_000_000_456;

    fn route(topic: &str) -> Result<(Arc<RouteSet>, Arc<CompiledRoute>)> {
        let routes = Arc::new(RouteSet::compile(
            routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
        )?);
        let route = routes
            .select(topic)
            .context("configured route should match test topic")?;
        Ok((routes, route))
    }

    fn record(route: &CompiledRoute, topic: &str, device_id: &str) -> TelemetryRecord {
        TelemetryRecord {
            route_id: route.id().to_owned(),
            device_id: device_id.to_owned(),
            measurement: route.config().measurement.clone(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::from([("device_id".to_owned(), device_id.to_owned())]),
            fields: BTreeMap::from([
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
                ("temp_c".to_owned(), TelemetryValue::Float(22.5)),
            ]),
            source_topic: topic.to_owned(),
            retained: false,
        }
    }

    fn context(route: Arc<CompiledRoute>, topic: &str, device_id: &str) -> PipelineContext {
        let mut ctx = PipelineContext::new_with_metadata(topic, vec![], RECEIVED_AT_MS, false);
        ctx.set_route(route.clone());
        ctx.set_telemetry_record(record(&route, topic, device_id));
        ctx
    }

    #[tokio::test]
    async fn run_caches_typed_record_with_frozen_receive_time_and_broadcasts() -> Result<()> {
        let (_routes, route) = route(TOPIC)?;
        let cache = CacheState::new(60_000, 16);
        let mut receiver = cache.subscribe_events();
        let stage = CacheUpdateStage::new(cache.clone());
        let mut ctx = context(route, TOPIC, "ESP32-1");

        assert!(matches!(stage.run(&mut ctx).await?, StageFlow::Continue));

        let (state, _) = cache
            .snapshot_device("esp32-1")
            .context("device state should be cached")?;
        assert_eq!(state.last_seen_ms, RECEIVED_AT_MS as u64);
        assert_eq!(
            state.value.get("temp_c"),
            Some(&TelemetryValue::Float(22.5))
        );
        let CacheEvent::Telemetry {
            device_id,
            last_seen_ms,
            ..
        } = receiver.try_recv()?;
        assert_eq!(device_id, "esp32-1");
        assert_eq!(last_seen_ms, RECEIVED_AT_MS as u64);
        Ok(())
    }

    #[tokio::test]
    async fn run_skips_routes_without_cache_opt_in() -> Result<()> {
        let topic = "smarthome/ESP32-1/status";
        let (_routes, route) = route(topic)?;
        let cache = CacheState::new(60_000, 16);
        let mut receiver = cache.subscribe_events();
        let stage = CacheUpdateStage::new(cache.clone());
        let mut ctx = context(route, topic, "ESP32-1");

        assert!(matches!(stage.run(&mut ctx).await?, StageFlow::Continue));
        assert!(cache.snapshot_all_devices().is_empty());
        assert!(receiver.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn run_without_selected_route_returns_error() {
        let cache = CacheState::new(60_000, 16);
        let stage = CacheUpdateStage::new(cache);
        let mut ctx = PipelineContext::new(TOPIC, vec![]);

        assert!(stage.run(&mut ctx).await.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
