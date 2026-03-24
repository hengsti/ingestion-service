use std::{future::Future, pin::Pin, time::Instant};

use metrics::{counter, histogram};

use crate::{
    infrastructure::cache::state::CacheState,
    model::messages::message::HandledMessage,
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

            let msg = ctx.handled_message()?;

            let kind = match msg {
                HandledMessage::Sensor(msg) => {
                    self.cache_state.update_sensor(msg);
                    "sensor"
                }
                HandledMessage::Status(_) => "status",
            };

            counter!("ingest_cache_updates_total", "kind" => kind).increment(1);
            histogram!("ingest_cache_update_duration_seconds", "kind" => kind)
                .record(start.elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        infrastructure::cache::state::CacheState,
        model::messages::{
            message::HandledMessage,
            sensor::{SensorData, SensorMessage},
            status::StatusMessage,
        },
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    // ── helpers ───────────────────────────────────────────────────────────────

    fn cache() -> CacheState {
        CacheState::new(60_000, 16)
    }

    fn sensor_message(device_id: &str, temp_c: f64) -> HandledMessage {
        HandledMessage::Sensor(SensorMessage {
            device_id: device_id.to_string(),
            room: "living_room".to_string(),
            device_class: "esp32p4-bme680".to_string(),
            fw_version: "1.0.0".to_string(),
            time_ms: 1_700_000_000_000,
            time_iso: "2023-11-14T22:13:20Z".to_string(),
            time_valid: true,
            data: SensorData {
                temp_c,
                rel_hum_perc: 45.0,
                pressure_hpa: 1013.25,
                gas_ohm: 50_000.0,
                iaq_score: 85.0,
                iaq_text: "Air quality is Good".to_string(),
                dew_point_c: 9.5,
                heat_index_c: 22.0,
                altitude_m: 500.0,
            },
        })
    }

    fn status_message() -> HandledMessage {
        HandledMessage::Status(StatusMessage {
            device_id: "esp32-1".to_string(),
            device_class: "esp32p4-bme680".to_string(),
            fw_version: "1.0.0".to_string(),
            ip: "192.168.1.42".to_string(),
            rssi: -65,
            time_ms: 1_700_000_000_000,
            time_iso: "2023-11-14T22:13:20Z".to_string(),
            time_valid: true,
            uptime: 3600,
            free_mem: 200_000,
            ssid: "HomeNet".to_string(),
        })
    }

    fn ctx_with_message(handled: HandledMessage) -> PipelineContext {
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);
        ctx.set_handled_message(handled);
        ctx
    }

    // ── run(): return values ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_on_sensor_message_returns_continue() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache);
        let mut ctx = ctx_with_message(sensor_message("esp32-1", 22.5));

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_status_message_returns_continue() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache);
        let mut ctx = ctx_with_message(status_message());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_without_handled_message_returns_error() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache);
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }

    // ── run(): cache state after sensor update ────────────────────────────────

    #[tokio::test]
    async fn run_stores_sensor_data_in_cache_retrievable_by_device_id() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache.clone());
        let mut ctx = ctx_with_message(sensor_message("esp32-1", 22.5));

        stage.run(&mut ctx).await.unwrap();

        let (state, _stale) = cache.snapshot_sensor("esp32-1").expect("device not found");
        assert!((state.value.temp_c - 22.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn run_normalizes_device_id_to_lowercase_in_cache() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache.clone());
        // Message uses uppercase device_id.
        let mut ctx = ctx_with_message(sensor_message("ESP32-1", 20.0));

        stage.run(&mut ctx).await.unwrap();

        // Must be findable by the lowercase key.
        assert!(
            cache.snapshot_sensor("esp32-1").is_some(),
            "device not found under lowercased key"
        );
    }

    #[tokio::test]
    async fn run_overwrites_previous_sensor_data_on_second_update() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache.clone());

        // First update.
        let mut ctx1 = ctx_with_message(sensor_message("esp32-1", 20.0));
        stage.run(&mut ctx1).await.unwrap();

        // Second update with different temp.
        let mut ctx2 = ctx_with_message(sensor_message("esp32-1", 30.0));
        stage.run(&mut ctx2).await.unwrap();

        let (state, _stale) = cache.snapshot_sensor("esp32-1").unwrap();
        assert!(
            (state.value.temp_c - 30.0).abs() < f64::EPSILON,
            "expected second temp value"
        );
    }

    #[tokio::test]
    async fn run_stores_multiple_devices_independently() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache.clone());

        let mut ctx_a = ctx_with_message(sensor_message("esp32-a", 21.0));
        let mut ctx_b = ctx_with_message(sensor_message("esp32-b", 25.0));

        stage.run(&mut ctx_a).await.unwrap();
        stage.run(&mut ctx_b).await.unwrap();

        let all = cache.snapshot_all_sensors();
        assert_eq!(all.len(), 2, "expected two separate cache entries");
    }

    // ── run(): status messages do not pollute sensor cache ────────────────────

    #[tokio::test]
    async fn run_on_status_message_does_not_insert_into_sensor_cache() {
        let cache = cache();
        let stage = CacheUpdateStage::new(cache.clone());
        let mut ctx = ctx_with_message(status_message());

        stage.run(&mut ctx).await.unwrap();

        assert!(
            cache.snapshot_all_sensors().is_empty(),
            "status message must not write to sensor cache"
        );
    }

    // ── run(): broadcast event ────────────────────────────────────────────────

    #[tokio::test]
    async fn run_broadcasts_cache_event_for_sensor_update() {
        let cache = cache();
        let mut rx = cache.subscribe_events();
        let stage = CacheUpdateStage::new(cache.clone());
        let mut ctx = ctx_with_message(sensor_message("esp32-1", 22.5));

        stage.run(&mut ctx).await.unwrap();

        let event = rx.try_recv().expect("expected a broadcast event");
        match event {
            crate::infrastructure::cache::state::CacheEvent::Sensor { device_id, .. } => {
                assert_eq!(device_id, "esp32-1");
            }
        }
    }
}
