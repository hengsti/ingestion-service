use std::{future::Future, pin::Pin};

use metrics::{counter, histogram};

use crate::pipeline::{
    context::PipelineContext,
    stage::{PipelineStage, StageFlow, StageResult},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct ObserveStage;

impl ObserveStage {
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for ObserveStage {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let route_id = ctx.route()?.id().to_owned();
            ctx.telemetry_record()?;
            counter!("ingest_messages_processed_total", "route" => route_id).increment(1);

            histogram!("ingest_pipeline_duration_seconds")
                .record(ctx.started_at().elapsed().as_secs_f64());

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
        infrastructure::router::RouteSet,
        model::messages::telemetry::{TelemetryRecord, TelemetryValue},
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    #[tokio::test]
    async fn run_on_configured_record_returns_continue() -> Result<()> {
        let topic = "smarthome/ESP32-1/sensor";
        let routes = Arc::new(RouteSet::compile(
            routes::load("config/routes.json")?,
            "smarthome/_dlq/ingest",
        )?);
        let route = routes.select(topic).context("route should match")?;
        let record = TelemetryRecord {
            route_id: route.id().to_owned(),
            device_id: "ESP32-1".to_owned(),
            measurement: route.config().measurement.clone(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::new(),
            fields: BTreeMap::from([("mqtt_retained".to_owned(), TelemetryValue::Boolean(false))]),
            source_topic: topic.to_owned(),
            retained: false,
        };
        let mut ctx = PipelineContext::new(topic, vec![]);
        ctx.set_route(route);
        ctx.set_telemetry_record(record);

        assert!(matches!(
            ObserveStage::new().run(&mut ctx).await?,
            StageFlow::Continue
        ));
        assert!(!ctx.should_publish_dlq());
        Ok(())
    }

    #[tokio::test]
    async fn run_without_selected_route_returns_error() {
        let mut ctx = PipelineContext::new("smarthome/ESP32-1/sensor", vec![]);

        assert!(ObserveStage::new().run(&mut ctx).await.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
