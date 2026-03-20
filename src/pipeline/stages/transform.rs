use std::{future::Future, pin::Pin, sync::Arc, time::Instant};

use anyhow::{Result, anyhow, bail};
use metrics::{counter, histogram};
use serde_json::{Number, Value};
use tracing::{debug, warn};

use crate::{
    infrastructure::router::Router,
    model::messages::message::MessageType,
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

#[derive(Clone)]
pub struct TransformStage {
    router: Arc<Router>,
}

impl TransformStage {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }

    fn transform_sensor_payload(&self, payload: &mut Value) -> Result<()> {
        Self::trim_root_string(payload, "device_id");
        Self::trim_root_string(payload, "room");
        Self::trim_root_string(payload, "device_class");
        Self::trim_root_string(payload, "fw_version");
        Self::trim_root_string(payload, "time_iso");

        let data = payload
            .get_mut("data")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("sensor.data missing or not an object"))?;

        let temp_c = Self::required_f64_from_object(data, "temp_c")?;
        let rel_hum_perc = Self::required_f64_from_object(data, "rel_hum_perc")?;
        let pressure_hpa = Self::required_f64_from_object(data, "pressure_hpa")?;
        let gas_ohm = Self::required_f64_from_object(data, "gas_ohm")?;
        let altitude_m = Self::required_f64_from_object(data, "altitude_m")?;

        if !(0.0 < rel_hum_perc && rel_hum_perc <= 100.0) {
            bail!(
                "sensor.data.rel_hum_perc must be in (0,100], got {}",
                rel_hum_perc
            );
        }

        if gas_ohm <= 0.0 {
            bail!("sensor.data.gas_ohm must be > 0, got {}", gas_ohm);
        }

        if !(300.0..=1200.0).contains(&pressure_hpa) {
            bail!(
                "sensor.data.pressure_hpa out of transform-safe range [300,1200], got {}",
                pressure_hpa
            );
        }

        let dew_point_c = Self::calc_dew_point_c(temp_c, rel_hum_perc);
        let heat_index_c = Self::calc_heat_index_c(temp_c, rel_hum_perc);
        let iaq_score = Self::calc_iaq_score(gas_ohm, rel_hum_perc);
        let iaq_text = Self::calc_iaq_text(iaq_score);

        Self::set_f64_in_object(data, "dew_point_c", dew_point_c)?;
        Self::set_f64_in_object(data, "heat_index_c", heat_index_c)?;
        Self::set_f64_in_object(data, "iaq_score", iaq_score)?;
        data.insert("iaq_text".to_string(), Value::String(iaq_text));

        // keep raw values canonical as well
        Self::set_f64_in_object(data, "pressure_hpa", pressure_hpa)?;
        Self::set_f64_in_object(data, "altitude_m", altitude_m)?;

        Ok(())
    }

    fn transform_status_payload(&self, payload: &mut Value) -> Result<()> {
        Self::trim_root_string(payload, "device_id");
        Self::trim_root_string(payload, "device_class");
        Self::trim_root_string(payload, "fw_version");
        Self::trim_root_string(payload, "ip");
        Self::trim_root_string(payload, "time_iso");
        Self::trim_root_string(payload, "ssid");

        // Status schema currently does not require these, but the canonical StatusMessage does.
        Self::ensure_i64(payload, "uptime", 0);
        Self::ensure_i64(payload, "free_mem", 0);
        Self::ensure_string(payload, "ssid", "");

        Ok(())
    }

    fn trim_root_string(payload: &mut Value, key: &str) {
        let trimmed = payload
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string());

        if let Some(trimmed_value) = trimmed {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert(key.to_string(), Value::String(trimmed_value));
            }
        }
    }

    fn ensure_i64(payload: &mut Value, key: &str, default: i64) {
        if payload.get(key).is_none() {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert(key.to_string(), Value::Number(default.into()));
            }
        }
    }

    fn ensure_string(payload: &mut Value, key: &str, default: &str) {
        if payload.get(key).is_none() {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert(key.to_string(), Value::String(default.to_string()));
            }
        }
    }

    fn required_f64_from_object(obj: &serde_json::Map<String, Value>, key: &str) -> Result<f64> {
        obj.get(key)
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow!("missing or non-numeric field: sensor.data.{}", key))
    }

    fn set_f64_in_object(
        obj: &mut serde_json::Map<String, Value>,
        key: &str,
        value: f64,
    ) -> Result<()> {
        let number = Number::from_f64(value)
            .ok_or_else(|| anyhow!("cannot store non-finite float in field {}", key))?;
        obj.insert(key.to_string(), Value::Number(number));
        Ok(())
    }

    fn calc_dew_point_c(temp_c: f64, rel_hum_perc: f64) -> f64 {
        let alpha = (rel_hum_perc / 100.0).ln() + ((17.27 * temp_c) / (237.7 + temp_c));
        (237.7 * alpha) / (17.27 - alpha)
    }

    fn temp_c_to_f(temp_c: f64) -> f64 {
        temp_c * 9.0 / 5.0 + 32.0
    }

    fn temp_f_to_c(temp_f: f64) -> f64 {
        (temp_f - 32.0) * 5.0 / 9.0
    }

    fn calc_heat_index_c(temp_c: f64, rel_hum_perc: f64) -> f64 {
        let temp_f = Self::temp_c_to_f(temp_c);

        let hi_rothfusz = -42.379 + 2.04901523 * temp_f + 10.14333127 * rel_hum_perc
            - 0.22475541 * temp_f * rel_hum_perc
            - 0.00683783 * temp_f * temp_f
            - 0.05481717 * rel_hum_perc * rel_hum_perc
            + 0.00122874 * temp_f * temp_f * rel_hum_perc
            + 0.00085282 * temp_f * rel_hum_perc * rel_hum_perc
            - 0.00000199 * temp_f * temp_f * rel_hum_perc * rel_hum_perc;

        if rel_hum_perc < 13.0 && (80.0..=112.0).contains(&temp_f) {
            let adjusted_hi =
                ((13.0 - rel_hum_perc) / 4.0) * (17.0 - (temp_f - 95.0).abs() / 17.0).sqrt();
            return Self::temp_f_to_c(hi_rothfusz - adjusted_hi);
        }

        if rel_hum_perc > 85.0 && (80.0..=87.0).contains(&temp_f) {
            let adjusted_hi = ((rel_hum_perc - 85.0) * (87.0 - temp_f)) / 5.0;
            return Self::temp_f_to_c(hi_rothfusz + adjusted_hi);
        }

        if temp_f < 80.0 {
            let hi_steadman =
                0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (rel_hum_perc * 0.094));
            return Self::temp_f_to_c(hi_steadman);
        }

        Self::temp_f_to_c(hi_rothfusz)
    }

    fn calc_iaq_score(gas_ohm: f64, rel_hum_perc: f64) -> f64 {
        let hum_ref = 40.0;

        let hum_score = if (38.0..=42.0).contains(&rel_hum_perc) {
            0.25 * 100.0
        } else if rel_hum_perc < 38.0 {
            (0.25 / hum_ref) * rel_hum_perc * 100.0
        } else {
            ((-0.25 / (100.0 - hum_ref) * rel_hum_perc) + 0.416666) * 100.0
        };

        let gas_lower_limit = 5_000.0;
        let gas_upper_limit = 50_000.0;
        let gas_reading = gas_ohm.clamp(gas_lower_limit, gas_upper_limit);

        let gas_score = ((0.75 / (gas_upper_limit - gas_lower_limit) * gas_reading)
            - (gas_lower_limit * (0.75 / (gas_upper_limit - gas_lower_limit))))
            * 100.0;

        hum_score + gas_score
    }

    fn calc_iaq_text(indoor_air_quality_score: f64) -> String {
        let score = (100.0 - indoor_air_quality_score) * 5.0;

        let suffix = if score >= 301.0 {
            "Hazardous"
        } else if score >= 201.0 {
            "Very Unhealthy"
        } else if score >= 176.0 {
            "Unhealthy"
        } else if score >= 151.0 {
            "Unhealthy for Sensitive Groups"
        } else if score >= 51.0 {
            "Moderate"
        } else {
            "Good"
        };

        format!("Air quality is {}", suffix)
    }
}

impl PipelineStage for TransformStage {
    fn name(&self) -> &'static str {
        "transform"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();

            let message_type = match self.router.message_type_for_topic(ctx.topic()) {
                Some(mt) => mt,
                None => {
                    counter!("ingest_transform_ignored_total").increment(1);

                    ctx.mark_ignored("no matching route");

                    histogram!("ingest_transform_duration_seconds", "result" => "ignored")
                        .record(start.elapsed().as_secs_f64());

                    return Ok(StageFlow::Stop);
                }
            };

            counter!("ingest_transform_attempt_total").increment(1);

            let transform_result = {
                let payload = ctx.payload_json_mut()?;
                match message_type {
                    MessageType::Sensor => self.transform_sensor_payload(payload),
                    MessageType::Status => self.transform_status_payload(payload),
                }
            };

            if let Err(err) = transform_result {
                counter!("ingest_transform_failed_total").increment(1);
                warn!(topic = %ctx.topic(), error = %err, "transform failed; marking for DLQ");

                ctx.mark_dlq(format!("transform failed: {}", err));

                histogram!("ingest_transform_duration_seconds", "result" => "failed")
                    .record(start.elapsed().as_secs_f64());

                return Ok(StageFlow::Stop);
            }

            let handled = match self
                .router
                .deserialize(ctx.topic(), ctx.payload_json()?.clone())
            {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    counter!("ingest_transform_ignored_total").increment(1);

                    ctx.mark_ignored("no matching route");

                    histogram!("ingest_transform_duration_seconds", "result" => "ignored")
                        .record(start.elapsed().as_secs_f64());

                    return Ok(StageFlow::Stop);
                }
                Err(err) => {
                    counter!("ingest_transform_deserialize_failed_total").increment(1);
                    warn!(topic = %ctx.topic(), error = %err, "post-transform deserialization failed; marking for DLQ");

                    ctx.mark_dlq(format!("post-transform deserialization failed: {}", err));

                    histogram!("ingest_transform_duration_seconds", "result" => "failed")
                        .record(start.elapsed().as_secs_f64());

                    return Ok(StageFlow::Stop);
                }
            };

            ctx.set_handled_message(handled);

            debug!(topic = %ctx.topic(), "transform stage normalized payload and produced canonical message");

            counter!("ingest_transform_success_total").increment(1);
            histogram!("ingest_transform_duration_seconds", "result" => "success")
                .record(start.elapsed().as_secs_f64());

            Ok(StageFlow::Continue)
        })
    }
}
