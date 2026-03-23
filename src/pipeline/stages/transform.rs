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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::{
        infrastructure::router::{Route, Router},
        model::messages::message::{HandledMessage, MessageType},
        pipeline::{context::PipelineContext, stage::StageFlow},
    };

    const SENSOR_SCHEMA: &str = include_str!("../../../schema/sensor.schema.json");
    const STATUS_SCHEMA: &str = include_str!("../../../schema/status.schema.json");

    // ── helpers ───────────────────────────────────────────────────────────────

    fn sensor_router() -> Arc<Router> {
        let route = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
        Arc::new(Router::new().add_route(route))
    }

    fn dual_router() -> Arc<Router> {
        let sensor = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
        let status = Route::new(MessageType::Status, STATUS_SCHEMA, "smarthome/+/status").unwrap();
        Arc::new(Router::new().add_route(sensor).add_route(status))
    }

    fn valid_sensor_payload() -> Value {
        json!({
            "device_id": "esp32-1",
            "room": "living_room",
            "device_class": "esp32p4-bme680",
            "fw_version": "1.0.0",
            "time_ms": 1_700_000_000_000_i64,
            "time_iso": "2023-11-14T22:13:20Z",
            "time_valid": true,
            "data": {
                "temp_c": 22.5,
                "rel_hum_perc": 45.0,
                "pressure_hpa": 1013.25,
                "gas_ohm": 50_000.0,
                "altitude_m": 500.0
            }
        })
    }

    fn valid_status_payload() -> Value {
        json!({
            "device_id": "esp32-1",
            "device_class": "esp32p4-bme680",
            "fw_version": "1.0.0",
            "ip": "192.168.1.42",
            "rssi": -65_i64,
            "time_ms": 1_700_000_000_000_i64,
            "time_iso": "2023-11-14T22:13:20Z",
            "time_valid": true,
            "uptime": 3600_i64,
            "free_mem": 200_000_i64,
            "ssid": "HomeNet"
        })
    }

    fn ctx_with_json(topic: &str, payload: Value) -> PipelineContext {
        let mut ctx = PipelineContext::new(topic, vec![]);
        ctx.set_payload_json(payload);
        ctx
    }

    // ── calc_dew_point_c ──────────────────────────────────────────────────────

    #[test]
    fn calc_dew_point_c_returns_expected_value_for_typical_conditions() {
        // At 20 °C and 50 % RH the dew point is approximately 9.27 °C.
        let result = TransformStage::calc_dew_point_c(20.0, 50.0);
        assert!((result - 9.27).abs() < 0.05, "got {result}");
    }

    #[test]
    fn calc_dew_point_c_equals_temp_at_100_percent_humidity() {
        // At 100 % RH the dew point must equal the air temperature.
        let temp = 25.0;
        let result = TransformStage::calc_dew_point_c(temp, 100.0);
        assert!((result - temp).abs() < 0.001, "got {result}");
    }

    // ── calc_heat_index_c ─────────────────────────────────────────────────────

    #[test]
    fn calc_heat_index_c_uses_steadman_formula_below_80f() {
        // 20 °C = 68 °F < 80 °F → Steadman path.
        // Steadman: 0.5 * (68 + 61 + 0 + 50*0.094) = 66.85 °F ≈ 19.36 °C.
        let result = TransformStage::calc_heat_index_c(20.0, 50.0);
        assert!((result - 19.36).abs() < 0.05, "got {result}");
    }

    #[test]
    fn calc_heat_index_c_uses_rothfusz_formula_above_80f() {
        // 35 °C = 95 °F, 50 % RH → main Rothfusz path (humidity not in low/high correction bands).
        let result = TransformStage::calc_heat_index_c(35.0, 50.0);
        // Rothfusz at 95 °F / 50 % RH → ~105.2 °F → ~40.68 °C.
        assert!((result - 40.68).abs() < 0.05, "got {result}");
    }

    #[test]
    fn calc_heat_index_c_applies_low_humidity_adjustment_above_80f() {
        // 33 °C ≈ 91.4 °F (in [80,112]), RH = 10 % (< 13) → Rothfusz minus adjustment.
        let result = TransformStage::calc_heat_index_c(33.0, 10.0);
        let without_adjustment = TransformStage::calc_heat_index_c(33.0, 15.0);
        // Low humidity makes the apparent temp feel lower than the base Rothfusz.
        assert!(result < without_adjustment, "got {result}");
    }

    #[test]
    fn calc_heat_index_c_applies_high_humidity_adjustment_80_to_87f() {
        // 29 °C ≈ 84.2 °F (in [80,87]), RH = 90 % (> 85) → Rothfusz plus adjustment.
        let result = TransformStage::calc_heat_index_c(29.0, 90.0);
        let without_adjustment = TransformStage::calc_heat_index_c(29.0, 84.0);
        // High humidity in that band raises the apparent temperature.
        assert!(result > without_adjustment, "got {result}");
    }

    // ── calc_iaq_score ────────────────────────────────────────────────────────

    #[test]
    fn calc_iaq_score_is_100_at_optimal_humidity_and_max_gas() {
        // Humidity in [38,42] → hum_score = 25; gas at upper limit → gas_score = 75.
        let result = TransformStage::calc_iaq_score(50_000.0, 40.0);
        assert!((result - 100.0).abs() < 0.001, "got {result}");
    }

    #[test]
    fn calc_iaq_score_is_25_at_optimal_humidity_and_min_gas() {
        // Gas at or below lower limit is clamped → gas_score = 0.
        let result = TransformStage::calc_iaq_score(5_000.0, 40.0);
        assert!((result - 25.0).abs() < 0.001, "got {result}");
    }

    #[test]
    fn calc_iaq_score_hum_score_increases_linearly_below_38_percent() {
        let low = TransformStage::calc_iaq_score(50_000.0, 20.0);
        let mid = TransformStage::calc_iaq_score(50_000.0, 30.0);
        assert!(
            low < mid,
            "hum score should increase as humidity rises toward 38 %"
        );
    }

    #[test]
    fn calc_iaq_score_hum_score_decreases_above_42_percent() {
        let at_42 = TransformStage::calc_iaq_score(50_000.0, 42.0);
        let at_80 = TransformStage::calc_iaq_score(50_000.0, 80.0);
        assert!(
            at_42 > at_80,
            "hum score should decrease as humidity rises above 42 %"
        );
    }

    // ── calc_iaq_text ─────────────────────────────────────────────────────────

    #[test]
    fn calc_iaq_text_good_when_score_above_89_8() {
        // score = (100 - 90) * 5 = 50 → "Good"
        assert_eq!(TransformStage::calc_iaq_text(90.0), "Air quality is Good");
    }

    #[test]
    fn calc_iaq_text_moderate_at_score_threshold_51() {
        // (100 - x) * 5 = 51  → x = 89.8; use 89.7 to land just above 51.
        assert_eq!(
            TransformStage::calc_iaq_text(89.7),
            "Air quality is Moderate"
        );
    }

    #[test]
    fn calc_iaq_text_unhealthy_for_sensitive_groups_at_threshold_151() {
        // (100 - x) * 5 = 151 → x = 69.8; use 69.7 to land just above 151.
        assert_eq!(
            TransformStage::calc_iaq_text(69.7),
            "Air quality is Unhealthy for Sensitive Groups"
        );
    }

    #[test]
    fn calc_iaq_text_unhealthy_at_threshold_176() {
        // (100 - x) * 5 = 176 → x = 64.8; use 64.7.
        assert_eq!(
            TransformStage::calc_iaq_text(64.7),
            "Air quality is Unhealthy"
        );
    }

    #[test]
    fn calc_iaq_text_very_unhealthy_at_threshold_201() {
        // (100 - x) * 5 = 201 → x = 59.8; use 59.7.
        assert_eq!(
            TransformStage::calc_iaq_text(59.7),
            "Air quality is Very Unhealthy"
        );
    }

    #[test]
    fn calc_iaq_text_hazardous_at_threshold_301() {
        // (100 - x) * 5 = 301 → x = 39.8; use 39.7.
        assert_eq!(
            TransformStage::calc_iaq_text(39.7),
            "Air quality is Hazardous"
        );
    }

    // ── transform_sensor_payload: derived fields ──────────────────────────────

    #[test]
    fn transform_sensor_payload_adds_all_derived_fields() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();

        stage.transform_sensor_payload(&mut payload).unwrap();

        let data = payload["data"].as_object().unwrap();
        assert!(data.contains_key("dew_point_c"), "dew_point_c missing");
        assert!(data.contains_key("heat_index_c"), "heat_index_c missing");
        assert!(data.contains_key("iaq_score"), "iaq_score missing");
        assert!(data.contains_key("iaq_text"), "iaq_text missing");
    }

    #[test]
    fn transform_sensor_payload_trims_whitespace_from_string_fields() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["device_id"] = json!("  esp32-1  ");
        payload["room"] = json!("  living_room  ");

        stage.transform_sensor_payload(&mut payload).unwrap();

        assert_eq!(payload["device_id"], json!("esp32-1"));
        assert_eq!(payload["room"], json!("living_room"));
    }

    // ── transform_sensor_payload: validation bounds ───────────────────────────

    #[test]
    fn transform_sensor_payload_returns_err_when_data_missing() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload.as_object_mut().unwrap().remove("data");

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_required_field_missing() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"].as_object_mut().unwrap().remove("temp_c");

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_rel_hum_is_zero() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["rel_hum_perc"] = json!(0.0);

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_rel_hum_exceeds_100() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["rel_hum_perc"] = json!(100.1);

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_accepts_rel_hum_at_boundary_100() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["rel_hum_perc"] = json!(100.0);

        assert!(stage.transform_sensor_payload(&mut payload).is_ok());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_gas_ohm_is_zero() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["gas_ohm"] = json!(0.0);

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_pressure_below_300() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["pressure_hpa"] = json!(299.9);

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_returns_err_when_pressure_above_1200() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["pressure_hpa"] = json!(1200.1);

        assert!(stage.transform_sensor_payload(&mut payload).is_err());
    }

    #[test]
    fn transform_sensor_payload_accepts_pressure_at_lower_boundary_300() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["pressure_hpa"] = json!(300.0);

        assert!(stage.transform_sensor_payload(&mut payload).is_ok());
    }

    #[test]
    fn transform_sensor_payload_accepts_pressure_at_upper_boundary_1200() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["pressure_hpa"] = json!(1200.0);

        assert!(stage.transform_sensor_payload(&mut payload).is_ok());
    }

    // ── transform_status_payload ──────────────────────────────────────────────

    #[test]
    fn transform_status_payload_succeeds_on_valid_payload() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_status_payload();

        assert!(stage.transform_status_payload(&mut payload).is_ok());
    }

    #[test]
    fn transform_status_payload_trims_whitespace_from_string_fields() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_status_payload();
        payload["device_id"] = json!("  esp32-1  ");
        payload["ip"] = json!("  192.168.1.42  ");

        stage.transform_status_payload(&mut payload).unwrap();

        assert_eq!(payload["device_id"], json!("esp32-1"));
        assert_eq!(payload["ip"], json!("192.168.1.42"));
    }

    #[test]
    fn transform_status_payload_sets_defaults_for_missing_optional_fields() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_status_payload();
        let obj = payload.as_object_mut().unwrap();
        obj.remove("uptime");
        obj.remove("free_mem");
        obj.remove("ssid");

        stage.transform_status_payload(&mut payload).unwrap();

        assert_eq!(payload["uptime"], json!(0));
        assert_eq!(payload["free_mem"], json!(0));
        assert_eq!(payload["ssid"], json!(""));
    }

    #[test]
    fn transform_status_payload_does_not_overwrite_existing_optional_fields() {
        let stage = TransformStage::new(sensor_router());
        let mut payload = valid_status_payload();

        stage.transform_status_payload(&mut payload).unwrap();

        assert_eq!(payload["uptime"], json!(3600_i64));
        assert_eq!(payload["free_mem"], json!(200_000_i64));
        assert_eq!(payload["ssid"], json!("HomeNet"));
    }

    // ── run(): stage-level behavior ───────────────────────────────────────────

    #[tokio::test]
    async fn run_on_valid_sensor_payload_sets_handled_message_and_returns_continue() {
        let stage = TransformStage::new(dual_router());
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", valid_sensor_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        assert!(ctx.ignored_reason().is_none());
        assert!(matches!(
            ctx.handled_message().unwrap(),
            HandledMessage::Sensor(_)
        ));
    }

    #[tokio::test]
    async fn run_on_valid_status_payload_sets_handled_message_and_returns_continue() {
        let stage = TransformStage::new(dual_router());
        let mut ctx = ctx_with_json("smarthome/esp32-1/status", valid_status_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Continue)));
        assert!(!ctx.should_publish_dlq());
        assert!(ctx.ignored_reason().is_none());
        assert!(matches!(
            ctx.handled_message().unwrap(),
            HandledMessage::Status(_)
        ));
    }

    #[tokio::test]
    async fn run_on_unknown_topic_marks_ignored_and_stops() {
        // strict=true (default) routes unknown topics to DLQ via Err, not ignore.
        // Use strict=false router to exercise the ignored path.
        let route = Route::new(MessageType::Sensor, SENSOR_SCHEMA, "smarthome/+/sensor").unwrap();
        let router = Arc::new(Router::new().strict(false).add_route(route));
        let stage = TransformStage::new(router);
        let mut ctx = ctx_with_json("home/unknown/device", valid_sensor_payload());

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.ignored_reason().is_some());
        assert!(!ctx.should_publish_dlq());
    }

    #[tokio::test]
    async fn run_on_invalid_sensor_bounds_marks_dlq_and_stops() {
        let stage = TransformStage::new(dual_router());
        let mut payload = valid_sensor_payload();
        payload["data"]["rel_hum_perc"] = json!(0.0); // violates (0, 100] bound
        let mut ctx = ctx_with_json("smarthome/esp32-1/sensor", payload);

        let result = stage.run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_publish_dlq());
        assert!(ctx.dlq_reason().unwrap().contains("transform failed"));
    }

    #[tokio::test]
    async fn run_without_payload_json_in_context_returns_error() {
        let stage = TransformStage::new(dual_router());
        let mut ctx = PipelineContext::new("smarthome/esp32-1/sensor", vec![]);

        let result = stage.run(&mut ctx).await;

        assert!(result.is_err());
        assert!(!ctx.should_publish_dlq());
    }
}
