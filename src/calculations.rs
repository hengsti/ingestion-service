//! Stateless calculations resolved against route mappings at startup.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};

use crate::{
    config::routes::{CalculationName, FieldMapping, FieldType},
    model::messages::telemetry::{TelemetryRecord, TelemetryValue},
};

const HUMIDITY_FIELD: &str = "rel_hum_perc";
const TEMPERATURE_FIELD: &str = "temp_c";
const GAS_FIELD: &str = "gas_ohm";
const WARMED_UP_FIELD: &str = "warmed_up";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Calculation {
    DewPoint,
    HeatIndex,
    AbsoluteHumidity,
    HumidityRatio,
    SurfaceTempAt80rh,
    DewPointDepression,
    ApparentPower,
    EstimatedPowerFactor,
    Iaq,
}

impl Calculation {
    fn output_field_types(self) -> &'static [(&'static str, FieldType)] {
        match self {
            Self::DewPoint => &[("dew_point_c", FieldType::Float)],
            Self::HeatIndex => &[("heat_index_c", FieldType::Float)],
            Self::AbsoluteHumidity => &[("absolute_humidity_g_m3", FieldType::Float)],
            Self::HumidityRatio => &[("humidity_ratio_g_kg", FieldType::Float)],
            Self::SurfaceTempAt80rh => &[("surface_temp_at_80rh_c", FieldType::Float)],
            Self::DewPointDepression => &[("dew_point_depression_c", FieldType::Float)],
            Self::ApparentPower => &[("apparent_power_va", FieldType::Float)],
            Self::EstimatedPowerFactor => &[("estimated_power_factor", FieldType::Float)],
            Self::Iaq => &[
                ("iaq_score", FieldType::Float),
                ("iaq_text", FieldType::String),
            ],
        }
    }
}

pub(crate) fn output_can_be_omitted(field: &str, record: &TelemetryRecord) -> bool {
    if field == "estimated_power_factor" {
        return matches!(estimated_power_factor(record), Ok(None));
    }
    if !matches!(
        record.fields.get(WARMED_UP_FIELD),
        Some(TelemetryValue::Boolean(false))
    ) {
        return false;
    }

    match field {
        "iaq_score" => !record.fields.contains_key("iaq_text"),
        "iaq_text" => !record.fields.contains_key("iaq_score"),
        _ => false,
    }
}

/// A route's compiled calculation sequence.
pub struct CalculationRegistry {
    calculations: Vec<Calculation>,
}

impl CalculationRegistry {
    /// Resolves ordered calculation names against a route's mapped fields.
    ///
    /// # Errors
    ///
    /// Returns an error when a calculation is repeated, a required input is not mapped with the
    /// expected type, or a mapped field collides with a calculated output.
    pub fn resolve(
        names: &[CalculationName],
        fields: &BTreeMap<String, FieldMapping>,
    ) -> Result<Self> {
        let mut calculations = Vec::with_capacity(names.len());
        let mut seen_names = BTreeSet::new();
        let mut outputs = BTreeSet::new();

        for name in names {
            let (calculation, name, inputs) = match name {
                CalculationName::DewPoint => (
                    Calculation::DewPoint,
                    "dew_point",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD][..],
                ),
                CalculationName::HeatIndex => (
                    Calculation::HeatIndex,
                    "heat_index",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD][..],
                ),
                CalculationName::AbsoluteHumidity => (
                    Calculation::AbsoluteHumidity,
                    "absolute_humidity",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD][..],
                ),
                CalculationName::HumidityRatio => (
                    Calculation::HumidityRatio,
                    "humidity_ratio",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD, "pressure_hpa"][..],
                ),
                CalculationName::SurfaceTempAt80rh => (
                    Calculation::SurfaceTempAt80rh,
                    "surface_temp_at_80rh",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD][..],
                ),
                CalculationName::DewPointDepression => (
                    Calculation::DewPointDepression,
                    "dew_point_depression",
                    &[TEMPERATURE_FIELD, HUMIDITY_FIELD][..],
                ),
                CalculationName::ApparentPower => (
                    Calculation::ApparentPower,
                    "apparent_power",
                    &["voltage_v", "current_a"][..],
                ),
                CalculationName::EstimatedPowerFactor => (
                    Calculation::EstimatedPowerFactor,
                    "estimated_power_factor",
                    &["voltage_v", "current_a", "power_w"][..],
                ),
                CalculationName::Iaq => (Calculation::Iaq, "iaq", &[GAS_FIELD, HUMIDITY_FIELD][..]),
            };

            if !seen_names.insert(name) {
                bail!("calculation '{name}' is configured more than once");
            }

            for input in inputs {
                require_mapping_type(fields, input, FieldType::Float)?;
            }
            if calculation == Calculation::Iaq {
                if let Some(mapping) = fields.get(WARMED_UP_FIELD) {
                    if mapping.field_type != FieldType::Boolean {
                        bail!("mapped field '{WARMED_UP_FIELD}' must have boolean type");
                    }
                }
            }

            for &(output, _) in calculation.output_field_types() {
                if fields.contains_key(output) || !outputs.insert(output) {
                    bail!("calculation output '{output}' collides with another telemetry field");
                }
            }
            calculations.push(calculation);
        }

        Ok(Self { calculations })
    }

    /// Returns the field names and types produced by this calculation sequence.
    pub(crate) fn output_field_types(
        &self,
    ) -> impl Iterator<Item = (&'static str, FieldType)> + '_ {
        self.calculations
            .iter()
            .flat_map(|calculation| calculation.output_field_types().iter().copied())
    }

    /// Applies the compiled calculations and returns the enriched record.
    ///
    /// # Errors
    ///
    /// Returns an error when a required value is absent, has the wrong type, is outside its
    /// calculation domain, or produces a non-finite result.
    pub fn apply(&self, mut record: TelemetryRecord) -> Result<TelemetryRecord> {
        for calculation in &self.calculations {
            match calculation {
                Calculation::DewPoint => {
                    let temp_c = required_float(&record, TEMPERATURE_FIELD)?;
                    let rel_hum_perc = required_float(&record, HUMIDITY_FIELD)?;
                    validate_humidity(rel_hum_perc)?;
                    let value = finite_result("dew_point_c", dew_point_c(temp_c, rel_hum_perc))?;
                    ensure_output_available(&record, "dew_point_c")?;
                    record
                        .fields
                        .insert("dew_point_c".to_owned(), TelemetryValue::Float(value));
                }
                Calculation::HeatIndex => {
                    let temp_c = required_float(&record, TEMPERATURE_FIELD)?;
                    let rel_hum_perc = required_float(&record, HUMIDITY_FIELD)?;
                    validate_humidity(rel_hum_perc)?;
                    let value = finite_result("heat_index_c", heat_index_c(temp_c, rel_hum_perc))?;
                    ensure_output_available(&record, "heat_index_c")?;
                    record
                        .fields
                        .insert("heat_index_c".to_owned(), TelemetryValue::Float(value));
                }
                Calculation::AbsoluteHumidity => {
                    let (temp_c, rel_hum_perc) = climate_inputs(&record)?;
                    let vapor_pressure = vapor_pressure_hpa(temp_c, rel_hum_perc)?;
                    let value = finite_result(
                        "absolute_humidity_g_m3",
                        216.7 * vapor_pressure / (temp_c + 273.15),
                    )?;
                    ensure_output_available(&record, "absolute_humidity_g_m3")?;
                    record.fields.insert(
                        "absolute_humidity_g_m3".to_owned(),
                        TelemetryValue::Float(value),
                    );
                }
                Calculation::HumidityRatio => {
                    let (temp_c, rel_hum_perc) = climate_inputs(&record)?;
                    let vapor_pressure = vapor_pressure_hpa(temp_c, rel_hum_perc)?;
                    let pressure = required_float(&record, "pressure_hpa")?;
                    if pressure <= vapor_pressure {
                        bail!("field 'pressure_hpa' must exceed vapor pressure");
                    }
                    let value = finite_result(
                        "humidity_ratio_g_kg",
                        621.98 * vapor_pressure / (pressure - vapor_pressure),
                    )?;
                    ensure_output_available(&record, "humidity_ratio_g_kg")?;
                    record.fields.insert(
                        "humidity_ratio_g_kg".to_owned(),
                        TelemetryValue::Float(value),
                    );
                }
                Calculation::SurfaceTempAt80rh => {
                    let (temp_c, rel_hum_perc) = climate_inputs(&record)?;
                    let vapor_pressure = vapor_pressure_hpa(temp_c, rel_hum_perc)?;
                    let x = (vapor_pressure / (0.8 * 6.112)).ln();
                    let value = finite_result("surface_temp_at_80rh_c", 243.5 * x / (17.67 - x))?;
                    ensure_output_available(&record, "surface_temp_at_80rh_c")?;
                    record.fields.insert(
                        "surface_temp_at_80rh_c".to_owned(),
                        TelemetryValue::Float(value),
                    );
                }
                Calculation::DewPointDepression => {
                    let (temp_c, rel_hum_perc) = climate_inputs(&record)?;
                    let value = finite_result(
                        "dew_point_depression_c",
                        temp_c - dew_point_c(temp_c, rel_hum_perc),
                    )?;
                    ensure_output_available(&record, "dew_point_depression_c")?;
                    record.fields.insert(
                        "dew_point_depression_c".to_owned(),
                        TelemetryValue::Float(value),
                    );
                }
                Calculation::ApparentPower => {
                    let value = apparent_power_va(&record)?;
                    ensure_output_available(&record, "apparent_power_va")?;
                    record
                        .fields
                        .insert("apparent_power_va".to_owned(), TelemetryValue::Float(value));
                }
                Calculation::EstimatedPowerFactor => {
                    ensure_output_available(&record, "estimated_power_factor")?;
                    if let Some(value) = estimated_power_factor(&record)? {
                        record.fields.insert(
                            "estimated_power_factor".to_owned(),
                            TelemetryValue::Float(value),
                        );
                    }
                }
                Calculation::Iaq => {
                    let warmed_up = match record.fields.get(WARMED_UP_FIELD) {
                        Some(TelemetryValue::Boolean(warmed_up)) => *warmed_up,
                        None => true,
                        Some(_) => bail!("mapped field '{WARMED_UP_FIELD}' must be a boolean"),
                    };

                    let gas_ohm = required_float(&record, GAS_FIELD)?;
                    let rel_hum_perc = required_float(&record, HUMIDITY_FIELD)?;
                    validate_humidity(rel_hum_perc)?;
                    if gas_ohm <= 0.0 {
                        bail!("field '{GAS_FIELD}' must be greater than zero");
                    }
                    if !warmed_up {
                        continue;
                    }

                    ensure_output_available(&record, "iaq_score")?;
                    ensure_output_available(&record, "iaq_text")?;
                    let score = finite_result("iaq_score", iaq_score(gas_ohm, rel_hum_perc))?;
                    let text = iaq_text(score);
                    record
                        .fields
                        .insert("iaq_score".to_owned(), TelemetryValue::Float(score));
                    record
                        .fields
                        .insert("iaq_text".to_owned(), TelemetryValue::String(text));
                }
            }
        }

        Ok(record)
    }
}

fn require_mapping_type(
    fields: &BTreeMap<String, FieldMapping>,
    key: &str,
    expected: FieldType,
) -> Result<()> {
    let mapping = fields
        .get(key)
        .with_context(|| format!("calculation input '{key}' is not mapped"))?;
    if mapping.field_type != expected {
        bail!("calculation input '{key}' must have {expected:?} type");
    }
    Ok(())
}

fn required_float(record: &TelemetryRecord, key: &str) -> Result<f64> {
    match record.fields.get(key) {
        Some(TelemetryValue::Float(value)) if value.is_finite() => Ok(*value),
        Some(TelemetryValue::Float(_)) => bail!("calculation input '{key}' must be finite"),
        Some(_) => bail!("calculation input '{key}' must be a float"),
        None => bail!("calculation input '{key}' is missing"),
    }
}

fn apparent_power_va(record: &TelemetryRecord) -> Result<f64> {
    let voltage = required_float(record, "voltage_v")?;
    let current = required_float(record, "current_a")?;
    if voltage < 0.0 || current < 0.0 {
        bail!("voltage_v and current_a must be nonnegative");
    }
    finite_result("apparent_power_va", voltage * current)
}

fn estimated_power_factor(record: &TelemetryRecord) -> Result<Option<f64>> {
    let power = required_float(record, "power_w")?;
    let apparent = apparent_power_va(record)?;
    if apparent < 1.0 {
        return Ok(None);
    }
    let ratio = power.abs() / apparent;
    Ok((0.0..=1.0).contains(&ratio).then_some(ratio))
}

fn validate_humidity(rel_hum_perc: f64) -> Result<()> {
    if !(0.0 < rel_hum_perc && rel_hum_perc <= 100.0) {
        bail!("field '{HUMIDITY_FIELD}' must be in (0,100]");
    }
    Ok(())
}

fn climate_inputs(record: &TelemetryRecord) -> Result<(f64, f64)> {
    let temp_c = required_float(record, TEMPERATURE_FIELD)?;
    let rel_hum_perc = required_float(record, HUMIDITY_FIELD)?;
    validate_humidity(rel_hum_perc)?;
    if temp_c <= -273.15 {
        bail!("field '{TEMPERATURE_FIELD}' must be above absolute zero");
    }
    Ok((temp_c, rel_hum_perc))
}

fn vapor_pressure_hpa(temp_c: f64, rel_hum_perc: f64) -> Result<f64> {
    let vapor_pressure = 6.112 * (17.67 * temp_c / (temp_c + 243.5)).exp() * rel_hum_perc / 100.0;
    if !vapor_pressure.is_finite() || vapor_pressure <= 0.0 {
        bail!("vapor pressure must be positive and finite");
    }
    Ok(vapor_pressure)
}

fn finite_result(field: &str, value: f64) -> Result<f64> {
    if !value.is_finite() {
        bail!("calculation output '{field}' is not finite");
    }
    Ok(value)
}

fn ensure_output_available(record: &TelemetryRecord, key: &str) -> Result<()> {
    if record.fields.contains_key(key) {
        bail!("calculation output '{key}' collides with an existing telemetry field");
    }
    Ok(())
}

pub(crate) fn dew_point_c(temp_c: f64, rel_hum_perc: f64) -> f64 {
    let alpha = (rel_hum_perc / 100.0).ln() + ((17.27 * temp_c) / (237.7 + temp_c));
    (237.7 * alpha) / (17.27 - alpha)
}

fn temp_c_to_f(temp_c: f64) -> f64 {
    temp_c * 9.0 / 5.0 + 32.0
}

fn temp_f_to_c(temp_f: f64) -> f64 {
    (temp_f - 32.0) * 5.0 / 9.0
}

pub(crate) fn heat_index_c(temp_c: f64, rel_hum_perc: f64) -> f64 {
    let temp_f = temp_c_to_f(temp_c);

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
        return temp_f_to_c(hi_rothfusz - adjusted_hi);
    }

    if rel_hum_perc > 85.0 && (80.0..=87.0).contains(&temp_f) {
        let adjusted_hi = ((rel_hum_perc - 85.0) * (87.0 - temp_f)) / 5.0;
        return temp_f_to_c(hi_rothfusz + adjusted_hi);
    }

    if temp_f < 80.0 {
        let hi_steadman = 0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (rel_hum_perc * 0.094));
        return temp_f_to_c(hi_steadman);
    }

    temp_f_to_c(hi_rothfusz)
}

pub(crate) fn iaq_score(gas_ohm: f64, rel_hum_perc: f64) -> f64 {
    let hum_ref = 40.0;

    let hum_score = if (38.0..=42.0).contains(&rel_hum_perc) {
        0.25 * 100.0
    } else if rel_hum_perc < 38.0 {
        (0.25 / hum_ref) * rel_hum_perc * 100.0
    } else {
        0.25 / (100.0 - hum_ref) * (100.0 - rel_hum_perc) * 100.0
    };

    let gas_lower_limit = 5_000.0;
    let gas_upper_limit = 50_000.0;
    let gas_reading = gas_ohm.clamp(gas_lower_limit, gas_upper_limit);

    let gas_score = ((0.75 / (gas_upper_limit - gas_lower_limit) * gas_reading)
        - (gas_lower_limit * (0.75 / (gas_upper_limit - gas_lower_limit))))
        * 100.0;

    hum_score + gas_score
}

pub(crate) fn iaq_text(indoor_air_quality_score: f64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::routes::FieldMapping;

    fn mapping(field_type: FieldType) -> FieldMapping {
        FieldMapping {
            pointer: "/value".to_owned(),
            field_type,
            optional: false,
            default: None,
        }
    }

    fn float_fields(names: &[&str]) -> BTreeMap<String, FieldMapping> {
        names
            .iter()
            .map(|name| ((*name).to_owned(), mapping(FieldType::Float)))
            .collect()
    }

    fn valid_record() -> TelemetryRecord {
        TelemetryRecord {
            route_id: "test".to_owned(),
            device_id: "device".to_owned(),
            measurement: "measurement".to_owned(),
            timestamp_ms: 0,
            tags: BTreeMap::new(),
            fields: BTreeMap::from([
                ("temp_c".to_owned(), TelemetryValue::Float(20.0)),
                ("rel_hum_perc".to_owned(), TelemetryValue::Float(50.0)),
                ("gas_ohm".to_owned(), TelemetryValue::Float(50_000.0)),
                ("pressure_hpa".to_owned(), TelemetryValue::Float(1013.25)),
            ]),
            source_topic: "test/topic".to_owned(),
            retained: false,
        }
    }

    fn electrical_record(voltage: f64, current: f64, power: f64) -> TelemetryRecord {
        let mut record = valid_record();
        record.fields = BTreeMap::from([
            ("voltage_v".to_owned(), TelemetryValue::Float(voltage)),
            ("current_a".to_owned(), TelemetryValue::Float(current)),
            ("power_w".to_owned(), TelemetryValue::Float(power)),
        ]);
        record
    }

    #[test]
    fn test_calculation_electrical_names_work_independently() -> Result<()> {
        for (name, output, expected) in [
            ("apparent_power", "apparent_power_va", 115.0),
            ("estimated_power_factor", "estimated_power_factor", 0.8),
        ] {
            let mut record = electrical_record(230.0, 0.5, 92.0);
            if name == "apparent_power" {
                record.fields.remove("power_w");
            }
            let fields =
                float_fields(&record.fields.keys().map(String::as_str).collect::<Vec<_>>());
            let registry = CalculationRegistry::resolve(&[calculation_name(name)?], &fields)?;
            let before = record.fields.len();
            let record = registry.apply(record)?;
            assert_eq!(record.fields.len(), before + 1);
            assert_eq!(record.fields[output], TelemetryValue::Float(expected));
            assert!(!output_can_be_omitted(output, &record));
        }
        Ok(())
    }

    #[test]
    fn test_calculation_electrical_zero_low_load_signed_and_inconsistent_readings() -> Result<()> {
        let fields = float_fields(&["voltage_v", "current_a", "power_w"]);
        let registry = CalculationRegistry::resolve(
            &[
                calculation_name("estimated_power_factor")?,
                calculation_name("apparent_power")?,
            ],
            &fields,
        )?;
        for (voltage, current, power, apparent, factor) in [
            (0.0, 0.5, 0.0, 0.0, None),
            (230.0, 0.0, 0.0, 0.0, None),
            (230.0, 0.001, 0.1, 0.23, None),
            (0.999, 1.0, 0.5, 0.999, None),
            (2.0, 0.5, 0.8, 1.0, Some(0.8)),
            (230.0, 0.5, -92.0, 115.0, Some(0.8)),
            (230.0, 0.5, 120.0, 115.0, None),
            (230.0, 0.5, 0.0, 115.0, Some(0.0)),
            (230.0, 0.5, 115.0, 115.0, Some(1.0)),
        ] {
            let input = electrical_record(voltage, current, power);
            let record = registry.apply(input.clone())?;
            assert_eq!(
                record.fields["apparent_power_va"],
                TelemetryValue::Float(apparent)
            );
            assert_eq!(
                record.fields.get("estimated_power_factor"),
                factor.map(TelemetryValue::Float).as_ref()
            );
            assert_eq!(
                output_can_be_omitted("estimated_power_factor", &record),
                factor.is_none()
            );
            assert!(!output_can_be_omitted("apparent_power_va", &record));
            assert!(!output_can_be_omitted("dew_point_c", &record));
            for (name, value) in input.fields {
                assert_eq!(record.fields[&name], value);
            }
        }
        Ok(())
    }

    #[test]
    fn test_calculation_electrical_preflight_rejects_invalid_mappings_and_collisions() -> Result<()>
    {
        for (name, output) in [
            ("apparent_power", "apparent_power_va"),
            ("estimated_power_factor", "estimated_power_factor"),
        ] {
            let fields = float_fields(&["voltage_v", "current_a", "power_w"]);
            assert!(CalculationRegistry::resolve(
                &[calculation_name(name)?, calculation_name(name)?],
                &fields
            )
            .is_err());
            for key in ["voltage_v", "current_a", "power_w"] {
                if key == "power_w" && name == "apparent_power" {
                    continue;
                }
                let mut missing = float_fields(&["voltage_v", "current_a", "power_w"]);
                missing.remove(key);
                assert!(
                    CalculationRegistry::resolve(&[calculation_name(name)?], &missing).is_err()
                );
                let mut wrong = float_fields(&["voltage_v", "current_a", "power_w"]);
                wrong.insert(key.to_owned(), mapping(FieldType::Integer));
                assert!(CalculationRegistry::resolve(&[calculation_name(name)?], &wrong).is_err());
            }
            let mut collision = fields;
            collision.insert(output.to_owned(), mapping(FieldType::Float));
            assert!(CalculationRegistry::resolve(&[calculation_name(name)?], &collision).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_calculation_electrical_rejects_invalid_inputs_and_product_overflow() -> Result<()> {
        let fields = float_fields(&["voltage_v", "current_a", "power_w"]);
        for name in ["apparent_power", "estimated_power_factor"] {
            let registry = CalculationRegistry::resolve(&[calculation_name(name)?], &fields)?;
            for key in ["voltage_v", "current_a", "power_w"] {
                if key == "power_w" && name == "apparent_power" {
                    continue;
                }
                let mut invalid_values = vec![
                    TelemetryValue::Float(f64::NAN),
                    TelemetryValue::Float(f64::INFINITY),
                    TelemetryValue::Float(f64::NEG_INFINITY),
                    TelemetryValue::Integer(1),
                ];
                if key != "power_w" {
                    invalid_values.push(TelemetryValue::Float(-1.0));
                }
                for value in invalid_values {
                    let mut record = electrical_record(0.0, 0.0, 0.0);
                    record.fields.insert(key.to_owned(), value);
                    assert!(registry.apply(record.clone()).is_err(), "{name}: {key}");
                    assert!(!output_can_be_omitted("estimated_power_factor", &record));
                }
                let mut record = electrical_record(0.0, 0.0, 0.0);
                record.fields.remove(key);
                assert!(registry.apply(record.clone()).is_err());
                assert!(!output_can_be_omitted("estimated_power_factor", &record));
            }
            let overflow = electrical_record(f64::MAX, 2.0, 0.0);
            assert!(registry.apply(overflow.clone()).is_err());
            assert!(!output_can_be_omitted("estimated_power_factor", &overflow));
            let mut collision = electrical_record(230.0, 0.0, 0.0);
            collision.fields.insert(
                if name == "apparent_power" {
                    "apparent_power_va"
                } else {
                    name
                }
                .to_owned(),
                TelemetryValue::Float(1.0),
            );
            assert!(registry.apply(collision).is_err());
        }
        Ok(())
    }

    const CLIMATE_CALCULATIONS: [(&str, &str, f64); 4] = [
        ("absolute_humidity", "absolute_humidity_g_m3", 8.63750),
        ("humidity_ratio", "humidity_ratio_g_kg", 7.25631),
        ("surface_temp_at_80rh", "surface_temp_at_80rh_c", 12.62770),
        ("dew_point_depression", "dew_point_depression_c", 10.74571),
    ];

    #[test]
    fn test_calculation_electrical_encoded_size_bound_covers_large_finite_outputs() -> Result<()> {
        use crate::infrastructure::sink::{influx::InfluxEncoder, Encoder};

        let fields = float_fields(&["voltage_v", "current_a", "power_w"]);
        let registry = CalculationRegistry::resolve(
            &[
                calculation_name("apparent_power")?,
                calculation_name("estimated_power_factor")?,
            ],
            &fields,
        )?;
        for input in [
            electrical_record(230.0, 0.5, 92.0),
            electrical_record(f64::MAX, 1.0, -f64::MAX),
        ] {
            let record = registry.apply(input)?;
            let mut encoded = String::new();
            InfluxEncoder.encode(&record, &mut encoded);
            assert!(encoded.len() <= InfluxEncoder.encoded_size_upper_bound(&record));
            assert!(!encoded.contains("NaN") && !encoded.contains("inf"));
        }
        Ok(())
    }

    fn calculation_name(name: &str) -> Result<CalculationName> {
        Ok(serde_json::from_value(serde_json::json!(name))?)
    }

    #[test]
    fn test_calculation_climate_reference_values_work_independently() -> Result<()> {
        for (name, output, expected) in CLIMATE_CALCULATIONS {
            let mut inputs = vec!["temp_c", "rel_hum_perc"];
            if name == "humidity_ratio" {
                inputs.push("pressure_hpa");
            }
            let registry =
                CalculationRegistry::resolve(&[calculation_name(name)?], &float_fields(&inputs))?;
            let mut input = valid_record();
            input.fields.retain(|key, _| inputs.contains(&key.as_str()));
            input
                .fields
                .insert("warmed_up".to_owned(), TelemetryValue::Boolean(false));
            let record = registry.apply(input.clone())?;
            assert_eq!(record.fields.len(), input.fields.len() + 1);
            let TelemetryValue::Float(value) = record.fields[output] else {
                panic!("{output} must be a float");
            };
            assert!((value - expected).abs() < 0.00001, "{name}: {value}");
            assert!(record.tags.is_empty());

            let mut saturated = input;
            saturated
                .fields
                .insert("rel_hum_perc".to_owned(), TelemetryValue::Float(100.0));
            let saturated = registry.apply(saturated)?;
            let TelemetryValue::Float(value) = saturated.fields[output] else {
                panic!("{output} must be a float");
            };
            assert!(value.is_finite());
            match name {
                "absolute_humidity" => assert!((value - 17.27500).abs() < 0.00001),
                "humidity_ratio" => assert!((value - 14.68394).abs() < 0.00001),
                "surface_temp_at_80rh" => assert!(value > 20.0),
                "dew_point_depression" => assert!(value.abs() < 1e-12),
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    #[test]
    fn test_calculation_climate_preflight_rejects_invalid_mappings_and_collisions() -> Result<()> {
        for (name, output, _) in CLIMATE_CALCULATIONS {
            let fields = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
            for input in ["temp_c", "rel_hum_perc", "pressure_hpa"] {
                if input == "pressure_hpa" && name != "humidity_ratio" {
                    continue;
                }
                let mut missing = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
                missing.remove(input);
                assert!(
                    CalculationRegistry::resolve(&[calculation_name(name)?], &missing).is_err()
                );
                let mut wrong = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
                wrong.insert(input.to_owned(), mapping(FieldType::Integer));
                assert!(CalculationRegistry::resolve(&[calculation_name(name)?], &wrong).is_err());
            }
            assert!(CalculationRegistry::resolve(
                &[calculation_name(name)?, calculation_name(name)?],
                &fields
            )
            .is_err());
            let mut collision = fields;
            collision.insert(output.to_owned(), mapping(FieldType::Float));
            assert!(CalculationRegistry::resolve(&[calculation_name(name)?], &collision).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_calculation_climate_rejects_invalid_domains_and_runtime_inputs() -> Result<()> {
        for (name, output, _) in CLIMATE_CALCULATIONS {
            let fields = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
            let registry = CalculationRegistry::resolve(&[calculation_name(name)?], &fields)?;
            for (field, value) in [
                ("rel_hum_perc", 0.0),
                ("rel_hum_perc", -1.0),
                ("rel_hum_perc", 100.1),
                ("temp_c", -273.15),
                ("temp_c", -300.0),
            ] {
                let mut record = valid_record();
                record
                    .fields
                    .insert(field.to_owned(), TelemetryValue::Float(value));
                assert!(registry.apply(record).is_err(), "{name}: {field}={value}");
            }
            for field in ["temp_c", "rel_hum_perc", "pressure_hpa"] {
                if field == "pressure_hpa" && name != "humidity_ratio" {
                    continue;
                }
                for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                    let mut record = valid_record();
                    record
                        .fields
                        .insert(field.to_owned(), TelemetryValue::Float(value));
                    assert!(registry.apply(record).is_err(), "{name}: {field}={value}");
                }
                let mut missing = valid_record();
                missing.fields.remove(field);
                assert!(registry.apply(missing).is_err());
                let mut wrong = valid_record();
                wrong
                    .fields
                    .insert(field.to_owned(), TelemetryValue::Integer(20));
                assert!(registry.apply(wrong).is_err());
            }
            let mut collision = valid_record();
            collision
                .fields
                .insert(output.to_owned(), TelemetryValue::Float(1.0));
            assert!(registry.apply(collision).is_err());
            let singular_temperatures: &[f64] = if name == "dew_point_depression" {
                &[-237.7, f64::MAX]
            } else {
                &[-243.5, -243.50001, -243.49999, f64::MAX]
            };
            for &temperature in singular_temperatures {
                let mut record = valid_record();
                record
                    .fields
                    .insert("temp_c".to_owned(), TelemetryValue::Float(temperature));
                assert!(registry.apply(record).is_err(), "{name}: T={temperature}");
            }
        }
        Ok(())
    }

    #[test]
    fn test_calculation_humidity_ratio_requires_pressure_above_vapor_pressure() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
        let registry =
            CalculationRegistry::resolve(&[calculation_name("humidity_ratio")?], &fields)?;
        // At 0 C and 50% RH, vapor pressure is exactly 6.112 / 2 hPa.
        for pressure in [-1.0, 0.0, 3.0, 3.056] {
            let mut record = valid_record();
            record
                .fields
                .insert("temp_c".to_owned(), TelemetryValue::Float(0.0));
            record
                .fields
                .insert("pressure_hpa".to_owned(), TelemetryValue::Float(pressure));
            assert!(registry.apply(record).is_err(), "p={pressure}");
        }
        Ok(())
    }

    #[test]
    fn test_calculation_surface_temperature_rejects_inverse_singularity() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        let registry =
            CalculationRegistry::resolve(&[calculation_name("surface_temp_at_80rh")?], &fields)?;
        let mut record = valid_record();
        // At this temperature and 100% RH, ln(e / (0.8 * 6.112)) reaches 17.67.
        record.fields.insert(
            "temp_c".to_owned(),
            TelemetryValue::Float(19_038.459_862_426_946),
        );
        record
            .fields
            .insert("rel_hum_perc".to_owned(), TelemetryValue::Float(100.0));
        assert!(registry.apply(record).is_err());
        Ok(())
    }

    #[test]
    fn test_calculation_climate_encoded_size_bound_covers_finite_outputs() -> Result<()> {
        use crate::infrastructure::sink::{influx::InfluxEncoder, Encoder};

        let names = CLIMATE_CALCULATIONS
            .iter()
            .map(|(name, _, _)| calculation_name(name))
            .collect::<Result<Vec<_>>>()?;
        let fields = float_fields(&["temp_c", "rel_hum_perc", "pressure_hpa"]);
        let registry = CalculationRegistry::resolve(&names, &fields)?;
        for humidity in [50.0, 100.0, 1e-200] {
            let mut record = valid_record();
            record
                .fields
                .insert("rel_hum_perc".to_owned(), TelemetryValue::Float(humidity));
            let record = registry.apply(record)?;
            let mut encoded = String::new();
            InfluxEncoder.encode(&record, &mut encoded);
            assert!(encoded.len() <= InfluxEncoder.encoded_size_upper_bound(&record));
            assert!(!encoded.contains("NaN") && !encoded.contains("inf"));
        }
        Ok(())
    }

    #[test]
    fn test_calculation_resolve_rejects_iaq_without_gas_mapping() {
        let fields = float_fields(&["rel_hum_perc"]);
        assert!(CalculationRegistry::resolve(&[CalculationName::Iaq], &fields).is_err());
    }

    #[test]
    fn test_calculation_resolve_rejects_unknown_name() {
        assert!(serde_json::from_str::<CalculationName>("\"unknown\"").is_err());
    }

    #[test]
    fn test_calculation_resolve_rejects_duplicate_and_colliding_outputs() {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        assert!(CalculationRegistry::resolve(
            &[CalculationName::DewPoint, CalculationName::DewPoint],
            &fields
        )
        .is_err());

        let mut colliding_fields = fields;
        colliding_fields.insert("dew_point_c".to_owned(), mapping(FieldType::Float));
        assert!(
            CalculationRegistry::resolve(&[CalculationName::DewPoint], &colliding_fields).is_err()
        );
    }

    #[test]
    fn test_calculation_resolve_requires_expected_mapping_types() {
        let missing_temperature = float_fields(&["rel_hum_perc"]);
        assert!(
            CalculationRegistry::resolve(&[CalculationName::HeatIndex], &missing_temperature)
                .is_err()
        );

        let mut wrong_type = float_fields(&["temp_c", "rel_hum_perc"]);
        wrong_type.insert("temp_c".to_owned(), mapping(FieldType::Integer));
        assert!(CalculationRegistry::resolve(&[CalculationName::HeatIndex], &wrong_type).is_err());

        let mut wrong_warmed_up = float_fields(&["gas_ohm", "rel_hum_perc"]);
        wrong_warmed_up.insert("warmed_up".to_owned(), mapping(FieldType::String));
        assert!(CalculationRegistry::resolve(&[CalculationName::Iaq], &wrong_warmed_up).is_err());
    }

    #[test]
    fn test_calculation_apply_empty_sequence_preserves_record() -> Result<()> {
        let record = valid_record();
        let registry = CalculationRegistry::resolve(&[], &BTreeMap::new())?;
        assert_eq!(registry.apply(record.clone())?, record);
        Ok(())
    }

    #[test]
    fn test_calculation_apply_populates_derived_values() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc", "gas_ohm"]);
        let registry = CalculationRegistry::resolve(
            &[
                CalculationName::DewPoint,
                CalculationName::HeatIndex,
                CalculationName::Iaq,
            ],
            &fields,
        )?;

        let record = registry.apply(valid_record())?;
        let TelemetryValue::Float(dew_point) = record.fields["dew_point_c"] else {
            panic!("dew point must be a float");
        };
        assert!((dew_point - 9.27).abs() < 0.05, "got {dew_point}");

        let TelemetryValue::Float(heat_index) = record.fields["heat_index_c"] else {
            panic!("heat index must be a float");
        };
        assert!((heat_index - 19.36).abs() < 0.05, "got {heat_index}");

        let TelemetryValue::Float(iaq) = record.fields["iaq_score"] else {
            panic!("IAQ score must be a float");
        };
        assert!((iaq - 95.833_333_333_333_33).abs() < 0.001, "got {iaq}");
        assert_eq!(
            record.fields["iaq_text"],
            TelemetryValue::String("Air quality is Good".to_owned())
        );
        Ok(())
    }

    #[test]
    fn test_calculation_apply_accepts_humidity_at_one_hundred_percent() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::DewPoint], &fields)?;
        let mut record = valid_record();
        record
            .fields
            .insert(HUMIDITY_FIELD.to_owned(), TelemetryValue::Float(100.0));

        let record = registry.apply(record)?;
        assert_eq!(record.fields["dew_point_c"], TelemetryValue::Float(20.0));
        Ok(())
    }

    #[test]
    fn test_calculation_apply_rejects_invalid_humidity() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::DewPoint], &fields)?;

        for humidity in [0.0, -1.0, 100.1, f64::NAN, f64::INFINITY] {
            let mut record = valid_record();
            record
                .fields
                .insert(HUMIDITY_FIELD.to_owned(), TelemetryValue::Float(humidity));
            assert!(registry.apply(record).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_calculation_apply_rejects_singular_or_non_finite_results() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::DewPoint], &fields)?;

        let mut singular = valid_record();
        singular
            .fields
            .insert(TEMPERATURE_FIELD.to_owned(), TelemetryValue::Float(-237.7));
        assert!(registry.apply(singular).is_err());

        let mut non_finite = valid_record();
        non_finite.fields.insert(
            TEMPERATURE_FIELD.to_owned(),
            TelemetryValue::Float(f64::MAX),
        );
        assert!(registry.apply(non_finite).is_err());
        Ok(())
    }

    #[test]
    fn test_calculation_apply_omits_iaq_when_not_warmed_up() -> Result<()> {
        let fields = float_fields(&["gas_ohm", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::Iaq], &fields)?;
        let mut record = valid_record();
        record
            .fields
            .insert(WARMED_UP_FIELD.to_owned(), TelemetryValue::Boolean(false));

        let record = registry.apply(record)?;
        assert!(!record.fields.contains_key("iaq_score"));
        assert!(!record.fields.contains_key("iaq_text"));
        Ok(())
    }

    #[test]
    fn test_calculation_apply_rejects_invalid_iaq_inputs_when_not_warmed_up() -> Result<()> {
        let fields = float_fields(&["gas_ohm", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::Iaq], &fields)?;

        for (field, value) in [
            (GAS_FIELD, -1.0),
            (GAS_FIELD, 0.0),
            (HUMIDITY_FIELD, 0.0),
            (HUMIDITY_FIELD, 100.1),
        ] {
            let mut record = valid_record();
            record
                .fields
                .insert(WARMED_UP_FIELD.to_owned(), TelemetryValue::Boolean(false));
            record
                .fields
                .insert(field.to_owned(), TelemetryValue::Float(value));

            assert!(
                registry.apply(record).is_err(),
                "invalid {field} value {value} must be rejected during warm-up"
            );
        }

        for field in [GAS_FIELD, HUMIDITY_FIELD] {
            let mut record = valid_record();
            record
                .fields
                .insert(WARMED_UP_FIELD.to_owned(), TelemetryValue::Boolean(false));
            record.fields.remove(field);

            assert!(
                registry.apply(record).is_err(),
                "missing {field} must be rejected during warm-up"
            );
        }

        Ok(())
    }

    #[test]
    fn test_calculation_apply_rejects_missing_or_wrong_runtime_inputs() -> Result<()> {
        let fields = float_fields(&["temp_c", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::DewPoint], &fields)?;
        let mut missing = valid_record();
        missing.fields.remove(TEMPERATURE_FIELD);
        assert!(registry.apply(missing).is_err());

        let mut wrong_type = valid_record();
        wrong_type
            .fields
            .insert(TEMPERATURE_FIELD.to_owned(), TelemetryValue::Integer(20));
        assert!(registry.apply(wrong_type).is_err());
        Ok(())
    }

    #[test]
    fn test_calculation_apply_rejects_non_positive_gas() -> Result<()> {
        let fields = float_fields(&["gas_ohm", "rel_hum_perc"]);
        let registry = CalculationRegistry::resolve(&[CalculationName::Iaq], &fields)?;

        for gas_ohm in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let mut record = valid_record();
            record
                .fields
                .insert(GAS_FIELD.to_owned(), TelemetryValue::Float(gas_ohm));
            assert!(registry.apply(record).is_err());
        }
        Ok(())
    }
}
