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
    Iaq,
}

impl Calculation {
    fn output_field_types(self) -> &'static [(&'static str, FieldType)] {
        match self {
            Self::DewPoint => &[("dew_point_c", FieldType::Float)],
            Self::HeatIndex => &[("heat_index_c", FieldType::Float)],
            Self::Iaq => &[
                ("iaq_score", FieldType::Float),
                ("iaq_text", FieldType::String),
            ],
        }
    }
}

pub(crate) fn output_can_be_omitted(field: &str, record: &TelemetryRecord) -> bool {
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

fn validate_humidity(rel_hum_perc: f64) -> Result<()> {
    if !(0.0 < rel_hum_perc && rel_hum_perc <= 100.0) {
        bail!("field '{HUMIDITY_FIELD}' must be in (0,100]");
    }
    Ok(())
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
            ]),
            source_topic: "test/topic".to_owned(),
            retained: false,
        }
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
