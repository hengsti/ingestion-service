use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde_json::to_value;

use crate::{
    calculations::output_can_be_omitted,
    config::routes::FieldType,
    infrastructure::{router::CompiledRoute, sink::Encoder, wal::codec::wal_record_frame_size},
    model::messages::telemetry::{validate_line_text, TelemetryRecord, TelemetryValue},
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
        stages::telemetry_mapping::validate_timestamp_range,
    },
};

/// Validates the normalized configured record before it reaches persistence.
pub struct ValidateOutputStage {
    encoder: Arc<dyn Encoder>,
}

impl ValidateOutputStage {
    /// Creates the normalized output validator.
    pub fn new(encoder: Arc<dyn Encoder>) -> Self {
        Self { encoder }
    }
}

impl PipelineStage for ValidateOutputStage {
    fn name(&self) -> &'static str {
        "validate_output"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            validate_record(
                ctx.route()?,
                ctx.telemetry_record()?,
                ctx.topic(),
                ctx.retained(),
                self.encoder.as_ref(),
            )?;
            Ok(StageFlow::Continue)
        })
    }
}

fn validate_record(
    route: &CompiledRoute,
    record: &TelemetryRecord,
    topic: &str,
    retained: bool,
    encoder: &dyn Encoder,
) -> Result<()> {
    let route_config = route.config();
    if record.route_id != route.id() || record.measurement != route_config.measurement {
        bail!("telemetry record identity does not match its selected route");
    }
    if record.source_topic != topic || record.retained != retained {
        bail!("telemetry record source metadata does not match intake metadata");
    }
    if record.device_id.is_empty() || record.measurement.is_empty() {
        bail!("telemetry record identity and measurement must not be empty");
    }
    validate_timestamp_range(record.timestamp_ms)?;

    let device_tag = record
        .tags
        .get("device_id")
        .context("telemetry record is missing the reserved device_id tag")?;
    if device_tag != &record.device_id {
        bail!("reserved device_id tag does not match telemetry record identity");
    }
    if record.fields.is_empty() {
        bail!("telemetry record must contain at least one field");
    }
    match record.fields.get("mqtt_retained") {
        Some(TelemetryValue::Boolean(value)) if *value == retained => {}
        Some(TelemetryValue::Boolean(_)) => {
            bail!("reserved mqtt_retained field does not match intake metadata");
        }
        _ => bail!("telemetry record is missing the reserved mqtt_retained boolean field"),
    }
    if record
        .tags
        .keys()
        .any(|key| record.fields.contains_key(key))
    {
        bail!("telemetry record tag and field keys must not collide");
    }

    if record.tags.len() != route_config.tags.len() + 1
        || record
            .tags
            .keys()
            .any(|key| key != "device_id" && !route_config.tags.contains_key(key.as_str()))
    {
        bail!("telemetry record tags do not match the selected route");
    }
    for (key, mapping) in &route_config.fields {
        match record.fields.get(key) {
            Some(value) if field_type_matches(value, mapping.field_type) => {}
            Some(_) => bail!("field '{key}' does not match its configured scalar type"),
            None if !mapping.optional => bail!("required configured field '{key}' is missing"),
            None => {}
        }
    }
    for (key, field_type) in route.calculations().output_field_types() {
        match record.fields.get(key) {
            Some(value) if field_type_matches(value, field_type) => {}
            Some(_) => bail!("calculated field '{key}' does not match its configured type"),
            None if output_can_be_omitted(key, record) => {}
            None => bail!("calculated field '{key}' is missing"),
        }
    }
    if record.fields.keys().any(|key| {
        key != "mqtt_retained"
            && !route_config.fields.contains_key(key.as_str())
            && !route
                .calculations()
                .output_field_types()
                .any(|(field, _)| field == key)
    }) {
        bail!("telemetry record contains a field not configured by its route");
    }

    for value in record.tags.values() {
        validate_line_text(value, "tag value")?;
    }
    for value in record.fields.values() {
        if let TelemetryValue::String(value) = value {
            validate_line_text(value, "string field")?;
        }
        if let TelemetryValue::Float(value) = value {
            if !value.is_finite() {
                bail!("float fields must be finite");
            }
        }
    }
    validate_line_text(&record.source_topic, "source topic")?;

    let line_bytes = encoder.encoded_size_upper_bound(record);
    wal_record_frame_size(topic.len(), line_bytes)
        .context("rendered telemetry exceeds the WAL reader record limit")?;

    if let Some(schema) = route.output_schema() {
        schema.validate(&to_value(record).context("serializing normalized telemetry")?)?;
    }

    Ok(())
}

fn field_type_matches(value: &TelemetryValue, field_type: FieldType) -> bool {
    matches!(
        (value, field_type),
        (TelemetryValue::Float(_), FieldType::Float)
            | (TelemetryValue::Integer(_), FieldType::Integer)
            | (TelemetryValue::Unsigned(_), FieldType::Unsigned)
            | (TelemetryValue::Boolean(_), FieldType::Boolean)
            | (TelemetryValue::String(_), FieldType::String)
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, sync::Arc};

    use anyhow::{Context, Result};
    use serde_json::json;
    use tempfile::TempDir;

    use crate::{
        config::routes::RouteBundle,
        infrastructure::{
            router::{CompiledRoute, RouteSet},
            sink::influx::InfluxEncoder,
            wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES,
        },
        model::messages::telemetry::{TelemetryRecord, TelemetryValue},
        pipeline::{context::PipelineContext, stage::PipelineStage},
    };

    const TOPIC: &str = "home/device-1/sensor";
    const DRAFT_7: &str = "http://json-schema.org/draft-07/schema#";

    fn validate_record(
        route: &CompiledRoute,
        record: &TelemetryRecord,
        topic: &str,
        retained: bool,
    ) -> Result<()> {
        super::validate_record(route, record, topic, retained, &InfluxEncoder)
    }

    fn test_case() -> Result<(TempDir, Arc<RouteSet>, Arc<CompiledRoute>, TelemetryRecord)> {
        test_case_with_reading_key("reading")
    }

    fn test_case_with_reading_key(
        reading_key: &str,
    ) -> Result<(TempDir, Arc<RouteSet>, Arc<CompiledRoute>, TelemetryRecord)> {
        let directory = tempfile::tempdir()?;
        let raw_schema_path = directory.path().join("raw.json");
        let output_schema_path = directory.path().join("output.json");
        fs::write(
            &raw_schema_path,
            serde_json::to_vec(&json!({"$schema": DRAFT_7, "type": "object"}))?,
        )?;
        fs::write(
            &output_schema_path,
            serde_json::to_vec(&json!({
                "$schema": DRAFT_7,
                "type": "object",
                "properties": {
                    "fields": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string", "minLength": 2}
                        }
                    }
                }
            }))?,
        )?;

        let mut fields = json!({
            "label": {"pointer": "/label", "type": "string", "optional": true}
        });
        fields[reading_key] = json!({"pointer": "/reading", "type": "float"});
        let bundle: RouteBundle = serde_json::from_value(json!({
            "version": 1,
            "schema_resources": {},
            "routes": [{
                "id": "test-route",
                "topic": "home/+/sensor",
                "schema": raw_schema_path,
                "output_schema": output_schema_path,
                "device_id": "device-1",
                "measurement": "test_measurement",
                "tags": {"room": "lab"},
                "fields": fields,
                "timestamp": {"source": "received_at"}
            }]
        }))?;
        let routes = Arc::new(RouteSet::compile(bundle, "home/dlq")?);
        let route = routes.select(TOPIC).context("test route should match")?;
        let record = TelemetryRecord {
            route_id: "test-route".to_owned(),
            device_id: "device-1".to_owned(),
            measurement: "test_measurement".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::from([
                ("device_id".to_owned(), "device-1".to_owned()),
                ("room".to_owned(), "lab".to_owned()),
            ]),
            fields: BTreeMap::from([
                ("label".to_owned(), TelemetryValue::String("ok".to_owned())),
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
                (reading_key.to_owned(), TelemetryValue::Float(21.5)),
            ]),
            source_topic: TOPIC.to_owned(),
            retained: false,
        };
        Ok((directory, routes, route, record))
    }

    #[test]
    fn test_telemetry_output_validation_rejects_invalid_timestamp_and_non_finite_values(
    ) -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record.timestamp_ms = i64::MAX;
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.timestamp_ms = 1_700_000_000_000;
        record
            .fields
            .insert("reading".to_owned(), TelemetryValue::Float(f64::NAN));
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_rejects_reserved_metadata_collisions() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record.fields.remove("mqtt_retained");
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record
            .fields
            .insert("mqtt_retained".to_owned(), TelemetryValue::Boolean(true));
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record
            .fields
            .insert("mqtt_retained".to_owned(), TelemetryValue::Boolean(false));
        record.fields.insert(
            "room".to_owned(),
            TelemetryValue::String("duplicate".to_owned()),
        );
        let error = validate_record(&route, &record, TOPIC, false)
            .expect_err("duplicate tag and field keys must fail");
        assert!(error.to_string().contains("tag and field keys"));

        record.fields.clear();
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_rejects_tags_not_configured_by_route() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record
            .tags
            .insert("unexpected".to_owned(), "value".to_owned());
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.tags.remove("unexpected");
        record.tags.remove("room");
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_enforces_configured_fields_and_types() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record
            .fields
            .insert("unexpected".to_owned(), TelemetryValue::Boolean(true));
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.fields.remove("unexpected");
        record.fields.insert(
            "reading".to_owned(),
            TelemetryValue::String("wrong".to_owned()),
        );
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.fields.remove("reading");
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_telemetry_output_validation_requires_complete_calculated_iaq() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let schema_path = directory.path().join("raw.json");
        fs::write(
            &schema_path,
            serde_json::to_vec(&json!({"$schema": DRAFT_7, "type": "object"}))?,
        )?;
        let bundle: RouteBundle = serde_json::from_value(json!({
            "version": 1,
            "routes": [{
                "id": "iaq-route",
                "topic": "home/+/sensor",
                "schema": schema_path,
                "device_id": "device-1",
                "measurement": "air",
                "fields": {
                    "gas_ohm": {"pointer": "/gas", "type": "float"},
                    "rel_hum_perc": {"pointer": "/humidity", "type": "float"},
                    "warmed_up": {"pointer": "/warmed_up", "type": "boolean", "optional": true}
                },
                "timestamp": {"source": "received_at"},
                "calculations": ["iaq"]
            }]
        }))?;
        let routes = RouteSet::compile(bundle, "home/dlq")?;
        let route = routes.select(TOPIC).context("IAQ route should match")?;
        let mut record = TelemetryRecord {
            route_id: "iaq-route".to_owned(),
            device_id: "device-1".to_owned(),
            measurement: "air".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::from([("device_id".to_owned(), "device-1".to_owned())]),
            fields: BTreeMap::from([
                ("gas_ohm".to_owned(), TelemetryValue::Float(100.0)),
                ("rel_hum_perc".to_owned(), TelemetryValue::Float(40.0)),
                ("warmed_up".to_owned(), TelemetryValue::Boolean(true)),
                ("iaq_score".to_owned(), TelemetryValue::Float(50.0)),
                (
                    "iaq_text".to_owned(),
                    TelemetryValue::String("Air quality is Good".to_owned()),
                ),
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
            ]),
            source_topic: TOPIC.to_owned(),
            retained: false,
        };
        let stage = super::ValidateOutputStage::new(Arc::new(InfluxEncoder));
        let mut ctx = PipelineContext::new_with_metadata(TOPIC, Vec::<u8>::new(), 1, false);
        ctx.set_route(route);

        ctx.set_telemetry_record(record.clone());
        assert!(stage.run(&mut ctx).await.is_ok());

        record.fields.remove("iaq_text");
        ctx.set_telemetry_record(record.clone());
        assert!(stage.run(&mut ctx).await.is_err());

        record
            .fields
            .insert("warmed_up".to_owned(), TelemetryValue::Boolean(false));
        ctx.set_telemetry_record(record.clone());
        assert!(stage.run(&mut ctx).await.is_err());

        record.fields.remove("iaq_score");
        ctx.set_telemetry_record(record.clone());
        assert!(stage.run(&mut ctx).await.is_ok());

        record.fields.remove("warmed_up");
        ctx.set_telemetry_record(record);
        assert!(stage.run(&mut ctx).await.is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_rejects_control_characters() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record.measurement = "bad\nmeasurement".to_owned();
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.measurement = "test_measurement".to_owned();
        record
            .tags
            .insert("room".to_owned(), "bad\rvalue".to_owned());
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        record.tags.insert("room".to_owned(), "lab".to_owned());
        record.fields.insert(
            "label".to_owned(),
            TelemetryValue::String("bad\0value".to_owned()),
        );
        assert!(validate_record(&route, &record, TOPIC, false).is_err());

        assert!(test_case_with_reading_key("bad\u{7f}key").is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_checks_configured_output_schema() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record
            .fields
            .insert("label".to_owned(), TelemetryValue::String("x".to_owned()));
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_output_validation_bounds_wal_output_before_encoding() -> Result<()> {
        let (_directory, _routes, route, mut record) = test_case()?;

        record.fields.insert(
            "label".to_owned(),
            TelemetryValue::String("x".repeat(MAX_WAL_RECORD_PAYLOAD_BYTES)),
        );
        assert!(validate_record(&route, &record, TOPIC, false).is_err());
        Ok(())
    }
}
