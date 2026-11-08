use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::calculations::CalculationRegistry;
use crate::config::routes::{DeviceIdSource, FieldType, RouteBundle, RouteConfig};
use crate::infrastructure::schema::{load_schema_resources, CompiledSchema};
use crate::model::topic::MqttTopicFilter;

/// An immutable route with schemas and topic filters compiled at startup.
pub struct CompiledRoute {
    config: RouteConfig,
    metric_label: Arc<str>,
    topic_filter: MqttTopicFilter,
    raw_schema: CompiledSchema,
    output_schema: Option<CompiledSchema>,
    calculations: CalculationRegistry,
}

impl CompiledRoute {
    /// Returns this route's configured identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.config.id
    }

    pub(crate) fn metric_label(&self) -> Arc<str> {
        Arc::clone(&self.metric_label)
    }

    /// Returns the complete validated route configuration.
    #[must_use]
    pub fn config(&self) -> &RouteConfig {
        &self.config
    }

    /// Returns the exact configured topic filter used for subscription.
    #[must_use]
    pub fn topic_filter(&self) -> &str {
        self.topic_filter.as_str()
    }

    /// Returns the optional compiled output schema.
    #[must_use]
    pub fn output_schema(&self) -> Option<&CompiledSchema> {
        self.output_schema.as_ref()
    }

    /// Returns this route's calculations resolved against its field mappings.
    #[must_use]
    pub fn calculations(&self) -> &CalculationRegistry {
        &self.calculations
    }

    /// Validates a raw payload against this route's compiled schema.
    ///
    /// # Errors
    ///
    /// Returns bounded, value-free schema diagnostics when validation fails.
    pub fn validate_raw(&self, payload: &Value) -> Result<()> {
        self.raw_schema.validate(payload)
    }
}

/// A compiled set of nonoverlapping routes and their exact broker subscriptions.
pub struct RouteSet {
    routes: Vec<Arc<CompiledRoute>>,
}

impl RouteSet {
    /// Compiles a validated route bundle and rejects DLQ feedback subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid DLQ topic, unsupported bundle version, empty route
    /// set, invalid/overlapping filters, DLQ feedback, or schema compilation failure.
    pub fn compile(bundle: RouteBundle, dlq_topic: &str) -> Result<Self> {
        if bundle.version != 1 {
            bail!("unsupported route bundle version {}", bundle.version);
        }
        if bundle.routes.is_empty() {
            bail!("route bundle must contain at least one route");
        }

        let dlq_filter = MqttTopicFilter::new(dlq_topic).context("invalid DLQ topic")?;
        if !dlq_filter.matches(dlq_topic) {
            bail!("DLQ destination must be a concrete MQTT topic name");
        }

        let mut compiled_filters = Vec::with_capacity(bundle.routes.len());
        for route in &bundle.routes {
            route
                .validate_static()
                .with_context(|| format!("route '{}' is invalid", route.id))?;
            if compiled_filters
                .iter()
                .any(|(id, _): &(String, MqttTopicFilter)| id == &route.id)
            {
                bail!("duplicate route id '{}'", route.id);
            }
            let filter = MqttTopicFilter::new(&route.topic)
                .with_context(|| format!("route '{}' has an invalid topic filter", route.id))?;
            let device_id_segment = match &route.device_id {
                DeviceIdSource::TopicSegment(config) => Some(config.topic_segment),
                DeviceIdSource::PointerAndTopicSegment(config) => Some(config.topic_segment),
                DeviceIdSource::Fixed(_) | DeviceIdSource::Pointer(_) => None,
            };
            if device_id_segment.is_some_and(|index| !filter.has_fixed_segment(index)) {
                bail!(
                    "route '{}' device identity topic_segment must reference a fixed topic level",
                    route.id
                );
            }
            if filter.matches(dlq_topic) {
                bail!("route '{}' subscribes to the DLQ destination", route.id);
            }
            if let Some((other_route, _)) =
                compiled_filters
                    .iter()
                    .find(|(_, other_filter): &&(String, MqttTopicFilter)| {
                        filter.overlaps(other_filter)
                    })
            {
                bail!(
                    "routes '{}' and '{}' have overlapping topic filters",
                    other_route,
                    route.id
                );
            }
            compiled_filters.push((route.id.clone(), filter));
        }

        let schema_resources = load_schema_resources("route bundle", &bundle.schema_resources)?;
        let mut field_types = HashMap::new();
        let routes = bundle
            .routes
            .into_iter()
            .zip(compiled_filters)
            .map(|(route, (_, topic_filter))| {
                let calculations = CalculationRegistry::resolve(&route.calculations, &route.fields)
                    .with_context(|| format!("route '{}' calculations are invalid", route.id))?;
                for (field, mapping) in &route.fields {
                    register_field_type(&mut field_types, &route, field, mapping.field_type)?;
                }
                register_field_type(
                    &mut field_types,
                    &route,
                    "mqtt_retained",
                    FieldType::Boolean,
                )?;
                for (field, field_type) in calculations.output_field_types() {
                    if route.tags.contains_key(field) {
                        bail!("calculation output '{field}' collides with a telemetry tag");
                    }
                    register_field_type(&mut field_types, &route, field, field_type)?;
                }

                let raw_schema = CompiledSchema::compile_with_resources(
                    &route.id,
                    &route.schema,
                    &schema_resources,
                )?;
                let output_schema = route
                    .output_schema
                    .as_deref()
                    .map(|path| {
                        CompiledSchema::compile_with_resources(&route.id, path, &schema_resources)
                    })
                    .transpose()?;
                // metrics-exporter-prometheus 0.18.1 treats backslash pairs as already escaped.
                let metric_label = if route.id.contains('\\') {
                    Arc::from(route.id.replace('\\', "\\\\"))
                } else {
                    Arc::from(route.id.as_str())
                };
                Ok(Arc::new(CompiledRoute {
                    config: route,
                    metric_label,
                    topic_filter,
                    raw_schema,
                    output_schema,
                    calculations,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { routes })
    }

    /// Selects the compiled route matching a topic, if any.
    #[must_use]
    pub fn select(&self, topic: &str) -> Option<Arc<CompiledRoute>> {
        self.routes
            .iter()
            .find(|route| route.topic_filter.matches(topic))
            .cloned()
    }

    /// Returns broker subscriptions in route order and exact configured spelling.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<&str> {
        self.routes
            .iter()
            .map(|route| route.topic_filter())
            .collect()
    }

    pub(crate) fn metric_labels(&self) -> impl Iterator<Item = Arc<str>> + '_ {
        self.routes.iter().map(|route| route.metric_label())
    }
}

fn register_field_type(
    field_types: &mut HashMap<(String, String), (FieldType, String)>,
    route: &RouteConfig,
    field: &str,
    field_type: FieldType,
) -> Result<()> {
    let key = (route.measurement.clone(), field.to_owned());
    if let Some((existing_type, existing_route)) = field_types.get(&key) {
        if *existing_type != field_type {
            bail!(
                "routes '{}' and '{}' declare incompatible types for field '{}' in measurement '{}'",
                existing_route,
                route.id,
                field,
                route.measurement
            );
        }
    } else {
        field_types.insert(key, (field_type, route.id.clone()));
    }

    Ok(())
}

#[cfg(test)]
mod configured_route_tests {
    use std::collections::BTreeMap;
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::config::routes::RouteBundle;
    use crate::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

    const DRAFT_7: &str = "http://json-schema.org/draft-07/schema#";

    fn write_schema(path: &std::path::Path, schema: serde_json::Value) -> Result<()> {
        fs::write(path, serde_json::to_vec(&schema)?)?;
        Ok(())
    }

    fn route(id: &str, topic: &str, schema: &std::path::Path) -> serde_json::Value {
        json!({
            "id": id,
            "topic": topic,
            "schema": schema,
            "device_id": {"pointer": "/device_id", "topic_segment": 1},
            "measurement": id,
            "fields": {
                "reading": {"pointer": "/reading", "type": "string"}
            },
            "timestamp": {"source": "received_at"}
        })
    }

    fn route_with_output_schema(
        id: &str,
        topic: &str,
        schema: &std::path::Path,
        output_schema: &std::path::Path,
    ) -> serde_json::Value {
        let mut route = route(id, topic, schema);
        route["output_schema"] = json!(output_schema);
        route
    }

    fn bundle(routes: Vec<serde_json::Value>) -> Result<RouteBundle> {
        Ok(serde_json::from_value(json!({
            "version": 1,
            "routes": routes,
            "schema_resources": {}
        }))?)
    }

    #[test]
    fn test_configured_route_set_selects_compiled_routes_and_exact_subscriptions() -> Result<()> {
        let directory = tempdir()?;
        let sensor_schema_path = directory.path().join("sensor.json");
        let status_schema_path = directory.path().join("status.json");
        let identity_schema_path = directory.path().join("identity.json");
        let output_schema_path = directory.path().join("output.json");
        write_schema(
            &sensor_schema_path,
            json!({
                "$schema": DRAFT_7,
                "$ref": "https://schemas.test/identity"
            }),
        )?;
        write_schema(
            &status_schema_path,
            json!({
                "$schema": DRAFT_7,
                "type": "object",
                "required": ["device_id"],
                "properties": {
                    "time_iso": {"type": "string", "format": "date-time"}
                }
            }),
        )?;
        write_schema(
            &identity_schema_path,
            json!({
                "$schema": DRAFT_7,
                "$id": "https://schemas.test/identity",
                "type": "object",
                "required": ["device_id"],
                "properties": {
                    "time_iso": {"type": "string", "format": "date-time"}
                }
            }),
        )?;
        write_schema(
            &output_schema_path,
            json!({"$schema": DRAFT_7, "type": "object", "required": ["temp_c"]}),
        )?;

        let sensor_filter = "smarthome/+/sensor";
        let status_filter = "smarthome/+/status";
        let mut route_bundle = bundle(vec![
            route_with_output_schema(
                "sensor",
                sensor_filter,
                &sensor_schema_path,
                &output_schema_path,
            ),
            route("status", status_filter, &status_schema_path),
        ])?;
        route_bundle.schema_resources.insert(
            "https://schemas.test/identity".to_owned(),
            identity_schema_path.clone(),
        );
        let routes = RouteSet::compile(route_bundle, "smarthome/dlq")?;

        fs::remove_file(sensor_schema_path)?;
        fs::remove_file(status_schema_path)?;
        fs::remove_file(identity_schema_path)?;
        fs::remove_file(output_schema_path)?;

        let sensor = routes
            .select("smarthome/lab/sensor")
            .context("sensor route should match")?;
        let status = routes
            .select("smarthome/lab/status")
            .context("status route should match")?;
        assert_eq!(sensor.id(), "sensor");
        assert_eq!(status.id(), "status");
        assert!(routes.select("smarthome/lab/other").is_none());
        assert_eq!(routes.subscriptions(), [sensor_filter, status_filter]);
        sensor.validate_raw(&json!({
            "device_id": "lab",
            "time_iso": "2026-11-01T00:00:00Z"
        }))?;
        assert!(sensor
            .validate_raw(&json!({
                "device_id": "lab",
                "time_iso": "not-a-date"
            }))
            .is_err());
        assert!(status.output_schema().is_none());
        let output_schema = sensor
            .output_schema()
            .context("sensor output schema should be compiled")?;
        output_schema.validate(&json!({"temp_c": 20.0}))?;
        assert!(output_schema.validate(&json!({})).is_err());
        Ok(())
    }

    #[test]
    fn test_configured_route_retains_mapping_and_resolved_calculations() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;

        let mut configured = route("sensor", "home/+/sensor", &schema_path);
        configured["fields"] = json!({
            "temp_c": {"pointer": "/temp_c", "type": "float"},
            "rel_hum_perc": {"pointer": "/rel_hum_perc", "type": "float"}
        });
        configured["calculations"] = json!(["dew_point"]);
        let routes = RouteSet::compile(bundle(vec![configured])?, "home/dlq")?;
        let compiled = routes
            .select("home/lab/sensor")
            .context("configured route should match")?;

        assert_eq!(compiled.config().measurement, "sensor");
        assert_eq!(compiled.config().fields.len(), 2);

        let record = TelemetryRecord {
            route_id: "sensor".to_owned(),
            device_id: "lab".to_owned(),
            measurement: "sensor".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::from([("device_id".to_owned(), "lab".to_owned())]),
            fields: BTreeMap::from([
                ("temp_c".to_owned(), TelemetryValue::Float(21.0)),
                ("rel_hum_perc".to_owned(), TelemetryValue::Float(45.0)),
                ("mqtt_retained".to_owned(), TelemetryValue::Boolean(false)),
            ]),
            source_topic: "home/lab/sensor".to_owned(),
            retained: false,
        };
        let calculated = compiled.calculations().apply(record)?;

        assert!(calculated.fields.contains_key("dew_point_c"));
        Ok(())
    }

    #[test]
    fn test_configured_route_set_rejects_cross_route_field_type_conflicts() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;

        let mut calculated = route("calculated", "home/+/sensor", &schema_path);
        calculated["measurement"] = json!("shared_measurement");
        calculated["fields"] = json!({
            "temp_c": {"pointer": "/temp_c", "type": "float"},
            "rel_hum_perc": {"pointer": "/rel_hum_perc", "type": "float"}
        });
        calculated["calculations"] = json!(["dew_point"]);

        let mut mapped = route("mapped", "home/+/status", &schema_path);
        mapped["measurement"] = json!("shared_measurement");
        mapped["fields"] = json!({
            "dew_point_c": {"pointer": "/dew_point_c", "type": "integer"}
        });

        let error = match RouteSet::compile(bundle(vec![calculated, mapped])?, "home/dlq") {
            Ok(_) => anyhow::bail!("conflicting field types must fail compilation"),
            Err(error) => error,
        };
        let message = format!("{error:#}");

        assert!(message.contains("dew_point_c"));
        assert!(message.contains("calculated"));
        assert!(message.contains("mapped"));
        Ok(())
    }

    #[test]
    fn test_configured_route_set_rejects_tag_and_calculated_field_collision() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;

        let mut configured = route("sensor", "home/+/sensor", &schema_path);
        configured["tags"] = json!({"dew_point_c": "fixed"});
        configured["fields"] = json!({
            "temp_c": {"pointer": "/temp_c", "type": "float"},
            "rel_hum_perc": {"pointer": "/rel_hum_perc", "type": "float"}
        });
        configured["calculations"] = json!(["dew_point"]);

        assert!(RouteSet::compile(bundle(vec![configured])?, "home/dlq").is_err());
        Ok(())
    }

    #[test]
    fn test_configured_route_set_rejects_duplicate_route_ids() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;
        let routes = bundle(vec![
            route("duplicate", "home/+/sensor", &schema_path),
            route("duplicate", "home/+/status", &schema_path),
        ])?;

        assert!(RouteSet::compile(routes, "home/dlq").is_err());
        Ok(())
    }

    #[test]
    fn test_configured_route_rejects_device_identity_from_multi_level_wildcard() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;

        let variable_suffix = bundle(vec![route("suffix", "example/#", &schema_path)])?;
        assert!(RouteSet::compile(variable_suffix, "dlq/topic").is_err());

        let fixed_segment = bundle(vec![route("fixed", "example/+/status/#", &schema_path)])?;
        assert!(RouteSet::compile(fixed_segment, "other/dlq").is_ok());
        Ok(())
    }

    #[test]
    fn test_configured_route_set_rejects_overlaps_and_dlq_feedback() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;

        let overlap = bundle(vec![
            route("wildcard", "home/+", &schema_path),
            route("literal", "home/lab", &schema_path),
        ])?;
        assert!(RouteSet::compile(overlap, "home/dlq").is_err());

        let feedback = bundle(vec![route("all-home", "home/#", &schema_path)])?;
        assert!(RouteSet::compile(feedback, "home/dlq").is_err());

        let wildcard_dlq = bundle(vec![route("sensor", "home/sensor", &schema_path)])?;
        assert!(RouteSet::compile(wildcard_dlq, "home/+").is_err());
        Ok(())
    }
}
