use anyhow::{bail, Context, Result};
use chrono::DateTime;
use serde_json::Value;

use crate::config::routes::{DeviceIdSource, RouteBundle};
use crate::infrastructure::schema::{load_schema_resources, CompiledSchema, JsonSchema};
use crate::model::messages::message::{HandledMessage, MessageType};
use crate::model::messages::sensor::SensorMessage;
use crate::model::messages::status::StatusMessage;
use crate::model::topic::{MqttTopicFilter, TopicPattern};

/// A single route combining pattern matching, schema validation, and expected message type.
pub struct Route {
    pub message_type: MessageType,
    pattern: TopicPattern,
    schema: JsonSchema,
}

impl Route {
    pub fn new(message_type: MessageType, schema_str: &str, topic_pattern: &str) -> Result<Self> {
        Ok(Self {
            message_type,
            pattern: TopicPattern::new(topic_pattern)?,
            schema: JsonSchema::new(schema_str)?,
        })
    }

    pub fn matches(&self, topic: &str) -> bool {
        self.pattern.matches(topic)
    }

    pub fn validate_raw(
        &self,
        topic: &str,
        payload: &Value,
        enforce_topic_device_match: bool,
    ) -> Result<MessageType> {
        self.schema
            .validate(payload)
            .context("schema validation failed")?;

        self.validate_time_iso_rfc3339(payload)?;

        if enforce_topic_device_match {
            self.enforce_topic_payload_device_match(topic, payload)?;
        }

        Ok(self.message_type)
    }

    pub fn deserialize(&self, topic: &str, payload: Value) -> Result<HandledMessage> {
        match self.message_type {
            MessageType::Sensor => {
                let msg: SensorMessage = serde_json::from_value(payload)
                    .with_context(|| format!("sensor: deserialization failed (topic={})", topic))?;
                Ok(HandledMessage::Sensor(msg))
            }
            MessageType::Status => {
                let msg: StatusMessage = serde_json::from_value(payload)
                    .with_context(|| format!("status: deserialization failed (topic={})", topic))?;
                Ok(HandledMessage::Status(msg))
            }
        }
    }

    fn validate_time_iso_rfc3339(&self, v: &Value) -> Result<()> {
        if let Some(s) = v.get("time_iso").and_then(|x| x.as_str()) {
            DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("time_iso not RFC3339: {}", s))?;
        }
        Ok(())
    }

    fn enforce_topic_payload_device_match(&self, topic: &str, payload: &Value) -> Result<()> {
        let topic_dev = self
            .pattern
            .device_id_from_topic(topic)
            .context("Failed to extract device_id from topic")?;

        let payload_dev = payload
            .get("device_id")
            .and_then(|x| x.as_str())
            .context("Failed to extract device_id from payload")?;

        if topic_dev != payload_dev {
            bail!(
                "device_id mismatch: topic has '{}', payload has '{}'",
                topic_dev,
                payload_dev
            );
        }

        Ok(())
    }
}

/// An immutable route with schemas and topic filters compiled at startup.
#[allow(dead_code)]
pub struct CompiledRoute {
    id: String,
    topic_filter: MqttTopicFilter,
    raw_schema: CompiledSchema,
    output_schema: Option<CompiledSchema>,
    topic_segment_index: Option<usize>,
}

#[allow(dead_code)]
impl CompiledRoute {
    /// Returns this route's configured identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
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

    /// Validates a raw payload against this route's compiled schema.
    ///
    /// # Errors
    ///
    /// Returns bounded, value-free schema diagnostics when validation fails.
    pub fn validate_raw(&self, payload: &Value) -> Result<()> {
        self.raw_schema.validate(payload)
    }

    /// Extracts the configured device identity topic segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the route has no topic-segment identity or the topic does not
    /// match this route / lacks the configured segment.
    pub fn topic_segment_identity<'a>(&self, topic: &'a str) -> Result<&'a str> {
        let index = self
            .topic_segment_index
            .with_context(|| format!("route '{}' has no topic-segment device identity", self.id))?;
        self.topic_filter
            .topic_segment(topic, index)
            .with_context(|| format!("extracting device identity for route '{}'", self.id))
    }
}

/// A compiled set of nonoverlapping routes and their exact broker subscriptions.
#[allow(dead_code)]
pub struct RouteSet {
    routes: Vec<CompiledRoute>,
}

#[allow(dead_code)]
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
            let filter = MqttTopicFilter::new(&route.topic)
                .with_context(|| format!("route '{}' has an invalid topic filter", route.id))?;
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
        let routes = bundle
            .routes
            .into_iter()
            .zip(compiled_filters)
            .map(|(route, (_, topic_filter))| {
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
                let topic_segment_index = match route.device_id {
                    DeviceIdSource::TopicSegment(source) => Some(source.topic_segment),
                    DeviceIdSource::PointerAndTopicSegment(source) => Some(source.topic_segment),
                    DeviceIdSource::Fixed(_) | DeviceIdSource::Pointer(_) => None,
                };
                Ok(CompiledRoute {
                    id: route.id,
                    topic_filter,
                    raw_schema,
                    output_schema,
                    topic_segment_index,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { routes })
    }

    /// Selects the compiled route matching a topic, if any.
    #[must_use]
    pub fn select(&self, topic: &str) -> Option<&CompiledRoute> {
        self.routes
            .iter()
            .find(|route| route.topic_filter.matches(topic))
    }

    /// Returns broker subscriptions in route order and exact configured spelling.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<&str> {
        self.routes
            .iter()
            .map(CompiledRoute::topic_filter)
            .collect()
    }
}

/// Holds configured routes and dispatches incoming messages.
pub struct Router {
    routes: Vec<Route>,
    strict: bool,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            strict: true,
        }
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn add_route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    fn find_route(&self, topic: &str) -> Option<&Route> {
        self.routes.iter().find(|r| r.matches(topic))
    }

    pub fn message_type_for_topic(&self, topic: &str) -> Option<MessageType> {
        self.find_route(topic).map(|r| r.message_type)
    }

    pub fn validate_raw(
        &self,
        topic: &str,
        payload: &Value,
        enforce_topic_device_match: bool,
    ) -> Result<Option<MessageType>> {
        if let Some(route) = self.find_route(topic) {
            return Ok(Some(route.validate_raw(
                topic,
                payload,
                enforce_topic_device_match,
            )?));
        }

        if self.strict {
            bail!("No route registered for topic: {}", topic);
        }

        Ok(None)
    }

    pub fn deserialize(&self, topic: &str, payload: Value) -> Result<Option<HandledMessage>> {
        if let Some(route) = self.find_route(topic) {
            return Ok(Some(route.deserialize(topic, payload)?));
        }

        if self.strict {
            bail!("No route registered for topic: {}", topic);
        }

        Ok(None)
    }
}

#[cfg(test)]
mod configured_route_tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::config::routes::RouteBundle;

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
                "device_id": {"pointer": "/device_id", "type": "string"}
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
        assert_eq!(
            sensor.topic_segment_identity("smarthome/lab/sensor")?,
            "lab"
        );
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

    #[test]
    fn test_configured_route_set_reports_missing_identity_segment() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("route.json");
        write_schema(&schema_path, json!({"$schema": DRAFT_7, "type": "object"}))?;
        let mut configured_route = route("sensor", "home/+/sensor", &schema_path);
        configured_route["device_id"]["topic_segment"] = json!(8);
        let routes = RouteSet::compile(bundle(vec![configured_route])?, "home/dlq")?;
        let selected = routes
            .select("home/lab/sensor")
            .context("sensor route should match")?;

        assert!(selected.topic_segment_identity("home/lab/sensor").is_err());
        assert!(selected.topic_segment_identity("home//sensor").is_err());
        Ok(())
    }
}
