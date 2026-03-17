use anyhow::{Context, Result, bail};
use chrono::DateTime;
use serde_json::Value;

use crate::infrastructure::schema::JsonSchema;
use crate::model::messages::message::{HandledMessage, MessageType};
use crate::model::messages::sensor::SensorMessage;
use crate::model::messages::status::StatusMessage;
use crate::model::topic::MqttTopicPattern;

/// A single route combining pattern matching, schema validation, and expected message type.
pub struct Route {
    pub message_type: MessageType,
    pattern: MqttTopicPattern,
    schema: JsonSchema,
}

impl Route {
    pub fn new(message_type: MessageType, schema_str: &str, topic_pattern: &str) -> Result<Self> {
        Ok(Self {
            message_type,
            pattern: MqttTopicPattern::new(topic_pattern)?,
            schema: JsonSchema::new(schema_str)?,
        })
    }

    pub fn matches(&self, topic: &str) -> bool {
        self.pattern.matches(topic)
    }

    pub fn process(
        &self,
        topic: &str,
        payload: Value,
        enforce_topic_device_match: bool,
    ) -> Result<HandledMessage> {
        // 1. Validate JSON schema
        self.schema
            .validate(&payload)
            .context("schema validation failed")?;

        // 2. Validate common data requirements
        self.validate_time_iso_rfc3339(&payload)?;

        if enforce_topic_device_match {
            self.enforce_topic_payload_device_match(topic, &payload)?;
        }

        // 3. Deserialize based on the assigned message type
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

    // --- Helper validation methods ---
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

/// Holds configured routes and dispatches incoming messages.
pub struct Router {
    routes: Vec<Route>,
    strict: bool,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            strict: true,
        }
    }

    /// Fluent builder method to set strictness
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Fluent builder method to add a new route
    pub fn add_route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Processes an incoming MQTT message
    pub fn process(
        &self,
        topic: &str,
        payload: Value,
        enforce_topic_device_match: bool,
    ) -> Result<Option<HandledMessage>> {
        if let Some(route) = self.routes.iter().find(|r| r.matches(topic)) {
            return Ok(Some(route.process(
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
}
