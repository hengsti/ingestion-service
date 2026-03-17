use std::time::Instant;

use anyhow::{Result, anyhow};
use serde_json::Value;

use crate::model::messages::message::HandledMessage;

#[derive(Debug)]
pub struct PipelineContext {
    topic: String,
    raw_payload: Vec<u8>,
    payload_utf8: Option<String>,
    payload_json: Option<Value>,
    handled_message: Option<HandledMessage>,
    line_protocol: Option<String>,
    dlq_reason: Option<String>,
    ignored_reason: Option<String>,
    started_at: Instant,
}

impl PipelineContext {
    pub fn new(topic: impl Into<String>, raw_payload: Vec<u8>) -> Self {
        Self {
            topic: topic.into(),
            raw_payload,
            payload_utf8: None,
            payload_json: None,
            handled_message: None,
            line_protocol: None,
            dlq_reason: None,
            ignored_reason: None,
            started_at: Instant::now(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn raw_payload(&self) -> &[u8] {
        &self.raw_payload
    }

    pub fn started_at(&self) -> Instant {
        self.started_at
    }

    pub fn set_payload_utf8(&mut self, payload: String) {
        self.payload_utf8 = Some(payload);
    }

    pub fn payload_utf8(&self) -> Result<&str> {
        self.payload_utf8
            .as_deref()
            .ok_or_else(|| anyhow!("payload_utf8 missing in pipeline context"))
    }

    pub fn set_payload_json(&mut self, payload: Value) {
        self.payload_json = Some(payload);
    }

    pub fn payload_json(&self) -> Result<&Value> {
        self.payload_json
            .as_ref()
            .ok_or_else(|| anyhow!("payload_json missing in pipeline context"))
    }

    pub fn set_handled_message(&mut self, msg: HandledMessage) {
        self.handled_message = Some(msg);
    }

    pub fn handled_message(&self) -> Result<&HandledMessage> {
        self.handled_message
            .as_ref()
            .ok_or_else(|| anyhow!("handled_message missing in pipeline context"))
    }

    pub fn set_line_protocol(&mut self, line: String) {
        self.line_protocol = Some(line);
    }

    pub fn line_protocol(&self) -> Option<&str> {
        self.line_protocol.as_deref()
    }

    pub fn mark_dlq(&mut self, reason: impl Into<String>) {
        if self.dlq_reason.is_none() {
            self.dlq_reason = Some(reason.into());
        }
    }

    pub fn dlq_reason(&self) -> Option<&str> {
        self.dlq_reason.as_deref()
    }

    pub fn should_publish_dlq(&self) -> bool {
        self.dlq_reason.is_some()
    }

    pub fn mark_ignored(&mut self, reason: impl Into<String>) {
        if self.ignored_reason.is_none() {
            self.ignored_reason = Some(reason.into());
        }
    }

    pub fn ignored_reason(&self) -> Option<&str> {
        self.ignored_reason.as_deref()
    }

    pub fn payload_for_dlq(&self) -> String {
        if let Some(payload) = &self.payload_utf8 {
            return payload.clone();
        }

        match std::str::from_utf8(&self.raw_payload) {
            Ok(s) => s.to_string(),
            Err(_) => "<non-utf8>".to_string(),
        }
    }
}
