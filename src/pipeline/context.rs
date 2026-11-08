use std::{sync::Arc, time::Instant};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde_json::Value;

use crate::{
    infrastructure::router::CompiledRoute, model::messages::telemetry::TelemetryRecord,
    pipeline::outcome::DeferredReason,
};

const MAX_DLQ_REASON_BYTES: usize = 4096;

pub struct PipelineContext {
    topic: String,
    raw_payload: Bytes,
    received_at_ms: i64,
    receive_sequence: u64,
    retained: bool,
    qos: u8,
    payload_utf8: Option<String>,
    payload_json: Option<Value>,
    route: Option<Arc<CompiledRoute>>,
    telemetry_record: Option<TelemetryRecord>,
    rejection_reason: Option<String>,
    deferred_reason: Option<DeferredReason>,
    output_stored: bool,
    started_at: Instant,
}

impl PipelineContext {
    /// Creates a context for locally constructed messages with current receive time.
    #[allow(dead_code)]
    pub fn new(topic: impl Into<String>, raw_payload: impl Into<Bytes>) -> Self {
        Self::new_with_metadata(
            topic,
            raw_payload,
            chrono::Utc::now().timestamp_millis(),
            false,
        )
    }

    /// Creates a context with metadata captured by the input source.
    pub fn new_with_metadata(
        topic: impl Into<String>,
        raw_payload: impl Into<Bytes>,
        received_at_ms: i64,
        retained: bool,
    ) -> Self {
        Self {
            topic: topic.into(),
            raw_payload: raw_payload.into(),
            received_at_ms,
            receive_sequence: 0,
            retained,
            qos: 0,
            payload_utf8: None,
            payload_json: None,
            route: None,
            telemetry_record: None,
            rejection_reason: None,
            deferred_reason: None,
            output_stored: false,
            started_at: Instant::now(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn raw_payload(&self) -> &[u8] {
        &self.raw_payload
    }

    /// Returns the receive time captured at intake.
    pub fn received_at_ms(&self) -> i64 {
        self.received_at_ms
    }

    /// Sets the source-assigned intake order for cache update ordering.
    pub fn set_receive_sequence(&mut self, receive_sequence: u64) {
        self.receive_sequence = receive_sequence;
    }

    /// Returns the source-assigned intake order.
    pub fn receive_sequence(&self) -> u64 {
        self.receive_sequence
    }

    /// Returns whether the broker marked the source message as retained.
    pub fn retained(&self) -> bool {
        self.retained
    }

    /// Returns the transport QoS level captured at intake.
    pub fn qos(&self) -> u8 {
        self.qos
    }

    /// Sets the transport QoS level captured at intake.
    pub fn set_qos(&mut self, qos: u8) {
        self.qos = qos;
    }

    pub fn started_at(&self) -> Instant {
        self.started_at
    }

    pub fn set_payload_utf8(&mut self, payload: String) {
        self.payload_utf8 = Some(payload);
    }

    #[allow(dead_code)]
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

    /// Stores the compiled route selected during raw validation.
    pub fn set_route(&mut self, route: Arc<CompiledRoute>) {
        self.route = Some(route);
    }

    /// Returns the selected compiled route.
    ///
    /// # Errors
    ///
    /// Returns an error if route selection has not completed.
    pub fn route(&self) -> Result<&CompiledRoute> {
        self.route
            .as_deref()
            .ok_or_else(|| anyhow!("selected route missing in pipeline context"))
    }

    /// Returns the selected route identifier, if route selection completed.
    pub fn route_id(&self) -> Option<&str> {
        self.route.as_deref().map(CompiledRoute::id)
    }

    /// Stores the mapped and calculated telemetry record.
    pub fn set_telemetry_record(&mut self, record: TelemetryRecord) {
        self.telemetry_record = Some(record);
    }

    /// Returns the mapped and calculated telemetry record.
    ///
    /// # Errors
    ///
    /// Returns an error if mapping has not completed.
    pub fn telemetry_record(&self) -> Result<&TelemetryRecord> {
        self.telemetry_record
            .as_ref()
            .ok_or_else(|| anyhow!("telemetry_record missing in pipeline context"))
    }

    /// Marks the input as invalid and eligible for durable quarantine.
    pub fn mark_rejection(&mut self, reason: impl Into<String>) {
        if self.rejection_reason.is_none() {
            let mut reason = reason.into();
            if reason.len() > MAX_DLQ_REASON_BYTES {
                let mut end = MAX_DLQ_REASON_BYTES;
                while !reason.is_char_boundary(end) {
                    end -= 1;
                }
                reason.truncate(end);
            }
            self.rejection_reason = Some(reason);
        }
    }

    /// Marks an operational failure that must not be stored as an input rejection.
    pub(crate) fn mark_deferred(&mut self, reason: DeferredReason) {
        self.deferred_reason = Some(reason);
    }

    /// Marks that the persistence stage received confirmation of a durable append.
    pub(crate) fn mark_output_stored(&mut self) {
        self.output_stored = true;
    }

    pub(crate) fn deferred_reason(&self) -> Option<DeferredReason> {
        self.deferred_reason
    }

    pub(crate) fn output_stored(&self) -> bool {
        self.output_stored
    }

    /// Returns the reason the input was marked for quarantine, if any.
    pub fn rejection_reason(&self) -> Option<&str> {
        self.rejection_reason.as_deref()
    }

    /// Returns whether deterministic rejection requires quarantine.
    pub fn should_quarantine(&self) -> bool {
        self.rejection_reason.is_some()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::outcome::DeferredReason;

    #[test]
    fn test_mark_rejection_reason_truncates_at_utf8_boundary() {
        let mut ctx = PipelineContext::new("topic", Vec::<u8>::new());
        ctx.mark_rejection("é".repeat(3000));

        assert_eq!(ctx.rejection_reason().map(str::len), Some(4096));
    }

    #[test]
    fn test_deferred_operational_failure_is_not_a_rejection() {
        let mut ctx = PipelineContext::new("topic", Vec::<u8>::new());

        ctx.mark_deferred(DeferredReason::PersistenceFailure);

        assert_eq!(
            ctx.deferred_reason(),
            Some(DeferredReason::PersistenceFailure)
        );
        assert!(!ctx.should_quarantine());
    }
}
