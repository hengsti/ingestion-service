use std::collections::BTreeMap;

use serde::Serialize;

/// A scalar value with the type required by a configured telemetry field.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TelemetryValue {
    /// A finite floating-point value.
    Float(f64),
    /// A signed 64-bit integer.
    Integer(i64),
    /// An unsigned 64-bit integer.
    Unsigned(u64),
    /// A boolean value.
    Boolean(bool),
    /// A string value.
    String(String),
}

/// Canonical mapped telemetry, independent of a particular transport or output sink.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TelemetryRecord {
    /// Identifier of the configured route that mapped this record.
    pub route_id: String,
    /// Canonical device identity.
    pub device_id: String,
    /// Output measurement name.
    pub measurement: String,
    /// Event time in signed Unix epoch milliseconds.
    pub timestamp_ms: i64,
    /// Explicit string-valued tags, including the reserved device identity tag.
    pub tags: BTreeMap<String, String>,
    /// Explicit typed fields, including the reserved retained flag.
    pub fields: BTreeMap<String, TelemetryValue>,
    /// Concrete topic name that supplied the payload.
    pub source_topic: String,
    /// Whether the source marked this message as retained.
    pub retained: bool,
}
