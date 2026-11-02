use std::fmt;
use std::fmt::Write;

use crate::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

const FLOAT_DISPLAY_UPPER_BOUND_BYTES: usize = 400;
const MAX_INFLUX_TIMESTAMP_TEXT_BYTES: usize = 14;
const MEASUREMENT_ESCAPES: &[u8] = b"\\, ";
const KEY_VALUE_ESCAPES: &[u8] = b"\\,= ";
const STRING_ESCAPES: &[u8] = b"\\\"";

/// Returns a conservative, allocation-free upper bound for a telemetry line.
pub(crate) fn line_protocol_size_upper_bound(record: &TelemetryRecord) -> usize {
    let mut size = escaped_size(&record.measurement, MEASUREMENT_ESCAPES);

    for (key, value) in &record.tags {
        size = size
            .saturating_add(1)
            .saturating_add(escaped_size(key, KEY_VALUE_ESCAPES))
            .saturating_add(1)
            .saturating_add(escaped_size(value, KEY_VALUE_ESCAPES));
    }

    size = size.saturating_add(1);
    for (index, (key, value)) in record.fields.iter().enumerate() {
        if index > 0 {
            size = size.saturating_add(1);
        }
        size = size
            .saturating_add(escaped_size(key, KEY_VALUE_ESCAPES))
            .saturating_add(1)
            .saturating_add(field_value_size_upper_bound(value));
    }

    size.saturating_add(1)
        .saturating_add(MAX_INFLUX_TIMESTAMP_TEXT_BYTES)
}

fn escaped_size(value: &str, escaped: &[u8]) -> usize {
    value
        .len()
        .saturating_add(value.bytes().filter(|byte| escaped.contains(byte)).count())
}

fn field_value_size_upper_bound(value: &TelemetryValue) -> usize {
    match value {
        TelemetryValue::Float(_) => FLOAT_DISPLAY_UPPER_BOUND_BYTES,
        TelemetryValue::Integer(_) | TelemetryValue::Unsigned(_) => 21,
        TelemetryValue::Boolean(_) => 5,
        TelemetryValue::String(value) => 2usize.saturating_add(escaped_size(value, STRING_ESCAPES)),
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum FieldValue {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(String),
}

#[derive(Clone)]
pub struct Point {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, FieldValue)>,
    timestamp_ms: Option<i64>,
}

impl Point {
    pub fn build(measurement: &str) -> PointBuilder {
        PointBuilder {
            measurement: measurement.to_string(),
            tags: vec![],
            fields: vec![],
            timestamp_ms: None,
        }
    }

    /// Appends this point's InfluxDB line protocol to `out` without allocating
    /// an intermediate per-point `String`. Used by the sink to build one batch
    /// body buffer across many points.
    pub fn write_line_protocol(&self, out: &mut String) {
        out.push_str(&esc_measurement(&self.measurement));

        for (k, v) in &self.tags {
            out.push(',');
            out.push_str(&esc_tag_key(k));
            out.push('=');
            out.push_str(&esc_tag_value(v));
        }

        out.push(' ');
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&esc_field_key(k));
            out.push('=');
            out.push_str(&format_field_value(v));
        }

        if let Some(ts) = self.timestamp_ms {
            // Infallible: writing into a String never errors.
            let _ = write!(out, " {ts}");
        }
    }
}

impl fmt::Debug for Point {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Point")
            .field("measurement", &self.measurement)
            .field("tags", &self.tags)
            .field("fields", &self.fields)
            .field("timestamp_ms", &self.timestamp_ms)
            .finish()
    }
}

pub struct PointBuilder {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, FieldValue)>,
    timestamp_ms: Option<i64>,
}

impl PointBuilder {
    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.push((key.to_string(), value.to_string()));
        self
    }

    pub fn field_f64(mut self, key: &str, value: f64) -> Self {
        self.fields
            .push((key.to_string(), FieldValue::Float(value)));
        self
    }

    pub fn field_i64(mut self, key: &str, value: i64) -> Self {
        self.fields.push((key.to_string(), FieldValue::Int(value)));
        self
    }

    #[allow(dead_code)]
    pub fn field_u64(mut self, key: &str, value: u64) -> Self {
        self.fields.push((key.to_string(), FieldValue::UInt(value)));
        self
    }

    pub fn field_bool(mut self, key: &str, value: bool) -> Self {
        self.fields.push((key.to_string(), FieldValue::Bool(value)));
        self
    }

    pub fn field_str(mut self, key: &str, value: &str) -> Self {
        self.fields
            .push((key.to_string(), FieldValue::Str(value.to_string())));
        self
    }

    /// Timestamp in milliseconds. If you don't call this, InfluxDB uses server time.
    pub fn timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    pub fn build(self) -> Point {
        let mut tags = self.tags;
        let mut fields = self.fields;

        tags.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        fields.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        Point {
            measurement: self.measurement,
            tags,
            fields,
            timestamp_ms: self.timestamp_ms,
        }
    }
}

// Escaping helpers for Influx line protocol.

fn esc_measurement(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace(' ', "\\ ")
}

fn esc_tag_key(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

fn esc_tag_value(s: &str) -> String {
    esc_tag_key(s)
}

fn esc_field_key(s: &str) -> String {
    esc_tag_key(s)
}

fn esc_string_field(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn format_field_value(v: &FieldValue) -> String {
    match v {
        FieldValue::Float(x) => {
            // Rust's default float formatting is valid Influx line protocol.
            format!("{}", x)
        }
        FieldValue::Int(x) => format!("{}i", x),
        FieldValue::UInt(x) => format!("{}u", x),
        FieldValue::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        FieldValue::Str(s) => format!("\"{}\"", esc_string_field(s)),
    }
}
