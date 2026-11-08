use std::fmt::Write;

use crate::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

const FLOAT_DISPLAY_UPPER_BOUND_BYTES: usize = 400;
const MAX_INFLUX_TIMESTAMP_TEXT_BYTES: usize = 20;
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

/// Appends a borrowed record's InfluxDB line protocol without temporary allocations.
pub(crate) fn write_line_protocol(record: &TelemetryRecord, out: &mut String) {
    write_escaped(out, &record.measurement, MEASUREMENT_ESCAPES);

    for (key, value) in &record.tags {
        out.push(',');
        write_escaped(out, key, KEY_VALUE_ESCAPES);
        out.push('=');
        write_escaped(out, value, KEY_VALUE_ESCAPES);
    }

    out.push(' ');
    for (index, (key, value)) in record.fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_escaped(out, key, KEY_VALUE_ESCAPES);
        out.push('=');
        match value {
            TelemetryValue::Float(value) => {
                let _ = write!(out, "{value}");
            }
            TelemetryValue::Integer(value) => {
                let _ = write!(out, "{value}i");
            }
            TelemetryValue::Unsigned(value) => {
                let _ = write!(out, "{value}u");
            }
            TelemetryValue::Boolean(value) => {
                out.push_str(if *value { "true" } else { "false" });
            }
            TelemetryValue::String(value) => {
                out.push('"');
                write_escaped(out, value, STRING_ESCAPES);
                out.push('"');
            }
        }
    }

    // Infallible: writing into a String never errors.
    let _ = write!(out, " {}", record.timestamp_ms);
}

fn write_escaped(out: &mut String, value: &str, escaped: &[u8]) {
    // Escapable bytes are ASCII, so their indices are UTF-8 boundaries.
    let mut start = 0;
    for (index, byte) in value.bytes().enumerate() {
        if escaped.contains(&byte) {
            out.push_str(&value[start..index]);
            out.push('\\');
            start = index;
        }
    }
    out.push_str(&value[start..]);
}
