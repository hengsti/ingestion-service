use std::{collections::BTreeMap, future::Future, pin::Pin};

use anyhow::{bail, Context, Result};
use chrono::DateTime;
use metrics::counter;
use serde_json::Value;
use tracing::warn;

use crate::{
    config::routes::{
        DeviceIdSource, FieldMapping, FieldType, RouteConfig, TagValue, TimestampConfig,
        TimestampFallback, TimestampFormat, TimestampSource,
    },
    infrastructure::wal::codec::MAX_WAL_RECORD_PAYLOAD_BYTES,
    model::messages::telemetry::{TelemetryRecord, TelemetryValue},
    pipeline::{
        context::PipelineContext,
        stage::{PipelineStage, StageFlow, StageResult},
    },
};

const MAX_INFLUX_TIMESTAMP_MS: i64 = 9_223_372_036_854;

/// Maps the selected route into a typed record and applies its compiled calculations.
#[derive(Debug, Default, Clone, Copy)]
pub struct TelemetryMappingStage;

impl TelemetryMappingStage {
    /// Creates the configured telemetry mapping stage.
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStage for TelemetryMappingStage {
    fn name(&self) -> &'static str {
        "transform"
    }

    fn run<'a>(
        &'a self,
        ctx: &'a mut PipelineContext,
    ) -> Pin<Box<dyn Future<Output = StageResult> + Send + 'a>> {
        Box::pin(async move {
            let result = {
                let route = ctx.route()?;
                let mapped = map_payload(
                    route.config(),
                    ctx.payload_json()?,
                    ctx.topic(),
                    ctx.received_at_ms(),
                    ctx.retained(),
                );
                mapped.and_then(|record| route.calculations().apply(record))
            };
            let record = match result {
                Ok(record) => record,
                Err(_) => {
                    warn!(topic = %ctx.topic(), "telemetry mapping failed; marking for quarantine");
                    ctx.mark_rejection("telemetry mapping failed");
                    return Ok(StageFlow::Stop);
                }
            };
            ctx.set_telemetry_record(record);
            counter!("ingest_transform_success_total").increment(1);
            Ok(StageFlow::Continue)
        })
    }
}

/// Maps one payload through its selected configured route into a canonical typed record.
///
/// The function uses only the supplied route, payload, topic, receive timestamp and retained
/// metadata. Route selection is the caller's responsibility; this function does not read the
/// clock or perform I/O.
///
/// # Errors
///
/// Returns an error when a configured mapping is absent or has the wrong type, identity sources
/// disagree, or a timestamp/value is outside its contract.
pub fn map_payload(
    route: &RouteConfig,
    payload: &Value,
    topic: &str,
    received_at_ms: i64,
    retained: bool,
) -> Result<TelemetryRecord> {
    let device_id = map_device_id(&route.device_id, payload, topic)?;
    let timestamp_ms = map_timestamp(&route.timestamp, payload, received_at_ms)?;
    let mut remaining_bytes = MAX_WAL_RECORD_PAYLOAD_BYTES;
    // Bound copied data before repeated mappings can amplify one input value.
    reserve_mapped_bytes(&mut remaining_bytes, route.id.len())?;
    reserve_mapped_bytes(&mut remaining_bytes, topic.len())?;
    reserve_mapped_bytes(&mut remaining_bytes, route.measurement.len())?;
    reserve_mapped_bytes(&mut remaining_bytes, device_id.len())?;
    reserve_mapped_bytes(
        &mut remaining_bytes,
        "device_id"
            .len()
            .saturating_add(device_id.len())
            .saturating_add(2),
    )?;
    reserve_mapped_bytes(&mut remaining_bytes, "mqtt_retained".len() + 2)?;

    let mut tags = BTreeMap::new();
    for (key, value) in &route.tags {
        if matches!(key.as_str(), "device_id" | "mqtt_retained") {
            bail!("tag '{key}' collides with a reserved telemetry tag");
        }
        let value = match value {
            TagValue::Static(value) => value.as_str(),
            TagValue::Pointer(pointer) => {
                let value = payload
                    .pointer(&pointer.pointer)
                    .with_context(|| format!("tag '{key}' pointer is absent"))?
                    .as_str()
                    .with_context(|| format!("tag '{key}' pointer must select a string"))?;
                if value.is_empty() {
                    bail!("tag '{key}' pointer must select a nonempty string");
                }
                value
            }
        };
        reserve_mapped_bytes(
            &mut remaining_bytes,
            key.len().saturating_add(value.len()).saturating_add(2),
        )?;
        tags.insert(key.clone(), value.to_owned());
    }
    if tags
        .insert("device_id".to_owned(), device_id.to_owned())
        .is_some()
    {
        bail!("device_id collides with a configured telemetry tag");
    }

    let mut fields = BTreeMap::new();
    for (key, mapping) in &route.fields {
        if matches!(key.as_str(), "device_id" | "mqtt_retained") {
            bail!("field '{key}' collides with reserved telemetry metadata");
        }
        if let Some(value) = map_field(mapping, payload, key, &mut remaining_bytes)? {
            fields.insert(key.clone(), value);
        }
    }
    if fields.is_empty() {
        bail!("route '{}' produced no mapped telemetry fields", route.id);
    }
    fields.insert(
        "mqtt_retained".to_owned(),
        TelemetryValue::Boolean(retained),
    );

    Ok(TelemetryRecord {
        route_id: route.id.clone(),
        device_id: device_id.to_owned(),
        measurement: route.measurement.clone(),
        timestamp_ms,
        tags,
        fields,
        source_topic: topic.to_owned(),
        retained,
    })
}

fn map_device_id<'a>(
    source: &'a DeviceIdSource,
    payload: &'a Value,
    topic: &'a str,
) -> Result<&'a str> {
    let device_id = match source {
        DeviceIdSource::Fixed(value) => value,
        DeviceIdSource::TopicSegment(config) => topic_segment(topic, config.topic_segment)?,
        DeviceIdSource::Pointer(config) => {
            mapped_string(payload, &config.pointer, "device identity")?
        }
        DeviceIdSource::PointerAndTopicSegment(config) => {
            let payload_id = mapped_string(payload, &config.pointer, "device identity")?;
            let topic_id = topic_segment(topic, config.topic_segment)?;
            if payload_id != topic_id {
                bail!("payload and topic device identities do not match");
            }
            payload_id
        }
    };
    if device_id.is_empty() {
        bail!("device identity must not be empty");
    }
    Ok(device_id)
}

fn topic_segment(topic: &str, index: usize) -> Result<&str> {
    topic
        .split('/')
        .nth(index)
        .filter(|segment| !segment.is_empty())
        .with_context(|| format!("topic segment {index} is missing or empty"))
}

fn mapped_string<'a>(payload: &'a Value, pointer: &str, label: &str) -> Result<&'a str> {
    let value = payload
        .pointer(pointer)
        .with_context(|| format!("{label} pointer is absent"))?;
    let value = value
        .as_str()
        .with_context(|| format!("{label} pointer must select a string"))?;
    if value.is_empty() {
        bail!("{label} must not be empty");
    }
    Ok(value)
}

fn map_field(
    mapping: &FieldMapping,
    payload: &Value,
    key: &str,
    remaining_bytes: &mut usize,
) -> Result<Option<TelemetryValue>> {
    let value = match payload.pointer(&mapping.pointer) {
        Some(value) => value,
        None => match (&mapping.default, mapping.optional) {
            (Some(default), _) => default,
            (None, true) => return Ok(None),
            (None, false) => bail!("required field '{key}' pointer is absent"),
        },
    };
    reserve_mapped_bytes(remaining_bytes, key.len().saturating_add(2))?;
    map_scalar(value, &mapping.field_type, key, remaining_bytes).map(Some)
}

fn map_scalar(
    value: &Value,
    field_type: &FieldType,
    key: &str,
    remaining_bytes: &mut usize,
) -> Result<TelemetryValue> {
    match field_type {
        FieldType::Float => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(TelemetryValue::Float)
            .with_context(|| format!("field '{key}' must be a finite number")),
        FieldType::Integer => value
            .as_i64()
            .map(TelemetryValue::Integer)
            .with_context(|| format!("field '{key}' must be a signed 64-bit integer")),
        FieldType::Unsigned => value
            .as_u64()
            .map(TelemetryValue::Unsigned)
            .with_context(|| format!("field '{key}' must be an unsigned 64-bit integer")),
        FieldType::Boolean => value
            .as_bool()
            .map(TelemetryValue::Boolean)
            .with_context(|| format!("field '{key}' must be a boolean")),
        FieldType::String => {
            let value = value
                .as_str()
                .with_context(|| format!("field '{key}' must be a string"))?;
            reserve_mapped_bytes(remaining_bytes, value.len())?;
            Ok(TelemetryValue::String(value.to_owned()))
        }
    }
}

fn reserve_mapped_bytes(remaining: &mut usize, bytes: usize) -> Result<()> {
    *remaining = remaining
        .checked_sub(bytes)
        .context("mapped telemetry exceeds the WAL-compatible size limit")?;
    Ok(())
}

fn map_timestamp(config: &TimestampConfig, payload: &Value, received_at_ms: i64) -> Result<i64> {
    let timestamp_ms = match &config.source {
        TimestampSource::ReceivedAt => received_at_ms,
        TimestampSource::Payload => {
            let use_received_at = match (&config.valid_flag, &config.fallback) {
                (Some(pointer), Some(TimestampFallback::ReceivedAt)) => {
                    let valid = payload
                        .pointer(pointer)
                        .with_context(|| "timestamp validity flag is absent")?
                        .as_bool()
                        .with_context(|| "timestamp validity flag must be a boolean")?;
                    !valid
                }
                (None, None) => false,
                _ => bail!("timestamp validity flag and fallback must be configured together"),
            };
            if use_received_at {
                received_at_ms
            } else {
                map_payload_timestamp(config, payload)?
            }
        }
    };
    validate_timestamp_range(timestamp_ms)
}

fn map_payload_timestamp(config: &TimestampConfig, payload: &Value) -> Result<i64> {
    let pointer = config
        .pointer
        .as_deref()
        .context("payload timestamp pointer is not configured")?;
    let value = payload
        .pointer(pointer)
        .with_context(|| "payload timestamp is absent")?;
    let timestamp_ms = match config
        .format
        .as_ref()
        .context("payload timestamp format is not configured")?
    {
        TimestampFormat::Rfc3339 => {
            let text = value
                .as_str()
                .context("RFC3339 payload timestamp must be a string")?;
            DateTime::parse_from_rfc3339(text)
                .context("payload timestamp is not valid RFC3339")?
                .timestamp_millis()
        }
        TimestampFormat::EpochMs => value
            .as_i64()
            .context("epoch_ms payload timestamp must be a signed integer")?,
    };
    validate_timestamp_range(timestamp_ms)
}

pub(crate) fn validate_timestamp_range(timestamp_ms: i64) -> Result<i64> {
    if !(-MAX_INFLUX_TIMESTAMP_MS..=MAX_INFLUX_TIMESTAMP_MS).contains(&timestamp_ms) {
        bail!("timestamp_ms is outside the supported Influx timestamp range");
    }
    Ok(timestamp_ms)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::{json, Value};

    use crate::{
        config::routes::{self, RouteConfig},
        infrastructure::router::RouteSet,
        pipeline::{context::PipelineContext, stage::PipelineStage},
    };

    use super::*;

    #[tokio::test]
    async fn test_telemetry_mapping_rejects_invalid_route_payload() -> Result<()> {
        let bundle_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/routes.json");
        let routes = RouteSet::compile(
            routes::load(bundle_path)?,
            "smarthome/_dlq/ingest",
            "test_bucket",
        )?;
        let topic = "smarthome/esp32-1/bme680";
        let route = routes.select(topic).expect("configured sensor route");
        let mut ctx = PipelineContext::new(topic, Vec::<u8>::new());
        ctx.set_route(route);
        ctx.set_payload_json(json!({"device_id": "esp32-1"}));

        let result = TelemetryMappingStage::new().run(&mut ctx).await;

        assert!(matches!(result, Ok(StageFlow::Stop)));
        assert!(ctx.should_quarantine());
        Ok(())
    }

    fn route_config(
        device_id: Value,
        fields: Value,
        timestamp: Value,
        tags: Value,
    ) -> Result<RouteConfig, serde_json::Error> {
        serde_json::from_value(json!({
            "id": "test-route",
            "topic": "home/+/sensor",
            "schema": "sensor.schema.json",
            "device_id": device_id,
            "measurement": "telemetry",
            "tags": tags,
            "fields": fields,
            "timestamp": timestamp
        }))
    }

    fn required_float_field() -> Value {
        json!({"value": {"pointer": "/value", "type": "float"}})
    }

    fn received_timestamp() -> Value {
        json!({"source": "received_at"})
    }

    fn empty_tags() -> Value {
        json!({})
    }

    #[test]
    fn test_telemetry_mapping_rejects_repeated_pointer_amplification() -> Result<()> {
        let payload = json!({"value": "a".repeat(60 * 1024)});

        for (count, accepted) in [(17, true), (18, false)] {
            let fields = (0..count)
                .map(|index| {
                    (
                        format!("field_{index}"),
                        json!({"pointer": "/value", "type": "string"}),
                    )
                })
                .collect();
            let route = route_config(
                json!("device"),
                Value::Object(fields),
                received_timestamp(),
                empty_tags(),
            )?;

            assert_eq!(
                map_payload(&route, &payload, "home/device/sensor", 1, false).is_ok(),
                accepted
            );
        }
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_bounds_repeated_tags_and_defaults() -> Result<()> {
        let large_value = "a".repeat(60 * 1024);
        let tags = (0..18)
            .map(|index| (format!("tag_{index}"), json!({"pointer": "/value"})))
            .collect();
        let route = route_config(
            json!("device"),
            json!({"enabled": {"pointer": "/enabled", "type": "boolean"}}),
            received_timestamp(),
            Value::Object(tags),
        )?;
        let payload = json!({"value": large_value, "enabled": true});

        assert!(map_payload(&route, &payload, "home/device/sensor", 1, false).is_err());

        let defaults = (0..18)
            .map(|index| {
                (
                    format!("field_{index}"),
                    json!({"pointer": "/absent", "type": "string", "default": &large_value}),
                )
            })
            .collect();
        let route = route_config(
            json!("device"),
            Value::Object(defaults),
            received_timestamp(),
            empty_tags(),
        )?;

        assert!(map_payload(&route, &json!({}), "home/device/sensor", 1, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_maps_zigbee_payload_without_firmware_envelope(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route: RouteConfig = serde_json::from_value(json!({
            "id": "zigbee-climate",
            "topic": "zigbee2mqtt/+/temperature",
            "schema": "climate.schema.json",
            "device_id": {"topic_segment": 1},
            "measurement": "climate",
            "tags": {
                "source": "zigbee",
                "room": {"pointer": "/location/room"}
            },
            "fields": {
                "temp_c": {"pointer": "/sensor~1readings/temp~0c", "type": "float"},
                "enabled": {"pointer": "/enabled", "type": "boolean"}
            },
            "timestamp": {"source": "received_at"}
        }))?;
        let payload = json!({
            "sensor/readings": {"temp~c": 21.75},
            "location": {"room": "Kitchen"},
            "enabled": false,
            "unmapped": "not persisted"
        });

        let record = map_payload(
            &route,
            &payload,
            "zigbee2mqtt/living_room/temperature",
            1_767_000_000_123,
            true,
        )?;
        let record = serde_json::to_value(record)?;

        assert_eq!(
            record,
            json!({
                "route_id": "zigbee-climate",
                "device_id": "living_room",
                "measurement": "climate",
                "timestamp_ms": 1_767_000_000_123i64,
                "tags": {
                    "device_id": "living_room",
                    "source": "zigbee",
                    "room": "Kitchen"
                },
                "fields": {
                    "temp_c": 21.75,
                    "enabled": false,
                    "mqtt_retained": true
                },
                "source_topic": "zigbee2mqtt/living_room/temperature",
                "retained": true
            })
        );
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_uses_defaults_and_omits_absent_optional_fields(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route: RouteConfig = serde_json::from_value(json!({
            "id": "firmware-status",
            "topic": "smarthome/+/status",
            "schema": "status.schema.json",
            "device_id": {"pointer": "/device_id"},
            "measurement": "device_status",
            "fields": {
                "rssi": {"pointer": "/rssi", "type": "integer"},
                "queued": {"pointer": "/queued", "type": "unsigned", "optional": true},
                "ssid": {"pointer": "/ssid", "type": "string", "default": ""}
            },
            "timestamp": {"source": "received_at"}
        }))?;
        let payload = json!({"device_id": "sensor-1", "rssi": 0});

        let record = map_payload(
            &route,
            &payload,
            "smarthome/sensor-1/status",
            1_767_000_000_123,
            false,
        )?;
        let record: Value = serde_json::to_value(record)?;

        assert_eq!(record["fields"]["rssi"], json!(0));
        assert_eq!(record["fields"]["ssid"], json!(""));
        assert_eq!(record["fields"]["mqtt_retained"], json!(false));
        assert!(record["fields"].get("queued").is_none());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_preserves_all_scalar_types_and_zero_values(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("fixed-device"),
            json!({
                "float": {"pointer": "/float", "type": "float"},
                "integer": {"pointer": "/integer", "type": "integer"},
                "unsigned": {"pointer": "/unsigned", "type": "unsigned"},
                "boolean": {"pointer": "/boolean", "type": "boolean"},
                "string": {"pointer": "/string", "type": "string"},
                "defaulted": {"pointer": "/missing", "type": "integer", "default": 0}
            }),
            received_timestamp(),
            empty_tags(),
        )?;
        let payload = json!({
            "float": 0.0,
            "integer": 0,
            "unsigned": u64::MAX,
            "boolean": false,
            "string": "  keep spaces  "
        });

        let record = map_payload(&route, &payload, "home/lab/sensor", 10, false)?;

        assert_eq!(record.fields["float"], TelemetryValue::Float(0.0));
        assert_eq!(record.fields["integer"], TelemetryValue::Integer(0));
        assert_eq!(
            record.fields["unsigned"],
            TelemetryValue::Unsigned(u64::MAX)
        );
        assert_eq!(record.fields["boolean"], TelemetryValue::Boolean(false));
        assert_eq!(
            record.fields["string"],
            TelemetryValue::String("  keep spaces  ".to_owned())
        );
        assert_eq!(record.fields["defaulted"], TelemetryValue::Integer(0));

        let json = serde_json::to_value(record)?;
        assert_eq!(json["fields"]["unsigned"], json!(u64::MAX));
        assert_eq!(json["fields"]["boolean"], json!(false));
        assert_eq!(json["fields"]["string"], json!("  keep spaces  "));
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_supports_each_device_identity_source(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let identities = [
            (json!("fixed"), "fixed", "payload-id"),
            (json!({"topic_segment": 1}), "lab", "payload-id"),
            (json!({"pointer": "/device_id"}), "payload-id", "payload-id"),
            (
                json!({"pointer": "/device_id", "topic_segment": 1}),
                "lab",
                "lab",
            ),
        ];
        let fields = required_float_field();

        for (identity, expected_id, payload_id) in identities {
            let route = route_config(identity, fields.clone(), received_timestamp(), empty_tags())?;
            let payload = json!({"value": 12.5, "device_id": payload_id});
            let record = map_payload(&route, &payload, "home/lab/sensor", 10, false)?;
            assert_eq!(record.device_id, expected_id);
            assert_eq!(record.tags["device_id"], expected_id);
        }

        let route = route_config(
            json!({"pointer": "/device_id", "topic_segment": 1}),
            fields,
            received_timestamp(),
            empty_tags(),
        )?;
        assert!(map_payload(
            &route,
            &json!({"device_id": "lab", "value": 12.5}),
            "home/other/sensor",
            10,
            false
        )
        .is_err());
        for topic in ["home", "home//sensor"] {
            assert!(map_payload(
                &route,
                &json!({"device_id": "lab", "value": 12.5}),
                topic,
                10,
                false
            )
            .is_err());
        }
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_rejects_missing_or_invalid_mapped_values(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let invalid = [
            ("integer", json!(1.5)),
            ("integer", json!(9_223_372_036_854_775_808u64)),
            ("unsigned", json!(-1)),
            ("boolean", json!(null)),
            ("string", json!(42)),
        ];

        for (field_type, value) in invalid {
            let fields = json!({
                "value": {"pointer": "/value", "type": field_type}
            });
            let route = route_config(json!("device"), fields, received_timestamp(), empty_tags())?;
            assert!(map_payload(
                &route,
                &json!({"value": value}),
                "home/lab/sensor",
                10,
                false
            )
            .is_err());
        }

        let route = route_config(
            json!("device"),
            json!({"value": {"pointer": "/value", "type": "float"}}),
            received_timestamp(),
            empty_tags(),
        )?;
        assert!(map_payload(
            &route,
            &json!({"value": null}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());
        assert!(map_payload(&route, &json!({}), "home/lab/sensor", 10, false).is_err());
        assert!(map_payload(
            &route,
            &json!({"value": 1.0}),
            "home/lab/sensor",
            MAX_INFLUX_TIMESTAMP_MS + 1,
            false
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_uses_default_only_when_field_is_absent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("device"),
            json!({"value": {"pointer": "/value", "type": "integer", "default": 7}}),
            received_timestamp(),
            empty_tags(),
        )?;

        let absent = map_payload(&route, &json!({}), "home/lab/sensor", 10, false)?;
        assert_eq!(absent.fields["value"], TelemetryValue::Integer(7));
        assert!(map_payload(
            &route,
            &json!({"value": null}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());
        assert!(map_payload(
            &route,
            &json!({"device_id": "", "value": 12.5}),
            "home//sensor",
            10,
            false
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_rejects_empty_results_when_all_fields_are_optional(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("device"),
            json!({"value": {"pointer": "/value", "type": "integer", "optional": true}}),
            received_timestamp(),
            empty_tags(),
        )?;

        assert!(map_payload(&route, &json!({}), "home/lab/sensor", 10, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_converts_rfc3339_timestamps_to_floor_milliseconds(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("device"),
            required_float_field(),
            json!({"source": "payload", "pointer": "/time", "format": "rfc3339"}),
            empty_tags(),
        )?;

        let record = map_payload(
            &route,
            &json!({"value": 1.0, "time": "1969-12-31T23:59:59.999500Z"}),
            "home/lab/sensor",
            10,
            false,
        )?;
        assert_eq!(record.timestamp_ms, -1);
        let timezone_offset = map_payload(
            &route,
            &json!({"value": 1.0, "time": "1970-01-01T01:00:00+01:00"}),
            "home/lab/sensor",
            10,
            false,
        )?;
        assert_eq!(timezone_offset.timestamp_ms, 0);
        assert!(map_payload(
            &route,
            &json!({"value": 1.0, "time": "not-a-timestamp"}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());
        assert!(map_payload(&route, &json!({"value": 1.0}), "home/lab/sensor", 10, false).is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_keeps_epoch_milliseconds_and_enforces_range(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("device"),
            required_float_field(),
            json!({"source": "payload", "pointer": "/time", "format": "epoch_ms"}),
            empty_tags(),
        )?;

        for timestamp_ms in [-MAX_INFLUX_TIMESTAMP_MS, MAX_INFLUX_TIMESTAMP_MS] {
            let record = map_payload(
                &route,
                &json!({"value": 1.0, "time": timestamp_ms}),
                "home/lab/sensor",
                10,
                false,
            )?;
            assert_eq!(record.timestamp_ms, timestamp_ms);
        }
        for timestamp_ms in [-MAX_INFLUX_TIMESTAMP_MS - 1, MAX_INFLUX_TIMESTAMP_MS + 1] {
            assert!(map_payload(
                &route,
                &json!({"value": 1.0, "time": timestamp_ms}),
                "home/lab/sensor",
                10,
                false
            )
            .is_err());
        }
        assert!(map_payload(
            &route,
            &json!({"value": 1.0, "time": 1.5}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_uses_received_time_only_for_false_valid_flag(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!("device"),
            required_float_field(),
            json!({
                "source": "payload",
                "pointer": "/time",
                "format": "epoch_ms",
                "valid_flag": "/time_valid",
                "fallback": "received_at"
            }),
            empty_tags(),
        )?;
        let receive_ms = 1_767_000_000_123;

        let fallback = map_payload(
            &route,
            &json!({"value": 1.0, "time_valid": false}),
            "home/lab/sensor",
            receive_ms,
            false,
        )?;
        assert_eq!(fallback.timestamp_ms, receive_ms);

        let payload_time = map_payload(
            &route,
            &json!({"value": 1.0, "time_valid": true, "time": 123}),
            "home/lab/sensor",
            receive_ms,
            false,
        )?;
        assert_eq!(payload_time.timestamp_ms, 123);
        assert!(map_payload(
            &route,
            &json!({"value": 1.0, "time_valid": true}),
            "home/lab/sensor",
            receive_ms,
            false
        )
        .is_err());
        assert!(map_payload(
            &route,
            &json!({"value": 1.0}),
            "home/lab/sensor",
            receive_ms,
            false
        )
        .is_err());
        assert!(map_payload(
            &route,
            &json!({"value": 1.0, "time_valid": "false"}),
            "home/lab/sensor",
            receive_ms,
            false
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_rejects_tag_collisions_and_invalid_pointer_tags(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let collision = route_config(
            json!("device"),
            required_float_field(),
            received_timestamp(),
            json!({"device_id": "collision"}),
        )?;
        assert!(map_payload(
            &collision,
            &json!({"value": 1.0}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());

        let pointer_tag = route_config(
            json!("device"),
            required_float_field(),
            received_timestamp(),
            json!({"room": {"pointer": "/room"}}),
        )?;
        for payload in [json!({}), json!({"room": null}), json!({"room": ""})] {
            assert!(map_payload(&pointer_tag, &payload, "home/lab/sensor", 10, false).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_rejects_payload_topic_identity_mismatch(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route = route_config(
            json!({"pointer": "/device_id", "topic_segment": 1}),
            required_float_field(),
            received_timestamp(),
            empty_tags(),
        )?;

        assert!(map_payload(
            &route,
            &json!({"device_id": "not-lab", "value": 1.0}),
            "home/lab/sensor",
            10,
            false
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn test_telemetry_mapping_maps_firmware_payload_and_validity_fallback(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let route: RouteConfig = serde_json::from_value(json!({
            "id": "firmware-sensor",
            "topic": "smarthome/+/sensor",
            "schema": "sensor.schema.json",
            "device_id": {"pointer": "/device_id", "topic_segment": 1},
            "measurement": "bme680",
            "tags": {
                "room": {"pointer": "/room"},
                "device_class": {"pointer": "/device_class"}
            },
            "fields": {
                "temp_c": {"pointer": "/data/temp_c", "type": "float"},
                "boot_id": {"pointer": "/boot_id", "type": "unsigned", "optional": true},
                "ssid": {"pointer": "/ssid", "type": "string", "default": ""}
            },
            "timestamp": {
                "source": "payload",
                "pointer": "/time_iso",
                "format": "rfc3339",
                "valid_flag": "/time_valid",
                "fallback": "received_at"
            }
        }))?;
        let topic = "smarthome/sensor-1/sensor";
        let receive_ms = 1_767_000_000_123;
        let valid_payload = json!({
            "device_id": "sensor-1",
            "room": "bedroom",
            "device_class": "bme680",
            "time_iso": "2026-11-02T07:46:04.591Z",
            "time_valid": true,
            "data": {"temp_c": 21.5},
            "boot_id": u64::MAX
        });

        let record = map_payload(&route, &valid_payload, topic, receive_ms, false)?;
        assert_eq!(record.timestamp_ms, 1_793_605_564_591);
        assert_eq!(record.tags["room"], "bedroom");
        assert_eq!(record.tags["device_id"], "sensor-1");
        assert_eq!(record.fields["boot_id"], TelemetryValue::Unsigned(u64::MAX));
        assert_eq!(record.fields["ssid"], TelemetryValue::String(String::new()));

        let fallback_payload = json!({
            "device_id": "sensor-1",
            "room": "bedroom",
            "device_class": "bme680",
            "time_valid": false,
            "data": {"temp_c": 21.5}
        });
        let fallback_record = map_payload(&route, &fallback_payload, topic, receive_ms, true)?;
        assert_eq!(fallback_record.timestamp_ms, receive_ms);
        assert_eq!(
            fallback_record.fields["mqtt_retained"],
            TelemetryValue::Boolean(true)
        );

        let mismatch_payload = json!({
            "device_id": "other-device",
            "room": "bedroom",
            "device_class": "bme680",
            "time_valid": false,
            "data": {"temp_c": 21.5}
        });
        assert!(map_payload(&route, &mismatch_payload, topic, receive_ms, false).is_err());
        Ok(())
    }
}
