use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::model::messages::telemetry::validate_line_text;

/// A versioned collection of configured MQTT routes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBundle {
    /// Version of the route configuration contract.
    pub version: u32,
    /// Routes available to the ingestion service.
    pub routes: Vec<RouteConfig>,
    /// Explicit schema URI to local-file mappings.
    #[serde(default)]
    pub schema_resources: BTreeMap<String, PathBuf>,
}

#[derive(Deserialize)]
struct BundleVersion {
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteManifest {
    version: u32,
    route_files: Vec<PathBuf>,
    #[serde(default)]
    schema_resources: BTreeMap<String, PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteFamily {
    routes: Vec<RouteBinding>,
    mapping: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteBinding {
    id: String,
    topic: String,
    #[serde(default, deserialize_with = "deserialize_sink_target")]
    sink_target: Option<String>,
}

/// A single topic route and its payload-to-record mappings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    /// Unique route identifier.
    pub id: String,
    /// MQTT topic filter subscribed for this route.
    pub topic: String,
    /// Path to the raw payload schema.
    pub schema: PathBuf,
    /// Source used to derive the canonical device ID.
    pub device_id: DeviceIdSource,
    /// Influx measurement name.
    pub measurement: String,
    /// Destination bucket; omitted routes inherit `INFLUX_BUCKET` at startup.
    #[serde(default, deserialize_with = "deserialize_sink_target")]
    pub sink_target: Option<String>,
    /// Explicit tag mappings.
    #[serde(default)]
    pub tags: BTreeMap<String, TagValue>,
    /// Explicit canonical field mappings.
    pub fields: BTreeMap<String, FieldMapping>,
    /// Source and policy for the record timestamp.
    pub timestamp: TimestampConfig,
    /// Ordered calculation names applied to this route.
    #[serde(default)]
    pub calculations: Vec<CalculationName>,
    /// Whether this route contributes to the latest-state cache.
    #[serde(default)]
    pub cache: bool,
    /// Optional schema for the mapped output record.
    #[serde(default)]
    pub output_schema: Option<PathBuf>,
}

/// Supported device ID sources.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DeviceIdSource {
    /// A fixed, nonempty device ID.
    Fixed(String),
    /// A zero-based topic segment.
    TopicSegment(TopicSegmentConfig),
    /// A JSON Pointer into the payload.
    Pointer(PointerConfig),
    /// A payload JSON Pointer that must equal a topic segment.
    PointerAndTopicSegment(PointerTopicSegmentConfig),
}

/// Configuration for extracting identity from a topic segment.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicSegmentConfig {
    /// Zero-based index of the topic segment.
    pub topic_segment: usize,
}

/// Configuration containing one JSON Pointer.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointerConfig {
    /// RFC 6901 JSON Pointer.
    pub pointer: String,
}

/// Configuration for matching payload identity to a topic segment.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointerTopicSegmentConfig {
    /// RFC 6901 JSON Pointer to the payload identity.
    pub pointer: String,
    /// Zero-based index of the topic segment to match.
    pub topic_segment: usize,
}

/// A static tag value or a JSON Pointer to a payload string.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TagValue {
    /// A static tag value.
    Static(String),
    /// A pointer to a tag value in the payload.
    Pointer(PointerConfig),
}

/// A mapped field and its missing-value policy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMapping {
    /// RFC 6901 JSON Pointer to the source value.
    pub pointer: String,
    /// Required scalar type of the mapped value.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether a missing value is omitted.
    #[serde(default)]
    pub optional: bool,
    /// Scalar value to use only when the pointer is absent.
    #[serde(default, deserialize_with = "deserialize_field_default")]
    pub default: Option<Value>,
}

/// Scalar types accepted by route field mappings.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// A finite floating-point number.
    Float,
    /// A signed 64-bit integer.
    Integer,
    /// An unsigned 64-bit integer.
    Unsigned,
    /// A boolean.
    Boolean,
    /// A string.
    String,
}

/// Timestamp source and payload fallback policy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampConfig {
    /// Timestamp source.
    pub source: TimestampSource,
    /// Payload pointer when `source` is `payload`.
    #[serde(default)]
    pub pointer: Option<String>,
    /// Payload timestamp encoding.
    #[serde(default)]
    pub format: Option<TimestampFormat>,
    /// Payload pointer to a boolean flag controlling fallback behavior.
    #[serde(default)]
    pub valid_flag: Option<String>,
    /// Explicit fallback when the valid flag is false.
    #[serde(default)]
    pub fallback: Option<TimestampFallback>,
}

/// Supported timestamp sources.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampSource {
    /// Use the receive time captured when the message was accepted.
    ReceivedAt,
    /// Read the timestamp from the payload.
    Payload,
}

/// Supported payload timestamp formats.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    /// RFC 3339 date-time string.
    Rfc3339,
    /// Unix epoch milliseconds.
    EpochMs,
}

/// Supported explicit payload timestamp fallbacks.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFallback {
    /// Use the receive time captured when the message was accepted.
    ReceivedAt,
}

/// Calculation names supported by the route contract.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CalculationName {
    /// Calculate dew point from temperature and relative humidity.
    DewPoint,
    /// Calculate heat index from temperature and relative humidity.
    HeatIndex,
    /// Calculate absolute humidity in grams per cubic metre.
    AbsoluteHumidity,
    /// Calculate water mass per dry-air mass in grams per kilogram.
    HumidityRatio,
    /// Calculate the hypothetical surface temperature at 80% relative humidity.
    #[serde(rename = "surface_temp_at_80rh")]
    SurfaceTempAt80rh,
    /// Calculate air temperature minus its dew point.
    DewPointDepression,
    /// Calculate indoor air quality from gas resistance and humidity.
    Iaq,
}

impl RouteBundle {
    fn validate(&mut self, bundle_path: &Path) -> Result<()> {
        if !matches!(self.version, 1 | 2) {
            bail!(
                "unsupported route bundle version {}; only versions 1 and 2 are supported",
                self.version
            );
        }
        if self.routes.is_empty() {
            bail!("route bundle must contain at least one route");
        }

        let bundle_dir = bundle_path.parent().unwrap_or_else(|| Path::new(""));
        let canonical_root = if self.version == 2 {
            Some(fs::canonicalize(if bundle_dir.as_os_str().is_empty() {
                Path::new(".")
            } else {
                bundle_dir
            })?)
        } else {
            None
        };
        let mut ids = BTreeSet::new();
        for (index, route) in self.routes.iter_mut().enumerate() {
            route
                .validate(bundle_dir)
                .with_context(|| format!("validating routes[{index}]"))?;
            if let Some(root) = &canonical_root {
                route.schema = contained_file(&route.schema, root, "schema")?;
                if let Some(output_schema) = &mut route.output_schema {
                    *output_schema = contained_file(output_schema, root, "output_schema")?;
                }
            }
            if !ids.insert(route.id.as_str()) {
                bail!("duplicate route id '{}'", route.id);
            }
        }

        for (uri, path) in &mut self.schema_resources {
            if uri.is_empty() {
                bail!("schema_resources keys must not be empty");
            }
            if path.as_os_str().is_empty() {
                bail!("schema_resources entry '{uri}' must have a nonempty file path");
            }
            *path =
                resolve_bundle_path(path, bundle_dir, &format!("schema_resources entry '{uri}'"))?;
            if let Some(root) = &canonical_root {
                *path = contained_file(path, root, "schema_resources")?;
            }
        }

        Ok(())
    }
}

impl RouteConfig {
    fn validate(&mut self, bundle_dir: &Path) -> Result<()> {
        self.validate_static()?;
        self.schema = resolve_bundle_path(&self.schema, bundle_dir, "schema")?;
        if let Some(output_schema) = &mut self.output_schema {
            *output_schema = resolve_bundle_path(output_schema, bundle_dir, "output_schema")?;
        }

        Ok(())
    }

    pub(crate) fn validate_static(&self) -> Result<()> {
        if let Some(target) = &self.sink_target {
            validate_sink_target(target)?;
        }
        if self.id.is_empty() {
            bail!("route id must not be empty");
        }
        if self.topic.is_empty() {
            bail!("route topic must not be empty");
        }
        if self.measurement.is_empty() {
            bail!("route measurement must not be empty");
        }
        if self.measurement.starts_with('#') {
            bail!("route measurement must not start with '#'");
        }
        if self.fields.is_empty() {
            bail!("route fields must not be empty");
        }
        validate_line_text(&self.id, "route identifier")?;
        validate_line_text(&self.topic, "route topic filter")?;
        validate_line_text(&self.measurement, "measurement")?;

        validate_device_id(&self.device_id)?;
        for (key, value) in &self.tags {
            validate_mapping_key(key)?;
            validate_tag_value(value)?;
            if self.fields.contains_key(key) {
                bail!("tag and field name '{key}' collides");
            }
        }
        for (key, mapping) in &self.fields {
            validate_mapping_key(key)?;
            validate_field_mapping(key, mapping)?;
        }
        validate_timestamp(&self.timestamp)?;

        Ok(())
    }
}

/// Validates a literal bucket name without including its value in diagnostics.
///
/// # Errors
/// Returns an error for empty, oversized, padded, or control-containing names.
pub fn validate_sink_target(target: &str) -> Result<()> {
    if target.is_empty()
        || target.len() > 255
        || target.trim() != target
        || target.chars().any(char::is_control)
    {
        bail!(
            "sink_target must be 1..255 bytes with no surrounding whitespace or control characters"
        );
    }
    Ok(())
}

fn deserialize_sink_target<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("sink_target must be a bucket-name string"))
}

fn validate_device_id(source: &DeviceIdSource) -> Result<()> {
    match source {
        DeviceIdSource::Fixed(value) => {
            if value.is_empty() {
                bail!("device_id fixed value must not be empty");
            }
            validate_line_text(value, "device identity")?;
        }
        DeviceIdSource::TopicSegment(_) => {}
        DeviceIdSource::Pointer(config) => {
            validate_nonempty_pointer(&config.pointer, "device_id.pointer")?;
        }
        DeviceIdSource::PointerAndTopicSegment(config) => {
            validate_nonempty_pointer(&config.pointer, "device_id.pointer")?;
        }
    }
    Ok(())
}

fn validate_tag_value(value: &TagValue) -> Result<()> {
    match value {
        TagValue::Static(value) => {
            if value.is_empty() {
                bail!("static tag value must not be empty");
            }
            validate_line_text(value, "tag value")?;
        }
        TagValue::Pointer(config) => validate_pointer(&config.pointer, "tag pointer")?,
    }
    Ok(())
}

fn validate_mapping_key(key: &str) -> Result<()> {
    if key.is_empty() {
        bail!("tag and field names must not be empty");
    }
    validate_line_text(key, "tag or field name")?;
    if matches!(
        key,
        "device_id" | "mqtt_retained" | "_field" | "_measurement" | "time"
    ) {
        bail!("tag or field name '{key}' is reserved");
    }
    Ok(())
}

fn validate_field_mapping(key: &str, mapping: &FieldMapping) -> Result<()> {
    validate_pointer(&mapping.pointer, &format!("field '{key}' pointer"))?;
    if mapping.default.is_some() && mapping.optional {
        bail!("field '{key}' cannot specify both default and optional: true");
    }
    if let Some(default) = &mapping.default {
        if !default_matches_type(default, &mapping.field_type) {
            bail!(
                "field '{key}' default must be a scalar matching type {:?}",
                mapping.field_type
            );
        }
        if let Value::String(value) = default {
            validate_line_text(value, "field default")?;
        }
    }
    Ok(())
}

fn default_matches_type(value: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Float => value.as_f64().is_some() && value.is_number(),
        FieldType::Integer => value.as_i64().is_some(),
        FieldType::Unsigned => value.as_u64().is_some(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::String => value.is_string(),
    }
}

fn deserialize_field_default<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

fn validate_timestamp(timestamp: &TimestampConfig) -> Result<()> {
    match &timestamp.source {
        TimestampSource::ReceivedAt => {
            if timestamp.pointer.is_some()
                || timestamp.format.is_some()
                || timestamp.valid_flag.is_some()
                || timestamp.fallback.is_some()
            {
                bail!("received_at timestamp must not specify payload timestamp properties");
            }
        }
        TimestampSource::Payload => {
            let pointer = timestamp
                .pointer
                .as_deref()
                .context("payload timestamp requires pointer")?;
            validate_nonempty_pointer(pointer, "timestamp.pointer")?;
            if timestamp.format.is_none() {
                bail!("payload timestamp requires format");
            }

            match (&timestamp.valid_flag, &timestamp.fallback) {
                (Some(flag), Some(TimestampFallback::ReceivedAt)) => {
                    validate_nonempty_pointer(flag, "timestamp.valid_flag")?;
                }
                (None, None) => {}
                (Some(_), None) => bail!("timestamp.valid_flag requires fallback"),
                (None, Some(_)) => bail!("timestamp.fallback requires valid_flag"),
            }
        }
    }
    Ok(())
}

fn validate_nonempty_pointer(pointer: &str, label: &str) -> Result<()> {
    if pointer.is_empty() {
        bail!("{label} must not be empty");
    }
    validate_pointer(pointer, label)
}

fn validate_pointer(pointer: &str, label: &str) -> Result<()> {
    if !pointer.is_empty() && !pointer.starts_with('/') {
        bail!("{label} must be an RFC 6901 JSON Pointer");
    }

    let bytes = pointer.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'~' {
            if !matches!(bytes.get(index + 1), Some(b'0' | b'1')) {
                bail!("{label} contains an invalid RFC 6901 escape");
            }
            index += 1;
        }
        index += 1;
    }
    Ok(())
}

fn resolve_bundle_path(path: &Path, bundle_dir: &Path, label: &str) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{label} path must not be empty");
    }
    if path.is_absolute() {
        bail!("{label} path must be relative to the bundle directory");
    }

    let mut depth = 0;
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(_) => depth += 1,
            Component::ParentDir if depth > 0 => depth -= 1,
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("{label} path must not escape the bundle directory");
            }
        }
    }

    Ok(bundle_dir.join(path))
}

fn contained_file(path: &Path, root: &Path, label: &str) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("resolving {label} file '{}'", path.display()))?;
    if !canonical.starts_with(root) {
        bail!("{label} path must not escape the bundle directory");
    }
    Ok(canonical)
}

fn load_manifest(contents: &str, path: &Path) -> Result<RouteBundle> {
    let manifest: RouteManifest = serde_json::from_str(contents)?;
    if manifest.route_files.is_empty() {
        bail!("route_files must contain at least one file");
    }
    let bundle_dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let root = fs::canonicalize(bundle_dir).context("resolving bundle directory")?;
    let mut files = BTreeSet::new();
    let mut routes = Vec::new();
    for (index, file) in manifest.route_files.iter().enumerate() {
        let file = resolve_bundle_path(file, &root, "route_files")?;
        let file = contained_file(&file, &root, "route_files")?;
        if !files.insert(file.clone()) {
            bail!("duplicate route_files reference at index {index}");
        }
        let contents = fs::read_to_string(&file)
            .with_context(|| format!("reading route family '{}'", file.display()))?;
        let family: RouteFamily = serde_json::from_str(&contents)
            .with_context(|| format!("parsing route family '{}'", file.display()))?;
        if family.routes.is_empty() {
            bail!(
                "route family '{}' must contain at least one binding",
                file.display()
            );
        }
        for key in ["id", "topic", "sink_target"] {
            if family.mapping.contains_key(key) {
                bail!(
                    "route family '{}' mapping must not define binding property '{key}'",
                    file.display()
                );
            }
        }
        for (index, binding) in family.routes.into_iter().enumerate() {
            // Only disjoint binding keys are added; RouteConfig owns the mapping contract.
            let mut route = family.mapping.clone();
            route.insert("id".into(), binding.id.into());
            route.insert("topic".into(), binding.topic.into());
            if let Some(target) = binding.sink_target {
                route.insert("sink_target".into(), target.into());
            }
            let context = || format!("route family '{}' binding[{index}]", file.display());
            let route: RouteConfig =
                serde_json::from_value(Value::Object(route)).with_context(context)?;
            route.validate_static().with_context(context)?;
            routes.push(route);
        }
    }
    Ok(RouteBundle {
        version: manifest.version,
        routes,
        schema_resources: manifest.schema_resources,
    })
}

/// Loads and validates a v1 inline bundle or v2 manifest from a JSON file.
///
/// All relative file paths resolve against the bundle's parent directory. V2 expands
/// explicitly listed family files into flat routes and checks canonical containment.
/// This function does not compile schemas, access the environment, or perform network I/O.
///
/// # Errors
///
/// Returns an error when the path is empty or unreadable, the JSON is invalid, the bundle
/// version is unsupported, or any file or route violates the configuration contract.
pub fn load(path: impl AsRef<Path>) -> Result<RouteBundle> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        bail!("route bundle path must not be empty");
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading route bundle '{}'", path.display()))?;
    let version: BundleVersion = serde_json::from_str(&contents)
        .with_context(|| format!("parsing route bundle '{}'", path.display()))?;
    let mut bundle = match version.version {
        1 => serde_json::from_str::<RouteBundle>(&contents)
            .with_context(|| format!("parsing route bundle '{}'", path.display()))?,
        2 => load_manifest(&contents, path)
            .with_context(|| format!("loading route manifest '{}'", path.display()))?,
        version => {
            bail!("unsupported route bundle version {version}; only versions 1 and 2 are supported")
        }
    };
    bundle
        .validate(path)
        .with_context(|| format!("validating route bundle '{}'", path.display()))?;
    Ok(bundle)
}

/// Reports deprecated topic-routing environment variable names.
///
/// `MQTT_TOPIC_DLQ` is retained and is not reported.
///
/// This function does not return a `Result`; it returns `None` when no deprecated
/// variables are present, otherwise a migration message naming them.
pub fn removed_route_environment_variables<I, S>(names: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let removed: Vec<String> = names
        .into_iter()
        .filter_map(|name| {
            let name = name.as_ref();
            let is_removed = name == "ENFORCE_TOPIC_DEVICE_MATCH"
                || (name.starts_with("MQTT_TOPIC_") && name != "MQTT_TOPIC_DLQ");
            is_removed.then(|| name.to_owned())
        })
        .collect();

    if removed.is_empty() {
        None
    } else {
        Some(format!(
            "Removed routing environment variables are set: {}. Define subscriptions in the route bundle and set INGEST_ROUTES_FILE instead.",
            removed.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::fs;

    fn valid_route() -> Value {
        json!({
            "id": "test-route",
            "topic": "test/topic",
            "schema": "schemas/test.json",
            "device_id": "test-device",
            "measurement": "test_measurement",
            "fields": {
                "temp_c": {"pointer": "/temperature", "type": "float"}
            },
            "timestamp": {"source": "received_at"}
        })
    }

    fn valid_bundle() -> Value {
        json!({
            "version": 1,
            "routes": [valid_route()]
        })
    }

    fn load_value(value: &Value) -> Result<RouteBundle> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("routes.json");
        fs::write(&path, serde_json::to_vec(value)?)?;
        load(path)
    }

    fn v2_family(id: &str, topic: &str) -> Value {
        let mut mapping = valid_route();
        let object = mapping.as_object_mut().unwrap();
        object.remove("id");
        object.remove("topic");
        json!({"routes": [{"id": id, "topic": topic}], "mapping": mapping})
    }

    fn write_v2_fixture(directory: &Path) -> Result<(PathBuf, Value, Value)> {
        fs::create_dir_all(directory.join("routes/nested"))?;
        fs::create_dir_all(directory.join("schemas"))?;
        fs::write(directory.join("schemas/test.json"), "{}")?;
        let manifest = json!({"version": 2, "route_files": ["routes/nested/first.json"]});
        let family = v2_family("first", "test/first");
        let path = directory.join("routes.json");
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        fs::write(
            directory.join("routes/nested/first.json"),
            serde_json::to_vec(&family)?,
        )?;
        Ok((path, manifest, family))
    }

    #[test]
    fn test_route_config_v2_expands_families_in_order_from_manifest_root() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let (path, mut manifest, mut family) = write_v2_fixture(directory.path())?;
        family["routes"].as_array_mut().unwrap().push(json!({
            "id": "second", "topic": "test/second", "sink_target": "metrics"
        }));
        family["mapping"]["output_schema"] = json!("schemas/test.json");
        manifest["route_files"]
            .as_array_mut()
            .unwrap()
            .push(json!("routes/last.json"));
        manifest["schema_resources"] = json!({"https://schemas.test/shared": "schemas/test.json"});
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        fs::write(
            directory.path().join("routes/nested/first.json"),
            serde_json::to_vec(&family)?,
        )?;
        fs::write(
            directory.path().join("routes/last.json"),
            serde_json::to_vec(&v2_family("third", "test/third"))?,
        )?;
        fs::write(directory.path().join("routes/inactive.json"), "not JSON")?;

        let bundle = load(&path)?;

        assert_eq!(bundle.version, 2);
        assert_eq!(
            bundle
                .routes
                .iter()
                .map(|r| r.id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second", "third"]
        );
        assert!(bundle.routes[0].sink_target.is_none());
        assert_eq!(bundle.routes[1].sink_target.as_deref(), Some("metrics"));
        assert_eq!(
            bundle.routes[0].schema,
            directory.path().join("schemas/test.json").canonicalize()?
        );
        assert_eq!(bundle.routes[0].schema, bundle.routes[1].schema);
        assert_eq!(
            bundle.routes[0].output_schema.as_ref(),
            Some(&bundle.routes[0].schema)
        );
        assert_eq!(
            bundle.schema_resources["https://schemas.test/shared"],
            bundle.routes[0].schema
        );
        assert_eq!(bundle.routes[1].fields["temp_c"].pointer, "/temperature");
        Ok(())
    }

    #[test]
    fn test_route_config_v2_rejects_invalid_manifest_and_file_references() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let (path, manifest, _) = write_v2_fixture(directory.path())?;
        for (key, value, expected) in [
            ("route_files", json!([]), "at least one"),
            ("route_files", json!([""]), "empty"),
            ("route_files", json!(["missing.json"]), "missing.json"),
            ("route_files", json!(["../outside.json"]), "escape"),
            (
                "route_files",
                json!([directory.path().join("routes/nested/first.json")]),
                "relative",
            ),
            (
                "route_files",
                json!([
                    "routes/nested/first.json",
                    "routes/nested/../nested/first.json"
                ]),
                "duplicate",
            ),
            ("routes", json!([]), "unknown field"),
        ] {
            let mut invalid = manifest.clone();
            invalid[key] = value;
            fs::write(&path, serde_json::to_vec(&invalid)?)?;
            let error = load(&path).expect_err("invalid manifest must fail");
            assert!(format!("{error:#}").contains(expected), "{key}: {error:#}");
        }
        Ok(())
    }

    #[test]
    fn test_route_config_v2_rejects_invalid_families_and_mapping_overrides() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let (path, _, family) = write_v2_fixture(directory.path())?;
        for (pointer, value) in [
            ("/routes", json!([])),
            ("/mapping", json!(null)),
            ("/mapping/id", json!("hidden")),
            ("/mapping/topic", json!("hidden/topic")),
            ("/mapping/sink_target", json!("hidden-target")),
            ("/mapping/unknown", json!(true)),
            ("/routes/0/fields", json!({})),
            ("/routes/0/sink_target", json!(null)),
            ("/routes/0/topic", json!(null)),
            ("/unknown", json!(true)),
        ] {
            let mut invalid = family.clone();
            let (parent, key) = pointer.rsplit_once('/').unwrap();
            invalid.pointer_mut(parent).unwrap()[key] = value;
            fs::write(
                directory.path().join("routes/nested/first.json"),
                serde_json::to_vec(&invalid)?,
            )?;
            let error = load(&path).expect_err("invalid family must fail");
            assert!(
                format!("{error:#}").contains("first.json"),
                "{pointer}: {error:#}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_route_config_v2_rejects_cross_file_duplicate_ids_and_schema_escapes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let (path, mut manifest, mut family) = write_v2_fixture(directory.path())?;
        manifest["route_files"]
            .as_array_mut()
            .unwrap()
            .push(json!("routes/duplicate.json"));
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        fs::write(
            directory.path().join("routes/duplicate.json"),
            serde_json::to_vec(&family)?,
        )?;
        assert!(format!("{:#}", load(&path).unwrap_err()).contains("duplicate route id"));
        family["routes"][0]["id"] = json!("other");
        for property in ["schema", "output_schema"] {
            let mut invalid = family.clone();
            invalid["mapping"][property] = json!("../outside.json");
            fs::write(
                directory.path().join("routes/duplicate.json"),
                serde_json::to_vec(&invalid)?,
            )?;
            assert!(format!("{:#}", load(&path).unwrap_err()).contains("escape"));
        }
        fs::write(
            directory.path().join("routes/duplicate.json"),
            serde_json::to_vec(&family)?,
        )?;
        manifest["schema_resources"] = json!({"https://schemas.test/outside": "../outside.json"});
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(format!("{:#}", load(&path).unwrap_err()).contains("escape"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_route_config_v2_rejects_symlink_escapes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        let (path, manifest, family) = write_v2_fixture(directory.path())?;
        std::os::unix::fs::symlink(outside.path(), directory.path().join("outside"))?;
        fs::write(outside.path().join("family.json"), "{}")?;
        let mut invalid_manifest = manifest.clone();
        invalid_manifest["route_files"] = json!(["outside/family.json"]);
        fs::write(&path, serde_json::to_vec(&invalid_manifest)?)?;
        assert!(format!("{:#}", load(&path).unwrap_err()).contains("escape"));
        fs::write(outside.path().join("schema.json"), "{}")?;
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        for property in ["schema", "output_schema"] {
            let mut invalid = family.clone();
            invalid["mapping"][property] = json!("outside/schema.json");
            fs::write(
                directory.path().join("routes/nested/first.json"),
                serde_json::to_vec(&invalid)?,
            )?;
            assert!(format!("{:#}", load(&path).unwrap_err()).contains("escape"));
        }
        fs::write(
            directory.path().join("routes/nested/first.json"),
            serde_json::to_vec(&family)?,
        )?;
        invalid_manifest = manifest;
        invalid_manifest["schema_resources"] =
            json!({"https://schemas.test/outside": "outside/schema.json"});
        fs::write(&path, serde_json::to_vec(&invalid_manifest)?)?;
        assert!(format!("{:#}", load(&path).unwrap_err()).contains("escape"));
        Ok(())
    }

    #[test]
    fn test_route_config_loads_v1_bundle_and_resolves_paths() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("routes.json");
        let mut bundle = valid_bundle();
        bundle["routes"][0]["output_schema"] = json!("schemas/output.json");
        bundle["schema_resources"] = json!({
            "https://schemas.example.test/climate": "schemas/climate-resource.json"
        });
        fs::write(&path, serde_json::to_vec(&bundle)?)?;

        let loaded = load(&path)?;

        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.routes.len(), 1);
        assert_eq!(
            loaded.routes[0].schema,
            dir.path().join("schemas/test.json")
        );
        let expected_output_schema = dir.path().join("schemas/output.json");
        assert_eq!(
            loaded.routes[0].output_schema.as_deref(),
            Some(expected_output_schema.as_path())
        );
        assert_eq!(
            loaded.schema_resources["https://schemas.example.test/climate"],
            dir.path().join("schemas/climate-resource.json")
        );
        assert!(loaded.routes[0].tags.is_empty());
        assert!(loaded.routes[0].calculations.is_empty());
        assert!(!loaded.routes[0].cache);
        assert!(loaded.routes[0].output_schema.is_some());
        assert!(!loaded.routes[0].fields["temp_c"].optional);
        assert!(loaded.routes[0].fields["temp_c"].default.is_none());
        Ok(())
    }

    #[test]
    fn test_route_config_rejects_empty_path() {
        let err = load("").expect_err("empty path must fail");

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_route_config_rejects_unreadable_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let err = load(dir.path().join("missing.json")).expect_err("missing file must fail");

        assert!(format!("{err:#}").contains("reading route bundle"));
        Ok(())
    }

    #[test]
    fn test_route_config_rejects_invalid_json() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("routes.json");
        fs::write(&path, "{ invalid")?;

        let err = load(path).expect_err("invalid JSON must fail");

        assert!(format!("{err:#}").contains("parsing route bundle"));
        Ok(())
    }

    #[test]
    fn test_route_config_rejects_unsupported_version() {
        let mut bundle = valid_bundle();
        bundle["version"] = json!(3);

        let err = load_value(&bundle).expect_err("unsupported version must fail");

        assert!(format!("{err:#}").contains("only versions 1 and 2 are supported"));
    }

    #[test]
    fn test_route_config_rejects_empty_routes() {
        let mut bundle = valid_bundle();
        bundle["routes"] = json!([]);

        let err = load_value(&bundle).expect_err("empty route set must fail");

        assert!(format!("{err:#}").contains("at least one route"));
    }

    #[test]
    fn test_route_config_rejects_unknown_nested_properties() {
        let mut cases = Vec::new();

        let mut root = valid_bundle();
        root["extra"] = json!(true);
        cases.push(root);

        let mut route = valid_bundle();
        route["routes"][0]["extra"] = json!(true);
        cases.push(route);

        let mut field = valid_bundle();
        field["routes"][0]["fields"]["temp_c"]["extra"] = json!(true);
        cases.push(field);

        let mut timestamp = valid_bundle();
        timestamp["routes"][0]["timestamp"]["extra"] = json!(true);
        cases.push(timestamp);

        let mut identity = valid_bundle();
        identity["routes"][0]["device_id"] = json!({
            "pointer": "/id",
            "extra": true
        });
        cases.push(identity);

        let mut tag = valid_bundle();
        tag["routes"][0]["tags"] = json!({
            "room": {"pointer": "/room", "extra": true}
        });
        cases.push(tag);

        for case in cases {
            assert!(
                load_value(&case).is_err(),
                "unknown nested properties must be rejected"
            );
        }
    }

    #[test]
    fn test_route_config_rejects_duplicate_ids() {
        let mut bundle = valid_bundle();
        bundle["routes"].as_array_mut().expect("routes array")[0] = valid_route();
        bundle["routes"]
            .as_array_mut()
            .expect("routes array")
            .push(valid_route());

        let err = load_value(&bundle).expect_err("duplicate route ID must fail");

        assert!(format!("{err:#}").contains("duplicate route id"));
    }

    #[test]
    fn test_route_config_rejects_empty_required_route_properties() {
        let properties = ["id", "topic", "schema", "measurement"];
        for property in properties {
            let mut bundle = valid_bundle();
            bundle["routes"][0][property] = json!("");
            assert!(
                load_value(&bundle).is_err(),
                "empty {property} must be rejected"
            );
        }

        let mut fixed_identity = valid_bundle();
        fixed_identity["routes"][0]["device_id"] = json!("");
        assert!(load_value(&fixed_identity).is_err());

        let mut fields = valid_bundle();
        fields["routes"][0]["fields"] = json!({});
        assert!(load_value(&fields).is_err());
    }

    #[test]
    fn test_route_config_rejects_measurement_starting_with_comment_marker() {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["measurement"] = json!("#weather");

        assert!(load_value(&bundle).is_err());
    }

    #[test]
    fn test_route_config_rejects_empty_static_tag_value() {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["tags"] = json!({"source": ""});

        assert!(load_value(&bundle).is_err());
    }

    #[test]
    fn test_route_config_accepts_each_device_id_shape() {
        let sources = [
            json!("fixed-device"),
            json!({"topic_segment": 1}),
            json!({"pointer": "/device_id"}),
            json!({"pointer": "/device_id", "topic_segment": 1}),
        ];

        for source in sources {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["device_id"] = source;
            assert!(load_value(&bundle).is_ok());
        }
    }

    #[test]
    fn test_route_config_rejects_empty_or_invalid_device_identity() {
        let sources = [
            json!(""),
            json!({"pointer": ""}),
            json!({"pointer": "/bad~escape"}),
            json!({"topic_segment": -1}),
        ];

        for source in sources {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["device_id"] = source;
            assert!(load_value(&bundle).is_err());
        }
    }

    #[test]
    fn test_route_config_accepts_static_and_pointer_tags() {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["tags"] = json!({
            "room": "living_room",
            "floor": {"pointer": "/floor"}
        });

        let loaded = load_value(&bundle);

        assert!(loaded.is_ok());
    }

    #[test]
    fn test_route_config_accepts_optional_field_default_policies() {
        let field_policies = [
            json!({"pointer": "/temperature", "type": "float"}),
            json!({"pointer": "/temperature", "type": "float", "optional": false}),
            json!({"pointer": "/temperature", "type": "float", "optional": true}),
            json!({"pointer": "/temperature", "type": "float", "default": 20.5}),
            json!({
                "pointer": "/temperature",
                "type": "float",
                "optional": false,
                "default": 20.5
            }),
        ];

        for field in field_policies {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["fields"]["temp_c"] = field;
            assert!(load_value(&bundle).is_ok());
        }
    }

    #[test]
    fn test_route_config_accepts_defaults_of_each_scalar_type() {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["fields"] = json!({
            "temp_c": {
                "pointer": "/temperature",
                "type": "float",
                "default": 20.5
            },
            "count": {
                "pointer": "/count",
                "type": "integer",
                "default": -3
            },
            "sequence": {
                "pointer": "/sequence",
                "type": "unsigned",
                "default": 3
            },
            "available": {
                "pointer": "/available",
                "type": "boolean",
                "default": true
            },
            "label": {
                "pointer": "/label",
                "type": "string",
                "default": "synthetic"
            }
        });

        assert!(load_value(&bundle).is_ok());
    }

    #[test]
    fn test_route_config_rejects_invalid_field_defaults() {
        let defaults = [
            json!({
                "pointer": "/temperature",
                "type": "float",
                "default": "20"
            }),
            json!({
                "pointer": "/temperature",
                "type": "integer",
                "default": 20.5
            }),
            json!({
                "pointer": "/temperature",
                "type": "unsigned",
                "default": -1
            }),
            json!({
                "pointer": "/temperature",
                "type": "boolean",
                "default": null
            }),
            json!({
                "pointer": "/temperature",
                "type": "float",
                "optional": true,
                "default": 20.5
            }),
        ];

        for field in defaults {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["fields"]["temp_c"] = field;
            assert!(load_value(&bundle).is_err());
        }
    }

    #[test]
    fn test_route_config_accepts_payload_timestamp_formats_and_fallback() {
        let timestamps = [
            json!({
                "source": "payload",
                "pointer": "/time",
                "format": "rfc3339"
            }),
            json!({
                "source": "payload",
                "pointer": "/time_ms",
                "format": "epoch_ms"
            }),
            json!({
                "source": "payload",
                "pointer": "/time_ms",
                "format": "epoch_ms",
                "valid_flag": "/time_valid",
                "fallback": "received_at"
            }),
        ];

        for timestamp in timestamps {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["timestamp"] = timestamp;
            assert!(load_value(&bundle).is_ok());
        }
    }

    #[test]
    fn test_route_config_rejects_invalid_timestamp_policies() {
        let timestamps = [
            json!({"source": "payload", "pointer": "/time"}),
            json!({"source": "payload", "format": "rfc3339"}),
            json!({
                "source": "payload",
                "pointer": "/time",
                "format": "rfc3339",
                "valid_flag": "/time_valid"
            }),
            json!({
                "source": "payload",
                "pointer": "/time",
                "format": "rfc3339",
                "fallback": "received_at"
            }),
            json!({
                "source": "received_at",
                "pointer": "/time",
                "format": "rfc3339"
            }),
        ];

        for timestamp in timestamps {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["timestamp"] = timestamp;
            assert!(load_value(&bundle).is_err());
        }
    }

    #[test]
    fn test_route_config_rejects_invalid_json_pointers() {
        let pointers = ["/bad~", "/bad~2", "not/a/pointer"];

        for pointer in pointers {
            let mut bundle = valid_bundle();
            bundle["routes"][0]["fields"]["temp_c"]["pointer"] = json!(pointer);
            assert!(load_value(&bundle).is_err(), "invalid pointer: {pointer}");
        }
    }

    #[test]
    fn test_route_config_accepts_json_pointer_token_escaping() -> Result<()> {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["fields"]["temp_c"]["pointer"] = json!("/payload/with~1slash/~0tilde");

        assert!(load_value(&bundle).is_ok());
        Ok(())
    }

    #[test]
    fn test_route_config_rejects_reserved_mapping_names() {
        let reserved = [
            "device_id",
            "mqtt_retained",
            "_field",
            "_measurement",
            "time",
        ];

        for key in reserved {
            let mut field = valid_bundle();
            field["routes"][0]["fields"][key] = json!({"pointer": "/value", "type": "string"});
            assert!(load_value(&field).is_err(), "reserved field {key}");

            let mut tag = valid_bundle();
            tag["routes"][0]["tags"][key] = json!("value");
            assert!(load_value(&tag).is_err(), "reserved tag {key}");
        }
    }

    #[test]
    fn test_route_config_rejects_tag_and_field_name_collisions() {
        let mut bundle = valid_bundle();
        bundle["routes"][0]["tags"] = json!({"temp_c": "temperature"});

        assert!(load_value(&bundle).is_err());
    }

    #[test]
    fn test_route_config_rejects_control_characters_in_static_output() {
        let mut cases = Vec::new();
        let mut bundle = valid_bundle();
        bundle["routes"][0]["id"] = json!("bad\nroute");
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["measurement"] = json!("bad\nmeasurement");
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["device_id"] = json!("bad\nidentity");
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["tags"] = json!({"bad\nkey": "value"});
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["tags"] = json!({"room": "bad\nvalue"});
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["fields"]["bad\nkey"] = json!({"pointer": "/value", "type": "string"});
        cases.push(bundle);
        let mut bundle = valid_bundle();
        bundle["routes"][0]["fields"]["label"] =
            json!({"pointer": "/label", "type": "string", "default": "bad\nvalue"});
        cases.push(bundle);

        for bundle in cases {
            assert!(load_value(&bundle).is_err(), "accepted invalid static text");
        }
    }

    #[test]
    fn test_route_config_resolves_schema_resource_paths_and_rejects_absolute_resource() {
        let mut bundle = valid_bundle();
        bundle["schema_resources"] = json!({
            "https://schemas.example.test/resource": "schemas/resource.json"
        });
        assert!(load_value(&bundle).is_ok());

        bundle["schema_resources"]["https://schemas.example.test/resource"] =
            json!(std::env::temp_dir()
                .join("schemas")
                .join("resource.json")
                .to_string_lossy()
                .into_owned());
        assert!(load_value(&bundle).is_err());
    }

    #[test]
    fn test_route_config_rejects_empty_schema_paths() {
        let mut schema = valid_bundle();
        schema["routes"][0]["schema"] = json!("");
        assert!(load_value(&schema).is_err());

        let mut output_schema = valid_bundle();
        output_schema["routes"][0]["output_schema"] = json!("");
        assert!(load_value(&output_schema).is_err());

        let mut resource = valid_bundle();
        resource["schema_resources"] = json!({
            "https://schemas.example.test/resource": ""
        });
        assert!(load_value(&resource).is_err());
    }

    #[test]
    fn test_route_config_rejects_schema_paths_outside_bundle() {
        let absolute = std::env::temp_dir().join("schema.json");
        for field in ["schema", "output_schema"] {
            let mut bundle = valid_bundle();
            bundle["routes"][0][field] = json!(absolute);
            assert!(
                load_value(&bundle).is_err(),
                "accepted absolute {field} path"
            );

            bundle["routes"][0][field] = json!("../../outside.json");
            assert!(
                load_value(&bundle).is_err(),
                "accepted escaping {field} path"
            );
        }

        let mut resource = valid_bundle();
        resource["schema_resources"] = json!({
            "https://schemas.example.test/resource": "../../outside.json"
        });
        assert!(load_value(&resource).is_err());
    }

    #[test]
    fn test_route_config_loads_synthetic_zigbee_fixture() -> Result<()> {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("routes")
            .join("zigbee-climate.json");

        let bundle = load(fixture_path.clone())?;
        let route = &bundle.routes[0];

        assert_eq!(route.id, "synthetic-zigbee-climate");
        assert_eq!(route.measurement, "zigbee_climate");
        assert_eq!(route.fields["temp_c"].field_type, FieldType::Float);
        assert_eq!(route.fields["rel_hum_perc"].field_type, FieldType::Float);
        assert_eq!(route.calculations, [CalculationName::DewPoint]);
        assert!(route.cache);
        assert!(route.tags.is_empty());
        assert!(bundle.schema_resources.is_empty());
        assert_eq!(
            route.schema,
            fixture_path
                .parent()
                .expect("fixture has a parent")
                .join("zigbee-climate.schema.json")
        );
        Ok(())
    }

    #[test]
    fn test_route_config_reports_removed_environment_variables() {
        let report = removed_route_environment_variables([
            "MQTT_TOPIC_SENSOR",
            "MQTT_TOPIC_DLQ",
            "ENFORCE_TOPIC_DEVICE_MATCH",
            "OUTPUT_SINK",
        ]);

        let report = report.expect("removed environment variables must be reported");
        assert!(report.contains("MQTT_TOPIC_SENSOR"));
        assert!(report.contains("ENFORCE_TOPIC_DEVICE_MATCH"));
        assert!(!report.contains("MQTT_TOPIC_DLQ"));
        assert!(!report.contains("OUTPUT_SINK"));
        assert!(report.contains("INGEST_ROUTES_FILE"));
    }

    #[test]
    fn test_route_config_ignores_kept_and_unrelated_environment_variables() {
        let report =
            removed_route_environment_variables(["MQTT_TOPIC_DLQ", "MQTT_HOST", "OUTPUT_SINK"]);

        assert!(report.is_none());
    }
}
