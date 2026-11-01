use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use jsonschema::{Draft, Resource, Retrieve, Uri, Validator as JsonSchemaValidator};
use serde_json::Value;

const BASE_SCHEMA: &str = include_str!("../../schema/base.schema.json");

/// Compiled JSON Schema validator with the shared base schema preloaded.
pub struct JsonSchema {
    schema: JsonSchemaValidator,
}

impl JsonSchema {
    pub fn new(schema: &str) -> Result<Self> {
        let base_json: Value =
            serde_json::from_str(BASE_SCHEMA).context("Failed to parse embedded base schema")?;

        let schema_json: Value =
            serde_json::from_str(schema).context("Failed to parse embedded JSON schema")?;

        let compiled = jsonschema::options()
            .with_resource(
                "https://smarthome-ingest/base.schema.json",
                Resource::from_contents(base_json),
            )
            .build(&schema_json)
            .context("Failed to compile JSON schema")?;

        Ok(Self { schema: compiled })
    }

    pub fn validate(&self, instance: &Value) -> Result<()> {
        if self.schema.is_valid(instance) {
            return Ok(());
        }

        let mut msgs = Vec::new();
        // Cap the error list so DLQ reasons stay readable.
        for err in self.schema.iter_errors(instance).take(10) {
            msgs.push(format!("{}", err));
        }
        bail!("schema validation failed: {}", msgs.join(" | "))
    }
}

/// A file-backed schema compiled for a configured route.
#[allow(dead_code)]
pub struct CompiledSchema {
    route_id: String,
    schema: JsonSchemaValidator,
}

#[allow(dead_code)]
impl CompiledSchema {
    /// Reads and compiles a route schema and its explicitly registered local resources.
    ///
    /// # Errors
    ///
    /// Returns an error if a schema cannot be read or parsed, does not declare a supported
    /// draft, references an unavailable resource, or fails compilation.
    pub fn compile(
        route_id: &str,
        schema_path: &Path,
        resource_paths: &BTreeMap<String, PathBuf>,
    ) -> Result<Self> {
        let resources = load_schema_resources(route_id, resource_paths)?;
        Self::compile_with_resources(route_id, schema_path, &resources)
    }

    pub(crate) fn compile_with_resources(
        route_id: &str,
        schema_path: &Path,
        resources: &[(String, Resource)],
    ) -> Result<Self> {
        let contents = read_schema(schema_path)?;
        let draft = declared_draft(&contents).with_context(|| {
            format!("route '{route_id}' schema has no supported declared draft")
        })?;

        let schema = jsonschema::options()
            .with_draft(draft)
            .should_validate_formats(true)
            .with_retriever(OfflineSchemaRetriever)
            .with_resources(resources.iter().cloned())
            .build(&contents)
            .with_context(|| format!("route '{route_id}' schema compilation failed"))?;

        Ok(Self {
            route_id: route_id.to_owned(),
            schema,
        })
    }

    /// Validates an instance without including rejected values in diagnostics.
    ///
    /// # Errors
    ///
    /// Returns bounded diagnostics containing only this route ID and schema/instance paths
    /// when the instance fails validation.
    pub fn validate(&self, instance: &Value) -> Result<()> {
        let mut errors = self.schema.iter_errors(instance).take(10);
        let Some(first_error) = errors.next() else {
            return Ok(());
        };

        let diagnostics = std::iter::once(first_error)
            .chain(errors)
            .map(|error| {
                let schema_path = error.schema_path().to_string();
                let keyword = schema_path.rsplit('/').next().unwrap_or(&schema_path);
                format!(
                    "instance {} violates schema keyword '{}' at {}",
                    error.instance_path(),
                    keyword,
                    schema_path
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        bail!(
            "route '{}' schema validation failed: {diagnostics}",
            self.route_id
        )
    }
}

#[allow(dead_code)]
pub(crate) fn load_schema_resources(
    route_id: &str,
    resource_paths: &BTreeMap<String, PathBuf>,
) -> Result<Vec<(String, Resource)>> {
    let mut resources = Vec::with_capacity(resource_paths.len());
    for (uri, path) in resource_paths {
        if uri.is_empty() {
            bail!("route '{route_id}' has an empty schema resource URI");
        }
        let contents = read_schema(path).with_context(|| {
            format!("route '{route_id}' could not load schema resource '{uri}'")
        })?;
        declared_draft(&contents).with_context(|| {
            format!("route '{route_id}' schema resource '{uri}' has no supported declared draft")
        })?;
        resources.push((uri.clone(), Resource::from_contents(contents)));
    }
    Ok(resources)
}

#[allow(dead_code)]
fn read_schema(path: &Path) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("reading schema '{}'", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing schema '{}'", path.display()))
}

#[allow(dead_code)]
fn declared_draft(schema: &Value) -> Result<Draft> {
    let uri = schema
        .get("$schema")
        .and_then(Value::as_str)
        .context("missing string $schema declaration")?;
    let draft = Draft::from_schema_uri(uri);
    if draft == Draft::Unknown {
        bail!("unsupported JSON Schema draft '{uri}'");
    }
    Ok(draft)
}

#[allow(dead_code)]
struct OfflineSchemaRetriever;

#[allow(dead_code)]
impl Retrieve for OfflineSchemaRetriever {
    fn retrieve(
        &self,
        _uri: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::other("external schema retrieval is disabled").into())
    }
}

#[cfg(test)]
mod configured_route_schema_tests {
    use std::collections::BTreeMap;
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    const DRAFT_7: &str = "http://json-schema.org/draft-07/schema#";

    fn write_schema(path: &std::path::Path, schema: Value) -> Result<()> {
        fs::write(path, serde_json::to_vec(&schema)?)?;
        Ok(())
    }

    #[test]
    fn test_configured_route_schema_compiles_local_recursive_resource() -> Result<()> {
        let directory = tempdir()?;
        let root_path = directory.path().join("root.json");
        let resource_path = directory.path().join("node.json");
        write_schema(
            &root_path,
            json!({"$schema": DRAFT_7, "$ref": "https://schemas.test/node"}),
        )?;
        write_schema(
            &resource_path,
            json!({
                "$schema": DRAFT_7,
                "$id": "https://schemas.test/node",
                "type": "object",
                "required": ["value"],
                "properties": {
                    "value": {"type": "string"},
                    "child": {"$ref": "https://schemas.test/node"}
                }
            }),
        )?;
        let resources = BTreeMap::from([("https://schemas.test/node".to_owned(), resource_path)]);

        let schema = CompiledSchema::compile("recursive-route", &root_path, &resources)?;

        assert!(schema
            .validate(&json!({"value": "ok", "child": {"value": "nested"}}))
            .is_ok());
        assert!(schema
            .validate(&json!({"value": "ok", "child": {}}))
            .is_err());
        Ok(())
    }

    #[test]
    fn test_configured_route_schema_rejects_unregistered_remote_reference_offline() -> Result<()> {
        let directory = tempdir()?;
        let root_path = directory.path().join("remote.json");
        write_schema(
            &root_path,
            json!({"$schema": DRAFT_7, "$ref": "https://unregistered.invalid/schema"}),
        )?;

        let error = CompiledSchema::compile("remote-route", &root_path, &BTreeMap::new())
            .err()
            .expect("unregistered remote reference must fail");

        assert!(error.to_string().contains("remote-route"));
        Ok(())
    }

    #[test]
    fn test_configured_route_schema_asserts_format_and_sanitizes_errors() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("date-time.json");
        write_schema(
            &schema_path,
            json!({
                "$schema": DRAFT_7,
                "type": "object",
                "properties": {"occurred_at": {"type": "string", "format": "date-time"}}
            }),
        )?;

        let schema = CompiledSchema::compile("date-route", &schema_path, &BTreeMap::new())?;
        let secret_value = "do-not-leak-not-a-date";
        let error = schema
            .validate(&json!({"occurred_at": secret_value}))
            .expect_err("malformed date-time must fail format assertion");
        let diagnostic = format!("{error:#}");

        assert!(diagnostic.contains("date-route"));
        assert!(diagnostic.contains("occurred_at"));
        assert!(diagnostic.contains("format"));
        assert!(!diagnostic.contains(secret_value));
        Ok(())
    }

    #[test]
    fn test_configured_route_schema_requires_supported_declared_draft() -> Result<()> {
        let directory = tempdir()?;
        let schema_path = directory.path().join("missing-draft.json");
        write_schema(&schema_path, json!({"type": "string"}))?;

        assert!(CompiledSchema::compile("missing-draft", &schema_path, &BTreeMap::new()).is_err());
        Ok(())
    }
}
