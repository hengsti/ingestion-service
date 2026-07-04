use anyhow::{bail, Context, Result};
use jsonschema::{Resource, Validator as JsonSchemaValidator};
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
