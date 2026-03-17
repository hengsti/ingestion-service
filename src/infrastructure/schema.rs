use anyhow::{Context, Result, bail};
use jsonschema::Validator as JsonSchemaValidator;
use serde_json::Value;

/// Shared JSON-Schema wrapper
pub struct JsonSchema {
    schema: JsonSchemaValidator,
}

impl JsonSchema {
    pub fn new(schema: &str) -> Result<Self> {
        let schema_json: Value =
            serde_json::from_str(schema).context("Failed to parse embedded JSON schema")?;

        let compiled = jsonschema::draft7::options()
            .build(&schema_json)
            .context("Failed to compile JSON schema (draft7)")?;

        Ok(Self { schema: compiled })
    }

    pub fn validate(&self, instance: &Value) -> Result<()> {
        if self.schema.is_valid(instance) {
            return Ok(());
        }

        let mut msgs = Vec::new();
        // Collect up to 10 errors
        for err in self.schema.iter_errors(instance).take(10) {
            msgs.push(format!("{}", err));
        }
        bail!("schema validation failed: {}", msgs.join(" | "))
    }
}
