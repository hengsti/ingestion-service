/// Versioned route-bundle DTOs and the pure configuration loader.
pub mod routes;

use anyhow::{bail, Context, Result};
use secrecy::SecretString;
use std::env;
use std::fmt;
use std::path::PathBuf;

/// Selects which input transport the service consumes from.
///
/// Only `Mqtt` is implemented today. Adding a variant here (e.g. `Kafka`) will
/// force a compile error at every `match` on this type until it's handled —
/// this is intentional, acting as a guardrail for future transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputSourceKind {
    Mqtt,
}

impl InputSourceKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_lowercase().as_str() {
            "mqtt" => Ok(Self::Mqtt),
            other => {
                bail!("unsupported INPUT_SOURCE '{other}': only 'mqtt' is currently implemented")
            }
        }
    }
}

/// Selects which output sink the service persists ingested messages to.
///
/// Only `Influx` is implemented today. Adding a variant here (e.g. `Kafka`) will
/// force a compile error at every `match` on this type until it's handled —
/// this is intentional, acting as a guardrail for future sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSinkKind {
    Influx,
}

impl OutputSinkKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_lowercase().as_str() {
            "influx" => Ok(Self::Influx),
            other => {
                bail!("unsupported OUTPUT_SINK '{other}': only 'influx' is currently implemented")
            }
        }
    }
}

/// MQTT broker connection settings, populated only when `INPUT_SOURCE=mqtt`.
#[derive(Clone)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: String,
    pub clean_session: bool,
}

impl fmt::Debug for MqttSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // username/password are intentionally omitted to avoid leaking secrets in logs.
        f.debug_struct("MqttSourceConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("client_id", &self.client_id)
            .field("clean_session", &self.clean_session)
            .finish()
    }
}

/// InfluxDB connection settings, populated only when `OUTPUT_SINK=influx`.
#[derive(Clone)]
pub struct InfluxSinkConfig {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: SecretString,
}

impl fmt::Debug for InfluxSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InfluxSinkConfig")
            .field("url", &self.url)
            .field("org", &self.org)
            .field("bucket", &self.bucket)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// Runtime configuration loaded from environment variables.
#[derive(Clone)]
pub struct Config {
    pub input_source: InputSourceKind,
    /// Present when `input_source` is [`InputSourceKind::Mqtt`].
    pub mqtt: Option<MqttSourceConfig>,
    /// Path to the configured route bundle.
    pub routes_file: PathBuf,
    /// Publish-only destination for invalid messages.
    pub dlq_topic: String,
    pub output_sink: OutputSinkKind,
    /// Present when `output_sink` is [`OutputSinkKind::Influx`].
    pub influx: Option<InfluxSinkConfig>,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub wal_dir: PathBuf,
    pub wal_max_bytes: u64,
    pub wal_segment_bytes: u64,
    pub wal_queue_capacity: usize,
    pub quarantine_dir: PathBuf,
    pub quarantine_max_bytes: u64,
    pub input_queue_capacity: usize,
    pub metrics_bind: String,
    pub cache_ttl_ms: u64,
    pub cache_bind: String,
    pub cache_buffer: usize,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("input_source", &self.input_source)
            .field("mqtt", &self.mqtt)
            .field("routes_file", &self.routes_file)
            .field("dlq_topic", &self.dlq_topic)
            .field("output_sink", &self.output_sink)
            .field("influx", &self.influx)
            .field("batch_size", &self.batch_size)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("wal_dir", &self.wal_dir)
            .field("wal_max_bytes", &self.wal_max_bytes)
            .field("wal_segment_bytes", &self.wal_segment_bytes)
            .field("wal_queue_capacity", &self.wal_queue_capacity)
            .field("quarantine_dir", &self.quarantine_dir)
            .field("quarantine_max_bytes", &self.quarantine_max_bytes)
            .field("input_queue_capacity", &self.input_queue_capacity)
            .field("metrics_bind", &self.metrics_bind)
            .field("cache_ttl_ms", &self.cache_ttl_ms)
            .field("cache_bind", &self.cache_bind)
            .field("cache_buffer", &self.cache_buffer)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        if let Some(message) =
            routes::removed_route_environment_variables(env::vars().map(|(name, _)| name))
        {
            bail!("{message}");
        }
        let routes_file = required_routes_file(env_var("INGEST_ROUTES_FILE"))?;

        let input_source =
            InputSourceKind::parse(&env_var("INPUT_SOURCE").context("INPUT_SOURCE must be set")?)?;

        let (mqtt, dlq_topic) = match input_source {
            InputSourceKind::Mqtt => {
                let host =
                    env_var("MQTT_HOST").context("MQTT_HOST is required when INPUT_SOURCE=mqtt")?;
                let port = env_var("MQTT_PORT")
                    .context("MQTT_PORT must be set when INPUT_SOURCE=mqtt")?
                    .parse::<u16>()
                    .context("MQTT_PORT must be a u16")?;

                let username = env_var("MQTT_USERNAME");
                let password = env_var("MQTT_PASSWORD");

                let client_id = env_var("MQTT_CLIENT_ID")
                    .context("MQTT_CLIENT_ID must be set when INPUT_SOURCE=mqtt")?;
                let (clean_session, client_id) =
                    mqtt_session(env_var("MQTT_CLEAN_SESSION"), client_id)?;

                let dlq_topic = required_dlq_topic(env_var("MQTT_TOPIC_DLQ"))?;
                (
                    Some(MqttSourceConfig {
                        host,
                        port,
                        username,
                        password,
                        client_id,
                        clean_session,
                    }),
                    dlq_topic,
                )
            }
        };

        let output_sink =
            OutputSinkKind::parse(&env_var("OUTPUT_SINK").context("OUTPUT_SINK must be set")?)?;

        let influx = match output_sink {
            OutputSinkKind::Influx => {
                let url = env_var("INFLUX_URL")
                    .context("INFLUX_URL is required when OUTPUT_SINK=influx")?;
                let org = env_var("INFLUX_ORG")
                    .context("INFLUX_ORG is required when OUTPUT_SINK=influx")?;
                let bucket = env_var("INFLUX_BUCKET")
                    .context("INFLUX_BUCKET is required when OUTPUT_SINK=influx")?;
                let token = env_var("INFLUX_TOKEN")
                    .context("INFLUX_TOKEN is required when OUTPUT_SINK=influx")?;

                Some(InfluxSinkConfig {
                    url,
                    org,
                    bucket,
                    token: SecretString::new(token),
                })
            }
        };

        let batch_size = env_var("BATCH_SIZE")
            .context("BATCH_SIZE must be set")?
            .parse::<usize>()
            .context("BATCH_SIZE must be a valid usize")?;

        let flush_interval_ms = env_var("FLUSH_INTERVAL_MS")
            .context("FLUSH_INTERVAL_MS must be set")?
            .parse::<u64>()
            .context("FLUSH_INTERVAL_MS must be a valid u64")?;

        let wal_dir = PathBuf::from(env_var("WAL_DIR").context("WAL_DIR must be set")?);
        let wal_max_bytes = required_positive_u64(env_var("WAL_MAX_BYTES"), "WAL_MAX_BYTES")?;
        let quarantine_dir =
            PathBuf::from(env_var("QUARANTINE_DIR").context("QUARANTINE_DIR must be set")?);
        let quarantine_max_bytes =
            required_positive_u64(env_var("QUARANTINE_MAX_BYTES"), "QUARANTINE_MAX_BYTES")?;

        let wal_segment_bytes = match env_var("WAL_SEGMENT_BYTES") {
            Some(v) => v
                .parse::<u64>()
                .context("WAL_SEGMENT_BYTES must be a valid u64")?,
            None => 64 * 1024 * 1024,
        };
        crate::infrastructure::wal::capacity::validate_limit(
            "WAL_MAX_BYTES",
            wal_max_bytes,
            wal_segment_bytes,
        )?;
        crate::infrastructure::wal::capacity::validate_limit(
            "QUARANTINE_MAX_BYTES",
            quarantine_max_bytes,
            wal_segment_bytes,
        )?;

        let wal_queue_capacity = match env_var("WAL_QUEUE_CAPACITY") {
            Some(v) => v
                .parse::<usize>()
                .context("WAL_QUEUE_CAPACITY must be a valid usize")?,
            None => 16_384,
        };
        if wal_queue_capacity == 0 {
            bail!("WAL_QUEUE_CAPACITY must be greater than zero");
        }

        let input_queue_capacity = parse_input_queue_capacity(env_var("INPUT_QUEUE_CAPACITY"))?;

        let metrics_bind = env_var("METRICS_BIND").context("METRICS_BIND must be set")?;

        let cache_ttl_ms = env_var("CACHE_TTL_MS")
            .context("CACHE_TTL_MS must be set")?
            .parse::<u64>()
            .context("CACHE_TTL_MS must be a valid u64")?;

        let cache_bind = env_var("CACHE_BIND").context("CACHE_BIND must be set")?;

        let cache_buffer = env_var("CACHE_BUFFER")
            .context("CACHE_BUFFER must be set")?
            .parse::<usize>()
            .context("CACHE_BUFFER must be a valid usize")?;

        Ok(Self {
            input_source,
            mqtt,
            routes_file,
            dlq_topic,
            output_sink,
            influx,
            batch_size,
            flush_interval_ms,
            wal_dir,
            wal_max_bytes,
            wal_segment_bytes,
            wal_queue_capacity,
            quarantine_dir,
            quarantine_max_bytes,
            input_queue_capacity,
            metrics_bind,
            cache_ttl_ms,
            cache_bind,
            cache_buffer,
        })
    }
}

fn required_positive_u64(value: Option<String>, name: &str) -> Result<u64> {
    let value = value.with_context(|| format!("{name} must be set"))?;
    let value = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a valid u64"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(value)
}

fn required_routes_file(value: Option<String>) -> Result<PathBuf> {
    let value = value.context("INGEST_ROUTES_FILE must be set")?;
    let value = value.trim();
    if value.is_empty() {
        bail!("INGEST_ROUTES_FILE must not be empty");
    }
    Ok(PathBuf::from(value))
}

fn required_dlq_topic(value: Option<String>) -> Result<String> {
    let value = value.context("MQTT_TOPIC_DLQ must be set")?;
    if value.trim().is_empty() {
        bail!("MQTT_TOPIC_DLQ must not be empty");
    }
    Ok(value)
}

fn env_var(k: &str) -> Option<String> {
    env::var(k)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}
fn mqtt_session(clean_session: Option<String>, client_id: String) -> Result<(bool, String)> {
    let clean_session = match clean_session {
        Some(v) => v
            .parse::<bool>()
            .context("MQTT_CLEAN_SESSION must be a valid bool")?,
        None => false,
    };
    if clean_session {
        bail!("MQTT_CLEAN_SESSION=true is unsupported; durable MQTT intake requires a persistent session");
    }
    Ok((clean_session, client_id))
}

fn parse_input_queue_capacity(value: Option<String>) -> Result<usize> {
    let capacity = match value {
        Some(value) => value
            .parse::<usize>()
            .context("INPUT_QUEUE_CAPACITY must be a valid usize")?,
        None => 16_384,
    };
    if capacity == 0 {
        bail!("INPUT_QUEUE_CAPACITY must be greater than zero");
    }
    Ok(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_source_kind_parse_accepts_mqtt_case_insensitive() {
        assert_eq!(
            InputSourceKind::parse("mqtt").unwrap(),
            InputSourceKind::Mqtt
        );
        assert_eq!(
            InputSourceKind::parse("MQTT").unwrap(),
            InputSourceKind::Mqtt
        );
        assert_eq!(
            InputSourceKind::parse(" Mqtt ").unwrap(),
            InputSourceKind::Mqtt
        );
    }

    #[test]
    fn input_source_kind_parse_rejects_unknown_value() {
        let err = InputSourceKind::parse("kafka").unwrap_err();
        assert!(err.to_string().contains("unsupported INPUT_SOURCE 'kafka'"));
    }

    #[test]
    fn input_source_kind_parse_rejects_empty_value() {
        assert!(InputSourceKind::parse("").is_err());
    }

    #[test]
    fn output_source_kind_parse_accepts_influx_case_insensitive() {
        assert_eq!(
            OutputSinkKind::parse("influx").unwrap(),
            OutputSinkKind::Influx
        );
        assert_eq!(
            OutputSinkKind::parse("INFLUX").unwrap(),
            OutputSinkKind::Influx
        );
        assert_eq!(
            OutputSinkKind::parse(" Influx ").unwrap(),
            OutputSinkKind::Influx
        );
    }

    #[test]
    fn output_source_kind_parse_rejects_unknown_value() {
        let err = OutputSinkKind::parse("kafka").unwrap_err();
        assert!(err.to_string().contains("unsupported OUTPUT_SINK 'kafka'"));
    }

    #[test]
    fn output_source_kind_parse_rejects_empty_value() {
        assert!(OutputSinkKind::parse("").is_err());
    }

    #[test]
    fn mqtt_session_unset_defaults_to_persistent_with_stable_client_id() -> Result<()> {
        let (clean_session, client_id) = mqtt_session(None, "ingest".to_string())?;

        assert!(!clean_session);
        assert_eq!(client_id, "ingest");
        Ok(())
    }

    #[test]
    fn mqtt_session_false_uses_client_id_verbatim() -> Result<()> {
        let (clean_session, client_id) =
            mqtt_session(Some("false".to_string()), "ingest".to_string())?;

        assert!(!clean_session);
        assert_eq!(client_id, "ingest");
        Ok(())
    }

    #[test]
    fn mqtt_session_true_rejects_nonpersistent_mode() {
        let result = mqtt_session(Some("true".to_string()), "ingest".to_string());

        assert!(format!("{:#}", result.unwrap_err()).contains("MQTT_CLEAN_SESSION"));
    }

    #[test]
    fn mqtt_session_invalid_value_returns_error() {
        let result = mqtt_session(Some("nope".to_string()), "ingest".to_string());

        assert!(result.is_err());
    }

    #[test]
    fn input_queue_capacity_rejects_zero() {
        assert!(parse_input_queue_capacity(Some("0".to_string())).is_err());
    }

    #[test]
    fn input_queue_capacity_defaults_to_positive_capacity() -> Result<()> {
        let capacity = parse_input_queue_capacity(None)?;

        assert!(capacity > 0);
        Ok(())
    }

    #[test]
    fn test_config_rejects_removed_route_environment() {
        for name in [
            "MQTT_TOPIC_SENSOR",
            "MQTT_TOPIC_STATUS",
            "MQTT_TOPIC_ZIGBEE",
            "MQTT_TOPIC_OTHER_DLQ",
            "ENFORCE_TOPIC_DEVICE_MATCH",
        ] {
            let err = routes::removed_route_environment_variables([name])
                .expect("removed configuration must fail");

            assert!(err.contains(name));
            assert!(err.contains("INGEST_ROUTES_FILE"));
        }

        assert!(routes::removed_route_environment_variables(["MQTT_TOPIC_DLQ"]).is_none());
    }

    #[test]
    fn test_config_requires_nonempty_routes_file() {
        for value in [None, Some(String::new()), Some("  ".to_string())] {
            assert!(required_routes_file(value).is_err());
        }
    }

    #[test]
    fn test_config_requires_dlq_topic() {
        assert!(required_dlq_topic(None).is_err());
        assert!(required_dlq_topic(Some(String::new())).is_err());
    }

    #[test]
    fn test_wal_capacity_rejects_nonpositive_or_invalid_limits() {
        for name in ["WAL_MAX_BYTES", "QUARANTINE_MAX_BYTES"] {
            for value in [None, Some("0"), Some("-1"), Some("invalid")] {
                let error = required_positive_u64(value.map(str::to_owned), name)
                    .expect_err("invalid byte limit must fail");
                assert!(format!("{error:#}").contains(name));
            }
        }
    }

    #[test]
    fn test_wal_capacity_requires_explicit_journal_configuration() -> Result<()> {
        const CHILD: &str = "INGEST_TEST_MISSING_JOURNAL_SETTING";
        if let Ok(missing) = env::var(CHILD) {
            let error = Config::from_env()
                .err()
                .context("missing journal configuration was accepted")?;
            assert!(format!("{error:#}").contains(&missing));
            return Ok(());
        }

        let dir = tempfile::tempdir()?;
        for missing in [
            "WAL_MAX_BYTES",
            "QUARANTINE_DIR",
            "QUARANTINE_MAX_BYTES",
            "WAL_QUEUE_CAPACITY",
        ] {
            let mut command = std::process::Command::new(env::current_exe()?);
            command
                .args([
                    "--exact",
                    "config::tests::test_wal_capacity_requires_explicit_journal_configuration",
                ])
                .env_clear()
                .env(CHILD, missing)
                .env("INGEST_ROUTES_FILE", "routes.json")
                .env("INPUT_SOURCE", "mqtt")
                .env("MQTT_HOST", "localhost")
                .env("MQTT_PORT", "1883")
                .env("MQTT_CLIENT_ID", "test")
                .env("MQTT_TOPIC_DLQ", "test/dlq")
                .env("OUTPUT_SINK", "influx")
                .env("INFLUX_URL", "http://localhost:8086")
                .env("INFLUX_ORG", "test")
                .env("INFLUX_BUCKET", "test")
                .env("INFLUX_TOKEN", "test")
                .env("BATCH_SIZE", "1")
                .env("FLUSH_INTERVAL_MS", "1")
                .env("WAL_DIR", dir.path().join("wal"))
                .env("WAL_MAX_BYTES", "268435456")
                .env("QUARANTINE_DIR", dir.path().join("quarantine"))
                .env("QUARANTINE_MAX_BYTES", "268435456")
                .env("METRICS_BIND", "127.0.0.1:9090")
                .env("CACHE_TTL_MS", "1")
                .env("CACHE_BIND", "127.0.0.1:8080")
                .env("CACHE_BUFFER", "1");
            if missing == "WAL_QUEUE_CAPACITY" {
                command.env(missing, "0");
            } else {
                command.env_remove(missing);
            }
            if let Some(system_root) = env::var_os("SystemRoot") {
                command.env("SystemRoot", system_root);
            }

            let output = command.output()?;
            assert!(
                output.status.success(),
                "configuration probe for {missing} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(!dir.path().join("wal").exists());
        assert!(!dir.path().join("quarantine").exists());
        Ok(())
    }
}
