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
    pub wal_segment_bytes: u64,
    pub wal_queue_capacity: usize,
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
            .field("wal_segment_bytes", &self.wal_segment_bytes)
            .field("wal_queue_capacity", &self.wal_queue_capacity)
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

        let wal_segment_bytes = match env_var("WAL_SEGMENT_BYTES") {
            Some(v) => v
                .parse::<u64>()
                .context("WAL_SEGMENT_BYTES must be a valid u64")?,
            None => 64 * 1024 * 1024,
        };

        let wal_queue_capacity = match env_var("WAL_QUEUE_CAPACITY") {
            Some(v) => v
                .parse::<usize>()
                .context("WAL_QUEUE_CAPACITY must be a valid usize")?,
            None => 16_384,
        };

        let input_queue_capacity = match env_var("INPUT_QUEUE_CAPACITY") {
            Some(v) => v
                .parse::<usize>()
                .context("INPUT_QUEUE_CAPACITY must be a valid usize")?,
            None => 16_384,
        };

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
            wal_segment_bytes,
            wal_queue_capacity,
            input_queue_capacity,
            metrics_bind,
            cache_ttl_ms,
            cache_bind,
            cache_buffer,
        })
    }
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
fn mqtt_session(clean_session: Option<String>, mut client_id: String) -> Result<(bool, String)> {
    let clean_session = match clean_session {
        Some(v) => v
            .parse::<bool>()
            .context("MQTT_CLEAN_SESSION must be a valid bool")?,
        None => true,
    };
    if clean_session {
        client_id.push_str(&format!("-{}", chrono::Utc::now().timestamp()));
    }
    Ok((clean_session, client_id))
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
    fn mqtt_session_unset_defaults_to_clean_with_suffixed_client_id() -> Result<()> {
        let (clean_session, client_id) = mqtt_session(None, "ingest".to_string())?;

        assert!(clean_session);
        let suffix = client_id
            .strip_prefix("ingest-")
            .context("client id must be suffixed")?;
        assert!(suffix.parse::<i64>().is_ok());
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
    fn mqtt_session_invalid_value_returns_error() {
        let result = mqtt_session(Some("nope".to_string()), "ingest".to_string());

        assert!(result.is_err());
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
}
