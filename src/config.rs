use anyhow::{bail, Context, Result};
use secrecy::SecretString;
use std::collections::HashMap;
use std::env;
use std::fmt;

#[derive(Clone)]
pub struct Config {
    // MQTT
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_username: Option<String>,
    pub mqtt_password: Option<String>,
    pub mqtt_client_id: String,
    pub mqtt_topics: HashMap<String, String>, // {"TOPIC NAME": "TOPIC STRING"}, e.g. {"MQTT_TOPIC_SENSOR": "home/sensor/+"}

    // InfluxDB v2 Write API
    pub influx_url: String, // e.g. http://influxdb:8086
    pub influx_org: String,
    pub influx_bucket: String,
    pub influx_token: SecretString,

    // batching
    pub batch_size: usize,
    pub flush_interval_ms: u64,

    // optional checks
    pub enforce_topic_device_match: bool,

    // Metrics
    pub metrics_bind: String,

    // Cache
    pub cache_ttl_ms: u64,
    pub cache_bind: String,
    pub cache_buffer: usize,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("mqtt_host", &self.mqtt_host)
            .field("mqtt_port", &self.mqtt_port)
            .field("mqtt_client_id", &self.mqtt_client_id)
            .field("mqtt_topics", &self.mqtt_topics)
            .field("influx_url", &self.influx_url)
            .field("influx_org", &self.influx_org)
            .field("influx_bucket", &self.influx_bucket)
            .field("influx_token", &"[REDACTED]")
            .field("batch_size", &self.batch_size)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field(
                "enforce_topic_device_match",
                &self.enforce_topic_device_match,
            )
            .field("metrics_bind", &self.metrics_bind)
            .field("cache_ttl_ms", &self.cache_ttl_ms)
            .field("cache_bind", &self.cache_bind)
            .field("cache_buffer", &self.cache_buffer)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let mqtt_host = env_var("MQTT_HOST").context("MQTT_HOST is required")?;
        let mqtt_port = env_var("MQTT_PORT")
            .context("MQTT_PORT must be set")?
            .parse::<u16>()
            .context("MQTT_PORT must be a u16")?;

        let mqtt_username = env_var("MQTT_USERNAME");
        let mqtt_password = env_var("MQTT_PASSWORD");

        let mut mqtt_client_id = env_var("MQTT_CLIENT_ID").context("MQTT_CLIENT_ID must be set")?;
        mqtt_client_id.push_str(&format!("-{}", chrono::Utc::now().timestamp()));

        let mut mqtt_topics = HashMap::new();
        for (k, v) in env::vars().filter(|(k, _)| k.starts_with("MQTT_TOPIC_")) {
            mqtt_topics.insert(k, v);
        }

        if mqtt_topics.is_empty() {
            bail!("At least one MQTT_TOPIC_<NAME> environment variable must be set");
        }

        let influx_url = env_var("INFLUX_URL").context("INFLUX_URL must be set")?;
        if !influx_url.starts_with("http://") && !influx_url.starts_with("https://") {
            bail!(
                "INFLUX_URL must start with http:// or https://, got: {}",
                influx_url
            );
        }
        let influx_org = env_var("INFLUX_ORG").context("INFLUX_ORG must be set")?;
        let influx_bucket = env_var("INFLUX_BUCKET").context("INFLUX_BUCKET must be set")?;
        let influx_token = SecretString::new(
            env_var("INFLUX_TOKEN")
                .context("INFLUX_TOKEN must be set (for InfluxDB v2 write API)")?,
        );

        let batch_size = env_var("BATCH_SIZE")
            .context("BATCH_SIZE must be set")?
            .parse::<usize>()
            .context("BATCH_SIZE must be a valid usize")?;

        let flush_interval_ms = env_var("FLUSH_INTERVAL_MS")
            .context("FLUSH_INTERVAL_MS must be set")?
            .parse::<u64>()
            .context("FLUSH_INTERVAL_MS must be a valid u64")?;

        let enforce_topic_device_match = env_var("ENFORCE_TOPIC_DEVICE_MATCH")
            .context("ENFORCE_TOPIC_DEVICE_MATCH must be set")?
            .parse::<bool>()
            .context("ENFORCE_TOPIC_DEVICE_MATCH must be a valid bool")?;

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
            mqtt_host,
            mqtt_port,
            mqtt_username,
            mqtt_password,
            mqtt_client_id,
            mqtt_topics,
            influx_url,
            influx_org,
            influx_bucket,
            influx_token,
            batch_size,
            flush_interval_ms,
            enforce_topic_device_match,
            metrics_bind,
            cache_ttl_ms,
            cache_bind,
            cache_buffer,
        })
    }
}

fn env_var(k: &str) -> Option<String> {
    env::var(k)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}
