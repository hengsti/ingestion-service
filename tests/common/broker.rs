use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Outgoing, QoS};
use serde_json::{json, Value};
use tempfile::TempDir;

use smarthome_ingest::infrastructure::wal::{codec, segment, types::WalEvent};

const TIMEOUT: Duration = Duration::from_secs(60);
const DIAGNOSTIC_BYTES: u64 = 64 * 1024;
const TOKEN: &str = "synthetic-integration-token";
const USER: &str = "ingest-test";
const PASSWORD: &str = "synthetic-test-password";

/// Checks the recovery set without assuming exactly-once delivery or Influx row retention.
///
/// # Errors
/// Returns an error for missing or unexpected publication IDs.
pub fn assert_publication_ids(
    expected: &BTreeSet<String>,
    output: &BTreeSet<String>,
    quarantine: &BTreeSet<String>,
) -> Result<()> {
    let actual: BTreeSet<_> = output.union(quarantine).cloned().collect();
    ensure!(
        &actual == expected,
        "publication oracle: missing {:?}, unexpected {:?}",
        expected.difference(&actual).collect::<Vec<_>>(),
        actual.difference(expected).collect::<Vec<_>>()
    );
    Ok(())
}

/// Real-container process driver. It only connects to a dedicated `ingest-it*` project.
pub struct Harness {
    project: String,
    pub dir: PathBuf,
    temp: Option<TempDir>,
    child: Option<Child>,
    client_id: String,
    mqtt_port: u16,
    api_url: String,
    influx_url: String,
    bucket: String,
    cache_port: u16,
    metrics_port: u16,
    http: reqwest::Client,
    success: bool,
    // Reservations reduce port races until immediately before the child binds.
    listeners: Vec<TcpListener>,
}

fn compose_command(project: &str) -> Result<Command> {
    ensure!(
        project.starts_with("ingest-it")
            && project.len() <= 64
            && project
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "integration project must be a dedicated ingest-it* name"
    );
    let mut command = Command::new("docker");
    command.args([
        "compose",
        "-f",
        "tests/compose.integration.yaml",
        "-p",
        project,
    ]);
    Ok(command)
}

fn compose(project: &str, args: &[&str]) -> Result<String> {
    let mut command = compose_command(project)?;
    command.args(args);
    run_command(command)
}

/// Executes a tracked test subprocess with bounded output and a deadline.
///
/// # Errors
/// Returns an error on spawn/exit/I/O failure or a 150-second deadline.
pub fn run_command(mut command: Command) -> Result<String> {
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    let mut child = command
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?))
        .spawn()
        .context("start test subprocess")?;
    let deadline = Instant::now() + Duration::from_secs(150);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("test subprocess deadline exceeded");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stderr_start = stderr.metadata()?.len().saturating_sub(DIAGNOSTIC_BYTES);
    stderr.seek(SeekFrom::Start(stderr_start))?;
    let mut error = String::new();
    stderr.take(DIAGNOSTIC_BYTES).read_to_string(&mut error)?;
    ensure!(status.success(), "test subprocess failed: {error}");
    ensure!(
        stdout.metadata()?.len() <= DIAGNOSTIC_BYTES,
        "test subprocess output exceeded bound"
    );
    stdout.seek(SeekFrom::Start(0))?;
    let mut output = String::new();
    stdout.read_to_string(&mut output)?;
    Ok(output)
}

fn service_port(project: &str, service: &str, port: &str) -> Result<u16> {
    let address = compose(project, &["port", service, port])?;
    ensure!(
        address.trim().starts_with("127.0.0.1:"),
        "test port must be loopback-only"
    );
    address
        .trim()
        .rsplit(':')
        .next()
        .context("Compose port")?
        .parse()
        .context("parse port")
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_json(path: impl AsRef<Path>) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

async fn bounded_text(mut response: reqwest::Response) -> Result<String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= DIAGNOSTIC_BYTES as usize,
            "HTTP evidence exceeded bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(bytes)?)
}

/// Reuses the shipped firmware bundle and existing Zigbee fixture, adding only the test oracle.
///
/// # Errors
/// Returns an error on fixture I/O or malformed JSON.
pub fn write_routes(dir: &Path) -> Result<()> {
    let mut bundle = read_json(crate::common::write_v1_shipped_bundle(dir)?)?;
    for route in bundle["routes"].as_array_mut().context("firmware routes")? {
        // Each scenario owns its destination through INFLUX_BUCKET.
        route
            .as_object_mut()
            .context("route object")?
            .remove("sink_target");
        route["fields"]["test_id"] = json!({"pointer": "/test_id", "type": "string"});
    }
    let mut zigbee = read_json(
        root()
            .join("tests")
            .join("fixtures")
            .join("routes")
            .join("zigbee-climate.json"),
    )?;
    let mut schema = read_json(
        root()
            .join("tests")
            .join("fixtures")
            .join("routes")
            .join("zigbee-climate.schema.json"),
    )?;
    schema["properties"]["test_id"] = json!({"type": "string", "minLength": 1});
    schema["properties"]["time_ms"] = json!({"type": "integer", "minimum": 1});
    fs::write(dir.join("zigbee.schema.json"), serde_json::to_vec(&schema)?)?;
    let route = &mut zigbee["routes"][0];
    route["schema"] = json!("zigbee.schema.json");
    route["fields"]["test_id"] = json!({"pointer": "/test_id", "type": "string"});
    route["timestamp"] = json!({"source": "payload", "pointer": "/time_ms", "format": "epoch_ms"});
    bundle["routes"]
        .as_array_mut()
        .context("routes")?
        .push(route.clone());
    fs::write(dir.join("routes.json"), serde_json::to_vec_pretty(&bundle)?)?;
    Ok(())
}

/// Synthetic publications have distinct point identities/timestamps, not per-event tags.
///
/// # Errors
/// Returns an error if the shared synthetic firmware payload cannot be decoded.
pub fn sensor_payload(device: &str, sequence: i64) -> Result<(String, Vec<u8>)> {
    payload_with_id(
        format!("smarthome/{device}/bme680"),
        crate::common::valid_sensor_payload(device),
        device,
        sequence,
    )
}

/// Builds a status publication using the shared firmware fixture.
///
/// # Errors
/// Returns an error if the shared synthetic firmware payload cannot be decoded.
pub fn status_payload(device: &str, sequence: i64) -> Result<(String, Vec<u8>)> {
    payload_with_id(
        format!("smarthome/{device}/status"),
        crate::common::valid_status_payload(device),
        device,
        sequence,
    )
}

/// Builds a synthetic Zigbee publication without an adapter.
///
/// # Errors
/// Returns an error if the payload cannot be encoded.
pub fn zigbee_payload(device: &str, sequence: i64) -> Result<(String, Vec<u8>)> {
    payload_with_id(
        "zigbee2mqtt/synthetic_lab_climate".into(),
        serde_json::to_vec(&json!({
            "device_id": device, "temperature": 22.5, "humidity": 45.0
        }))?,
        device,
        sequence,
    )
}

fn payload_with_id(
    topic: String,
    payload: Vec<u8>,
    device: &str,
    sequence: i64,
) -> Result<(String, Vec<u8>)> {
    ensure!(
        sequence > 0 && sequence < 100_000,
        "bounded positive test sequence required"
    );
    let mut payload: Value = serde_json::from_slice(&payload)?;
    payload["test_id"] = json!(format!("{device}-{sequence}"));
    payload["time_ms"] = json!(1_700_000_000_000_i64 + sequence);
    Ok((topic, serde_json::to_vec(&payload)?))
}

impl Harness {
    /// Returns this scenario's exclusively tracked persistent MQTT client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns a synthetic identity unique to this scenario and process invocation.
    pub fn device_id(&self, label: &str) -> String {
        format!("{label}-{}", self.client_id)
    }

    /// Attaches to the explicitly selected test project and records actual broker configuration.
    ///
    /// # Errors
    /// Returns an error if services are unavailable, not isolated, or lack durable sessions.
    pub async fn connect() -> Result<Self> {
        let project = std::env::var("INGEST_IT_PROJECT").unwrap_or_else(|_| "ingest-it".into());
        let mqtt_port = service_port(&project, "emqx", "1883")?;
        let api_port = service_port(&project, "emqx", "18083")?;
        let influx_port = service_port(&project, "influxdb", "8086")?;
        let artifacts = root().join("target").join("broker-integration");
        fs::create_dir_all(&artifacts)?;
        let temp = tempfile::Builder::new()
            .prefix("ingest-it-")
            .tempdir_in(artifacts)?;
        let dir = temp.path().to_path_buf();
        write_routes(&dir)?;
        let listeners = vec![
            TcpListener::bind(("127.0.0.1", 0))?,
            TcpListener::bind(("127.0.0.1", 0))?,
        ];
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let harness = Self {
            project,
            dir,
            temp: Some(temp),
            child: None,
            client_id: format!("ingest-it-{}-{nonce}", std::process::id()),
            mqtt_port,
            api_url: format!("http://127.0.0.1:{api_port}"),
            influx_url: format!("http://127.0.0.1:{influx_port}"),
            bucket: USER.to_owned(),
            cache_port: listeners[0].local_addr()?.port(),
            metrics_port: listeners[1].local_addr()?.port(),
            listeners,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
            success: false,
        };
        let nodes = harness.api("/nodes").await?;
        ensure!(nodes.contains("6.3.1"), "unexpected deployed EMQX version");
        let config = harness.api("/configs").await?;
        let durable = config
            .split("durable_sessions {")
            .nth(1)
            .context("durable session configuration")?;
        ensure!(
            durable
                .split('}')
                .next()
                .context("durable settings")?
                .contains("enable = true"),
            "pinned EMQX edition/license has not enabled durable sessions; harness blocked"
        );
        let mqtt = config
            .split("\nmqtt {")
            .nth(1)
            .context("MQTT settings")?
            .split("\n}")
            .next()
            .context("MQTT settings end")?;
        let durable = durable
            .split("\n}")
            .next()
            .context("durable settings end")?;
        fs::write(
            harness.dir.join("broker-settings.txt"),
            format!("{nodes}\ndurable_sessions {{{durable}\n}}\nmqtt {{{mqtt}\n}}\n"),
        )?;
        let response = harness
            .http
            .post(format!(
                "{}/api/v5/authentication/password_based:built_in_database/users",
                harness.api_url
            ))
            .basic_auth("ingest-it-api", Some("synthetic-api-secret"))
            .body(json!({"user_id": USER, "password": PASSWORD, "is_superuser": true}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        ensure!(
            response.status().is_success() || response.status().as_u16() == 409,
            "create synthetic MQTT user failed: {}",
            response.status()
        );
        harness
            .http
            .get(format!("{}/health", harness.influx_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(harness)
    }

    /// Starts the compiled debug test binary with disposable journal directories.
    ///
    /// # Errors
    /// Returns an error if another tracked child exists or the binary cannot start.
    pub fn start(&mut self, barrier: Option<&str>) -> Result<()> {
        self.start_with_env(barrier, &[])
    }

    /// Starts the same test binary with scenario-specific synthetic settings.
    ///
    /// # Errors
    /// Returns an error if another tracked child exists or the binary cannot start.
    pub fn start_with_env(
        &mut self,
        barrier: Option<&str>,
        overrides: &[(&str, &str)],
    ) -> Result<()> {
        ensure!(self.child.is_none(), "ingest child already running");
        let shutdown = self.dir.join("shutdown.request");
        if shutdown.exists() {
            fs::remove_file(shutdown)?;
        }
        for name in [
            "before_append",
            "after_sync",
            "before_sink",
            "before_read",
            "before_quarantine",
            "after_quarantine",
            "ack_queued",
            "after_dispatch",
            "wal_write",
            "wal_sync",
            "quarantine_write",
            "quarantine_sync",
        ] {
            for suffix in ["reached", "release"] {
                let path = self.dir.join(format!("{name}.{suffix}"));
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.dir.join("ingest.log"))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_smarthome-ingest"));
        // Do not inherit production secrets or removed MQTT topic variables.
        let inherited: Vec<_> = std::env::vars_os()
            .filter(|(key, _)| {
                let key = key.to_string_lossy();
                matches!(
                    key.to_ascii_uppercase().as_str(),
                    "PATH" | "SYSTEMROOT" | "WINDIR" | "TEMP" | "TMP" | "HOME"
                )
            })
            .collect();
        command
            .env_clear()
            .envs(inherited)
            .envs([
                ("RUST_LOG", "info"),
                ("INPUT_SOURCE", "mqtt"),
                ("MQTT_HOST", "127.0.0.1"),
                ("MQTT_USERNAME", USER),
                ("MQTT_PASSWORD", PASSWORD),
                ("MQTT_CLEAN_SESSION", "false"),
                ("MQTT_TOPIC_DLQ", "ingest-it/dlq"),
                ("OUTPUT_SINK", "influx"),
                ("INFLUX_ORG", USER),
                ("INFLUX_BUCKET", USER),
                ("INFLUX_TOKEN", TOKEN),
                ("BATCH_SIZE", "1"),
                ("FLUSH_INTERVAL_MS", "50"),
                ("WAL_MAX_BYTES", "16777216"),
                ("QUARANTINE_MAX_BYTES", "16777216"),
                ("WAL_SEGMENT_BYTES", "1048576"),
                ("WAL_QUEUE_CAPACITY", "16"),
                ("INPUT_QUEUE_CAPACITY", "16"),
                ("CACHE_TTL_MS", "60000"),
                ("CACHE_BUFFER", "32"),
            ])
            .env("MQTT_PORT", self.mqtt_port.to_string())
            .env("MQTT_CLIENT_ID", &self.client_id)
            .env("INFLUX_URL", &self.influx_url)
            .env("INFLUX_BUCKET", &self.bucket)
            .env("INGEST_ROUTES_FILE", self.dir.join("routes.json"))
            .env("WAL_DIR", self.dir.join("wal"))
            .env("QUARANTINE_DIR", self.dir.join("quarantine"))
            .env("CACHE_BIND", format!("127.0.0.1:{}", self.cache_port))
            .env("METRICS_BIND", format!("127.0.0.1:{}", self.metrics_port))
            .env("INGEST_TEST_HOOK_DIR", &self.dir)
            .env("INGEST_TEST_BARRIER", barrier.unwrap_or(""))
            .envs(overrides.iter().copied())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        self.listeners.clear();
        self.child = Some(command.spawn().context("start test ingest binary")?);
        Ok(())
    }

    /// Terminates and reaps only this driver's tracked ingest process (a process-crash probe).
    ///
    /// # Errors
    /// Returns an error if process termination or reaping fails.
    pub fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
        }
        Ok(())
    }

    /// Signals the debug-only file hook for the ordinary service shutdown path.
    ///
    /// # Errors
    /// Returns an error on marker I/O.
    pub fn request_shutdown(&self) -> Result<()> {
        fs::write(self.dir.join("shutdown.request"), [])?;
        Ok(())
    }

    /// Requires a clean exit within 15 seconds instead of killing a deadlocked process.
    ///
    /// # Errors
    /// Returns an error on unsuccessful exit, missing child, or timeout.
    pub async fn wait_exit(&mut self) -> Result<()> {
        let child = self.child.as_mut().context("tracked ingest process")?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = child.try_wait()? {
                ensure!(status.success(), "shutdown exited unsuccessfully");
                self.child = None;
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "service shutdown exceeded 15 seconds"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits for a specific operational event in bounded process logs.
    ///
    /// # Errors
    /// Returns an error on log I/O or timeout.
    pub async fn wait_log(&self, event: &str) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let mut log = File::open(self.dir.join("ingest.log"))?;
            log.seek(SeekFrom::Start(
                log.metadata()?.len().saturating_sub(DIAGNOSTIC_BYTES),
            ))?;
            let mut tail = String::new();
            log.take(DIAGNOSTIC_BYTES).read_to_string(&mut tail)?;
            if tail.contains(event) {
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "log event {event} timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Polls actual intake readiness rather than waiting a fixed startup interval.
    ///
    /// # Errors
    /// Returns an error on timeout or early child exit.
    pub async fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(child) = self.child.as_mut() {
                ensure!(
                    child.try_wait()?.is_none(),
                    "test ingest exited before readiness"
                );
            }
            if let Ok(response) = self
                .http
                .get(format!("http://127.0.0.1:{}/readyz", self.cache_port))
                .send()
                .await
            {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            ensure!(Instant::now() < deadline, "ingest readiness timed out");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Observes unavailable intake or process exit after a storage fault.
    ///
    /// # Errors
    /// Returns an error if the live process keeps serving ready status.
    pub async fn wait_unready(&mut self) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(child) = self.child.as_mut() {
                if child.try_wait()?.is_some() {
                    return Ok(());
                }
            }
            if let Ok(response) = self
                .http
                .get(format!("http://127.0.0.1:{}/readyz", self.cache_port))
                .send()
                .await
            {
                if response.status().as_u16() == 503 {
                    return Ok(());
                }
            }
            ensure!(Instant::now() < deadline, "intake stayed ready after fault");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Publishes one QoS 1 message and observes its matching publisher PUBACK from EMQX.
    ///
    /// # Errors
    /// Returns an error on MQTT failure or bounded receipt timeout.
    pub async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        self.publish_with(topic, payload, QoS::AtLeastOnce, false)
            .await
    }

    /// Publishes with explicit delivery/retained semantics; QoS 0 observes send, not PUBACK.
    ///
    /// # Errors
    /// Returns an error on MQTT failure or deadline.
    pub async fn publish_with(
        &self,
        topic: &str,
        payload: Vec<u8>,
        qos: QoS,
        retained: bool,
    ) -> Result<()> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut options =
            MqttOptions::new(format!("it-publisher-{nonce}"), "127.0.0.1", self.mqtt_port);
        options.set_credentials(USER, PASSWORD);
        options.set_max_packet_size(128 * 1024, 128 * 1024);
        let (client, mut eventloop) = AsyncClient::new(options, 4);
        client.publish(topic, qos, retained, payload).await?;
        tokio::time::timeout(TIMEOUT, async {
            let mut packet_id = None;
            loop {
                match eventloop.poll().await? {
                    Event::Outgoing(Outgoing::Publish(0)) if qos == QoS::AtMostOnce => {
                        return Ok::<_, anyhow::Error>(())
                    }
                    Event::Outgoing(Outgoing::Publish(id)) => packet_id = Some(id),
                    Event::Incoming(Incoming::PubAck(ack)) if packet_id == Some(ack.pkid) => {
                        return Ok::<_, anyhow::Error>(())
                    }
                    _ => {}
                }
            }
        })
        .await
        .context("publisher PUBACK timeout")??;
        Ok(())
    }

    /// Pairs the source's post-flush PUBACK trace with actual broker inflight state.
    ///
    /// # Errors
    /// Returns an error if the trace or broker acknowledgement state never agrees.
    pub async fn wait_subscriber_ack(&self, count: usize) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let trace = fs::read_to_string(self.dir.join("source.trace")).unwrap_or_default();
            if trace
                .lines()
                .filter(|line| line.starts_with("puback "))
                .count()
                >= count
            {
                let state: Value = serde_json::from_str(
                    &self.api(&format!("/clients/{}", self.client_id)).await?,
                )?;
                fs::write(
                    self.dir.join("broker-client.json"),
                    serde_json::to_vec_pretty(&state)?,
                )?;
                ensure!(
                    state["durable"] == true && state["is_persistent"] == true,
                    "broker session is not actually durable; pinned edition/license cannot prove restart"
                );
                ensure!(
                    state["expiry_interval"] == 3600 && state["inflight_max"] == 16,
                    "unexpected broker session capacity/expiry"
                );
                ensure!(
                    state["send_msg.dropped"].as_u64() == Some(0),
                    "broker dropped test messages"
                );
                if state["inflight_cnt"].as_u64() == Some(0) {
                    return Ok(());
                }
            }
            ensure!(
                Instant::now() < deadline,
                "subscriber PUBACK/broker state timed out"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Queries fields by scalar test ID, not by row counts or high-cardinality event tags.
    ///
    /// # Errors
    /// Returns an error on query failure or missing point timeout.
    pub async fn query_fields(&self, test_id: &str) -> Result<String> {
        ensure!(
            test_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "unsafe test ID"
        );
        let query = format!(
            "from(bucket: \"{}\") |> range(start: 0) |> pivot(rowKey:[\"_time\"], columnKey:[\"_field\"], valueColumn:\"_value\") |> filter(fn:(r) => r.test_id == \"{test_id}\")", self.bucket
        );
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let response = bounded_text(
                self.http
                    .post(format!("{}/api/v2/query?org={USER}", self.influx_url))
                    .header("Authorization", format!("Token {TOKEN}"))
                    .header("Content-Type", "application/json")
                    .body(
                        json!({
                            "query": query,
                            "dialect": {"annotations": ["datatype", "group", "default"]}
                        })
                        .to_string(),
                    )
                    .send()
                    .await?
                    .error_for_status()?,
            )
            .await?;
            if response.contains(test_id) {
                fs::write(self.dir.join(format!("{test_id}.csv")), &response)?;
                return Ok(response);
            }
            ensure!(
                Instant::now() < deadline,
                "Influx query timed out for {test_id}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Queries every test-ID field for one synthetic device, including unexpected IDs.
    ///
    /// # Errors
    /// Returns an error on HTTP/CSV failure.
    pub async fn output_ids(&self, device: &str) -> Result<BTreeSet<String>> {
        ensure!(
            device
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "unsafe device ID"
        );
        let query = format!(
                    "from(bucket: \"{}\") |> range(start: 0) |> filter(fn:(r) => r._field == \"test_id\" and r.device_id == \"{device}\") |> keep(columns:[\"_value\"])", self.bucket
                );
        let csv = bounded_text(
            self.http
                .post(format!("{}/api/v2/query?org={USER}", self.influx_url))
                .header("Authorization", format!("Token {TOKEN}"))
                .header("Content-Type", "application/vnd.flux")
                .body(query)
                .send()
                .await?
                .error_for_status()?,
        )
        .await?;
        fs::write(self.dir.join(format!("{device}-ids.csv")), &csv)?;
        let mut ids = BTreeSet::new();
        for line in csv
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let value = line.rsplit(',').next().context("test-ID CSV value")?;
            if value == "_value" {
                continue;
            }
            ensure!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
                "unexpected test-ID CSV"
            );
            ids.insert(value.to_owned());
        }
        Ok(ids)
    }

    /// Inspects complete journal frames at a barrier or after stopping the tracked process.
    ///
    /// # Errors
    /// Returns an error on I/O, corruption or excessive evidence.
    pub fn journal_records(&self, journal: &str) -> Result<Vec<WalEvent>> {
        ensure!(
            matches!(journal, "wal" | "quarantine"),
            "unknown test journal"
        );
        let dir = self.dir.join(journal);
        let mut events = Vec::new();
        let mut scratch = Vec::new();
        for id in segment::list_segments(&dir)? {
            let mut file = File::open(segment::segment_path(&dir, id))?;
            while let Some((event, _)) = codec::decode_into(&mut file, &mut scratch)? {
                ensure!(events.len() < 1024, "journal evidence record bound");
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Extracts scalar IDs from JSON rejection fixtures without using rejection counts.
    ///
    /// # Errors
    /// Returns an error on journal/envelope decoding or missing test IDs.
    pub fn quarantine_ids(&self) -> Result<BTreeSet<String>> {
        let mut ids = BTreeSet::new();
        for record in self.journal_records("quarantine")? {
            let rejection: Value = serde_json::from_str(&record.payload)?;
            let raw: Vec<u8> = serde_json::from_value(rejection["raw_payload"].clone())?;
            let payload: Value = serde_json::from_slice(&raw)?;
            ids.insert(
                payload["test_id"]
                    .as_str()
                    .context("quarantine test ID")?
                    .to_owned(),
            );
        }
        Ok(ids)
    }

    /// Waits for all expected rejection IDs, then confirms the subscriber ACKs with EMQX.
    ///
    /// # Errors
    /// Returns an error on timeout or unexpected IDs.
    pub async fn wait_quarantine_ids(&self, expected: &BTreeSet<String>) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Ok(ids) = self.quarantine_ids() {
                ensure!(ids.is_subset(expected), "unexpected rejection IDs");
                if &ids == expected {
                    return Ok(());
                }
            }
            ensure!(
                Instant::now() < deadline,
                "quarantine ID recovery timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Reads the bounded test-build source trace.
    ///
    /// # Errors
    /// Returns an error on trace I/O.
    pub fn trace(&self) -> Result<String> {
        Ok(fs::read_to_string(self.dir.join("source.trace"))?)
    }

    /// Waits for a bounded source trace event without fixed crash timing.
    ///
    /// # Errors
    /// Returns an error on timeout or trace I/O.
    pub async fn wait_trace(&self, prefix: &str) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if self.trace()?.lines().any(|line| line.starts_with(prefix)) {
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "source trace {prefix} timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Disconnects only this driver's subscriber without deleting its persistent session.
    ///
    /// # Errors
    /// Returns an error on MQTT takeover failure or timeout.
    pub async fn disconnect(&self) -> Result<()> {
        // Management DELETE also destroys the session; MQTT takeover preserves it.
        let mut options = MqttOptions::new(&self.client_id, "127.0.0.1", self.mqtt_port);
        options.set_credentials(USER, PASSWORD);
        options.set_clean_session(false);
        options.set_manual_acks(true);
        let (_client, mut eventloop) = AsyncClient::new(options, 4);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Event::Incoming(Incoming::ConnAck(connack)) = eventloop.poll().await? {
                    ensure!(
                        connack.session_present,
                        "takeover must preserve the broker session"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await
        .context("session takeover timeout")??;
        Ok(())
    }

    /// Observes the actual broker's unacknowledged delivery count.
    ///
    /// # Errors
    /// Returns an error if the expected inflight count is not observed.
    pub async fn wait_inflight(&self, count: u64) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let state: Value =
                serde_json::from_str(&self.api(&format!("/clients/{}", self.client_id)).await?)?;
            if state["inflight_cnt"].as_u64() == Some(count) {
                fs::write(
                    self.dir.join("inflight.json"),
                    serde_json::to_vec_pretty(&state)?,
                )?;
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "broker inflight count timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    /// Reads a bounded management response using the synthetic API key.
    ///
    /// # Errors
    /// Returns an error on HTTP failure or oversized response.
    pub async fn api(&self, path: &str) -> Result<String> {
        bounded_text(
            self.http
                .get(format!("{}/api/v5{path}", self.api_url))
                .basic_auth("ingest-it-api", Some("synthetic-api-secret"))
                .send()
                .await?
                .error_for_status()?,
        )
        .await
    }

    /// Waits until the forwarder has durably committed the expected number of entries.
    ///
    /// # Errors
    /// Returns an error on timeout or metrics failure.
    pub async fn wait_committed(&self, count: u64) -> Result<()> {
        self.wait_metric("wal_forwarder_committed_total", count as f64)
            .await
    }

    /// Waits for a named operational metric to reach a scenario threshold.
    ///
    /// # Errors
    /// Returns an error on timeout or metrics failure.
    pub async fn wait_metric(&self, name: &str, minimum: f64) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let text = self.metrics().await?;
            let committed = text
                .lines()
                .find_map(|line| line.strip_prefix(&format!("{name} ")));
            if let Some(committed) = committed {
                if committed.parse::<f64>()? >= minimum {
                    return Ok(());
                }
            }
            ensure!(Instant::now() < deadline, "metric {name} timed out");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Captures bounded operational evidence from the running test process.
    ///
    /// # Errors
    /// Returns an error on HTTP failure.
    pub async fn metrics(&self) -> Result<String> {
        let text = bounded_text(
            self.http
                .get(format!("http://127.0.0.1:{}/metrics", self.metrics_port))
                .send()
                .await?
                .error_for_status()?,
        )
        .await?;
        fs::write(self.dir.join("metrics.txt"), &text)?;
        Ok(text)
    }

    /// Opens the real HomeKit SSE endpoint before publishing the test input.
    ///
    /// # Errors
    /// Returns an error on HTTP failure.
    pub async fn stream(&self) -> Result<reqwest::Response> {
        Ok(self
            .http
            .get(format!("http://127.0.0.1:{}/v1/stream", self.cache_port))
            .send()
            .await?
            .error_for_status()?)
    }

    /// Reads one bounded sensor event, allowing HTTP chunks to split SSE frames.
    ///
    /// # Errors
    /// Returns an error on stream failure, missing frame or timeout.
    pub async fn sensor_event(mut response: reqwest::Response) -> Result<Value> {
        tokio::time::timeout(TIMEOUT, async {
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len() + chunk.len() <= DIAGNOSTIC_BYTES as usize,
                    "SSE bound"
                );
                bytes.extend_from_slice(&chunk);
                let text = std::str::from_utf8(&bytes)?;
                for frame in text.split("\n\n").take(text.matches("\n\n").count()) {
                    if frame.lines().any(|line| line == "event: sensor") {
                        let data = frame
                            .lines()
                            .find_map(|line| line.strip_prefix("data: "))
                            .context("SSE sensor data")?;
                        return Ok(serde_json::from_str(data)?);
                    }
                }
            }
            anyhow::bail!("SSE closed before sensor event")
        })
        .await
        .context("SSE timeout")?
    }

    /// Restarts only the test broker, preserving its project-scoped persistent volumes.
    ///
    /// # Errors
    /// Returns an error if Compose restart or subsequent health verification fails.
    pub fn restart_broker(&self) -> Result<()> {
        let api_port = service_port(&self.project, "emqx", "18083")?;
        // Docker can reassign published port 0 on restart. Preserve the MQTT manifest identity.
        let mut command = compose_command(&self.project)?;
        command
            .env("INGEST_IT_MQTT_PORT", self.mqtt_port.to_string())
            .env("INGEST_IT_EMQX_API_PORT", api_port.to_string())
            .args([
                "up",
                "-d",
                "--force-recreate",
                "--wait",
                "--wait-timeout",
                "120",
                "emqx",
            ]);
        run_command(command)?;
        Ok(())
    }

    /// Selects a bucket owned by this synthetic scenario.
    pub fn use_owned_bucket(&mut self, suffix: &str) {
        self.bucket = format!("{}-{suffix}", self.client_id);
    }

    /// Creates the selected owned bucket without deleting existing data.
    ///
    /// # Errors
    /// Returns an error on ownership guard or Influx administration failure.
    pub async fn create_bucket(&self) -> Result<()> {
        ensure!(
            self.bucket.starts_with(&format!("{}-", self.client_id)),
            "not an owned test bucket"
        );
        let orgs: Value = self
            .http
            .get(format!("{}/api/v2/orgs?org={USER}", self.influx_url))
            .header("Authorization", format!("Token {TOKEN}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let org_id = orgs["orgs"][0]["id"].as_str().context("synthetic org ID")?;
        self.http
            .post(format!("{}/api/v2/buckets", self.influx_url))
            .header("Authorization", format!("Token {TOKEN}"))
            .json(&json!({"orgID":org_id,"name":self.bucket,"retentionRules":[]}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Seeds a historical type conflict only in the scenario's owned bucket.
    ///
    /// # Errors
    /// Returns an error on ownership guard or write failure.
    pub async fn write_line(&self, line: &str) -> Result<()> {
        ensure!(
            self.bucket.starts_with(&format!("{}-", self.client_id)),
            "not an owned test bucket"
        );
        self.http
            .post(format!(
                "{}/api/v2/write?org={USER}&bucket={}&precision=ns",
                self.influx_url, self.bucket
            ))
            .header("Authorization", format!("Token {TOKEN}"))
            .body(line.to_owned())
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Stops only the isolated project's database, keeping its volume.
    ///
    /// # Errors
    /// Returns an error on Compose failure.
    pub fn stop_influx(&self) -> Result<()> {
        compose(&self.project, &["stop", "influxdb"])?;
        Ok(())
    }

    /// Restores only the test database with its original port and persistent volume.
    ///
    /// # Errors
    /// Returns an error on Compose failure.
    pub fn restore_influx(&self) -> Result<()> {
        let port = self.influx_url.rsplit(':').next().context("Influx port")?;
        let mut command = compose_command(&self.project)?;
        command.env("INGEST_IT_INFLUX_PORT", port).args([
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "120",
            "influxdb",
        ]);
        run_command(command)?;
        Ok(())
    }

    /// Waits for a deterministic test-build barrier marker.
    ///
    /// # Errors
    /// Returns an error on timeout.
    pub async fn wait_barrier(&self, name: &str) -> Result<()> {
        let deadline = Instant::now() + TIMEOUT;
        while !self.dir.join(format!("{name}.reached")).exists() {
            ensure!(
                Instant::now() < deadline,
                "test barrier {name} timed out; build with integration-test-hooks"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Releases one test barrier without an HTTP administrative endpoint.
    ///
    /// # Errors
    /// Returns an error if the release marker cannot be written.
    pub fn release(&self, name: &str) -> Result<()> {
        fs::write(self.dir.join(format!("{name}.release")), [])?;
        Ok(())
    }

    /// Lists bounded journal metadata without acquiring ownership or resolving quarantine.
    ///
    /// # Errors
    /// Returns an error on journal inspection failure.
    pub fn journal_evidence(&self) -> Result<String> {
        let mut evidence = String::new();
        for journal in ["wal", "quarantine"] {
            let path = self.dir.join(journal);
            if !path.exists() {
                continue;
            }
            for entry in fs::read_dir(path)?.take(64) {
                let entry = entry?;
                evidence.push_str(&format!(
                    "{journal}/{}: {} bytes\n",
                    entry.file_name().to_string_lossy(),
                    entry.metadata()?.len()
                ));
            }
        }
        Ok(evidence)
    }

    /// Preserves bounded evidence on success, full disposable journals on failure.
    ///
    /// # Errors
    /// Returns the scenario error or a process teardown failure.
    pub async fn finish(&mut self, result: Result<()>) -> Result<()> {
        if let Ok(metrics) = self.metrics().await {
            fs::write(self.dir.join("metrics.txt"), metrics)?;
        }
        self.stop()?;
        result?;
        let evidence = self
            .dir
            .parent()
            .context("artifact parent")?
            .join(format!("passed-{}", self.client_id));
        fs::create_dir(&evidence)?;
        for entry in fs::read_dir(&self.dir)?.take(128) {
            let entry = entry?;
            if entry.file_type()?.is_file() && entry.metadata()?.len() <= DIAGNOSTIC_BYTES {
                fs::copy(entry.path(), evidence.join(entry.file_name()))?;
            }
        }
        let mut log = File::open(self.dir.join("ingest.log"))?;
        log.seek(SeekFrom::Start(
            log.metadata()?.len().saturating_sub(DIAGNOSTIC_BYTES),
        ))?;
        let mut tail = Vec::new();
        log.take(DIAGNOSTIC_BYTES).read_to_end(&mut tail)?;
        fs::write(evidence.join("ingest-tail.log"), tail)?;
        fs::write(evidence.join("journals.txt"), self.journal_evidence()?)?;
        for journal in ["wal", "quarantine"] {
            let cursor = self.dir.join(journal).join("commit.cursor");
            if cursor.is_file() {
                fs::copy(cursor, evidence.join(format!("{journal}-commit.cursor")))?;
            }
        }
        self.http
            .delete(format!(
                "{}/api/v5/clients/{}",
                self.api_url, self.client_id
            ))
            .basic_auth("ingest-it-api", Some("synthetic-api-secret"))
            .send()
            .await?
            .error_for_status()?;
        fs::write(
            evidence.join("result.txt"),
            "passed; process-crash evidence, not power-loss or exactly-once proof\n",
        )?;
        self.success = true;
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if !self.success {
            if let Some(temp) = self.temp.take() {
                eprintln!(
                    "integration evidence preserved at {}",
                    temp.keep().display()
                );
            }
        }
        let _ = self.stop();
        if !self.success {
            if let Ok(logs) = compose(&self.project, &["logs", "--no-color", "--tail", "100"]) {
                let start = logs.len().saturating_sub(DIAGNOSTIC_BYTES as usize);
                let _ = fs::write(self.dir.join("containers.log"), &logs.as_bytes()[start..]);
            }
            if let Ok(evidence) = self.journal_evidence() {
                let _ = fs::write(self.dir.join("journals.txt"), evidence);
            }
            if let Ok(mut log) = File::open(self.dir.join("ingest.log")) {
                let _ = log.seek(SeekFrom::End(-(DIAGNOSTIC_BYTES as i64)));
                let mut tail = Vec::new();
                let _ = log.take(DIAGNOSTIC_BYTES).read_to_end(&mut tail);
                let _ = fs::write(self.dir.join("ingest-tail.log"), tail);
            }
        }
    }
}
