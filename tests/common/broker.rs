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

const TIMEOUT: Duration = Duration::from_secs(60);
const DIAGNOSTIC_BYTES: u64 = 64 * 1024;
const TOKEN: &str = "synthetic-integration-token";
const USER: &str = "ingest-test";
const PASSWORD: &str = "synthetic-test-password";

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
    let mut bundle = read_json(root().join("config").join("routes.json"))?;
    fs::create_dir_all(dir.join("schemas"))?;
    for entry in fs::read_dir(root().join("config").join("schemas"))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), dir.join("schemas").join(entry.file_name()))?;
        }
    }
    for route in bundle["routes"].as_array_mut().context("firmware routes")? {
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
        ensure!(self.child.is_none(), "ingest child already running");
        for name in ["before_append", "after_sync", "before_sink"] {
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
            .env("INGEST_ROUTES_FILE", self.dir.join("routes.json"))
            .env("WAL_DIR", self.dir.join("wal"))
            .env("QUARANTINE_DIR", self.dir.join("quarantine"))
            .env("CACHE_BIND", format!("127.0.0.1:{}", self.cache_port))
            .env("METRICS_BIND", format!("127.0.0.1:{}", self.metrics_port))
            .env("INGEST_TEST_HOOK_DIR", &self.dir)
            .env("INGEST_TEST_BARRIER", barrier.unwrap_or(""))
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

    /// Publishes one QoS 1 message and observes its matching publisher PUBACK from EMQX.
    ///
    /// # Errors
    /// Returns an error on MQTT failure or bounded receipt timeout.
    pub async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut options =
            MqttOptions::new(format!("it-publisher-{nonce}"), "127.0.0.1", self.mqtt_port);
        options.set_credentials(USER, PASSWORD);
        let (client, mut eventloop) = AsyncClient::new(options, 4);
        client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await?;
        tokio::time::timeout(TIMEOUT, async {
            let mut packet_id = None;
            loop {
                match eventloop.poll().await? {
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
            "from(bucket: \"{USER}\") |> range(start: 0) |> pivot(rowKey:[\"_time\"], columnKey:[\"_field\"], valueColumn:\"_value\") |> filter(fn:(r) => r.test_id == \"{test_id}\")"
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
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let text = bounded_text(
                self.http
                    .get(format!("http://127.0.0.1:{}/metrics", self.metrics_port))
                    .send()
                    .await?
                    .error_for_status()?,
            )
            .await?;
            let committed = text
                .lines()
                .find_map(|line| line.strip_prefix("wal_forwarder_committed_total "));
            if let Some(committed) = committed {
                if committed.parse::<f64>()? >= count as f64 {
                    return Ok(());
                }
            }
            ensure!(Instant::now() < deadline, "durable cursor commit timed out");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

    /// Preserves diagnostics on failure and removes disposable process state on success.
    ///
    /// # Errors
    /// Returns the scenario error or a process teardown failure.
    pub fn finish(&mut self, result: Result<()>) -> Result<()> {
        self.stop()?;
        self.success = result.is_ok();
        result
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
