# Refactor: decouple MQTT input behind a `Source` abstraction

Detailed implementation reference for the `input_refactor` branch. Goal: make the ingestion
service's input transport swappable via config (`INPUT_SOURCE`), with MQTT as the only real
implementation today. Kafka (or anything else) is **not** implemented here — only the seam for it.

This mirrors the existing `infrastructure::sink::Sink` trait pattern (already used for the
InfluxDB output side), applied symmetrically to the input side.

---

## 1. Current state (what's coupled today)

- `src/main.rs` builds `rumqttc::MqttOptions` / `AsyncClient`, subscribes to topics, polls the
  event loop, converts `Incoming::Publish` into an `IngestJob`, and round-robins into per-worker
  channels — all inline (lines ~123–283 of current `main.rs`).
- `src/pipeline/stages/dlq.rs`'s `DlqPublishStage` holds a concrete `rumqttc::AsyncClient` and
  calls `client.publish(...)` directly.
- `src/config.rs` has flat `mqtt_host`, `mqtt_port`, `mqtt_username`, `mqtt_password`,
  `mqtt_client_id` fields, all unconditionally required.
- The MQTT connection readiness flag (`mqtt_ready: Arc<AtomicBool>`) is checked by the `/readyz`
  HTTP endpoint (`infrastructure/cache/http.rs`).

## 2. Target module layout

```
src/infrastructure/source/
  mod.rs   - IngestJob, IngestDispatcher, Source trait, DlqPublisher trait, build_source()
  mqtt.rs  - MqttSource, MqttDlqPublisher, mqtt::build()
```

`pipeline/stages/dlq.rs` no longer depends on `rumqttc` directly — it holds `Arc<dyn DlqPublisher>`.

---

## 3. Implementation steps (named, with explicit dependencies)

Each step below is independently named so it can be picked up/prompted on its own. "Depends on"
lists steps whose code must exist first. "Parallel-safe with" lists steps that touch disjoint
files and have no ordering requirement between them — safe to implement concurrently (e.g. by
separate agents/sessions) or in any order.

| Step ID | Name | File(s) | Depends on | Parallel-safe with |
|---|---|---|---|---|
| **A** `define-source-trait` | Define `Source` / `DlqPublisher` traits, `IngestJob`, `IngestDispatcher` | `src/infrastructure/source/mod.rs` (new) | *(none)* | C, F, I |
| **B** `implement-mqtt-source` | Implement `MqttSource` / `MqttDlqPublisher` | `src/infrastructure/source/mqtt.rs` (new) | A | C, F, I |
| **C** `update-config-input-source` | Add `INPUT_SOURCE` selector, `MqttSourceConfig` | `src/config.rs` | *(none)* | A, B, F, I |
| **D** `update-dlq-stage` | Switch `DlqPublishStage` to `Arc<dyn DlqPublisher>` | `src/pipeline/stages/dlq.rs` | A (prod code); B (tests only) | C, F, I |
| **E** `refactor-main` | Rewire `main.rs` to use `build_source`/`IngestDispatcher`, rename readiness flag, fix shutdown drain | `src/main.rs` | A, B, C, D | *(none — integrates everything)* |
| **F** `rename-readyz-source-agnostic` | Rename `mqtt_ready` → `source_ready` param | `src/infrastructure/cache/http.rs` | *(none)* | A, B, C, D, I |
| **G** `update-tests-common` | Add `Arc<dyn DlqPublisher>` test helper | `tests/common/mod.rs` | B, D | H |
| **H** `update-existing-tests` | Rename `mqtt_ready` references in tests | `tests/http_cache.rs`, `pipeline/stages/dlq.rs` tests | E, F, G | G |
| **I** `update-docker-compose` | Add `INPUT_SOURCE=mqtt` | `Server/docker-compose.yaml` | *(none — env var name is already decided)* | A, B, C, D, F |
| **J** `add-dispatcher-tests` | Unit tests for `IngestDispatcher` | `src/infrastructure/source/mod.rs` | A | K, everything except A |
| **K** `add-config-tests` | Unit tests for `InputSourceKind` parsing | `src/config.rs` | C | J, everything except C |
| **L** `update-docs` | Update `docs/configuration.md`, `docs/architecture.md`, `readme.md` | docs | E, C, I | *(best done last, but no hard code dependency)* |
| **M** `verify-build-test-lint` | Run fmt/clippy/test + manual checks | *(whole repo)* | H, J, K, L | *(final gate)* |

**Truly independent starting points (no dependencies on any other step): A, C, F, I.** These four
can be implemented in parallel from the start (e.g. by separate sub-agents) since they touch
disjoint files (`infrastructure/source/mod.rs`, `config.rs`, `infrastructure/cache/http.rs`,
`docker-compose.yaml`) and none of their production code imports from the others.

B depends only on A. D's production code depends only on A (its unit tests additionally need B,
since they reuse `MqttDlqPublisher` as the test double instead of hand-rolling a second mock).
J depends only on A; K depends only on C — J and K can run in parallel with each other and with
B/D once A/C land.

E is the integration point: it cannot start until A, B, C, and D are all in place, because it
constructs the pipeline (needs D's new `DlqPublishStage::new` signature), calls `build_source`
(needs A+B), and reads `cfg.input_source`/`cfg.mqtt` (needs C).

---

## 4. `src/infrastructure/source/mod.rs` (new file)

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::warn;

pub mod mqtt;

/// A single unit of work dispatched from a `Source` into the worker pool.
#[derive(Debug)]
pub struct IngestJob {
    pub topic: String,
    pub payload: Bytes,
}

/// Round-robins `IngestJob`s across a fixed set of per-worker bounded channels.
///
/// Owned by `main` and handed to `Source::run` by value so a source never needs
/// to know about worker count or pool internals — it just calls `dispatch`.
/// No extra channel hop is introduced: this replaces the round-robin logic that
/// used to live inline in `main.rs`'s consume loop.
#[derive(Clone)]
pub struct IngestDispatcher {
    senders: Arc<[mpsc::Sender<IngestJob>]>,
    next: Arc<AtomicUsize>,
}

impl IngestDispatcher {
    pub fn new(senders: Vec<mpsc::Sender<IngestJob>>) -> Self {
        Self {
            senders: senders.into(),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Dispatches `job` to the next worker in round-robin order. If that
    /// worker's queue is full, the job is dropped, a warning is logged, and
    /// `ingest_event_queue_full_total` is incremented (same behavior as the
    /// current inline main.rs dispatch loop — DLQ is not used for this
    /// pre-pipeline drop).
    pub fn dispatch(&self, job: IngestJob) {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        if let Err(err) = self.senders[idx].try_send(job) {
            metrics::counter!("ingest_event_queue_full_total").increment(1);
            warn!(error = %err, "event queue full; dropping incoming message before pipeline");
        }
    }
}

/// A transport that produces `IngestJob`s (e.g. an MQTT client, a future Kafka consumer).
///
/// `run` takes ownership of `self` (boxed, for object safety) and drives the
/// transport's event loop until `shutdown_rx` signals shutdown or an
/// unrecoverable error occurs. Mirrors `Sink`'s boxed-future convention.
pub trait Source: Send {
    fn run(
        self: Box<Self>,
        dispatcher: IngestDispatcher,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}

/// Publishes a rejected message to a dead-letter destination on the same
/// transport as the active `Source` (see `build_source`: both come from one
/// factory call so they always match).
pub trait DlqPublisher: Send + Sync {
    /// # Errors
    /// Returns an error if the publish fails. Callers (the DLQ pipeline stage)
    /// must not propagate this as a pipeline failure — log and continue.
    fn publish<'a>(
        &'a self,
        dlq_topic: &'a str,
        src_topic: &'a str,
        payload: &'a str,
        err: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Builds the configured input source and its matching DLQ publisher.
///
/// # Errors
/// Returns an error if `Config::input_source` names an unsupported source, or
/// if the underlying transport fails to connect/subscribe during construction.
pub async fn build_source(
    cfg: &crate::config::Config,
    ready: Arc<AtomicBool>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    match cfg.input_source {
        crate::config::InputSourceKind::Mqtt => mqtt::build(cfg, ready).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // test_ingest_dispatcher_dispatch_round_robins_across_senders
    // test_ingest_dispatcher_dispatch_drops_job_and_increments_metric_when_queue_full
    // (see section 10 below for full test bodies)
}
```

### Verification (step A)

- [ ] `cargo check --lib` compiles with only this new file added (it has no callers yet, so it
      compiles standalone — `Source`/`DlqPublisher` are unused trait warnings are expected until
      step B/E land; do not suppress with `#[allow(dead_code)]`, just confirm no *errors*).
- [ ] `cargo test infrastructure::source` passes once the `IngestDispatcher` tests from step J are
      added (round-robin + queue-full cases).
- [ ] No `rumqttc` import appears in this file — it must stay transport-agnostic.

---

## 5. `src/infrastructure/source/mqtt.rs` (new file)

Moves the exact logic currently in `main.rs` (MQTT options/client/subscribe/event-loop-poll) and
in `pipeline/stages/dlq.rs` (`publish_dlq`), unchanged in behavior:

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS};
use serde_json::json;
use tokio::sync::watch;
use tracing::info;

use super::{DlqPublisher, IngestDispatcher, IngestJob, Source};
use crate::config::Config;

/// MQTT-backed `Source`. Holds only the event loop + readiness flag: the
/// `AsyncClient` handle used for subscribing is dropped after `build()`
/// completes (a live clone is kept alive inside `MqttDlqPublisher`, and the
/// event loop owns the actual network connection regardless of client handle
/// count).
pub struct MqttSource {
    eventloop: EventLoop,
    ready: Arc<AtomicBool>,
}

impl Source for MqttSource {
    fn run(
        mut self: Box<Self>,
        dispatcher: IngestDispatcher,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        // Either the channel closed or shutdown was signalled — stop polling.
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    event = self.eventloop.poll() => {
                        let event = match event {
                            Ok(ev) => ev,
                            Err(err) => {
                                self.ready.store(false, Ordering::Relaxed);
                                return Err(err).context("MQTT poll failed");
                            }
                        };

                        match &event {
                            Event::Incoming(Incoming::ConnAck(_)) => {
                                self.ready.store(true, Ordering::Relaxed);
                                info!("MQTT connected");
                            }
                            Event::Incoming(Incoming::Disconnect) => {
                                self.ready.store(false, Ordering::Relaxed);
                            }
                            _ => {}
                        }

                        if let Event::Incoming(Incoming::Publish(publish)) = event {
                            dispatcher.dispatch(IngestJob {
                                topic: publish.topic,
                                payload: publish.payload,
                            });
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

/// MQTT-backed `DlqPublisher`. Moved verbatim from the old free function
/// `pipeline::stages::dlq::publish_dlq`.
pub struct MqttDlqPublisher {
    client: AsyncClient,
}

impl DlqPublisher for MqttDlqPublisher {
    fn publish<'a>(
        &'a self,
        dlq_topic: &'a str,
        src_topic: &'a str,
        payload: &'a str,
        err: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let dlq = json!({
                "received_at": chrono::Utc::now().to_rfc3339(),
                "src_topic": src_topic,
                "error": err,
                "payload_raw": payload,
            });

            info!(src_topic = %src_topic, error = %err, "publishing message to DLQ topic");

            let bytes = serde_json::to_vec(&dlq)?;
            self.client
                .publish(dlq_topic, QoS::AtLeastOnce, false, bytes)
                .await?;

            Ok(())
        })
    }
}

/// Builds an `MqttSource` + `MqttDlqPublisher` pair: connects, subscribes to
/// every configured non-DLQ `MQTT_TOPIC_*` route, and returns both handles.
///
/// # Errors
/// Returns an error if `cfg.mqtt` is `None` (should not happen — `Config::from_env`
/// guarantees it's populated when `input_source == InputSourceKind::Mqtt`), or if
/// subscribing to any configured topic fails.
pub async fn build(
    cfg: &Config,
    ready: Arc<AtomicBool>,
) -> Result<(Box<dyn Source>, Arc<dyn DlqPublisher>)> {
    let mqtt_cfg = cfg
        .mqtt
        .as_ref()
        .context("INPUT_SOURCE=mqtt requires MQTT_* connection variables")?;

    let mut mqttoptions = MqttOptions::new(&mqtt_cfg.client_id, &mqtt_cfg.host, mqtt_cfg.port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    if let (Some(username), Some(password)) = (&mqtt_cfg.username, &mqtt_cfg.password) {
        mqttoptions.set_credentials(username, password);
    }

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

    for (_, topic) in cfg.mqtt_topics.iter().filter(|(k, _)| !k.ends_with("DLQ")) {
        client.subscribe(topic, QoS::AtLeastOnce).await?;
        info!(topic = %topic, "subscribed to MQTT topic");
    }

    let source: Box<dyn Source> = Box::new(MqttSource { eventloop, ready });
    let publisher: Arc<dyn DlqPublisher> = Arc::new(MqttDlqPublisher { client });

    Ok((source, publisher))
}

#[cfg(test)]
mod tests {
    // Move the existing dlq.rs test helpers here (client_with_live_eventloop /
    // client_with_dropped_eventloop) and add MqttDlqPublisher-level coverage
    // equivalent to what pipeline/stages/dlq.rs currently tests at the stage level.
}
```

### Verification (step B)

- [ ] `cargo check --lib` compiles with A + B present (`MqttSource`/`MqttDlqPublisher` implement
      the traits from step A — a type-mismatch here is a compile error, not a runtime surprise).
- [ ] `cargo clippy --all-targets --all-features --locked -- -D warnings` passes for this file
      specifically (`cargo clippy -p smarthome-ingest --lib -- -D warnings` if you want a faster
      partial check before running the full suite).
- [ ] Manually diff this file's `run()` body against the old `main.rs` event-loop `select!` arm
      (section 1) to confirm the ConnAck/Disconnect/Publish handling logic is unchanged, only
      relocated.
- [ ] Manually diff `MqttDlqPublisher::publish` against the old `publish_dlq` free function in
      `dlq.rs` to confirm the JSON envelope shape (`received_at`, `src_topic`, `error`,
      `payload_raw`) is byte-for-byte the same.

---

## 6. `src/config.rs` changes

Add (near the top, alongside other types):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputSourceKind {
    Mqtt,
}

impl InputSourceKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_lowercase().as_str() {
            "mqtt" => Ok(Self::Mqtt),
            other => bail!(
                "unsupported INPUT_SOURCE '{other}': only 'mqtt' is currently implemented"
            ),
        }
    }
}

#[derive(Clone)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: String,
}

impl fmt::Debug for MqttSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Same fields shown today — username/password were never in the old Debug impl either.
        f.debug_struct("MqttSourceConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("client_id", &self.client_id)
            .finish()
    }
}
```

`Config` struct: replace the 5 flat `mqtt_*` connection fields with:

```rust
pub struct Config {
    pub input_source: InputSourceKind,
    pub mqtt: Option<MqttSourceConfig>,
    pub mqtt_topics: HashMap<String, String>, // unchanged — transport-agnostic topic routing
    // ...(influx/batching/wal/queue/metrics/cache fields unchanged)
}
```

`Config::from_env()`: replace the current unconditional MQTT parsing block with:

```rust
let input_source = InputSourceKind::parse(
    &env_var("INPUT_SOURCE").context("INPUT_SOURCE must be set")?,
)?;

let mqtt = match input_source {
    InputSourceKind::Mqtt => {
        let host = env_var("MQTT_HOST")
            .context("MQTT_HOST is required when INPUT_SOURCE=mqtt")?;
        let port = env_var("MQTT_PORT")
            .context("MQTT_PORT must be set when INPUT_SOURCE=mqtt")?
            .parse::<u16>()
            .context("MQTT_PORT must be a u16")?;
        let username = env_var("MQTT_USERNAME");
        let password = env_var("MQTT_PASSWORD");
        let mut client_id = env_var("MQTT_CLIENT_ID")
            .context("MQTT_CLIENT_ID must be set when INPUT_SOURCE=mqtt")?;
        client_id.push_str(&format!("-{}", chrono::Utc::now().timestamp()));

        Some(MqttSourceConfig { host, port, username, password, client_id })
    }
};

// mqtt_topics parsing/validation (at least one MQTT_TOPIC_<NAME>) stays exactly as today —
// it is not gated by input_source since topic routing is transport-agnostic.
```

Update the `Debug for Config` impl: replace the `mqtt_host` / `mqtt_port` / `mqtt_client_id`
fields with `.field("input_source", &self.input_source).field("mqtt", &self.mqtt)`.

Update the final `Ok(Self { ... })` constructor to use `input_source, mqtt, mqtt_topics, ...`.

**Note:** keeping `match` (not `if let`) on a single-variant enum is intentional — adding a
`Kafka` variant later forces a compile error here until it's handled, acting as a guardrail.

### Verification (step C)

- [ ] `cargo check --lib` compiles standalone (this step has no dependency on A/B/D).
- [ ] Run locally with `INPUT_SOURCE=mqtt` and all `MQTT_*` vars set — `Config::from_env()`
      succeeds (existing behavior preserved).
- [ ] Run with `INPUT_SOURCE` unset — startup fails with a clear `"INPUT_SOURCE must be set"`
      error (matches the "no code-level defaults" convention).
- [ ] Run with `INPUT_SOURCE=mqtt` but `MQTT_HOST` unset — fails with
      `"MQTT_HOST is required when INPUT_SOURCE=mqtt"`.
- [ ] Run with `INPUT_SOURCE=kafka` (or any unsupported value) — fails with
      `"unsupported INPUT_SOURCE 'kafka': only 'mqtt' is currently implemented"`.
- [ ] `cfg.mqtt_topics` parsing/requirement is unchanged (still requires at least one
      `MQTT_TOPIC_<NAME>` regardless of `INPUT_SOURCE`).

---

## 7. `src/pipeline/stages/dlq.rs` changes

Replace:

```rust
use rumqttc::AsyncClient;
...
pub async fn publish_dlq(client: &AsyncClient, dlq_topic: &str, ...) -> Result<()> { ... }

#[derive(Clone)]
pub struct DlqPublishStage {
    client: AsyncClient,
    dlq_topic: String,
}

impl DlqPublishStage {
    pub fn new(client: AsyncClient, dlq_topic: impl Into<String>) -> Self {
        Self { client, dlq_topic: dlq_topic.into() }
    }
}
```

with:

```rust
use std::sync::Arc;
use crate::infrastructure::source::DlqPublisher;
...
// publish_dlq free function removed — logic now lives in MqttDlqPublisher::publish.

pub struct DlqPublishStage {
    publisher: Arc<dyn DlqPublisher>,
    dlq_topic: String,
}

impl DlqPublishStage {
    pub fn new(publisher: Arc<dyn DlqPublisher>, dlq_topic: impl Into<String>) -> Self {
        Self { publisher, dlq_topic: dlq_topic.into() }
    }
}
```

Update `PipelineStage::run` body: replace the `publish_dlq(&self.client, &self.dlq_topic, ...)`
call with `self.publisher.publish(&self.dlq_topic, ctx.topic(), &payload, &reason).await`.

Remove `#[derive(Clone)]` from `DlqPublishStage` if `Arc<dyn DlqPublisher>` doesn't need it to be
`Clone` elsewhere (it doesn't — `PipelineRunner::with_failure_stage` takes it by value once).

Unit tests: replace `client_with_live_eventloop()` / `client_with_dropped_eventloop()` usage —
wrap the returned `AsyncClient` in `MqttDlqPublisher` (moved to `infrastructure/source/mqtt.rs`,
re-exported or constructed via a small `pub(crate)` helper if needed for tests), then
`Arc::new(...)` before passing to `DlqPublishStage::new`.

### Verification (step D)

- [ ] `cargo check --lib` compiles (depends on A; test code additionally depends on B).
- [ ] `cargo test pipeline::stages::dlq` — all 4 existing unit tests
      (`run_without_dlq_reason_returns_stop_without_publishing`,
      `run_with_dlq_reason_returns_stop_when_publish_succeeds`,
      `run_with_dlq_reason_returns_stop_even_when_publish_fails`,
      `run_uses_configured_dlq_topic`) still pass unmodified in intent, just retargeted to
      `Arc<dyn DlqPublisher>` construction.
- [ ] `grep -rn "rumqttc" src/pipeline/stages/dlq.rs` returns nothing — confirms the stage no
      longer depends on the concrete MQTT client type.

---

## 8. `src/main.rs` changes

Remove: `use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};` and the `IngestJob` struct
(moved to `infrastructure::source`).

Add: `use infrastructure::source::{build_source, IngestDispatcher};`

**New wiring order** (keep everything else — cache/http, metrics, router, WAL/sink/forwarder —
in their current relative order; only the MQTT block and the worker/dispatch section change):

```rust
// renamed from mqtt_ready
let source_ready = Arc::new(AtomicBool::new(false));
// ... cache/http server spawn uses source_ready instead of mqtt_ready ...

// ... metrics server, router, dlq_topic lookup, WAL+sink+forwarder: unchanged ...

// ------------------------------------------------------------
// Input source (MQTT today; swappable via INPUT_SOURCE)
// ------------------------------------------------------------
let (source, dlq_publisher) = build_source(&cfg, source_ready.clone()).await?;

// ------------------------------------------------------------
// Pipeline (DlqPublishStage now takes the source-agnostic publisher)
// ------------------------------------------------------------
let pipeline = Arc::new(
    PipelineRunner::new()
        .add_stage(DecodeStage::new())
        .add_stage(ValidateRawStage::new(router.clone(), cfg.enforce_topic_device_match))
        .add_stage(TransformStage::new(router.clone()))
        .add_stage(ValidateBusinessStage::new()?)
        .add_stage(CacheUpdateStage::new(app_state.clone()))
        .add_stage(PersistStage::new(wal.clone()))
        .add_stage(ObserveStage::new())
        .with_failure_stage(DlqPublishStage::new(dlq_publisher, dlq_topic.clone())),
);

// ------------------------------------------------------------
// Worker queue — one channel per worker, round-robin dispatch
// ------------------------------------------------------------
// (unchanged worker spawn loop that builds `job_txs: Vec<mpsc::Sender<IngestJob>>`)

let dispatcher = IngestDispatcher::new(job_txs.clone()); // see note below on job_txs ownership
let mut source_task = tokio::spawn(source.run(dispatcher, shutdown_rx.clone()));
```

> **Note on `job_txs`:** today `job_txs` is used both by the dispatch loop and later `drop(job_txs)`
> during shutdown to close worker channels. Since `IngestDispatcher` now owns its own clone of the
> senders (`Arc<[mpsc::Sender<IngestJob>]>`), keep `job_txs: Vec<mpsc::Sender<IngestJob>>` in `main`
> exactly as today (for the later `drop(job_txs)` shutdown step), and pass `job_txs.clone()` into
> `IngestDispatcher::new`. Dropping `main`'s `job_txs` later still closes the channels once
> `IngestDispatcher` (held only by the now-finished `source_task`) is also dropped.

**Main select loop**: replace the `event = eventloop.poll() => { ... }` arm with:

```rust
res = &mut source_task => {
    let _ = shutdown_tx.send(true);
    match res {
        Ok(Ok(())) => info!("input source stopped"),
        Ok(Err(err)) => {
            source_ready.store(false, Ordering::Relaxed);
            error!(error = %err, "input source failed");
            fatal_source_err = Some(err);
        }
        Err(join_err) => {
            error!(error = %join_err, "input source task panicked");
            fatal_source_err = Some(join_err.into());
        }
    }
    break;
}
```

Declare `let mut fatal_source_err: Option<anyhow::Error> = None;` before the loop. After the
existing drain sequence (`drop(job_txs)` → join workers → `drop(pipeline)` / `drop(wal)` → drain
forwarder → `info!("ingestion service stopped")`), change the final line from `Ok(())` to:

```rust
match fatal_source_err {
    Some(err) => Err(err),
    None => Ok(()),
}
```

This is the one behavior change beyond pure decoupling: **a fatal source error now drains workers
and the WAL before the process exits**, instead of returning immediately and skipping the drain
(today's `return Err(err).context("MQTT poll failed")` bypasses cleanup entirely). Tightly coupled
to this refactor since the error now flows through the same shutdown path as ctrl-c — call this out
in the PR description.

Rename every other `mqtt_ready` reference in `main.rs` to `source_ready`.

### Verification (step E)

- [ ] `cargo build --release` succeeds — this is the integration point, so a clean build here
      confirms A, B, C, and D all fit together correctly.
- [ ] `grep -rn "rumqttc" src/main.rs` returns nothing — all transport-specific code has moved
      out of `main.rs`.
- [ ] Manually trace the new startup order against section 8 above: cache/http → metrics →
      router → dlq_topic → WAL/sink/forwarder → `build_source` → pipeline (with
      `dlq_publisher`) → workers (`job_txs`) → `IngestDispatcher` → `source_task` spawn → select
      loop.
- [ ] Start the service against a local/test broker and confirm messages still flow end-to-end
      (sensor/status payloads land in the WAL, as before).
- [ ] Simulate a fatal source error (e.g. stop the broker mid-run) and confirm from logs that
      workers stop and the WAL forwarder drains *before* the process exits (new behavior — this
      did not happen before this refactor).
- [ ] Ctrl-C shutdown still drains workers and the WAL exactly as before (no regression).

---

## 9. `src/infrastructure/cache/http.rs` changes

Rename the parameter and `Extension` type usage from `mqtt_ready` to `source_ready`:

```rust
pub fn router(state: CacheState, source_ready: Arc<AtomicBool>) -> Router {
    Router::new()
        // ...
        .layer(Extension(source_ready))
}

async fn readyz(Extension(source_ready): Extension<Arc<AtomicBool>>) -> StatusCode {
    if source_ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
```

### Verification (step F)

- [ ] `cargo check --lib` compiles standalone (no dependency on A/B/C/D — pure rename).
- [ ] `curl http://localhost:8085/readyz` still returns `503` before the source connects and
      `200` after (same observable behavior, only the internal parameter name changed).

---

## 10. Test updates

### `tests/common/mod.rs`

Add a helper that returns `Arc<dyn DlqPublisher>`:

```rust
use smarthome_ingest::infrastructure::source::mqtt::MqttDlqPublisher; // adjust path/visibility as needed
use smarthome_ingest::infrastructure::source::DlqPublisher;

pub fn dlq_publisher() -> Arc<dyn DlqPublisher> {
    Arc::new(MqttDlqPublisher::new(dlq_client())) // expose a `new` on MqttDlqPublisher for tests
}
```

Update `build_pipeline()`'s `.with_failure_stage(DlqPublishStage::new(dlq_client(), "smarthome/_dlq/ingest"))`
to `.with_failure_stage(DlqPublishStage::new(dlq_publisher(), "smarthome/_dlq/ingest"))`.

> `MqttDlqPublisher`'s `client` field is private — either add a `pub(crate) fn new(client: AsyncClient) -> Self`
> constructor, or make the field `pub(crate)`. Prefer the constructor for encapsulation.

### `tests/http_cache.rs`

Rename `mqtt_ready` parameter names and test function names to `source_ready`, e.g.
`readyz_returns_200_when_mqtt_ready` → `readyz_returns_200_when_source_ready`. Behavior/assertions
unchanged.

### `pipeline/stages/dlq.rs` unit tests

Update `client_with_live_eventloop()` / `client_with_dropped_eventloop()` call sites to wrap the
returned client via `MqttDlqPublisher::new(client)` and `Arc::new(...)` before constructing
`DlqPublishStage`.

### New: `IngestDispatcher` unit tests (`infrastructure/source/mod.rs`)

```rust
#[tokio::test]
async fn ingest_dispatcher_dispatch_round_robins_across_senders() {
    // Arrange: 2 channels with capacity 4 each.
    // Act: dispatch 4 jobs.
    // Assert: each channel receives exactly 2, in order.
}

#[tokio::test]
async fn ingest_dispatcher_dispatch_drops_job_and_increments_metric_when_queue_full() {
    // Arrange: 1 channel with capacity 1, pre-filled.
    // Act: dispatch one more job.
    // Assert: no panic; channel still holds only the original job (new one dropped).
}
```

### New: `config.rs` unit tests

```rust
#[test]
fn input_source_kind_parse_accepts_mqtt_case_insensitive() { /* "MQTT", "mqtt", " Mqtt " */ }

#[test]
fn input_source_kind_parse_rejects_unknown_value() { /* e.g. "kafka" -> Err */ }

// If from_env() is refactored to allow injecting a var lookup (or tested via env::set_var in a
// serial test — check for existing patterns before adding one), add:
// config_from_env_requires_mqtt_fields_when_input_source_is_mqtt
// config_from_env_errors_when_input_source_unset
```

> `Config::from_env()` reads real process env vars; if there's no existing precedent for testing it
> (checked: there isn't one today), keep new tests scoped to `InputSourceKind::parse` in isolation
> rather than fighting global env-var mutation in parallel test runs.

### Verification (steps G, H, J, K)

- [ ] `cargo test --all-features --locked` — full suite passes, including:
      - [ ] `tests/http_cache.rs` (renamed `source_ready` tests) — step H
      - [ ] `pipeline::stages::dlq` unit tests (using `MqttDlqPublisher` test doubles) — step D/H
      - [ ] `tests/pipeline_end_to_end.rs` and `tests/batcher.rs` — unaffected, but must still
            pass since `tests/common/mod.rs` changed (step G)
      - [ ] new `infrastructure::source` tests (`ingest_dispatcher_dispatch_round_robins_across_senders`,
            `ingest_dispatcher_dispatch_drops_job_and_increments_metric_when_queue_full`) — step J
      - [ ] new `config` tests (`input_source_kind_parse_accepts_mqtt_case_insensitive`,
            `input_source_kind_parse_rejects_unknown_value`) — step K
- [ ] `grep -rn "mqtt_ready" tests/ src/` returns nothing — confirms the rename is complete
      everywhere, not just in production code.

---

## 11. `Server/docker-compose.yaml`

Add to the `ingest` service's `environment:` block (after `MQTT_CLIENT_ID`, before the topic vars):

```yaml
      - INPUT_SOURCE=mqtt
```

### Verification (step I)

- [ ] `docker compose config` (run from `Server/`) parses without error and shows
      `INPUT_SOURCE=mqtt` in the resolved `ingest` service environment.
- [ ] `docker compose up ingest --build` (with the rest of the stack available/uncommented as
      needed) starts successfully and reaches a ready state (`/readyz` returns `200`).

---

## 12. Documentation updates

### `docs/configuration.md`

Add a new row/section before "## MQTT":

```markdown
## Input Source

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INPUT_SOURCE` | Yes | None | Selects the input transport. Only `mqtt` is implemented today |

`MQTT_HOST`, `MQTT_PORT`, `MQTT_CLIENT_ID`, `MQTT_USERNAME`, `MQTT_PASSWORD` are required only
when `INPUT_SOURCE=mqtt`. `MQTT_TOPIC_*` variables are unconditional — they configure
transport-agnostic topic routing/schema selection regardless of `INPUT_SOURCE`.
```

Update the "Minimal Local Configuration" example to include `INPUT_SOURCE=mqtt`.

### `docs/architecture.md`

- Update "Runtime Components" diagram to show `Config -> Source (build_source) -> ...` instead of
  `Config -> MQTT options and topic map`.
- Add a short "## Input Source Abstraction" section describing the `Source`/`DlqPublisher` traits
  and that `build_source()` selects the implementation based on `INPUT_SOURCE`.
- Note the readiness flag rename (`mqtt_ready` → `source_ready`) if referenced.

### `readme.md`

Update any `MQTT_HOST`/etc. references or config snippets to include `INPUT_SOURCE=mqtt`.

### Verification (step L)

- [ ] Every `MQTT_*`-only config example in `docs/` and `readme.md` now also shows
      `INPUT_SOURCE=mqtt` alongside it.
- [ ] `docs/architecture.md`'s "Runtime Components" section and "MQTT Ingestion" section (or its
      renamed equivalent) accurately describe `build_source()`/`Source`/`DlqPublisher` instead of
      inline MQTT wiring in `main.rs`.
- [ ] No doc references the now-removed `mqtt_ready` name.

---

## 13. Final verification checklist (step M — final gate, depends on everything above)

Run from the repo root once every step (A–L) above is implemented:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

All three must pass with zero warnings/failures before opening a PR (matches CI: `.github/workflows/ci.yaml` runs fmt → clippy → tests on every push).

Manually sanity-check:
- [ ] `docker compose config` (or a local `.env` + `cargo run`) still starts with
      `INPUT_SOURCE=mqtt` set and no other env changes.
- [ ] `/readyz` returns 503 before MQTT connects and 200 after (same as before the rename).
- [ ] A DLQ-routed message still appears on the configured `MQTT_TOPIC_DLQ` topic.
- [ ] Killing the broker connection mid-run still surfaces as a fatal error, but now workers/WAL
      drain first (new behavior — verify via logs: worker stop + WAL drain messages appear before
      process exit).
- [ ] `grep -rn "rumqttc" src/main.rs src/pipeline/` returns nothing — confirms `rumqttc` is now
      confined to `src/infrastructure/source/`.
- [ ] `git diff --stat` shows no unrelated files changed (e.g. no accidental formatting-only
      diffs in files untouched by this refactor).

### Per-step verification quick reference

| Step | Verification location |
|---|---|
| A | Section 4, "Verification (step A)" |
| B | Section 5, "Verification (step B)" |
| C | Section 6, "Verification (step C)" |
| D | Section 7, "Verification (step D)" |
| E | Section 8, "Verification (step E)" |
| F | Section 9, "Verification (step F)" |
| G, H, J, K | Section 10, "Verification (steps G, H, J, K)" |
| I | Section 11, "Verification (step I)" |
| L | Section 12, "Verification (step L)" |
| M | This section |

---

## 14. Explicitly out of scope

- No working Kafka `Source`/`DlqPublisher` implementation — only the trait + factory seam.
- `MqttTopicPattern`, `Router`, and `MQTT_TOPIC_*` naming stay as-is (topic-pattern matching and
  routing are already transport-agnostic in behavior).
- No Prometheus metric renames (e.g. `mqtt_messages_received_total` in `decode.rs` stays put).
