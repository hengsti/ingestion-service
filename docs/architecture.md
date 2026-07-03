# Architecture

This document describes the runtime structure and why each boundary exists.

## Responsibilities

`smarthome-ingest` owns the ingestion path between an input source and an output sink (InfluxDB today):

- Consume telemetry from a configurable input source (MQTT today; see [Input Source](#input-source)).
- Decode and validate JSON payloads.
- Normalize payloads into canonical Rust message structs.
- Compute derived sensor fields.
- Update an in-memory latest sensor cache.
- Render the wire-format payload via the active output sink's `Encoder` (InfluxDB line protocol today; see [Output Sink](#output-sink)).
- Store the rendered payload in a local WAL before forwarding.
- Publish rejected payloads to a DLQ destination on the active input source.
- Export HTTP state and Prometheus metrics.

The service does not own the input broker lifecycle, InfluxDB lifecycle, device firmware, dashboards, or long-term query APIs.

## Runtime Components

```text
Config
  -> Input source selection (INPUT_SOURCE) and MQTT topic map
  -> Router with embedded schemas
  -> Cache state
  -> Metrics server
  -> WAL and WAL subscription
  -> Output sink selection (OUTPUT_SINK) and matching encoder
  -> Pipeline runner (with a DlqPublisher from the active source, and an Encoder from the active sink)
```

`main.rs` wires these components together. It starts the cache API, metrics API, the input source's event loop, the WAL forwarder, and the worker pool.

## Input Source

Input ingestion is decoupled behind a `Source` abstraction (`src/infrastructure/source/mod.rs`), mirroring the `Sink`/`Encoder` boundary used for output:

- **`Source` trait** — owns a transport's connect/subscribe/event-loop and pushes decoded `IngestJob`s into a shared `IngestDispatcher`. `run` takes `self: Box<Self>` and a cloned shutdown `watch::Receiver<bool>`, matching the shutdown pattern already used by workers.
- **`DlqPublisher` trait** — abstracts "publish a rejected message back out". It is coupled 1:1 with the active `Source`: `build_source()` returns both from one factory call, matched on `Config::input_source`.
- **`IngestDispatcher`** — round-robins `IngestJob`s across the worker pool's per-worker bounded channels. It is handed to `Source::run` by value so a source never needs to know about worker count or pool internals, and there is no extra channel hop between the source and the workers.
- **`MqttSource` / `MqttDlqPublisher`** (`src/infrastructure/source/mqtt.rs`) — the only implementation today. `MqttSource` holds just the `rumqttc::EventLoop` and a readiness flag; the `AsyncClient` handle used for subscribing is cloned into `MqttDlqPublisher` and the original handle dropped, since the event loop owns the actual network connection independent of client handle count.

`INPUT_SOURCE` (env var) selects which source `build_source()` constructs. Only `mqtt` is implemented; adding a future transport (e.g. Kafka) means adding an `InputSourceKind` variant plus a matching `Source`/`DlqPublisher` pair — `main.rs`'s wiring does not need to change.

The service reads all environment variables whose names start with `MQTT_TOPIC_`. These stay unconditional regardless of `INPUT_SOURCE` because topic-based routing/schema selection (the `Router`) is transport-agnostic:

- `MQTT_TOPIC_SENSOR` maps to `MessageType::Sensor`.
- `MQTT_TOPIC_STATUS` maps to `MessageType::Status`.
- `MQTT_TOPIC_DLQ` is used only for DLQ publishing.

The MQTT source subscribes to every configured `MQTT_TOPIC_*` value except keys ending in `DLQ`. The router only creates routes for `SENSOR` and `STATUS`. Avoid unknown non-DLQ `MQTT_TOPIC_*` keys unless you intentionally want to subscribe to topics that will not match a route.

Incoming messages are dispatched round-robin into per-worker bounded channels via `IngestDispatcher`. Worker count is based on available CPU parallelism and clamped to 2 through 8.

If a worker queue is full, the message is dropped before the pipeline and `ingest_event_queue_full_total` is incremented. The DLQ is not used for this pre-pipeline drop.

## Pipeline

Each worker creates a `PipelineContext` and runs a fixed sequential pipeline:

| Order | Stage | Main behavior |
|---|---|---|
| 1 | `decode` | Enforces a 64 KiB payload limit, decodes UTF-8, parses JSON |
| 2 | `validate_raw` | Matches topic route, validates raw JSON Schema, validates `time_iso`, optionally checks topic device id |
| 3 | `transform` | Trims strings, computes derived sensor fields, fills status defaults, deserializes to canonical Rust structs |
| 4 | `validate_business` | Validates canonical messages against stricter business schemas |
| 5 | `cache_update` | Updates latest sensor cache for sensor messages only |
| 6 | `persist` | Renders the active sink's payload format via `Encoder` and appends it to the WAL |
| 7 | `observe` | Emits processed-message metrics |
| Failure | `dlq_publish` | Publishes DLQ JSON when a previous stage marks the context for DLQ |

Stages return `Continue` or `Stop`. Stage errors are converted into a DLQ reason by `PipelineRunner`, then the failure stage is invoked.

## Routing Model

Routes combine:

- A message type.
- An MQTT topic pattern.
- A raw JSON Schema.

Topic patterns support:

- Literal segments, such as `smarthome`.
- `+` for one topic segment.
- `#` for the rest of the topic, only at the end.

The first `+` segment is treated as the device id position for topic/payload matching. If there is no `+`, the pattern `smarthome/<device_id>/...` convention treats segment 2 as the device id.

## Data Model

The service uses two message families:

- `SensorMessage` for BME680-style telemetry.
- `StatusMessage` for device status and health information.

Raw schemas accept device payloads at the boundary. Business schemas validate the post-transform canonical form. See [Messages and Routing](messages-and-routing.md).

## Persistence Path

The persist stage renders canonical messages into the active sink's wire format via its `Encoder` (InfluxDB line protocol today; see [Output Sink](#output-sink) for how the format is chosen):

- Sensors use measurement `bme680`.
- Status messages use measurement `device_status`.

The rendered payload is stored in a `WalEvent`:

```rust
pub struct WalEvent {
    pub topic: String,
    pub ts_ms: i64,
    pub payload: String,
}
```

`ts_ms` is the ingest time of the WAL event. The InfluxDB point timestamp comes from the payload `time_ms` only when `time_valid=true` and `time_ms > 0`; otherwise InfluxDB assigns server time.

## Output Sink

Output persistence is decoupled behind `Sink` and `Encoder` abstractions (`src/infrastructure/sink/mod.rs`), mirroring the `Source`/`DlqPublisher` pattern used for input:

- **`Sink` trait** — writes a batch of `WalEvent`s to the destination store. `write` returns a boxed `Send` future so the trait is object-safe (`Arc<dyn Sink>`); the WAL forwarder is generic over it and has no InfluxDB-specific knowledge.
- **`Encoder` trait** — turns a canonical `HandledMessage` into the wire-format payload a `WalEvent` carries. `encode` is a plain sync method (no `Box::pin` allocation) because it never performs I/O — it runs once per message on the pipeline's hot path, before the WAL append.
- **`build_output(cfg) -> Result<(Arc<dyn Encoder>, Arc<dyn Sink>)>`** — one factory call, matched on `Config::output_sink`, that builds a sink and its matching encoder together so they can never mismatch (same reasoning as `build_source`/`DlqPublisher`).
- **`InfluxSink` / `InfluxEncoder`** (`src/infrastructure/sink/influx.rs`) — the only implementation today. `InfluxSink` posts batched line protocol to InfluxDB v2's write endpoint with a bounded retry loop; `InfluxEncoder` renders `SensorMessage`/`StatusMessage` into line protocol via `Point`.

`OUTPUT_SINK` (env var) selects which sink `build_output()` constructs. Only `influx` is implemented; adding a future sink (e.g. Kafka) means adding an `OutputSinkKind` variant plus a matching `Sink`/`Encoder` pair — `main.rs`'s wiring does not need to change beyond the one `build_output` match arm.

`PersistStage` holds the active `Arc<dyn Encoder>` and calls it to render the payload before appending to the WAL, so the pipeline stage stays sink-agnostic — it never imports InfluxDB-specific code.

## Forwarding Path

The WAL forwarder reads `WalEntry` values from the subscription, buffers them, and writes batches to the sink. A flush is triggered by:

- `BATCH_SIZE` entries, or
- `FLUSH_INTERVAL_MS` with a non-empty batch, or
- WAL shutdown with a final partial batch.

After a successful sink write, the forwarder commits the WAL cursor up to the last entry's `offset_after`. Permanent sink failures are dropped and committed. Retryable failures hold the batch and do not advance the cursor.

## Shutdown

On Ctrl+C, HTTP task failure, or a fatal input source error, the service:

1. Signals workers (and the input source) to stop accepting new work.
2. Lets workers drain queued jobs.
3. Drops the pipeline and WAL handles so the writer flushes and closes.
4. Waits up to 5 seconds for the forwarder to drain its final batch.
5. Aborts the forwarder if it cannot drain in time.

This avoids intentionally dropping queued messages during normal shutdown, but it is still bounded by the 5 second final forwarder drain timeout. A fatal input source error (e.g. broker unreachable) still exits the process non-zero, but only after this drain sequence runs — it flows through the same shutdown path as Ctrl+C rather than exiting immediately.
