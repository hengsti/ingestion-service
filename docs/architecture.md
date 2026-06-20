# Architecture

This document explains how the service is put together and why each boundary exists.

## Responsibilities

`smarthome-ingest` owns the ingestion path between MQTT and InfluxDB:

- Subscribe to configured MQTT telemetry topics.
- Decode and validate JSON payloads.
- Normalize payloads into canonical Rust message structs.
- Compute derived sensor fields.
- Update an in-memory latest sensor cache.
- Render InfluxDB line protocol.
- Store line protocol in a local WAL before forwarding.
- Publish rejected payloads to a DLQ topic.
- Export HTTP state and Prometheus metrics.

The service does not own MQTT broker lifecycle, InfluxDB lifecycle, device firmware, dashboards, or long-term query APIs.

## Runtime Components

```text
Config
  -> MQTT options and topic map
  -> Router with embedded schemas
  -> Cache state
  -> Metrics server
  -> WAL and WAL subscription
  -> Influx sink
  -> Pipeline runner
```

`main.rs` wires these components together. It starts the cache API, metrics API, MQTT event loop, WAL forwarder, and worker pool.

## MQTT Ingestion

The service reads all environment variables whose names start with `MQTT_TOPIC_`.

- `MQTT_TOPIC_SENSOR` maps to `MessageType::Sensor`.
- `MQTT_TOPIC_STATUS` maps to `MessageType::Status`.
- `MQTT_TOPIC_DLQ` is used only for DLQ publishing.

The MQTT client subscribes to every configured `MQTT_TOPIC_*` value except keys ending in `DLQ`. The router only creates routes for `SENSOR` and `STATUS`. Avoid unknown non-DLQ `MQTT_TOPIC_*` keys unless you intentionally want to subscribe to topics that will not match a route.

Incoming MQTT publishes are dispatched round-robin into per-worker bounded channels. Worker count is based on available CPU parallelism and clamped to 2 through 8.

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
| 6 | `persist` | Renders InfluxDB line protocol and appends it to the WAL |
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

The persist stage converts canonical messages into InfluxDB line protocol:

- Sensors use measurement `bme680`.
- Status messages use measurement `device_status`.

The rendered line is stored in a `WalEvent`:

```rust
pub struct WalEvent {
    pub topic: String,
    pub ts_ms: i64,
    pub line_protocol: String,
}
```

`ts_ms` is the ingest time of the WAL event. The InfluxDB point timestamp comes from the payload `time_ms` only when `time_valid=true` and `time_ms > 0`; otherwise InfluxDB assigns server time.

## Forwarding Path

The WAL forwarder reads `WalEntry` values from the subscription, buffers them, and writes batches to the sink. A flush is triggered by:

- `BATCH_SIZE` entries, or
- `FLUSH_INTERVAL_MS` with a non-empty batch, or
- WAL shutdown with a final partial batch.

After a successful sink write, the forwarder commits the WAL cursor up to the last entry's `offset_after`. Permanent sink failures are dropped and committed. Retryable failures hold the batch and do not advance the cursor.

## Shutdown

On Ctrl+C or HTTP task failure, the service:

1. Signals workers to stop accepting new work.
2. Lets workers drain queued jobs.
3. Drops the pipeline and WAL handles so the writer flushes and closes.
4. Waits up to 5 seconds for the forwarder to drain its final batch.
5. Aborts the forwarder if it cannot drain in time.

This avoids intentionally dropping queued messages during normal shutdown, but it is still bounded by the 5 second final forwarder drain timeout.
