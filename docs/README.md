# smarthome-ingest Documentation

`smarthome-ingest` is a Rust service that consumes smart home telemetry from MQTT, validates and normalizes it, stores pre-rendered InfluxDB line protocol in a local write-ahead log (WAL), and forwards batches to InfluxDB v2. Invalid messages are published to an MQTT dead letter queue (DLQ). The latest sensor values are also exposed through HTTP and Server-Sent Events (SSE).

This documentation follows the Diataxis model:

| Need | Document |
|---|---|
| Learn the service by running it | [Tutorial: First Local Run](tutorial.md) |
| Understand the design | [Architecture](architecture.md) and [WAL and Reliability](wal-and-reliability.md) |
| Configure or operate the service | [Configuration Reference](configuration.md), [Operations Guide](operations.md), and [Releasing and Deploying](releasing.md) |
| Integrate publishers or consumers | [Messages and Routing](messages-and-routing.md) and [HTTP API and Metrics](http-and-metrics.md) |
| Change the Rust code safely | [Development Guide](development.md) |

## Audience

These docs are written for:

- Rust developers maintaining the service.
- Operators deploying the service next to MQTT, InfluxDB, Prometheus, or Telegraf.
- Firmware or integration developers publishing compatible MQTT payloads.

## Project Map

| Path | Purpose |
|---|---|
| `src/main.rs` | Runtime wiring: config, MQTT client, workers, pipeline, WAL, HTTP, metrics, shutdown |
| `src/config.rs` | Environment variable parsing and validation |
| `src/model/` | MQTT topic matching and canonical message structs |
| `src/pipeline/` | Sequential processing pipeline and stages |
| `src/infrastructure/router.rs` | Topic to schema and message type routing |
| `src/infrastructure/schema.rs` | Embedded JSON Schema compilation and validation |
| `src/infrastructure/cache/` | Latest sensor state cache and HTTP/SSE API |
| `src/infrastructure/database/` | InfluxDB line protocol point builder |
| `src/infrastructure/sink/` | InfluxDB v2 write sink and sink error classification |
| `src/infrastructure/wal/` | WAL writer, reader subscription, cursor, recovery, and forwarder |
| `schema/` | Raw and business JSON Schemas embedded at compile time |
| `tests/` | Integration tests for pipeline, cache API, batching, and Influx forwarding |
| `.github/workflows/` | CI and CD workflows |

## Runtime Flow

```text
MQTT broker
  -> bounded worker queues
  -> decode
  -> raw validation
  -> transform
  -> business validation
  -> cache update
  -> WAL append
  -> metrics observation
  -> WAL forwarder
  -> InfluxDB v2
```

Failures before persistence are routed to the configured MQTT DLQ when a pipeline stage marks the message for DLQ. Messages dropped before entering the pipeline, for example because a worker queue is full, are counted in metrics but are not published to the DLQ.

## Rust Compatibility

The project uses Rust edition `2021`. CI currently validates the code with Rust `1.87.0`, `rustfmt`, `clippy`, and the locked dependency graph.

Use these commands before submitting changes:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

Generate Rust API documentation locally with:

```bash
cargo doc --all-features --no-deps
```
