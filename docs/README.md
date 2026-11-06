# smarthome-ingest Documentation

`smarthome-ingest` is a Rust service that consumes smart home telemetry from a configurable input source (MQTT today), validates and normalizes it, stores pre-rendered sink payloads in a local write-ahead log (WAL), and forwards batches to a configurable output sink (InfluxDB v2 today). Invalid messages are published to a DLQ on the active input source. The latest sensor values are also exposed through HTTP and Server-Sent Events (SSE).

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
| `src/main.rs` | Runtime wiring: config, input source, workers, pipeline, WAL, output sink, HTTP, metrics, shutdown |
| `src/config.rs` | Environment variable parsing and validation |
| `src/model/` | MQTT topic matching and typed telemetry records |
| `src/pipeline/` | Sequential processing pipeline and stages |
| `src/infrastructure/source/` | `Source`/`DlqPublisher` traits and the MQTT implementation |
| `src/infrastructure/router.rs` | Route bundle compilation, topic filters, schemas, mapping, and calculations |
| `src/infrastructure/schema.rs` | JSON Schema loading and validation from the configured route bundle |
| `src/infrastructure/cache/` | Latest sensor state cache and HTTP/SSE API |
| `src/infrastructure/sink/` | `Sink`/`Encoder` traits, the InfluxDB implementation, and the line protocol point builder |
| `src/infrastructure/wal/` | WAL writer, reader subscription, cursor, recovery, and forwarder |
| `config/routes.json` and `config/schemas/` | Shipped firmware routes and their runtime-loaded raw/output schemas |
| `config/examples/climate/` | Synthetic copyable route, schemas, and environment reference |
| `tests/` | Integration tests for pipeline, cache API, batching, and sink forwarding |
| `.github/workflows/` | CI and CD workflows |

## Runtime Flow

```text
Input source (MQTT today)
  -> bounded worker queues
  -> decode
  -> raw validation
  -> typed mapping/calculations
  -> output validation
  -> optional cache update
  -> synced WAL append (payload rendered by the active sink's Encoder)
  -> metrics observation
  -> WAL forwarder
  -> output sink (InfluxDB v2 today)
```

Deterministic invalid input is durably quarantined before a supplementary DLQ publication.
Operational failures are deferred, not rejected. When a worker queue is full, admission is
backpressured and the MQTT source withholds the acknowledgement; this is not a drop counter.
QoS 1 redelivery remains bounded by broker persistence, expiry, queue capacity, and local
journal capacity.

## Route examples and roadmap

- [Synthetic climate route walkthrough](../config/examples/climate/README.md)
- [Shipped firmware routes](../config/routes.json)
- [Epic #23](https://github.com/hengsti/ingestion-service/issues/23) and its
  [unified route contract #19](https://github.com/hengsti/ingestion-service/issues/19)

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
