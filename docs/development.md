# Development Guide

This guide helps contributors modify the Rust code safely.

## Toolchain

The package uses Rust edition `2021`. CI installs Rust `1.87.0` with `rustfmt` and `clippy`.

Recommended local checks:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

Build release binary:

```bash
cargo build --release
```

Generate Rust API docs:

```bash
cargo doc --all-features --no-deps
```

## Module Guide

| Area | Files | Notes |
|---|---|---|
| Runtime wiring | `src/main.rs` | Owns task startup, MQTT loop, worker dispatch, shutdown |
| Config | `src/config.rs` | Environment parsing and defaults |
| Messages | `src/model/messages/` | Canonical Rust structs |
| Topic matching | `src/model/topic.rs` | MQTT wildcard matching and device id extraction |
| Pipeline | `src/pipeline/` | Stage trait, runner, context, and stage implementations |
| Routing/schema | `src/infrastructure/router.rs`, `src/infrastructure/schema.rs` | Topic route and JSON Schema validation |
| Cache API | `src/infrastructure/cache/` | Latest sensor state and HTTP/SSE API |
| Influx mapping | `src/infrastructure/database/` | Line protocol point builder |
| Sink | `src/infrastructure/sink/` | InfluxDB write API and retry classification |
| WAL | `src/infrastructure/wal/` | Segment writer, subscription, cursor, recovery, forwarder |

## Test Layout

| File | Purpose |
|---|---|
| `tests/pipeline_end_to_end.rs` | Full pipeline happy paths and failure paths |
| `tests/http_cache.rs` | Cache HTTP API behavior |
| `tests/batcher.rs` | Influx sink and WAL forwarder behavior |
| `tests/common/mod.rs` | Shared integration test helpers |

Many modules also contain unit tests next to implementation code.

## Add a Pipeline Stage

1. Add a new stage module under `src/pipeline/stages/`.
2. Implement `PipelineStage`.
3. Use `PipelineContext` for shared state. Prefer adding explicit getters/setters over reaching around stage ordering.
4. Return `StageFlow::Continue` for success and `StageFlow::Stop` when the pipeline should stop.
5. To route a rejected message to the DLQ, call `ctx.mark_dlq(...)`.
6. To stop without DLQ, call `ctx.mark_ignored(...)`.
7. Register the stage in `main.rs` and in integration test pipeline builders.
8. Add unit tests for stage behavior and an integration test if it changes end-to-end behavior.

## Add a Message Type

1. Add a raw schema in `schema/<name>.schema.json`.
2. Add a business schema in `schema/<name>.business.schema.json`.
3. Add Rust message structs under `src/model/messages/`.
4. Add a variant to `MessageType` and `HandledMessage`.
5. Update `build_router` in `main.rs` to map `MQTT_TOPIC_<NAME>` to the new type and schema.
6. Update `Route::deserialize`.
7. Update `TransformStage` if normalization or derived fields are needed.
8. Update `ValidateBusinessStage` to load and apply the business schema.
9. Update `CacheUpdateStage` if the new type should appear in the cache API.
10. Update `PersistStage` and `src/infrastructure/database/influx.rs` to render line protocol.
11. Add tests for routing, transform, business validation, persistence, and end-to-end behavior.
12. Update docs in `docs/messages-and-routing.md`, `docs/http-and-metrics.md`, and `docs/configuration.md`.

## Change Schemas

The schemas are embedded with `include_str!`, so schema files are compiled into the binary.

When changing schemas:

- Update raw schemas for publisher input contracts.
- Update business schemas for canonical post-transform contracts.
- Keep transform output compatible with business schemas.
- Run full tests because schema failures often surface in end-to-end tests.

## Change WAL Behavior

WAL changes are high risk because they affect replay and data loss behavior.

Before changing WAL code, identify which guarantee is affected:

- Append acknowledgement boundary.
- Segment rotation.
- Torn tail recovery.
- Cursor commit atomicity.
- Segment garbage collection.
- Retryable versus permanent sink behavior.
- Shutdown drain.

Add or update tests in the relevant WAL module and `tests/batcher.rs`.

## Change InfluxDB Mapping

Influx line protocol is built through `PointBuilder`.

Rules:

- Tags and fields are sorted by key before rendering.
- Measurement, tag keys, tag values, and field keys are escaped.
- String fields are quoted and escaped.
- Integers are suffixed with `i`.
- Unsigned integers are suffixed with `u`.
- Timestamp is in milliseconds because the sink uses `precision=ms`.

If you add or remove tags or fields, update:

- Unit tests for point rendering or persistence.
- Integration tests that assert line protocol contents.
- `docs/messages-and-routing.md`.

## CI

The CI workflow runs on pushes and pull requests targeting `master`.

Steps:

1. Checkout.
2. Install Rust `1.87.0` with `rustfmt` and `clippy`.
3. Cache Cargo artifacts.
4. Run `cargo fmt --all --check`.
5. Run `cargo clippy --all-targets --all-features --locked -- -D warnings`.
6. Run `cargo test --all-features --locked`.

## CD

The CD workflow:

- Builds and pushes ARM64 Docker images to GHCR on SemVer tags or manual dispatch.
- Uses tags `latest`, `vX.Y.Z`, or `sha-<12hex>`.
- Deploys the `ingest` service on the self-hosted ARM64 runner.

See [Releasing and Deploying](releasing.md).
