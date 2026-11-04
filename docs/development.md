# Development Guide

Contributor notes for changing the Rust service safely.

## Toolchain

The package uses Rust edition `2021`. CI installs Rust `1.87.0` with `rustfmt` and `clippy`.

Recommended local checks:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

`tests/influx_timestamp_semantics.rs` is skipped unless all four `INFLUX_TEST_URL`,
`INFLUX_TEST_ORG`, `INFLUX_TEST_BUCKET`, and `INFLUX_TEST_TOKEN` variables are explicitly
set. It writes to a unique measurement in that test bucket.

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
| Runtime wiring | `src/main.rs` | Owns task startup, source/sink wiring, worker dispatch, shutdown |
| Config | `src/config.rs` | Environment parsing and defaults |
| Telemetry | `src/model/messages/` | Typed records and scalar values |
| Topic matching | `src/model/topic.rs` | Strict MQTT filter matching |
| Pipeline | `src/pipeline/` | Stage trait, runner, context, and stage implementations |
| Calculations | `src/calculations.rs` | Route-compiled calculation registry and implementations |
| Routing/schema | `src/infrastructure/router.rs`, `src/infrastructure/schema.rs` | Topic route and JSON Schema validation |
| Input source | `src/infrastructure/source/` | `Source`/`DlqPublisher`, MQTT implementation, and ingest dispatch |
| Cache API | `src/infrastructure/cache/` | Configured latest-state cache and HTTP/SSE API |
| Output sink | `src/infrastructure/sink/` | `Sink`/`Encoder`, Influx sink, and line-protocol point builder |
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
5. To route deterministic invalid input to quarantine, call `ctx.mark_rejection(...)`.
6. Operational failures are deferred, not rejected. Return `StageFlow::Stop` only with
   an explicit rejection or deferred disposition; an unmarked stop is deferred.
7. Register the stage in `main.rs` and in integration test pipeline builders.
8. Add unit tests for stage behavior and an integration test if it changes end-to-end behavior.

## Add a telemetry route

1. Add or update raw/output schemas under `config/schemas/`.
2. Add a route to `config/routes.json` with its topic filter, identity source,
   measurement, mappings, timestamp, and optional calculations/cache setting.
3. Use schema paths relative to the route bundle. The bundle is compiled at startup;
   invalid schemas, duplicate route IDs, overlapping filters, conflicting names,
   control characters in static output text, and incompatible field types fail startup.
4. Add a focused route test and a pipeline integration case for the new payload.
5. Update the cache/API or line-protocol goldens if the configured route changes them.
6. Document the route contract in `docs/messages-and-routing.md`.

## Change Schemas

Route schemas are loaded from `INGEST_ROUTES_FILE` at startup. Raw schemas validate publisher
input; optional output schemas validate the normalized typed record before persistence.
Relative schema/resource paths are resolved from the route bundle. Run the full suite after
changing schemas because failures often surface in end-to-end tests.

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

The current Influx encoder builds line protocol through `PointBuilder` in `src/infrastructure/sink/point.rs`.

Rules:

- Tags and fields are sorted by key before rendering.
- Measurement, tag keys, tag values, and field keys are escaped.
- String fields are quoted and escaped.
- Integers are suffixed with `i`.
- Unsigned integers are suffixed with `u`.
- Every validated record timestamp is written in milliseconds because the sink uses
  `precision=ms`; records never fall back to InfluxDB server time.
- `Encoder::encoded_size_upper_bound` must bound all bytes appended by `encode`,
  including escaping, and saturate on overflow. Output validation checks this before
  caching or encoding; persistence also checks the actual encoded size.

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
