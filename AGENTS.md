# Agent guide: smarthome-ingest

This guide applies to the repository and its subdirectories. Check for more-specific
`AGENTS.md` files before editing. Use source code, tests, and current CI configuration
to verify behavior; older explanatory docs can drift.

## Priorities

**Do not overengineer. Optimize for high performance, low latency, and low memory usage.**

- Make the smallest change that fully solves the requested problem. Avoid speculative
  abstractions, unused extension points, unrelated refactors, and unnecessary dependencies.
- Read the affected implementation, callers, and tests before editing. Use Rust
  language-server navigation and version-appropriate Rust/crate documentation when available.
- Preserve bounded memory and concurrency. Avoid extra channel hops, payload copies,
  per-message allocations, and blocking work on async runtime threads.
- Explain performance tradeoffs; do not claim an optimization without evidence.

## Application and code map

A Rust 2021 service that ingests smart-home telemetry, validates and transforms it,
stages encoded output in a local write-ahead log (WAL), and forwards batches to a sink.
MQTT and InfluxDB v2 are the only implemented input/output pair today. Axum serves
latest sensor state over HTTP/SSE; Prometheus metrics and JSON tracing expose operation.
Broker/database administration, device firmware, and dashboards are outside this repo.

| Area | Start here |
| --- | --- |
| Startup, worker pool, shutdown, route registration | `src\main.rs` |
| Environment parsing and source/sink selection | `src\config.rs` |
| Input transport, dispatch, DLQ publishing | `src\infrastructure\source\mod.rs`, `src\infrastructure\source\mqtt.rs` |
| Stage interface, context, runner, implementations | `src\pipeline\stage.rs`, `src\pipeline\context.rs`, `src\pipeline\runner.rs`, `src\pipeline\stages` |
| Message types, topic matching, routing, schemas | `src\model`, `src\infrastructure\router.rs`, `src\infrastructure\schema.rs`, `schema` |
| Encoding, sink errors, InfluxDB line protocol | `src\infrastructure\sink\mod.rs`, `src\infrastructure\sink\influx.rs`, `src\infrastructure\sink\point.rs` |
| WAL append, writer thread, recovery, cursor, forwarding | `src\infrastructure\wal` |
| Latest sensor cache, HTTP/SSE, metrics | `src\infrastructure\cache`, `src\infrastructure\prometheus.rs` |
| Integration tests and reusable fixtures | `tests`, `tests\common\mod.rs` |
| Build/deployment contracts | `Cargo.toml`, `Cargo.lock`, `Dockerfile`, `compose.yaml`, `.github\workflows` |

```text
Source -> IngestDispatcher -> bounded worker queues -> Pipeline
Pipeline: decode -> validate_raw -> transform -> validate_business
          -> cache_update -> persist -> observe
persist: Encoder -> WAL append -> WAL forwarder -> Sink
Context marked for DLQ -> failure stage -> active source's DlqPublisher
```

Stages run sequentially for each message, but workers run concurrently. Round-robin
dispatch does not guarantee per-device processing order across workers.

## Contracts to preserve

- **Validate before parsing:** enforce the 64 KiB input limit before UTF-8/JSON decoding.
  Raw schemas validate publisher input; business schemas validate canonical transformed
  messages. Schemas are embedded with `include_str!`, so changes require recompilation.
- **Stop is not rejection:** `Err` from a stage makes `PipelineRunner` mark the context
  for DLQ. `StageFlow::Stop` only stops execution; it does not automatically mark DLQ.
  Use `ctx.mark_dlq(reason)` for explicit rejection, or `ctx.mark_ignored(reason)` for
  intentional non-DLQ stops. A DLQ publish failure is logged, not recursively republished.
- **Context carries stage state:** use `PipelineContext` accessors and propagate missing
  state errors with `?`. Do not bypass validation or silently synthesize missing values.
- **Transport boundaries stay paired:** `build_source` constructs `Source` and
  `DlqPublisher` together; `build_output` constructs `Encoder` and `Sink` together.
  Keep concrete MQTT/InfluxDB dependencies out of generic pipeline and WAL code.
  `Encoder::encode` remains synchronous and performs no I/O.
- **WAL acknowledgement is a flush boundary:** `PersistStage` must await
  `Wal::append_durable`, not substitute `try_append`. Acknowledgement means
  `BufWriter::flush`, not `fsync`/power-loss durability. WAL records contain already
  encoded sink payloads, not raw input JSON; account for queued records when changing
  encoders, sink formats, or on-disk serialization.
- **Replay and commit are coupled:** both retryable and permanent sink errors retain
  the batch without advancing the cursor. Only successful sink writes advance through
  `offset_after`. Retry cursor commits without repeating an already-completed sink
  write. A crash before commit can replay writes:
  do not claim exactly-once or lossless end-to-end delivery.
- **Overflow paths differ:** a full input worker queue drops the message before the
  pipeline, increments `ingest_event_queue_full_total`, and does not publish DLQ.
  WAL append failures inside `PersistStage` do mark DLQ. Do not silently change either
  policy or replace bounded queues with unbounded buffering.
- **Cache is not a delivery receipt:** only sensor messages update the latest-state
  cache, and that update occurs before WAL persistence. Preserve HTTP/SSE contracts;
  cached state does not prove successful persistence or sink delivery.
- **Shutdown must drain:** preserve queued-worker draining and the forwarder's final
  partial-batch flush. WAL recovery, cursor commits, and segment deletion must not
  discard uncommitted data.

## Rust conventions and change boundaries

- Follow neighboring code and rustfmt defaults. Group imports as standard library,
  external crates, then internal modules. Getter names omit `get_`.
- Async object-safe traits use explicit `Pin<Box<dyn Future<...> + Send + 'a>>` and
  `Box::pin(async move { ... })`; do not introduce `async_trait` just for a new stage.
  Keep the consuming builder style used by `PipelineRunner` and `Router`.
- Reuse existing error types; use `anyhow::Result` and `.context`/`.with_context` for
  service errors. Prefer `?` over production `unwrap()`. Document new public APIs,
  including `# Errors` and `# Panics` where applicable. Do not add unsafe service logic.
- Keep lock scopes short and never hold a blocking lock across `.await`. Preserve the
  WAL writer's dedicated thread and existing blocking-I/O boundaries.
- New stages must be registered in both `src\main.rs` and test pipeline builders.
  Message changes must keep Rust models, raw/business schemas, transforms, routing,
  encoders, and relevant cache/API contracts consistent. See `docs\development.md`.
- Do not casually change persisted formats, external payloads, retry classifications,
  or configuration defaults. Make compatibility implications explicit and cover the
  changed behavior with focused tests. Keep dependency and lockfile edits scoped.

## Commands and tests

Run commands from the repository root. CI and Docker use **Rust 1.87.0**; keep changes
compatible with that toolchain rather than relying on a newer local compiler.

```text
cargo build --locked
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

Use `cargo fmt --all` to apply formatting. Iterate with the smallest relevant test
selection, then run the fmt, clippy, and full-test commands above for Rust changes.
Schema changes also require the full test suite. Documentation-only changes need
content/path review, not a Rust build.

| Changed behavior | Targeted command |
| --- | --- |
| Pipeline, validation, transformation, persistence | `cargo test --all-features --locked --test pipeline_end_to_end` |
| HTTP/cache API | `cargo test --all-features --locked --test http_cache` |
| Sink writes and WAL forwarding | `cargo test --all-features --locked --test batcher` |
| One unit test or module | `cargo test --all-features --locked <test_name_or_module>` |

- Keep unit tests inline in `#[cfg(test)] mod tests`; integration tests belong in
  `tests`. Use descriptive behavior names, Arrange-Act-Assert, `#[tokio::test]` for
  async cases, and fallible test return values where practical.
- Reuse `tests\common\mod.rs`: `build_pipeline`, `build_router`, `open_temp_wal`,
  `valid_sensor_payload`, and `valid_status_payload`. Keep the returned `TempDir`
  alive for the whole test or its WAL files disappear.
- The shared DLQ client's event loop is intentionally dropped: publish warnings are
  expected, and these tests do not prove delivery to a broker. Sink integration tests
  use local mock HTTP servers. Prefer these fixtures to real external services.
- For WAL changes, cover the affected flush, recovery, rotation, commit, replay,
  overflow, or shutdown guarantee in WAL unit tests and relevant integration tests.

Release build: `cargo build --release --locked`. Container build:
`docker build -t smarthome-ingest .`. Local API documentation:
`cargo doc --all-features --no-deps`.

## Configuration, safety, and handoff

- The binary reads process environment through `Config::from_env`; it does not load
  `.env` itself. Most runtime settings are required. Existing defaults include the
  WAL segment/queue sizes, input queue capacity, and `MQTT_CLEAN_SESSION=true`.
  Compose supplies its own values; do not confuse them with code defaults.
- MQTT configuration populates generic `topic_routes` keyed by suffix
  (`SENSOR`, `STATUS`, `DLQ`). DLQ is required at startup and is not a subscription.
  Keep source-specific environment parsing out of the generic router.
- Change `src\config.rs`, `compose.yaml`, and `docs\configuration.md` together when
  changing the environment contract. CI renders Compose and checks its environment
  keys against the code; update that check when the contract requires it.
- The Docker healthcheck targets port 8085 and `/healthz`; keep `CACHE_BIND` compatible.
  Keep the WAL volume mount and `WAL_DIR` aligned. Use disposable WAL directories and
  separate MQTT client IDs for local experiments, never production state.
- Never expose tokens, passwords, raw sensitive payloads, or personal data in logs,
  fixtures, commits, or responses. Preserve `SecretString` handling and redacted
  configuration debug output. Use structured `tracing` fields and bounded metric
  labels; keep internal errors out of public API responses.
- Check worktree status and preserve unrelated edits. Do not commit, push, tag,
  deploy, or modify live services unless explicitly requested.
- Before handoff, review the diff for scope and accidental secrets, update affected
  documentation, and report the outcome and any blockers accurately. Never imply
  that an unrun check passed. Keep this guide current when its contracts change.

## Read more only as needed

| Need | Reference |
| --- | --- |
| Overview and documentation index | `readme.md`, `docs\README.md` |
| Runtime design and extension recipes | `docs\architecture.md`, `docs\development.md` |
| Environment contract and local startup | `docs\configuration.md`, `docs\tutorial.md` |
| Publisher payloads, HTTP/SSE, metrics | `docs\messages-and-routing.md`, `docs\http-and-metrics.md` |
| Durability, replay, recovery | `docs\wal-and-reliability.md` |
| Containers, operation, releases | `docs\operations.md`, `docs\releasing.md` |
