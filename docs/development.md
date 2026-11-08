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

## Add a calculation

Calculations are a closed, stateless registry in `src/calculations.rs`, not plugins.
`CalculationRegistry::resolve` checks configured names, mapped input types, duplicate
calculations, and output-field collisions when the route bundle is compiled.
`CalculationRegistry::apply` runs the resolved sequence on each record. The inline tests
are in `src/calculations.rs` under `#[cfg(test)] mod tests`.

The current calculations use these canonical field names:

| Route name | Required mapped inputs | Output fields | Domain |
|---|---|---|---|
| `dew_point` | `temp_c` float in °C; `rel_hum_perc` float in percent | `dew_point_c` float in °C | Finite temperature; humidity must be in `(0, 100]`; output must be finite |
| `heat_index` | `temp_c` float in °C; `rel_hum_perc` float in percent | `heat_index_c` float in °C | Finite temperature; humidity must be in `(0, 100]`; output must be finite |
| `iaq` | `gas_ohm` float in ohms; `rel_hum_perc` float in percent | `iaq_score` float and `iaq_text` string | Gas must be positive; humidity must be in `(0, 100]`; output must be finite |

`iaq` clamps its gas contribution to 5,000–50,000 ohms. If the optional mapped
`warmed_up` boolean is `false`, the calculation still validates its inputs but omits
both IAQ outputs; if the field is absent, IAQ is calculated. Calculations do not
accept parameters or carry state between messages.

The current focused tests include
`test_calculation_resolve_requires_expected_mapping_types`,
`test_calculation_apply_rejects_invalid_humidity`, and
`test_calculation_apply_omits_iaq_when_not_warmed_up`.

To add a calculation, make the smallest matching edits in `src/calculations.rs`:
add its private enum variant and output field types, add its accepted route name and
input mappings in `CalculationRegistry::resolve`, and implement its checked operation
in `CalculationRegistry::apply`. Add the public route spelling to `CalculationName` in
`src/config/routes.rs`. Add focused inline tests for resolution, mapped types, domains,
outputs, and collision/error cases. Exercise a real route through existing preflight
and pipeline tests. Update the synthetic bundle/docs only when they help explain the
new calculation. Do not add dynamic loading, an expression language, or
history-dependent calculations.

## Test Layout

| File | Purpose |
|---|---|
| `tests/pipeline_end_to_end.rs` | Full pipeline happy paths and failure paths |
| `tests/http_cache.rs` | Cache HTTP API behavior |
| `tests/batcher.rs` | Influx sink and WAL forwarder behavior |
| `tests/common/mod.rs` | Shared integration test helpers |
| `tests/broker_integration.rs`, `tests/common/broker.rs` | Isolated real EMQX/InfluxDB process driver and baseline probes |

Many modules also contain unit tests next to implementation code.

## Run the isolated broker integration tests

These tests need Docker's Linux engine. They never use the Server stack, a Zigbee adapter,
production credentials, or production journals. The container tests are `#[ignore]`d in
ordinary runs; the synthetic route/oracle check still runs without Docker.

From the repository root, on Windows:

```powershell
docker compose -f .\tests\compose.integration.yaml -p ingest-it up -d --wait
cargo test --all-features --locked --test broker_integration -- --ignored --test-threads=1
```

The firmware baseline selector is `test_firmware_sensor_happy_path`. The suite also covers
typed status/Zigbee output, firmware SSE, arbitrary-topic contract changes, three crash
windows repeated three times, stale receipts/queued ACKs with packet-ID reuse, tiny queues,
journal capacity and write/sync failures, durable rejection, fail-closed reader gaps,
resumed A-to-B subscriptions, broker backlog restart, and sink outage/auth/missing-bucket
recovery. A mixed type-conflict batch demonstrates partial Influx acceptance without cursor
advance and replays the whole batch into a fresh scenario-owned bucket. No existing bucket,
journal, or quarantine record is deleted to repair it.

`ingest-it` is a local example, not a shared CI project. For simultaneous runs use separate
projects and set `INGEST_IT_PROJECT` to the exact project used by Compose:

```powershell
$env:INGEST_IT_PROJECT = "ingest-it-local-35"
docker compose -f .\tests\compose.integration.yaml -p $env:INGEST_IT_PROJECT up -d --wait
cargo test --all-features --locked --test broker_integration -- --ignored --test-threads=1
```

Linux/CI uses the same test command, `export INGEST_IT_PROJECT=ingest-it-<unique-run>`,
and `docker compose -f ./tests/compose.integration.yaml -p "$INGEST_IT_PROJECT" up -d --wait`.
The integration job uses the GitHub run ID and attempt. Keep `--test-threads=1`: probes within
one project share its broker and can restart it.

All published ports bind to `127.0.0.1` and default to Docker-assigned ports, discovered using
`docker compose port`. Optional `INGEST_IT_MQTT_PORT`, `INGEST_IT_EMQX_API_PORT`, and
`INGEST_IT_INFLUX_PORT` overrides select unused host ports on Windows or Linux. Each ingest
process gets reserved ephemeral HTTP/metrics ports and a unique stable MQTT client ID.
The restart driver recreates only the test EMQX container with its discovered ports fixed,
preserving named volumes and the broker identity stored in the subscription manifest.

The stack uses the deployment image tags `emqx/emqx:6.3.1` and `influxdb:2.7`; the latter
is a floating patch tag, not an immutable digest. Keep these aligned when deployment tags change.
Credentials in this fixture are deliberately synthetic and valid only for this loopback test stack.
The MQTT test user is created through the synthetic management API key.

EMQX durable sessions are explicitly enabled. The driver records the actual broker
version/edition, durable settings and client state, and rejects non-durable sessions rather
than substituting a different broker or assuming `clean_session=false` implies persistence.
The single-node Community License in the pinned image supports the internal test deployment.
Session expiry is one hour and inflight is 16. The configured regular-session queue limit is
1,000; it does **not** cap durable-session disk backlog. Effective message expiry and durable
retention are recorded in `broker-settings.txt` (currently infinity and one day respectively).
These test settings do not establish the deployed broker's persistence, publisher QoS,
outage budget, filesystem quota, or ARM64 capacity.

The generated bundle reuses `config/routes.json`, its schemas, and the existing synthetic
Zigbee fixture. Only test mappings add a scalar string `test_id`; no production per-event tags
are introduced. Recovery publications use distinct device identities or millisecond timestamps
and compare actual output/quarantine ID sets, allowing duplicates but failing omissions or
unexpected IDs. The separate collision test deliberately shares a point identity and verifies
field merge plus last-value overwrite; Influx is not an event ledger. QoS 0 is observed as a
durable policy rejection, not claimed replayable; the retained snapshot test clears only its
own retained topic after stopping ingest.

Publisher PUBACK proves broker receipt only. The debug test build traces ingest's incoming
packet IDs/connection generations and outgoing subscriber PUBACK **after rumqttc's transport
flush**, then waits for actual EMQX inflight state to clear. Trace output contains no payloads
or credentials and is capped at 64 KiB. Workers still execute concurrently; ACK selection
uses the earliest pending receive job ID, as MQTT 3.1.1 requires receive-order PUBACKs even
when completions arrive out of order. This scans existing pending state without adding
another queue or copying payloads; later ACKs wait for the earliest durable completion.
Three file barriers prepare deterministic crash probes:
`before_append`, `after_sync` (before ACK eligibility), and `before_sink`. The sink barrier
can hold writes while the source sends its ACK; the driver observes that ACK before proceeding.
Additional barriers cover quarantine, reading, writer I/O and ACK queueing. A `<name>.arm`
file arms a barrier and `<name>.release` releases it; `<journal>_<step>.fail` injects a
write, sync or deletion failure, and `dlq.fail` disables supplementary publication.
Barrier waits expire rather than hang indefinitely.

The shutdown test reduces only the debug build's shared MQTT request capacity to two using
`INGEST_TEST_MQTT_REQUEST_CAPACITY` (bounded to 1..10). It first establishes subscriptions
and queues five QoS 1 inputs while ingest is stopped; this avoids assuming live durable-stream
prefetch advances while all PUBACKs are held. It pauses transport polling after those five
admissions with `INGEST_TEST_DISPATCH_BARRIER_AT`, fills the queue with supplementary DLQ
requests, and observes a waiting ACK before signaling `shutdown.request`. The ordinary
shutdown path must exit successfully within 15 seconds and broker replay must recover all IDs.
This does not claim saturation of the separate 100-entry receipt channel.

Hooks compile only with **both** `integration-test-hooks` and debug assertions.
`--all-features` enables them for these debug tests; normal builds without the feature and all
release builds omit the hooks, even with `--all-features`. There is no production fault-injection
HTTP endpoint. Process termination/restart probes are not evidence of physical power-loss durability.

On failure the tracked ingest process is killed/reaped and its generated bundle, journals,
broker settings/client snapshots, bounded logs and packet trace remain under
`target/broker-integration/ingest-it-*`. Successful scenarios preserve bounded broker settings,
queries, trace, logs, metrics when the process is live, journal metadata and available cursor
files under `target/broker-integration/passed-*`, then remove their disposable process state
and their own persistent MQTT session. CI uploads evidence on both success and failure.
The external Compose stack remains available for inspection.
After a successful local run, remove **only that test project**:

```powershell
docker compose -f .\tests\compose.integration.yaml -p ingest-it down -v --remove-orphans
```

For a custom project use its exact `-p` value. On failure retain diagnostic files and volumes;
`down` without `-v` stops that project's containers without deleting its named test volumes.
Never point this command at the Server project or production volume names.

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

The current Influx encoder renders borrowed `TelemetryRecord` values directly through
`write_line_protocol` in `src/infrastructure/sink/point.rs`, without an owned intermediate point.

Rules:

- Tags and fields render in their existing `BTreeMap` key order without copying or sorting.
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

The CI workflow runs on every pull request and on pushes to `master`. CD reuses
it to validate the exact source revision selected for release or rollback.

Steps:

1. Checkout.
2. Install Rust `1.87.0` with `rustfmt` and `clippy`.
3. Cache Cargo artifacts.
4. Run `cargo fmt --all --check`.
5. Run `cargo clippy --all-targets --all-features --locked -- -D warnings`.
6. Run `cargo test --all-features --locked`.

Two parallel jobs run the isolated EMQX/InfluxDB tests and validate Compose plus
its environment contract against `src/config.rs`. Historical revisions called
by CD run the broker suite only if their source includes its complete harness.

## CD

The CD workflow:

- Validates the exact source revision before building or redeploying an ARM64 image.
- Publishes a version tag and full-source-SHA tag, then deploys the registry digest;
  it does not publish or deploy `latest` or short SHA tags.
- Snapshots and preflights the release revision's repository config bundle with the candidate image,
  waits for durable-intake readiness, and restores the previous image/config pair on
  failure without removing WAL or quarantine volumes.

See [Releasing and Deploying](releasing.md) and the remotely accessible
[Epic #23](https://github.com/hengsti/ingestion-service/issues/23).
