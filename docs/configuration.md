# Configuration Reference

Runtime settings are supplied through environment variables. Empty strings are treated as unset.
The required `INGEST_ROUTES_FILE` points to the versioned route bundle; route and schema paths
inside the bundle are resolved relative to the bundle file.

## Input source

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INPUT_SOURCE` | Yes | None | Selects the input transport. Only `mqtt` is implemented today (case-insensitive) |

The service ingests messages through a swappable `Source` abstraction (see `docs/architecture.md`). `INPUT_SOURCE` selects which implementation `build_source()` constructs at startup. Adding a new transport (e.g. Kafka) means adding a new `InputSourceKind` variant and a matching `Source`/`DlqPublisher` pair — no changes to `main.rs`'s wiring are required beyond that.

## Route bundle

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INGEST_ROUTES_FILE` | Yes | None | Path to the v1 JSON route bundle. Subscriptions, schemas, mapping, calculations, output validation, measurements, and cache opt-in are loaded from this file. |

The shipped bundle is [`config/routes.json`](../config/routes.json). It is compiled once at startup;
an empty route list or invalid route/schema fails startup. Schema-resource paths are relative to the
bundle's directory. Keep the bundle and its `schemas/` directory together when deploying. The
image includes them at `/app/config`; Compose can override them with a bind mount.

The bundled firmware sensor filter is `smarthome/+/bme680`. If publishers use a different
topic, configure the sensor route's `topic` to match.

The former `MQTT_TOPIC_SENSOR`, `MQTT_TOPIC_STATUS`, other non-DLQ `MQTT_TOPIC_*` variables, and
`ENFORCE_TOPIC_DEVICE_MATCH` are rejected at startup with a migration hint to use the route bundle.
Do not set them alongside `INGEST_ROUTES_FILE`.

## MQTT

The broker variables below are required only when `INPUT_SOURCE=mqtt`. MQTT subscriptions come
exclusively from the compiled route bundle. `MQTT_TOPIC_DLQ` remains required and is only used as
the publish destination for rejected messages; it is not a subscription.

| Variable | Required | Default | Description |
|---|---:|---|---|
| `MQTT_HOST` | Only if `INPUT_SOURCE=mqtt` | None | MQTT broker host |
| `MQTT_PORT` | Only if `INPUT_SOURCE=mqtt` | None | MQTT broker port as `u16` |
| `MQTT_CLIENT_ID` | Only if `INPUT_SOURCE=mqtt` | None | Stable client id used verbatim for the required persistent MQTT session |
| `MQTT_CLEAN_SESSION` | No | `false` | Boolean. `true` is rejected; durable intake requires a persistent session so the broker can queue QoS 1 messages while ingest is offline |
| `MQTT_USERNAME` | No | None | Optional MQTT username |
| `MQTT_PASSWORD` | No | None | Optional MQTT password |
| `MQTT_TOPIC_DLQ` | Yes at runtime | None | DLQ publish topic |

The MQTT client enforces a fixed 128 KiB maximum wire-packet size, including packet headers,
topic, and payload. The application rejects payloads larger than 64 KiB before JSON decoding;
messages between these limits can be received and durably quarantined as oversized. Packets
above the MQTT wire limit are outside the local quarantine guarantee.

With `MQTT_CLEAN_SESSION=false`, a client id must be used by only one ingest instance at a time. A second instance with the same id takes over the session from the first.

The source reconciles the route bundle's filters at startup and after reconnect. A synced,
two-slot subscription manifest in `WAL_DIR` records the broker/client identity and known filters;
each slot is limited to 16 KiB within the WAL metadata reserve. The
service persists the union of known and desired filters before changing the broker session, then
reports ready only after the current filters receive QoS 1 grants and obsolete filters are
acknowledged as unsubscribed. Messages already queued or in flight for removed filters can still
arrive; unmatched messages are durably rejected and may also be published to the DLQ.

MQTT 3.1.1 cannot enumerate subscriptions in a resumed session. If a persistent session predates
the manifest, startup fails closed until an operator verifies and supplies the old filters. See
[route changes and subscription migration](operations.md#route-changes-and-subscription-migration).
Do not delete the manifest or replace the broker session as a shortcut; either can hide queued
work.

## InfluxDB

The variables below are **only required when `OUTPUT_SINK=influx`** (see [Output sink](#output-sink)).

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INFLUX_URL` | Only if `OUTPUT_SINK=influx` | None | Base URL. Must start with `http://` or `https://` |
| `INFLUX_ORG` | Only if `OUTPUT_SINK=influx` | None | InfluxDB organization |
| `INFLUX_BUCKET` | Only if `OUTPUT_SINK=influx` | None | InfluxDB bucket |
| `INFLUX_TOKEN` | Only if `OUTPUT_SINK=influx` | None | InfluxDB v2 write token. Stored as `SecretString` and redacted from `Debug` output |

The write endpoint is built as:

```text
<INFLUX_URL>/api/v2/write?org=<urlencoded org>&bucket=<urlencoded bucket>&precision=ms
```

## Output sink

| Variable | Required | Default | Description |
|---|---:|---|---|
| `OUTPUT_SINK` | Yes | None | Selects the output destination. Only `influx` is implemented today (case-insensitive) |

The service persists messages through a swappable `Sink`/`Encoder` pair (see `docs/architecture.md`). `OUTPUT_SINK` selects which implementation `build_output()` constructs at startup. Adding a new sink (e.g. Kafka) means adding a new `OutputSinkKind` variant and a matching `Sink`/`Encoder` pair — no changes to `main.rs`'s wiring are required beyond that.

## Batching

| Variable | Required | Default | Description |
|---|---:|---|---|
| `BATCH_SIZE` | Yes | None | Maximum WAL entries per sink write |
| `FLUSH_INTERVAL_MS` | Yes | None | Time trigger for partial WAL batches |

Use a larger `BATCH_SIZE` for higher throughput and fewer HTTP writes. Use a smaller `FLUSH_INTERVAL_MS` for lower end-to-end latency.

## WAL

| Variable | Required | Default | Description |
|---|---:|---|---|
| `WAL_DIR` | Yes | None | Directory for WAL segments, `commit.cursor`, and the MQTT subscription manifest |
| `WAL_MAX_BYTES` | Yes | None | Maximum accounted WAL bytes, including pending appends and metadata reserves |
| `WAL_SEGMENT_BYTES` | No | `67108864` | Segment rotation threshold in bytes |
| `WAL_QUEUE_CAPACITY` | No | `16384` | Capacity of the bounded channel into the WAL writer; must be greater than zero |
| `QUARANTINE_DIR` | Yes | None | Separate, exclusively owned journal for rejected input |
| `QUARANTINE_MAX_BYTES` | Yes | None | Maximum accounted bytes for the quarantine journal |

Use separate writable, persistent local directories for `WAL_DIR` and `QUARANTINE_DIR`.
The service acquires an OS-backed exclusive lock and checks file creation, writing, syncing,
renaming, and reading in each directory before starting MQTT. Rejected messages are synced to
the quarantine journal before the supplementary MQTT DLQ publish. Locks are advisory and require
a filesystem that supports them; all journal users must honor the same lock file. Do not remove
`.journal.lock` while a process may still own the directory.

Both byte limits must be positive and at least:

```text
65536 + 2 * (max(WAL_SEGMENT_BYTES, 1048580) + 4096) + 4096
```

The minimum provides room for a retained segment, an empty post-rotation segment, one maximum
1 MiB record plus its four-byte frame header, and metadata. With the default segment size it is
134295552 bytes. Existing journal metadata or a recovered retained segment larger than the
configured segment size can require a larger WAL limit; startup reports the computed minimum.

Accounting covers segment file lengths, existing journal lock/cursor/probe file lengths, a
4096-byte allowance per segment, a 65536-byte fixed metadata reserve, and reservations for queued
appends. The reserve covers up to 32768 bytes of existing metadata while keeping at least 32768
bytes for future housekeeping; existing metadata above that amount is charged on top,
conservatively until restart. These are logical accounting limits, not filesystem quotas: other
processes, filesystem allocation overhead, or a shared disk can still exhaust free space. Provision
and monitor the underlying storage separately.

When WAL capacity is exhausted, a pipeline worker keeps its current event and waits for committed
segments to be reclaimed. If the sink does not recover, graceful shutdown can wait indefinitely.
For QoS 1 messages, MQTT acknowledgement is deferred until the output WAL or quarantine append is
durable. A full input queue leaves the message unacknowledged; persistent broker sessions are
required for redelivery after a disconnect. QoS 0 and clean-session operation do not provide
crash-safe broker redelivery, so these limits do not provide end-to-end delivery guarantees.

## Input Queue

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INPUT_QUEUE_CAPACITY` | No | `16384` | Total ingest queue capacity distributed across workers |

The service divides `INPUT_QUEUE_CAPACITY` by worker count to create one bounded queue per worker. Keep this value at least as large as the maximum worker count, which is 8, so every worker receives a non-zero channel capacity.

## HTTP, Cache, and Metrics

| Variable | Required | Default | Description |
|---|---:|---|---|
| `CACHE_BIND` | Yes | None | Cache API bind address, for example `0.0.0.0:8085` |
| `CACHE_TTL_MS` | Yes | None | Age threshold used to mark cached device states as stale |
| `CACHE_BUFFER` | Yes | None | Maximum cached devices and SSE broadcast buffer size |
| `METRICS_BIND` | Yes | None | Prometheus endpoint bind address, for example `0.0.0.0:9090` |

`CACHE_BUFFER` is used for two limits:

- Maximum number of devices retained in the latest-state cache.
- Number of events retained in the SSE broadcast channel.

Only routes with `cache: true` update the cache. When it is full, a new device replaces the
least recently received entry only if its receive sequence is newer; older delayed records
are ignored by the cache but continue through the pipeline.

## Docker Compose

[`compose.yaml`](../compose.yaml) in the repository root defines the `ingest` service with the full environment contract above. Only the variables below are read from the Compose environment (shell or the project root `.env`, which is git-ignored); change other values with an override file. The file has no fallback values: `docker compose` fails with an error naming the variable if a required one is missing.

| Variable | Required | Used for |
|---|---:|---|
| `INGEST_TAG` | Yes | Image tag: a release tag (`v1.2.3`) or `sha-<12hex>`. CD rewrites it in the stack `.env` |
| `GHCR_OWNER` | Yes | Image owner in `ghcr.io/<owner>/ingestion-service` |
| `INFLUX_TOKEN` | Yes | Passed to the container. Never commit it |
| `MQTT_HOST` | Yes | Broker hostname |
| `MQTT_PORT` | Yes | Broker port |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | No | Passed to the container only when set |
| `INFLUX_URL` | Yes | InfluxDB URL |
| `INFLUX_ORG` / `INFLUX_BUCKET` | Yes | |
| `WAL_MAX_BYTES` / `QUARANTINE_MAX_BYTES` | Yes | Independent journal byte limits |
| `RUST_LOG` | Yes | Log filter, e.g. `info` |

`WAL_DIR` is `/data/wal` and `QUARANTINE_DIR` is `/data/quarantine`; each has a matching named
volume. Keep paths and mounts aligned. `WAL_SEGMENT_BYTES` and `WAL_QUEUE_CAPACITY` are not set,
so the code defaults apply. The image `HEALTHCHECK` is used as-is.

Compose mounts the repository's `config/` directory read-only at `/app/config` and sets
`INGEST_ROUTES_FILE=/app/config/routes.json`; no route-file path is needed in the host `.env`.

CI renders the file with `docker compose config` and compares its `environment` keys with the variables read in `src/config.rs`. A variable that the code does not read, or a missing required variable, fails the build. Update `compose.yaml`, `src/config.rs`, and this page together.

See [Operations](operations.md#run-with-docker-compose) for standalone use and inclusion in the Server stack.

## Logging

The Docker image sets:

```text
RUST_LOG=info
```

The binary initializes JSON structured tracing using `tracing_subscriber` and reads `RUST_LOG` through the default environment filter.

Useful examples:

```bash
RUST_LOG=info cargo run --release
RUST_LOG=smarthome_ingest=debug cargo run --release
```

## Minimal Local Configuration

```bash
INPUT_SOURCE=mqtt
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=smarthome-ingest
MQTT_TOPIC_DLQ=smarthome/_dlq/ingest
INGEST_ROUTES_FILE=./config/routes.json
OUTPUT_SINK=influx
INFLUX_URL=http://localhost:8086
INFLUX_ORG=smarthome
INFLUX_BUCKET=sensors
INFLUX_TOKEN=change-me
BATCH_SIZE=500
FLUSH_INTERVAL_MS=1000
WAL_DIR=./data/wal
WAL_MAX_BYTES=268435456
QUARANTINE_DIR=./data/quarantine
QUARANTINE_MAX_BYTES=268435456
METRICS_BIND=0.0.0.0:9090
CACHE_BIND=0.0.0.0:8085
CACHE_TTL_MS=60000
CACHE_BUFFER=1024
```
