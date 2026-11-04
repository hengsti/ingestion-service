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
| `MQTT_CLIENT_ID` | Only if `INPUT_SOURCE=mqtt` | None | Base client id. The service appends `-<unix_timestamp>` at startup only when `MQTT_CLEAN_SESSION=true`; otherwise it is used verbatim |
| `MQTT_CLEAN_SESSION` | No | `true` | Boolean. `false` requests a persistent session: the client id is used verbatim and the broker queues QoS 1 messages while ingest is offline |
| `MQTT_USERNAME` | No | None | Optional MQTT username |
| `MQTT_PASSWORD` | No | None | Optional MQTT password |
| `MQTT_TOPIC_DLQ` | Yes at runtime | None | DLQ publish topic |

With `MQTT_CLEAN_SESSION=false`, a client id must be used by only one ingest instance at a time. A second instance with the same id takes over the session from the first.

The source subscribes to the route bundle's filters on each connection. A persistent broker
session retains obsolete subscriptions when filters change; remove those subscriptions or
migrate to a new client ID. Resetting or replacing a session can discard its queued messages.
Messages from obsolete subscriptions that reach the pipeline without a matching route are
routed to the DLQ.

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
| `WAL_DIR` | Yes | None | Directory for WAL segment files and `commit.cursor` |
| `WAL_SEGMENT_BYTES` | No | `67108864` | Segment rotation threshold in bytes |
| `WAL_QUEUE_CAPACITY` | No | `16384` | Capacity of the bounded channel into the WAL writer |

Use persistent local storage for `WAL_DIR` in production. The WAL buffers sink outages (InfluxDB today) and replays uncommitted records after restart.

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
| `RUST_LOG` | Yes | Log filter, e.g. `info` |

`WAL_DIR` is `/data/wal`, and the `wal` named volume is mounted at that path. Keep the two in sync. `WAL_SEGMENT_BYTES` and `WAL_QUEUE_CAPACITY` are not set, so the code defaults apply. The image `HEALTHCHECK` is used as-is.

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
METRICS_BIND=0.0.0.0:9090
CACHE_BIND=0.0.0.0:8085
CACHE_TTL_MS=60000
CACHE_BUFFER=1024
```
