# Operations Guide

## Run with Docker Compose

[`compose.yaml`](../compose.yaml) is the service definition. It requires an immutable image
reference and the runtime settings listed in [Configuration](configuration.md#docker-compose).
For a local standalone deployment, create a git-ignored `.env` beside `compose.yaml`:

```text
INGEST_IMAGE=ghcr.io/hengsti/ingestion-service@sha256:<registry-digest>
INGEST_CONFIG_DIR=./config
RUST_LOG=info
MQTT_HOST=<broker-host>
MQTT_PORT=1883
INFLUX_URL=http://<influx-host>:8086
INFLUX_ORG=<org>
INFLUX_BUCKET=<bucket>
INFLUX_TOKEN=<write-token>
WAL_MAX_BYTES=268435456
QUARANTINE_MAX_BYTES=268435456
```

```bash
docker compose config --quiet
docker compose up -d
docker compose exec ingest wget -qO- http://localhost:8085/readyz
```

The service only uses `expose`, so nothing is published on the host. To reach the API
from the host, add an override file with `ports`. The default config mount is the local
`./config`; CD snapshots the route/schema bundle from the exact release revision.
See [Releasing and Deploying](releasing.md) for immutable
image/config snapshots, preflight, readiness, and rollback. This repository does not
include its Compose file in a surrounding Server stack.

## Route Changes and Subscription Migration

For ordinary route changes, update the complete route/schema bundle and restart the service
with the same `MQTT_HOST`, `MQTT_PORT`, `MQTT_CLIENT_ID`, and `WAL_DIR`. The service
persists the desired filter set before reconciliation, subscribes desired filters,
unsubscribes known obsolete filters, and keeps `/readyz` false until current-generation
QoS 1 grants and unsubscribe acknowledgements are received. A reconnect is not proof of
reconciliation. Do not remove manifest files or clear a broker session while the service
is running. This procedure implements the
[broker reconciliation contract in issue #40](https://github.com/hengsti/ingestion-service/issues/40).

An existing persistent broker session created before the subscription manifest was introduced
cannot be enumerated over MQTT 3.1.1. Before upgrading such a session:

1. Stop every ingest instance using the client ID.
2. Verify the complete filter set for that broker/client from broker session diagnostics and
   historical configuration. Include old `MQTT_TOPIC_SENSOR` and `MQTT_TOPIC_STATUS` values and
   any other previously configured filters; never include the DLQ destination.
3. In `WAL_DIR`, replace the untrusted `.mqtt-subscriptions.1.json` created during the failed
   startup (or create it if absent) with a trusted generation `1` record containing the exact
   broker, client ID, and verified filters:

   ```json
   {
     "generation": 1,
     "trusted": true,
     "identity": {
       "host": "<MQTT_HOST>",
       "port": 1883,
       "client_id": "<MQTT_CLIENT_ID>"
     },
     "filters": ["smarthome/+/sensor", "smarthome/+/status"]
   }
   ```

   Replace the example filters with the full verified set. Keep this file at most 16 KiB. The
   manifest is migration state only; after startup, the runtime's desired subscriptions still
   come exclusively from `INGEST_ROUTES_FILE`.
4. Start one instance and confirm `/readyz` reports ready. If the manifest identity differs from
   the configured broker/client, or the service reports an unknown resumed session, stop and
   re-check the migration input rather than clearing the broker session or guessing.

Changing broker or client identity requires a separate, stopped-ingest migration. Do not reuse a
manifest from another identity or switch client IDs to bypass reconciliation; first account for
the old session's queued QoS 1 messages and verify the new identity's existing filters. Then
update the `identity` in the highest-generation trusted manifest slot while stopped, preserving
its verified filter list (or explicitly seed the new session's verified filter list). Remove the
other slot only after preserving the current trusted record, so startup cannot select metadata
for the old identity. Broker expiry or queue/storage limits can discard messages; MQTT 3.1.1
cannot recover messages the broker has already expired or dropped.

## Run with Docker

Build:

```bash
docker build -t smarthome-ingest .
```

Run:

```bash
docker run --rm \
  -p 8085:8085 \
  -p 9090:9090 \
  -e INPUT_SOURCE=mqtt \
  -e MQTT_HOST=host.docker.internal \
  -e MQTT_PORT=1883 \
  -e MQTT_CLIENT_ID=smarthome-ingest \
  -e MQTT_TOPIC_DLQ=smarthome/_dlq/ingest \
  -e INGEST_ROUTES_FILE=/app/config/routes.json \
  -e OUTPUT_SINK=influx \
  -e INFLUX_URL=http://host.docker.internal:8086 \
  -e INFLUX_ORG=smarthome \
  -e INFLUX_BUCKET=sensors \
  -e INFLUX_TOKEN=change-me \
  -e BATCH_SIZE=500 \
  -e FLUSH_INTERVAL_MS=1000 \
  -e WAL_DIR=/data/wal \
  -e WAL_MAX_BYTES=268435456 \
  -e QUARANTINE_DIR=/data/quarantine \
  -e QUARANTINE_MAX_BYTES=268435456 \
  -e METRICS_BIND=0.0.0.0:9090 \
  -e CACHE_BIND=0.0.0.0:8085 \
  -e CACHE_TTL_MS=60000 \
  -e CACHE_BUFFER=1024 \
  -v smarthome-ingest-wal:/data/wal \
  -v smarthome-ingest-quarantine:/data/quarantine \
  smarthome-ingest
```

The Dockerfile healthcheck probes:

```text
http://localhost:8085/healthz
```

Set `CACHE_BIND=0.0.0.0:8085` in containers unless you also change the image healthcheck.

## Check Service Health

```bash
curl -i http://localhost:8085/healthz
curl -i http://localhost:8085/readyz
```

Interpretation:

- `/healthz` validates the HTTP server process.
- `/readyz` validates reconciled MQTT subscriptions, matching QoS 1 grants, live workers
  and forwarder, available bounded admission, and writable WAL and quarantine capacity.
  Sink retry by itself does not make intake unready while local capacity remains.

## Check Cache State

```bash
curl http://localhost:8085/v1/state
curl http://localhost:8085/v1/state/esp32-1
```

Use `stale=true` as an indicator that the service has not received a recent cache-enabled
record for that device within `CACHE_TTL_MS`.

## Watch Live Sensor Updates

```bash
curl -N http://localhost:8085/v1/stream
```

If the stream emits a `lagged` event, the client fell behind the broadcast buffer. Poll `/v1/state` to resynchronize.

## Monitor Ingestion

Scrape:

```text
http://<METRICS_BIND>/metrics
```

Useful signals:

| Symptom | Metrics to check |
|---|---|
| MQTT input is arriving | `mqtt_messages_received_total` |
| Input workers are overloaded | `ingest_event_queue_full_total` |
| Payloads are invalid | `ingest_incoming_invalid_json_total`, `ingest_validate_raw_failed_total`, configured output-schema validation failures |
| WAL is saturated or failing | `wal_accounted_bytes`, `wal_capacity_bytes`, `wal_capacity_full_total`, `ingest_queue_full_total`, `ingest_queue_closed_total`, `ingest_durability_ack_failed_total`, `wal_writer_fatal_total` |
| InfluxDB writes are failing | `influx_write_errors_total`, `wal_forwarder_retry_total`, `wal_forwarder_retry_outage_active` |
| Forwarder is blocked by a permanent sink error | `wal_forwarder_retry_outage_active`; inspect the logged HTTP status and repair the destination or pending output |
| Quarantine storage usage | `quarantine_accounted_bytes`, `quarantine_capacity_bytes` |

The quarantine export command reports the unresolved record count without logging payloads.

## Recover from intake, storage, and sink faults

| Symptom | Action |
|---|---|
| `ingest_event_queue_full_total` rises or admission pauses | Check `/v1/operations`, queue availability, worker liveness, and downstream storage pressure. Worker queues stay bounded; MQTT pauses polling and retains at most one unadmitted message. Let workers progress or reduce publisher rate; do not treat queue pressure as a drop counter or replace bounded queues with unbounded buffering. |
| WAL or quarantine approaches its byte budget | Check the relevant accounted-byte/capacity metrics and underlying filesystem free space. For WAL pressure, restore the sink or disk capacity and let committed segments be reclaimed. For quarantine pressure, stop ingestion and use the explicit export/resolve procedure below; never delete journal files to make room. |
| Journal is unwritable, lock acquisition fails, or preflight fails | Check the mounted directory, ownership/permissions, free space, and filesystem support for locking, sync, rename, and read operations. Keep WAL and quarantine in separate exclusively owned directories. Do not remove `.journal.lock` while a process may own the directory. |
| Forwarder exits, `wal_forwarder_read_failed` is set, or cursor commit/reclaim stays blocked | `/readyz` remains `503`. Stop the service and preserve the WAL, cursor, and segment files. Diagnose the reported segment/offset and storage error; do not skip a record, advance the cursor manually, truncate a sealed segment, or use an unsupported repair tool. |
| Influx authentication or destination error | Inspect the logged HTTP status and check the token, organization, bucket, URL, and write permissions. Retryable errors retain the batch; permanent 4xx errors also retain it and block later writes until corrected. |
| Influx field-type conflict | Preserve the pending WAL batch and inspect the conflicting measurement, tags, timestamp, and field type. Make the destination accept the already-encoded line, or use a compatible destination for that retained payload. A route change affects future records only; it does not rewrite pending WAL bytes. Do not discard the batch or advance the cursor past it. |
| Broker reconnect does not become ready or a pre-upgrade session is unknown | Keep the stable client ID and `WAL_DIR`. Verify the complete old filter set using broker diagnostics and historical configuration, then use the stopped-ingest migration above. MQTT 3.1.1 cannot enumerate filters for an unknown resumed session; reconnect is not a substitute. |
| Broker session expired or its queue was lost | Confirm the broker's session-present state, expiry, persistence, and queue/storage limits. A fresh session can subscribe the current route set, but the broker cannot replay already-expired or discarded publications. Record the loss of that recovery window; do not claim that reconnect restores it. |

For QoS 1, PUBACK follows a synced output-WAL or quarantine append; failure to admit or
persist the input leaves it unacknowledged. This is local durable-intake eligibility, not
proof that InfluxDB has accepted the point. Broker session expiry/queue limits and the
local WAL/quarantine budgets bound recovery. If those bounds are exceeded, or storage does
not honor sync, the service cannot promise no loss. Replays can create duplicate writes.

## Handle Sink Outage (InfluxDB today)

For any sink failure, the forwarder holds the current batch and does not advance the WAL cursor. `InfluxSink` classifies network errors, timeouts, HTTP 5xx, `408`, and `429` as retryable; other 4xx failures remain pending and block later writes until the cause is repaired. New events continue to accumulate in WAL segment files.

Steps:

1. Confirm `wal_forwarder_retry_outage_active` is `1`.
2. Check InfluxDB availability, credentials, organization, bucket, and the pending line-protocol output.
3. Watch `wal_accounted_bytes`, `wal_capacity_bytes`, and available disk space for both journal directories.
4. After recovery, confirm `wal_forwarder_retry_outage_active` returns to `0`.
5. Confirm `wal_forwarder_committed_total` increases.

Do not delete WAL files while the service is running. Deleting segments can make the cursor point outside available segments and cause startup failure.
When the WAL budget is full, workers wait for reclaim. Shutdown releases these waits and gives
workers and the forwarder a shared five-second drain deadline. Queue pressure withholds new
input acknowledgements; neither durable local intake nor persistent broker sessions alone
guarantee end-to-end delivery outside the configured broker and storage bounds.

## Handle Rejected Input

Each rejection is synced to the local quarantine journal before the best-effort MQTT DLQ
publication. The journal is authoritative if publishing the supplementary copy fails. Its
accounted bytes and capacity are available from the metrics above.

The application size guard is 64 KiB and runs before JSON decoding; the MQTT wire limit is
128 KiB for the full packet, including headers and topic. An oversized payload that fits
within that wire limit can be quarantined. A packet larger than the wire limit is rejected
by the MQTT client/broker boundary and is outside the local quarantine guarantee. Rejection
reasons are truncated at a UTF-8 boundary to 4,096 bytes, and the quarantine journal has its
own configured byte budget. The quarantine exporter also bounds each JSONL line to 2 MiB.

MQTT DLQ messages contain:

- `received_at`
- `src_topic`
- `error`
- `payload_raw`

Common causes:

| Error source | Typical fix |
|---|---|
| `payload too large` | Keep MQTT payloads at or below 64 KiB |
| `payload not utf8` | Publish UTF-8 JSON |
| `payload not valid JSON` | Fix publisher serialization |
| `raw validation failed` | Compare payload with the raw schema configured for its route in `config/routes.json` |
| `transform failed` | Check the route's mappings, defaults, and configured calculations |
| `output validation failed` | Compare the mapped record with its configured output schema and output constraints |
| `quarantine journal is full` | Export and explicitly resolve an inspected prefix, or increase `QUARANTINE_MAX_BYTES` |
| `quarantine append was not durable` | Check quarantine storage and filesystem health; the rejection was not accepted |
| `wal queue full` | Increase `WAL_QUEUE_CAPACITY`, lower ingest rate, or fix disk/WAL writer pressure |

## Export and Resolve Quarantine

Stop ingestion before maintenance. Both commands take the journal's exclusive lock, run without
starting MQTT or HTTP, and require the configured quarantine volume to be mounted.

Export the complete unresolved journal to a new file outside the journal directory:

```bash
docker compose stop ingest
mkdir -p ./quarantine-export
docker compose run --rm --no-deps \
  -v "$PWD/quarantine-export:/export" \
  ingest /app/smarthome-ingest --quarantine-export /export/rejections.jsonl
```

The command reports the unresolved record count. The JSONL archive includes a journal identity,
the exact WAL offsets and a completion footer. Preserve and inspect the archive before resolving
anything. Choose an exported record's `offset_after` as the end of the prefix to resolve; do not
select an offset inside a record.

After confirming the export and selected prefix, resolve only through that boundary:

```bash
docker compose run --rm --no-deps \
  -v "$PWD/quarantine-export:/export" \
  ingest /app/smarthome-ingest --quarantine-resolve 1:12345 \
  --export /export/rejections.jsonl
```

Replace `1:12345` with the selected `offset_after`. Resolution verifies the full archive against
the current journal and refuses an incomplete, altered, wrong-journal, live-owned, or
non-boundary export. The cursor advances only after verification, leaving all later records
unresolved. Restart the service after maintenance. This is archival and explicit prefix
resolution, not replay or repair of invalid output.

## Tune Throughput

Treat queue, batch, and flush settings as workload-specific bounds, not performance
recommendations. Increase a bounded queue only after measuring the pressure and memory
cost; increasing `BATCH_SIZE` can reduce HTTP overhead, while decreasing
`FLUSH_INTERVAL_MS` changes partial-batch latency. Use metrics and the worksheet below to
size capacity; a larger queue does not increase sustainable sink throughput.

## Plan an outage budget and record capacity evidence

Measure the encoded output bytes per second for the intended route mix; do not substitute
raw MQTT payload bytes. For a selected sink-outage duration, estimate:

```text
required_WAL_bytes =
    observed_encoded_bytes_per_second * outage_seconds
    + framing_and_segment_allowances
    + fixed_metadata_reserve
    + operational_headroom
```

Fill in the workload-specific values:

| Input | Measured/selected value |
|---|---|
| Route mix and routes enabled | |
| Encoded output bytes per second | |
| Selected sink-outage duration | |
| Framing and segment-allowance bytes | |
| Metadata reserve and additional headroom | |
| Estimated required WAL bytes | |
| Configured `WAL_MAX_BYTES` | |
| Configured `QUARANTINE_MAX_BYTES` and expected rejection volume | |
| Free local disk after both journal budgets and other service data | |
| Peak broker publications per second and estimated backlog bytes | |
| Broker per-client queue/storage limits and session expiry | |

The estimate must fit both configured local journal budgets and actual available disk.
WAL and quarantine limits are independent logical accounting budgets, not filesystem
quotas. Separately check that broker queue capacity and session expiry cover the selected
outage; if either broker or local bounds are exceeded, message replay is not guaranteed.
Use the [broker prerequisites](../config/examples/deployment/README.md) to record the
effective broker settings rather than assuming test-fixture defaults.

### ARM64 measurement status

**Blocked:** target ARM64 hardware was unavailable for this documentation change. No
target throughput, latency, RSS, disk-rate, backlog-drain, or sustainable-rate values
are reported. Do not treat x86_64 development or container-test results as ARM64
performance evidence.

To unblock the comparison, run the same fixed mixed firmware/Zigbee workload on actual
target ARM64 hardware through both the pre-cutover baseline and unified route path. Keep
publication rate, route mix/count, payload sizes, outage duration, and sync policy
identical. Record hardware and OS/kernel, Rust/image revision, exact route/config
revision, queue capacities and WAL/quarantine budgets, p50/p95 sync-to-ACK latency, RSS,
disk write rate, and backlog drain rate after the selected outage. A sustainable rate
requires the backlog to drain without unbounded RSS or queue growth. This project has no
additional latency SLA. The baseline-versus-unified comparison remains unmeasured until
that evidence is collected.

## Restart Safely

Use a normal stop signal. After stopping the source, the service releases persistence pressure
waits and gives queued workers and the forwarder a shared five-second drain deadline. Workers
can still persist while capacity is available. After workers finish or are aborted at the
deadline, dropping WAL senders lets the writer flush and sync and the forwarder attempt its
final partial batch within the remaining time.

Unfinished QoS 1 input stays unacknowledged for broker redelivery, subject to the persistent
session and broker limits. The deadline bounds async draining, not filesystem sync latency.

If shutdown logs show:

```text
wal forwarder drain deadline reached; uncommitted entries remain for replay
```

then uncommitted WAL entries should replay on next start, subject to the WAL files still being present.

## Release and Deploy

Release flow is documented in [Releasing and Deploying](releasing.md).
