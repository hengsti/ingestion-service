# Operations Guide

## Run with Docker Compose

[`compose.yaml`](../compose.yaml) is the service definition used in production. Variables are listed in [Configuration](configuration.md#docker-compose).

### Standalone

`compose.yaml` has no fallback values. Create a `.env` next to it (git-ignored, never commit it) with every required variable; Compose fails with an error naming any missing one:

```bash
GHCR_OWNER=<github-owner>
INGEST_TAG=v1.2.3
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
docker compose -f compose.yaml config --quiet   # validate
docker compose -f compose.yaml up -d
docker compose -f compose.yaml exec ingest wget -qO- http://localhost:8085/readyz
```

The service only uses `expose`, so nothing is published on the host. To reach the API from the host, add an override file with `ports`.

### Inside the Server stack

The Server stack (`DEPLOY_PATH`) checks out this repository as `./smarthome-ingest` and includes it. Requires Docker Compose 2.24 or later.

```yaml
# docker-compose.yaml (stack)
include:
  - path:
      - ./smarthome-ingest/compose.yaml
      - ./ingest.override.yaml
    env_file: ./.env
```

- The stack `.env` provides all required variables listed in [Configuration](configuration.md#docker-compose), including `INGEST_TAG` (rewritten by CD). The same token is used by `influxdb` and `grafana`. Rotate it if it was ever committed.
- `ingest.override.yaml` in the stack holds the stack-only wiring: `depends_on` (`influxdb` healthy, `emqx` healthy) and the Traefik labels (`ingest.smarthome.local` and `/ingest` with strip-prefix; the HomeKit bridge uses `/ingest/v1/stream`).
- The stack file must not define an `ingest` service or duplicate the included `wal` and
  `quarantine` volumes. The included `wal` key keeps the existing `<project>_wal` volume,
  so unforwarded WAL data survives the switch.
- CD runs `docker compose pull ingest` and `docker compose up -d --no-deps ingest` in `DEPLOY_PATH`. This works unchanged through the include.

Check the effective service with `docker compose config ingest` in `DEPLOY_PATH`.

## Route Changes and Subscription Migration

For ordinary route changes, update the route bundle and restart the service with the same
`MQTT_HOST`, `MQTT_PORT`, `MQTT_CLIENT_ID`, and `WAL_DIR`. The service subscribes the desired
filters, unsubscribes known obsolete filters, and keeps `/readyz` false until both operations are
acknowledged. Do not remove the manifest files while the service is running.

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
for the old identity.

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
- `/readyz` validates active input-source readiness (successful `SubAck` for the current MQTT source).

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

## Handle Sink Outage (InfluxDB today)

For any sink failure, the forwarder holds the current batch and does not advance the WAL cursor. `InfluxSink` classifies network errors, timeouts, HTTP 5xx, `408`, and `429` as retryable; other 4xx failures remain pending and block later writes until the cause is repaired. New events continue to accumulate in WAL segment files.

Steps:

1. Confirm `wal_forwarder_retry_outage_active` is `1`.
2. Check InfluxDB availability, credentials, organization, bucket, and the pending line-protocol output.
3. Watch `wal_accounted_bytes`, `wal_capacity_bytes`, and available disk space for both journal directories.
4. After recovery, confirm `wal_forwarder_retry_outage_active` returns to `0`.
5. Confirm `wal_forwarder_committed_total` increases.

Do not delete WAL files while the service is running. Deleting segments can make the cursor point outside available segments and cause startup failure.
When the WAL budget is full, workers wait for reclaim and graceful shutdown can wait indefinitely
until the sink recovers. The current MQTT acknowledgement path can still lose messages when
input queues fill or the process is forcibly terminated; journal capacity limits alone do not
guarantee broker-to-sink delivery.

## Handle Rejected Input

Each rejection is synced to the local quarantine journal before the best-effort MQTT DLQ
publication. The journal is authoritative if publishing the supplementary copy fails. Its
accounted bytes and capacity are available from the metrics above.

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

Start with:

- `INPUT_QUEUE_CAPACITY=16384`
- `WAL_QUEUE_CAPACITY=16384`
- `BATCH_SIZE=500`
- `FLUSH_INTERVAL_MS=1000`

Adjust:

- Increase `BATCH_SIZE` to reduce InfluxDB HTTP write overhead.
- Decrease `FLUSH_INTERVAL_MS` to reduce latency for low-volume streams.
- Increase `INPUT_QUEUE_CAPACITY` if worker queues fill during bursts.
- Increase `WAL_QUEUE_CAPACITY` if persist stage sees `wal queue full`.
- Ensure `INPUT_QUEUE_CAPACITY / worker_count` is at least `1`.

## Restart Safely

Use a normal stop signal. The service drains worker queues, drops WAL senders, flushes and syncs the writer, and gives the forwarder up to 5 seconds to write its final batch.

If shutdown logs show:

```text
wal forwarder drain timed out after 5s; aborting and exiting
```

then uncommitted WAL entries should replay on next start, subject to the WAL files still being present.

## Release and Deploy

Release flow is documented in [Releasing and Deploying](releasing.md).
