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
- The stack file must not define an `ingest` service or a `wal` volume itself. The included `wal` key keeps the existing `<project>_wal` volume, so unforwarded WAL data survives the switch.
- CD runs `docker compose pull ingest` and `docker compose up -d --no-deps ingest` in `DEPLOY_PATH`. This works unchanged through the include.

Check the effective service with `docker compose config ingest` in `DEPLOY_PATH`.

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
  -e MQTT_TOPIC_SENSOR=smarthome/+/sensor \
  -e MQTT_TOPIC_STATUS=smarthome/+/status \
  -e MQTT_TOPIC_DLQ=smarthome/_dlq/ingest \
  -e OUTPUT_SINK=influx \
  -e INFLUX_URL=http://host.docker.internal:8086 \
  -e INFLUX_ORG=smarthome \
  -e INFLUX_BUCKET=sensors \
  -e INFLUX_TOKEN=change-me \
  -e BATCH_SIZE=500 \
  -e FLUSH_INTERVAL_MS=1000 \
  -e WAL_DIR=/app/wal \
  -e ENFORCE_TOPIC_DEVICE_MATCH=true \
  -e METRICS_BIND=0.0.0.0:9090 \
  -e CACHE_BIND=0.0.0.0:8085 \
  -e CACHE_TTL_MS=60000 \
  -e CACHE_BUFFER=1024 \
  -v smarthome-ingest-wal:/app/wal \
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

Use `stale=true` as an indicator that the service has not received a recent sensor message for that device within `CACHE_TTL_MS`.

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
| Payloads are invalid | `ingest_incoming_invalid_json_total`, `ingest_validate_raw_failed_total`, `ingest_validate_business_failed_total` |
| Transform logic rejects payloads | `ingest_transform_failed_total` |
| WAL is saturated or failing | `ingest_queue_full_total`, `ingest_queue_closed_total`, `ingest_durability_ack_failed_total`, `wal_writer_fatal_total` |
| InfluxDB writes are failing | `influx_write_errors_total`, `wal_forwarder_retry_total`, `wal_forwarder_retry_outage_active` |
| Data is dropped as poison | `wal_forwarder_drop_total` |

## Handle Sink Outage (InfluxDB today)

During a retryable sink outage, the forwarder holds the current batch and does not advance the WAL cursor. With the current `InfluxSink`, that includes network errors, timeouts, HTTP 5xx, `408`, and `429`. New events continue to accumulate in WAL segment files.

Steps:

1. Confirm `wal_forwarder_retry_outage_active` is `1`.
2. Check InfluxDB availability and credentials.
3. Watch available disk space for `WAL_DIR`.
4. After recovery, confirm `wal_forwarder_retry_outage_active` returns to `0`.
5. Confirm `wal_forwarder_committed_total` increases.

Do not delete WAL files while the service is running. Deleting segments can make the cursor point outside available segments and cause startup failure.

## Handle DLQ Growth

DLQ messages contain:

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
| `raw validation failed` | Compare payload with `schema/*.schema.json` |
| `device_id mismatch` | Align topic device id and payload `device_id`, or set `ENFORCE_TOPIC_DEVICE_MATCH=false` |
| `transform failed` | Check humidity, gas, and pressure bounds |
| `business validation failed` | Compare post-transform canonical shape with `schema/*.business.schema.json` |
| `wal queue full` | Increase `WAL_QUEUE_CAPACITY`, lower ingest rate, or fix disk/WAL writer pressure |

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

Use a normal stop signal. The service drains worker queues, drops WAL senders, flushes the writer, and gives the forwarder up to 5 seconds to write its final batch.

If shutdown logs show:

```text
wal forwarder drain timed out after 5s; aborting and exiting
```

then uncommitted WAL entries should replay on next start, subject to the WAL files still being present.

## Release and Deploy

Release flow is documented in [Releasing and Deploying](releasing.md).
