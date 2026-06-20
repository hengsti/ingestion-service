# Tutorial: First Local Run

This tutorial starts the service locally with explicit environment variables. It is intended for a developer who wants to see one valid MQTT payload move through the ingest path.

## Prerequisites

- Rust toolchain compatible with edition `2021`. CI uses Rust `1.87.0`.
- An MQTT broker reachable from the service.
- An InfluxDB v2 instance with an organization, bucket, and write token.
- A writable local directory for the WAL.

## 1. Build and Test

From the repository root:

```bash
cargo test --all-features --locked
cargo build --release
```

## 2. Prepare Runtime Configuration

Use topic patterns that put the device id in the first `+` segment. The service can then compare the topic device id with the payload `device_id` when `ENFORCE_TOPIC_DEVICE_MATCH=true`.

PowerShell example:

```powershell
$env:MQTT_HOST="localhost"
$env:MQTT_PORT="1883"
$env:MQTT_CLIENT_ID="smarthome-ingest"
$env:MQTT_TOPIC_SENSOR="smarthome/+/sensor"
$env:MQTT_TOPIC_STATUS="smarthome/+/status"
$env:MQTT_TOPIC_DLQ="smarthome/_dlq/ingest"
$env:INFLUX_URL="http://localhost:8086"
$env:INFLUX_ORG="smarthome"
$env:INFLUX_BUCKET="sensors"
$env:INFLUX_TOKEN="change-me"
$env:BATCH_SIZE="500"
$env:FLUSH_INTERVAL_MS="1000"
$env:WAL_DIR="./data/wal"
$env:ENFORCE_TOPIC_DEVICE_MATCH="true"
$env:METRICS_BIND="0.0.0.0:9090"
$env:CACHE_BIND="0.0.0.0:8085"
$env:CACHE_TTL_MS="60000"
$env:CACHE_BUFFER="1024"
```

POSIX shell example:

```bash
export MQTT_HOST=localhost
export MQTT_PORT=1883
export MQTT_CLIENT_ID=smarthome-ingest
export MQTT_TOPIC_SENSOR='smarthome/+/sensor'
export MQTT_TOPIC_STATUS='smarthome/+/status'
export MQTT_TOPIC_DLQ='smarthome/_dlq/ingest'
export INFLUX_URL='http://localhost:8086'
export INFLUX_ORG=smarthome
export INFLUX_BUCKET=sensors
export INFLUX_TOKEN=change-me
export BATCH_SIZE=500
export FLUSH_INTERVAL_MS=1000
export WAL_DIR=./data/wal
export ENFORCE_TOPIC_DEVICE_MATCH=true
export METRICS_BIND=0.0.0.0:9090
export CACHE_BIND=0.0.0.0:8085
export CACHE_TTL_MS=60000
export CACHE_BUFFER=1024
```

## 3. Run the Service

```bash
cargo run --release
```

The service starts:

- The cache HTTP API on `CACHE_BIND`.
- The Prometheus endpoint on `METRICS_BIND`.
- The MQTT connection and subscriptions for non-DLQ MQTT topics.
- The WAL writer and InfluxDB forwarder.
- A worker pool with 2 to 8 workers, based on available CPU parallelism.

## 4. Publish a Sensor Payload

Publish this JSON to `smarthome/esp32-1/sensor`:

```json
{
  "device_id": "esp32-1",
  "room": "living_room",
  "device_class": "esp32p4-bme680",
  "fw_version": "1.0.0",
  "time_ms": 1700000000000,
  "time_iso": "2023-11-14T22:13:20Z",
  "time_valid": true,
  "data": {
    "temp_c": 22.5,
    "rel_hum_perc": 45.0,
    "pressure_hpa": 1013.25,
    "gas_ohm": 50000.0,
    "altitude_m": 500.0
  }
}
```

Expected results:

- The message passes raw schema validation.
- The transform stage adds `dew_point_c`, `heat_index_c`, `iaq_score`, and `iaq_text`.
- The latest sensor state appears in the cache API.
- A `bme680` line protocol record is appended to the WAL.
- The WAL forwarder eventually writes the line to InfluxDB.

## 5. Query the Cache

```bash
curl http://localhost:8085/v1/state
curl http://localhost:8085/v1/state/esp32-1
```

Expected shape:

```json
{
  "ttl_ms": 60000,
  "sensors": [
    {
      "device_id": "esp32-1",
      "stale": false,
      "last_seen_ms": 1700000000000,
      "value": {
        "temp_c": 22.5,
        "rel_hum_perc": 45.0,
        "pressure_hpa": 1013.25,
        "gas_ohm": 50000.0,
        "iaq_score": 85.0,
        "iaq_text": "Air quality is Good",
        "dew_point_c": 9.5,
        "heat_index_c": 22.0,
        "altitude_m": 500.0
      }
    }
  ]
}
```

`last_seen_ms` is generated when the service updates the cache. Numeric derived values depend on the formulas in `TransformStage`.

## 6. Check Readiness and Metrics

```bash
curl -i http://localhost:8085/healthz
curl -i http://localhost:8085/readyz
curl http://localhost:9090/metrics
```

`/healthz` returns `200` when the HTTP server is alive. `/readyz` returns `200` after MQTT has acknowledged the connection and `503` before that.
