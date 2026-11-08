# Tutorial: First Local Run

This tutorial starts the service locally with explicit environment variables and the shipped
route bundle. It follows one firmware sensor payload through the configured ingest path.

## Prerequisites

- Rust toolchain compatible with edition `2021`. CI uses Rust `1.87.0`.
- An MQTT broker reachable from the service.
- An InfluxDB v2 instance with an organization, bucket, and write token.
- Separate writable local directories for the WAL and reserved quarantine journal.

## 1. Build and Test

From the repository root:

```bash
cargo test --all-features --locked
cargo build --release
```

## 2. Prepare Runtime Configuration

`config/routes.json` subscribes to `smarthome/+/bme680` and `smarthome/+/status`.
`INGEST_ROUTES_FILE` is required; schema paths in that bundle are relative to its directory.

PowerShell example:

```powershell
$env:INPUT_SOURCE="mqtt"
$env:MQTT_HOST="localhost"
$env:MQTT_PORT="1883"
$env:MQTT_CLIENT_ID="smarthome-ingest"
$env:MQTT_TOPIC_DLQ="smarthome/_dlq/ingest"
$env:INGEST_ROUTES_FILE="./config/routes.json"
$env:OUTPUT_SINK="influx"
$env:INFLUX_URL="http://localhost:8086"
$env:INFLUX_ORG="smarthome"
$env:INFLUX_BUCKET="sensors"
$env:INFLUX_TOKEN="change-me"
$env:BATCH_SIZE="500"
$env:FLUSH_INTERVAL_MS="1000"
$env:WAL_DIR="./data/wal"
$env:WAL_MAX_BYTES="268435456"
$env:QUARANTINE_DIR="./data/quarantine"
$env:QUARANTINE_MAX_BYTES="268435456"
$env:METRICS_BIND="0.0.0.0:9090"
$env:CACHE_BIND="0.0.0.0:8085"
$env:CACHE_TTL_MS="60000"
$env:CACHE_BUFFER="1024"
```

POSIX shell example:

```bash
export INPUT_SOURCE=mqtt
export MQTT_HOST=localhost
export MQTT_PORT=1883
export MQTT_CLIENT_ID=smarthome-ingest
export MQTT_TOPIC_DLQ='smarthome/_dlq/ingest'
export INGEST_ROUTES_FILE=./config/routes.json
export OUTPUT_SINK=influx
export INFLUX_URL='http://localhost:8086'
export INFLUX_ORG=smarthome
export INFLUX_BUCKET=sensors
export INFLUX_TOKEN=change-me
export BATCH_SIZE=500
export FLUSH_INTERVAL_MS=1000
export WAL_DIR=./data/wal
export WAL_MAX_BYTES=268435456
export QUARANTINE_DIR=./data/quarantine
export QUARANTINE_MAX_BYTES=268435456
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
- The MQTT connection and subscriptions configured in `config/routes.json`.
- The WAL writer and sink forwarder (InfluxDB today).
- A worker pool with 2 to 8 workers, based on available CPU parallelism.

## 4. Publish a Sensor Payload

Publish this JSON at MQTT QoS 1 to `smarthome/esp32-1/bme680` (for example, use `mosquitto_pub -q 1`):

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

- The configured sensor route validates the raw schema, maps fields, and calculates derived values.
- The latest sensor state appears in the cache API.
- The typed encoder renders a `bme680` line-protocol record with the mapped timestamp in
  milliseconds and appends it to the WAL.
- The WAL forwarder eventually writes the batch to the active sink (InfluxDB today).

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

`last_seen_ms` is the frozen MQTT receive time. Numeric derived values depend on the
calculations configured for the selected route.

## 6. Check Readiness and Metrics

```bash
curl -i http://localhost:8085/healthz
curl -i http://localhost:8085/readyz
curl http://localhost:9090/metrics
```

`/healthz` returns `200` when the HTTP server is alive. `/readyz` returns `200` after the broker has acknowledged the current MQTT source's topic subscription and `503` before that.
