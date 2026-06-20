# Configuration Reference

All runtime configuration is supplied through environment variables. Empty strings are treated as unset because `Config::from_env` trims values and filters empty results.

## MQTT

| Variable | Required | Default | Description |
|---|---:|---|---|
| `MQTT_HOST` | Yes | None | MQTT broker host |
| `MQTT_PORT` | Yes | None | MQTT broker port as `u16` |
| `MQTT_CLIENT_ID` | Yes | None | Base client id. The service appends `-<unix_timestamp>` at startup |
| `MQTT_USERNAME` | No | None | Optional MQTT username |
| `MQTT_PASSWORD` | No | None | Optional MQTT password |
| `MQTT_TOPIC_SENSOR` | Expected | None | Sensor subscription and route, for example `smarthome/+/sensor` |
| `MQTT_TOPIC_STATUS` | Optional | None | Status subscription and route, for example `smarthome/+/status` |
| `MQTT_TOPIC_DLQ` | Yes at runtime | None | DLQ publish topic |
| `ENFORCE_TOPIC_DEVICE_MATCH` | Yes | None | Boolean. When `true`, payload `device_id` must match the device id extracted from the topic |

At least one `MQTT_TOPIC_<NAME>` variable must be set for config parsing. `MQTT_TOPIC_DLQ` is required later during startup because the DLQ stage needs a publish topic.

Unknown `MQTT_TOPIC_*` keys are not routed. They are still subscribed if they do not end in `DLQ`, so avoid unknown topic keys in production.

## InfluxDB

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INFLUX_URL` | Yes | None | Base URL. Must start with `http://` or `https://` |
| `INFLUX_ORG` | Yes | None | InfluxDB organization |
| `INFLUX_BUCKET` | Yes | None | InfluxDB bucket |
| `INFLUX_TOKEN` | Yes | None | InfluxDB v2 write token. Stored as `SecretString` and redacted from `Debug` output |

The write endpoint is built as:

```text
<INFLUX_URL>/api/v2/write?org=<urlencoded org>&bucket=<urlencoded bucket>&precision=ms
```

## Batching

| Variable | Required | Default | Description |
|---|---:|---|---|
| `BATCH_SIZE` | Yes | None | Maximum WAL entries per InfluxDB write |
| `FLUSH_INTERVAL_MS` | Yes | None | Time trigger for partial WAL batches |

Use a larger `BATCH_SIZE` for higher throughput and fewer HTTP writes. Use a smaller `FLUSH_INTERVAL_MS` for lower end-to-end latency.

## WAL

| Variable | Required | Default | Description |
|---|---:|---|---|
| `WAL_DIR` | Yes | None | Directory for WAL segment files and `commit.cursor` |
| `WAL_SEGMENT_BYTES` | No | `67108864` | Segment rotation threshold in bytes |
| `WAL_QUEUE_CAPACITY` | No | `16384` | Capacity of the bounded channel into the WAL writer |

Use persistent local storage for `WAL_DIR` in production. The WAL buffers InfluxDB outages and replays uncommitted records after restart.

## Input Queue

| Variable | Required | Default | Description |
|---|---:|---|---|
| `INPUT_QUEUE_CAPACITY` | No | `16384` | Total MQTT ingest queue capacity distributed across workers |

The service divides `INPUT_QUEUE_CAPACITY` by worker count to create one bounded queue per worker. Keep this value at least as large as the maximum worker count, which is 8, so every worker receives a non-zero channel capacity.

## HTTP, Cache, and Metrics

| Variable | Required | Default | Description |
|---|---:|---|---|
| `CACHE_BIND` | Yes | None | Cache API bind address, for example `0.0.0.0:8085` |
| `CACHE_TTL_MS` | Yes | None | Age threshold used to mark cached sensor states as stale |
| `CACHE_BUFFER` | Yes | None | Maximum cached sensor devices and SSE broadcast buffer size |
| `METRICS_BIND` | Yes | None | Prometheus endpoint bind address, for example `0.0.0.0:9090` |

`CACHE_BUFFER` is used for two limits:

- Maximum number of sensor devices retained in the cache.
- Number of events retained in the SSE broadcast channel.

When the sensor cache is full and a new device arrives, the stalest cached device is evicted.

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
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=smarthome-ingest
MQTT_TOPIC_SENSOR=smarthome/+/sensor
MQTT_TOPIC_STATUS=smarthome/+/status
MQTT_TOPIC_DLQ=smarthome/_dlq/ingest
INFLUX_URL=http://localhost:8086
INFLUX_ORG=smarthome
INFLUX_BUCKET=sensors
INFLUX_TOKEN=change-me
BATCH_SIZE=500
FLUSH_INTERVAL_MS=1000
WAL_DIR=./data/wal
ENFORCE_TOPIC_DEVICE_MATCH=true
METRICS_BIND=0.0.0.0:9090
CACHE_BIND=0.0.0.0:8085
CACHE_TTL_MS=60000
CACHE_BUFFER=1024
```
