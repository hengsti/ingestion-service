# smarthome-ingest

A high-performance Rust microservice to ingest smarthome telemetry and device statuses from an MQTT broker, validate the data, and batch-insert it into InfluxDB v2. Invalid data is automatically diverted to a Dead Letter Queue (DLQ) topic on the MQTT broker.

## Features

- **MQTT Integration**: Subscribes to configured MQTT topics via `rumqttc`.
- **JSON Schema Validation**: Validates incoming messages strictly against defined `.schema.json` files for sensors and status messages.
- **Dead Letter Queue (DLQ)**: Invalid payloads (e.g. non-UTF-8, malformed JSON, schema mismatch) are cleanly diverted to a DLQ topic.
- **InfluxDB Batching**: Converts valid JSON inputs to InfluxDB Line Protocol points and batches them for high-throughput writes. 
- **Prometheus Metrics**: Exposes metrics natively on a dedicated bind address (default `:9090/metrics`) for observability.

## Schemas & Routing

Incoming messages are routed and validated based on their topic.

- `schema/bme680.schema.json` - Describes the payload structure for BME680 sensor telemetry.
- `schema/status.schema.json` - Describes the payload structure for general device status updates.

If `ENFORCE_TOPIC_DEVICE_MATCH` is set to `true`, the ingestion service ensures that the `{device_id}` included in the JSON payload matches the MQTT topic it arrived on.

## Configuration

The service is fully configured via environment variables.

| Environment Variable | Description | Default |
| --- | --- | --- |
| `MQTT_HOST` | Hostname of the MQTT broker. | `nanomq` |
| `MQTT_PORT` | Port of the MQTT broker. | `1883` |
| `MQTT_USERNAME` | (Optional) Username for MQTT authentication. | |
| `MQTT_PASSWORD` | (Optional) Password for MQTT authentication. | |
| `MQTT_CLIENT_ID` | Unique Client ID for the MQTT connection. | `smarthome-ingest-<unix_timestamp>` |
| `MQTT_TOPIC` | Topic pattern to subscribe to for sensor telemetry. | `smarthome/+/bme680` |
| `DLQ_TOPIC` | Topic pattern to publish invalid/failed messages. | `smarthome/_dlq/ingest` |
| `INFLUX_URL` | Base URL of the InfluxDB v2 API. | `http://influxdb:8086` |
| `INFLUX_ORG` | InfluxDB Organization string. | `smarthome` |
| `INFLUX_BUCKET` | InfluxDB Bucket string to push data into. | `sensors` |
| `INFLUX_TOKEN` | **[Required]** InfluxDB API token with write permissions. | |
| `BATCH_SIZE` | Maximum number of data points to accumulate before writing to Influx. | `500` |
| `FLUSH_INTERVAL_MS` | Maximum amount of time (in milliseconds) to wait before flushing the current payload batch to InfluxDB. | `1000` |
| `ENFORCE_TOPIC_DEVICE_MATCH`| Ensure the `device_id` in the JSON payload matches the topic. | `true` |
| `METRICS_BIND` | Bind address/port for the Prometheus metrics server. | `0.0.0.0:9090` |
| `RUST_LOG` | Log level (e.g., `info`, `debug`, `error`). | `info` (via env-filter) |

## Observability & Metrics

A Prometheus-compatible endpoint is available at `http://<METRICS_BIND>/metrics`. It emits tracking details around ingest pipeline throughput, MQTT state, DLQ routing hits, and validation failures.

**Key Metrics Include**:
* `mqtt_messages_received_total`
* `ingest_incoming_non_utf8_total`
* `ingest_incoming_invalid_json_total`
* `dlq_messages_published_total`
* `dlq_publish_errors_total`

## Building and Running

### Running Locally

Ensure that you have an MQTT broker and InfluxDB instance available. 

```bash
export INFLUX_TOKEN="your_token_here"
export MQTT_HOST="localhost"
cargo run --release
```

### Building with Docker

A multi-stage `Dockerfile` is included. To build the container image:

```bash
docker build -t smarthome-ingest .
```

To run the container:

```bash
docker run -d \
  -e INFLUX_TOKEN="your_token" \
  -e INFLUX_URL="http://influxdb:8086" \
  -e MQTT_HOST="mqtt-broker" \
  smarthome-ingest
```

## Architecture

1. **Reception**: Data flows in from the rumqttc `AsyncClient` loop.
2. **Validation (`src/validate.rs`)**: Payloads are matched against compiled JSON Schemas based on their respective topics.
3. **Rejection (`src/dlq.rs`)**: Failures are republished with error context attached to the DLQ topic.
4. **Transformation (`src/db/influx.rs`)**: Messages are reshaped into tagged `Point` instances.
5. **Batching & Egress**: Points are channeled to a dedicated async batcher loop, writing flushed bodies to InfluxDB via `reqwest` at the configured interval or size threshold.
