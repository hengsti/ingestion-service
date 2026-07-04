# HTTP API and Metrics

The service exposes two HTTP servers:

- Cache API on `CACHE_BIND`.
- Prometheus metrics API on `METRICS_BIND`.

## Cache API

The cache stores only latest sensor states. Status messages are forwarded to the active sink (InfluxDB today) but are not exposed through the cache API.

### `GET /healthz`

Returns `200 OK` when the cache HTTP server is alive.

### `GET /readyz`

Returns:

| Status | Meaning |
|---:|---|
| `200` | Active input source is ready (`ConnAck` for the current MQTT source) |
| `503` | Active input source is not ready |

With the current MQTT source, readiness flips to `200` after `ConnAck`. If event-loop polling fails, the main task exits with context `MQTT poll failed`.

### `GET /v1/state`

Returns all cached sensor states.

Example response:

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

`stale` is computed from `now_ms - last_seen_ms > CACHE_TTL_MS`.
`last_seen_ms` is the service cache update time, not the payload `time_ms`.

### `GET /v1/state/{device_id}`

Returns a single cached sensor state or `null`.

Example response:

```json
{
  "ttl_ms": 60000,
  "device_id": "esp32-1",
  "sensor": {
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
}
```

Unknown device response:

```json
{
  "ttl_ms": 60000,
  "device_id": "unknown-device",
  "sensor": null
}
```

Device lookup normalizes the requested id by trimming and lowercasing.

### `GET /v1/stream`

Returns an SSE stream of sensor cache updates.

Sensor event:

```text
event: sensor
data: {"kind":"sensor","device_id":"esp32-1","last_seen_ms":1700000000000,"value":{...}}
```

Lag event:

```text
event: lagged
data: {"hint":"poll /v1/state"}
```

The server sends keepalive text `ping` every 15 seconds.

## Metrics API

### `GET /metrics`

Returns Prometheus text exposition format with content type:

```text
text/plain; version=0.0.4
```

The metrics recorder runs upkeep every 10 seconds.

## Important Metrics

### Input and Decode

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `mqtt_messages_received_total` | counter | none | Messages that entered the decode stage from the current MQTT source |
| `ingest_event_queue_full_total` | counter | none | Incoming messages dropped before pipeline because a worker queue was full |
| `ingest_decode_payload_bytes` | histogram | none | Raw payload size |
| `ingest_decode_success_total` | counter | none | Payloads decoded as JSON |
| `ingest_incoming_oversized_total` | counter | none | Payloads larger than 64 KiB |
| `ingest_incoming_non_utf8_total` | counter | none | Non-UTF-8 payloads |
| `ingest_incoming_invalid_json_total` | counter | none | UTF-8 payloads that are not valid JSON |
| `ingest_decode_duration_seconds` | histogram | `result` | Decode stage duration |

### Validation and Transform

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `ingest_validate_raw_success_total` | counter | none | Raw schema validation success |
| `ingest_validate_raw_ignored_total` | counter | none | Non-strict router ignored a message |
| `ingest_validate_raw_failed_total` | counter | none | Raw validation failure |
| `ingest_validate_raw_duration_seconds` | histogram | `result` | Raw validation duration |
| `ingest_transform_attempt_total` | counter | none | Transform attempts |
| `ingest_transform_success_total` | counter | none | Transform successes |
| `ingest_transform_failed_total` | counter | none | Transform bound or normalization failures |
| `ingest_transform_deserialize_failed_total` | counter | none | Post-transform deserialization failures |
| `ingest_transform_ignored_total` | counter | none | Transform ignored a non-routed message |
| `ingest_transform_duration_seconds` | histogram | `result` | Transform duration |
| `ingest_validate_business_success_total` | counter | `kind` | Business schema success |
| `ingest_validate_business_failed_total` | counter | `kind` | Business schema failure |
| `ingest_validate_business_duration_seconds` | histogram | `kind`, `result` | Business validation duration |

### Cache, Persist, and DLQ

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `ingest_cache_updates_total` | counter | `kind` | Cache stage processed message by kind |
| `ingest_cache_update_duration_seconds` | histogram | `kind` | Cache stage duration |
| `ingest_messages_enqueued_total` | counter | `kind` | Messages appended to the WAL |
| `ingest_queue_full_total` | counter | `kind` | WAL writer queue was full |
| `ingest_queue_closed_total` | counter | `kind` | WAL writer queue was closed |
| `ingest_durability_ack_failed_total` | counter | `kind` | WAL writer failed a durability acknowledgement |
| `ingest_persist_duration_seconds` | histogram | `kind`, `result` | Persist stage duration |
| `dlq_messages_published_total` | counter | none | DLQ publish successes |
| `dlq_publish_errors_total` | counter | none | DLQ publish failures |
| `ingest_dlq_publish_duration_seconds` | histogram | `result` | DLQ publish duration |

### Pipeline and Influx Sink

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `ingest_messages_processed_total` | counter | none | Full pipeline successes reaching observe |
| `ingest_sensor_messages_processed_total` | counter | none | Successful sensor messages |
| `ingest_status_messages_processed_total` | counter | none | Successful status messages |
| `ingest_pipeline_duration_seconds` | histogram | none | Total successful pipeline duration |
| `influx_write_duration_seconds` | histogram | none | Successful Influx write duration |
| `influx_lines_written_total` | counter | none | Lines accepted by Influx write calls |
| `influx_write_success_total` | counter | none | Successful Influx write calls |
| `influx_write_errors_total` | counter | none | Influx write errors |

### WAL

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wal_forwarder_committed_total` | counter | none | WAL events committed after sink success |
| `wal_forwarder_drop_total` | counter | none | WAL events dropped after permanent sink failure |
| `wal_forwarder_retry_total` | counter | none | Forwarder retry loops after retryable sink failure |
| `wal_forwarder_commit_retry_total` | counter | none | Cursor commit retries after sink terminal outcome |
| `wal_forwarder_retry_outage_seconds` | histogram | none | Duration of retryable sink outage windows |
| `wal_forwarder_retry_outage_active` | gauge | none | `1` while holding a retryable failed batch, else `0` |
| `wal_subscription_corrupt_skipped_total` | counter | none | Durable corrupt WAL records skipped by subscription |
| `wal_writer_fatal_total` | counter | `reason` | Fatal WAL writer exits |
| `wal_segment_rotations_total` | counter | none | WAL segment rotations |
