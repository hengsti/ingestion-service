# HTTP API and Metrics

The service exposes a cache API on `CACHE_BIND` and Prometheus metrics on `METRICS_BIND`.

## Cache API

Routes with `cache: true` store the latest mapped record fields by normalized `device_id`.
The cache uses the receive timestamp captured at intake for `last_seen_ms`; it does not
represent persistence or sink delivery.

### `GET /healthz`

Returns `200 OK` when the cache HTTP server is alive.

### `GET /readyz`

Returns `200` when the active input source is ready (for MQTT, after a successful `SubAck`),
the WAL forwarder is running, and the WAL has room for one maximum-sized record. Returns
`503` otherwise. A ready service has acquired exclusive locks on both journal directories;
startup fails if either lock cannot be acquired.

### `GET /v1/state`

Returns the cached states in the existing `sensors` response envelope. Each `value` is the
typed field map produced by the selected route:

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
        "iaq_score": 85.0,
        "iaq_text": "Air quality is Good",
        "mqtt_retained": false
      }
    }
  ]
}
```

`stale` is computed from the current time and `last_seen_ms` using `CACHE_TTL_MS`.
When the cache is full, a new device replaces the least recently received entry only if its
receive sequence is newer; older delayed records are ignored by the cache, not the pipeline.

### `GET /v1/state/{device_id}`

Returns one device in the existing `sensor` envelope, or `null` when it is not cached.
Device lookup trims and lowercases the requested ID.

### `GET /v1/stream`

Streams cache updates using the existing SSE event name `sensor`; the JSON `value` is the
same typed field map returned by the state endpoints. A `lagged` event means the subscriber
fell behind; poll `/v1/state` to resynchronize. Keepalive text `ping` is sent every 15
seconds.

## Metrics API

### `GET /metrics`

Returns Prometheus text exposition with content type
`text/plain; version=0.0.4`. The recorder runs upkeep every 10 seconds.

Configured route metrics use a `route` label from the finite route IDs in
`INGEST_ROUTES_FILE`; message/device IDs and topics are not metric labels.
The six route-labeled counters below are registered at zero for every configured route at
startup. Route-labeled histograms appear when first recorded.

## Useful metrics

| Metric | Labels | Meaning |
|---|---|---|
| `mqtt_messages_received_total` | none | MQTT messages received |
| `ingest_event_queue_full_total` | none | Messages dropped before entering the pipeline |
| `ingest_decode_success_total` | none | Payloads decoded as JSON |
| `ingest_incoming_oversized_total` | none | Payloads larger than 64 KiB |
| `ingest_incoming_non_utf8_total` | none | Non-UTF-8 payloads |
| `ingest_incoming_invalid_json_total` | none | Invalid JSON payloads |
| `ingest_validate_raw_success_total` / `ingest_validate_raw_failed_total` | none | Configured raw-schema validation outcomes |
| `ingest_transform_success_total` | none | Successful configured mapping/calculation |
| `ingest_cache_updates_total` | `route` | Records cached by route |
| `ingest_cache_update_duration_seconds` | `route` | Cache-stage duration |
| `ingest_messages_enqueued_total` | `route` | Records durably appended to the WAL |
| `ingest_queue_full_total` / `ingest_queue_closed_total` | `route` | WAL append failures |
| `ingest_durability_ack_failed_total` | `route` | WAL acknowledgement failures |
| `ingest_persist_duration_seconds` | `route`, `result` | Persistence-stage duration |
| `ingest_messages_processed_total` | `route` | Records reaching the observation stage |
| `influx_write_duration_seconds` | none | Successful Influx write duration |
| `influx_lines_written_total` | none | Lines accepted by Influx write calls |
| `influx_write_success_total` / `influx_write_errors_total` | none | Influx write outcomes |
| `wal_forwarder_retry_outage_active` | none | Whether the forwarder is retaining a failed batch |
| `wal_accounted_bytes` / `wal_capacity_bytes` | none | Current accounted WAL bytes and configured limit |
| `quarantine_accounted_bytes` / `quarantine_capacity_bytes` | none | Accounted quarantine bytes and configured limit |
| `wal_capacity_full_total` | none | Events that encountered WAL capacity pressure |

Other decode, validation, DLQ, WAL, and forwarder counters are listed by their metric names
in the `/metrics` output. Journal gauges read in-memory accounting when scraped; they do not
measure total filesystem free space.
