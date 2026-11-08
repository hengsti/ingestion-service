# HTTP API and Metrics

The service exposes a cache API on `CACHE_BIND` and Prometheus metrics on `METRICS_BIND`.

## Cache API

Routes with `cache: true` store the latest mapped record fields by normalized `device_id`.
The cache uses the receive timestamp captured at intake for `last_seen_ms`; it does not
represent persistence or sink delivery. It is process-local and starts empty after restart;
a retained broker publication can repopulate it if delivered after subscription. This cached
snapshot is not an event history or a delivery receipt. The shipped
[`firmware-sensor` route](../config/routes.json) enables the cache; `firmware-status` does not.

### `GET /healthz`

Returns `200 OK` while the process serves HTTP. This is liveness, not proof of
MQTT connectivity, writable storage, or database delivery.

### `GET /readyz`

Returns `200` only when intake is active, MQTT subscriptions have been reconciled with
matching QoS 1 acknowledgements, all workers and the forwarder are alive, bounded
admission is available, and both journal writers are writable with room for a
maximum-sized record. Reader failure or cursor commit/reclaim retry also returns `503`.
Shutdown disables readiness before draining workers.

| Condition | `/healthz` | `/readyz` |
|---|---|---|
| Reconciled source, available admission/storage, live forwarder | 200 | 200 |
| Influx retrying or permanently blocked, local capacity still available | 200 | 200 |
| Reconnect, pending/rejected subscriptions or downgraded QoS grant | 200 | 503 |
| Admission paused/full, either journal full/unwritable | 200 | 503 |
| Dead worker/forwarder, reader failure, cursor commit/reclaim retry, shutdown | 200 while HTTP is serving | 503 |

Readiness uses in-memory state, not network probes or filesystem scans. It reads the
existing bounded worker queues (at most eight) through weak handles, without retaining
their senders or interrupting MQTT polling. Journal accounting and task-exit flags
update directly. Startup still requires exclusive locks on both journal directories.

### `GET /v1/operations`

Returns `200` with a bounded operational JSON snapshot, independently of readiness.
Boolean fields describe `ready`, `source_ready`, `workers_alive`, `accepting`,
`admission_available`, `wal_writable`, `quarantine_writable`, `forwarder_alive`,
`read_failed` and `commit_retrying`. `sink` is `healthy`, `retrying` or `blocked`.
Numeric fields expose `wal_bytes`, `wal_budget_bytes`, `quarantine_bytes`,
`quarantine_budget_bytes`, `pending_acks`, `queue_available`, `queue_capacity` and
`oldest_pending_output_age_seconds`.

The endpoint contains no device IDs, topics, payloads, secrets or internal error text.
Sink health is separate from durable intake readiness: a growing backlog can be
accepted locally during database maintenance until bounded storage is exhausted.
The cache and SSE envelopes below are unchanged and are not delivery receipts.

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
Route-labeled counters are registered at zero for every configured route at startup.
Pipeline/stage outcomes also use `route=""` before a route can be selected, including
decode failures and unrouted input. Stage labels are the eight configured pipeline/failure
stage names or `other`; stage outcomes are `continue`, `stop` or `error`. Terminal
pipeline outcomes distinguish stored output, stored rejection and deferred work.
Route-labeled histograms appear when first recorded.

## Useful metrics

| Metric | Labels | Meaning |
|---|---|---|
| `mqtt_messages_received_total` | none | MQTT messages received |
| `ingest_event_queue_full_total` | none | Admission attempts with no available worker; MQTT withholds ACK and applies backpressure |
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
| `ingest_persist_duration_seconds` | `route`, `result` | Persistence-stage duration; `result=stopped` covers pressure waits stopped by shutdown or forwarder termination (previously `forwarder_stopped`) |
| `ingest_pipeline_outcomes_total` | `route`, `outcome` | Local terminal results: `output_stored`, `rejection_stored`, or `deferred`; not database delivery |
| `ingest_stage_outcomes_total` | `route`, `stage`, `outcome` | Completed stage invocations: `continue`, `stop`, `error` |
| `ingest_pipeline_duration_seconds` | `route` | Duration of pipelines reaching the observe stage |
| `quarantine_appends_total` | none | Rejections durably appended locally; supplementary MQTT DLQ publication is separate |
| `influx_write_duration_seconds` | none | Successful Influx write duration |
| `influx_lines_written_total` | none | Lines accepted by Influx write calls |
| `influx_write_success_total` / `influx_write_errors_total` | none | Influx write outcomes |
| `wal_forwarder_retry_outage_active` | none | Whether the forwarder is retaining a failed batch |
| `wal_forwarder_delivered_total` | none | Records in successful sink writes; replay can count duplicate writes |
| `wal_forwarder_committed_total` | none | Delivered records whose cursor commit/reclaim completed successfully |
| `ingest_ready` / `ingest_source_ready` / `ingest_workers_alive` | none | Durable readiness, reconciled source readiness and worker liveness (0/1) |
| `ingest_admission_pressure` | none | No admission available or source admission paused (0/1) |
| `ingest_queue_available` / `ingest_queue_capacity` | none | Free worker queue slots and configured total slots; excludes in-flight jobs |
| `mqtt_pending_acks` | none | Current-generation admitted QoS 1 jobs awaiting successful PUBACK queueing; not broker-confirmed ACK delivery |
| `wal_writable` / `quarantine_writable` | none | Journal writer has not stopped or failed (0/1) |
| `wal_forwarder_alive` / `wal_forwarder_read_failed` / `wal_forwarder_commit_retrying` | none | Task liveness, explicit reader failure and cursor commit/reclaim retry (0/1) |
| `ingest_sink_state` | `state` | One-hot `healthy`, `retrying`, `blocked`; inspect alongside forwarder liveness |
| `wal_oldest_pending_output_age_seconds` | none | Age since append of the oldest observed pending output; includes sink and cursor retries |
| `wal_accounted_bytes` / `wal_capacity_bytes` | none | Current accounted WAL bytes and configured limit |
| `quarantine_accounted_bytes` / `quarantine_capacity_bytes` | none | Accounted quarantine bytes and configured limit |
| `wal_capacity_full_total` | none | Events that encountered WAL capacity pressure |

Other decode, validation, DLQ, WAL, and forwarder counters are listed by their metric names
in the `/metrics` output. Journal gauges read in-memory accounting when scraped; they do not
measure total filesystem free space. Counters reset with the process. Capacity gauges
recover from retained journal files and change after synced reclaim; pending ACKs reset
on connection replacement. Backlog age is rebuilt from persisted WAL timestamps when
the forwarder reads its first pending record after restart. Between batches it retains
a conservative age until the next pending record is read, rather than reporting zero
while unread output remains. It becomes zero after committing through the durable head
or observing a drained reader, including an empty post-rotation segment. It does not
count work still in the input/worker queues.

### Dashboard and alert compatibility

Use configured `route` IDs instead of the removed message-family `kind=sensor|status`
labels. `ingest_pipeline_outcomes_total` and `ingest_pipeline_duration_seconds` now
include `route`; aggregate across it when retaining an existing total-series panel.
`wal_forwarder_committed_total` now counts successful cursor commits, rather than sink
write success before cursor commit. Use `wal_forwarder_delivered_total` or
`influx_lines_written_total` for database delivery, and local pipeline/quarantine
counters for durable acceptance. Neither set of counters proves exactly-once delivery.
