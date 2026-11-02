# Architecture

`smarthome-ingest` receives telemetry from an input source, validates and maps it using a
configured route, appends the encoded output to a local WAL, and forwards WAL batches to an
output sink.

## Runtime flow

```text
Config + route bundle
  -> compile one immutable RouteSet
  -> MQTT subscriptions and pipeline route selection
  -> bounded worker queues
  -> decode -> raw schema -> map/calculations -> output validation
  -> optional cache -> typed encoder -> durable WAL append
  -> WAL forwarder -> sink
```

`main.rs` compiles the route bundle once. MQTT subscriptions come from that compiled set,
and each message selects a route by its configured topic filter. The route's schemas,
identity mapping, field mappings, calculations, measurement, output validation, and cache
opt-in determine processing. There is no message-type inference from topic suffixes or
publisher origins.

## Pipeline and record

Each worker carries the raw payload, frozen MQTT receive timestamp and retain flag, selected
route, and mapped record in a `PipelineContext`. The pipeline stages are:

1. **decode** — reject inputs over 64 KiB before UTF-8/JSON decoding.
2. **validate_raw** — select the configured route and validate its raw JSON schema.
3. **transform** — map configured JSON pointers, apply configured defaults, and run the
   route's resolved calculations into a typed `TelemetryRecord`.
4. **validate_output** — validate the normalized record against its optional output schema
   and enforce timestamp, key, value, control-character, and WAL-size constraints.
5. **cache_update** — update latest state only when the route enables caching.
6. **persist** — synchronously encode the record and await `Wal::append_durable`.
7. **observe** — record success metrics labeled with configured route ID.
8. **dlq_publish** — publish rejected input through the active source's DLQ publisher.

Stage errors mark the context for DLQ; an intentional `StageFlow::Stop` does not. The
failure publisher does not recursively republish its own failures.

The canonical record contains a configured measurement, typed tags and fields, device ID,
source topic, frozen timestamp in epoch milliseconds, and MQTT retained metadata. The
reserved `device_id` tag and `mqtt_retained` boolean field are added for every route.
Calculations are resolved during route compilation; there are no firmware-only transform
branches.

## Route configuration

`INGEST_ROUTES_FILE` is required. The versioned JSON bundle and its schema resources are
ordinary runtime configuration under `config/`; relative schema paths are resolved from
the bundle's directory. The shipped bundle preserves the firmware filters and the
`bme680` / `device_status` Influx measurements while allowing additional formats and topic
layouts through configuration.

The old `MQTT_TOPIC_SENSOR`, `MQTT_TOPIC_STATUS`, other non-DLQ topic variables, and
`ENFORCE_TOPIC_DEVICE_MATCH` are rejected with a migration hint. `MQTT_TOPIC_DLQ` remains
the publish-only destination for rejected messages.

## Input and output boundaries

Input transports implement `Source` and `DlqPublisher`; `build_source()` constructs the
matching pair. The MQTT source preserves bounded dispatch, QoS 1, reconnect behavior, and
readiness tracking while subscribing to the route set.

Output destinations implement `Sink` and `Encoder`; `build_output()` constructs the matching
pair. The synchronous encoder receives a validated `TelemetryRecord` and performs no I/O.
The Influx encoder uses the existing point builder for sorting and escaping, handles every
configured scalar type, and always writes the record timestamp with `precision=ms`.

The payload is encoded once before WAL append. `WalEvent` serialization and the on-disk WAL
format are unchanged. WAL append acknowledgement means the writer flushed its buffer; it
does not imply `fsync` or power-loss durability.

## Cache and HTTP

Routes may opt into the bounded latest-state cache. It stores typed record field maps by
normalized device ID, uses frozen intake time for freshness, and preserves the existing
`/v1/state`, `/v1/state/{device_id}`, and SSE envelopes. Cache state is not a delivery
receipt. See [HTTP API and Metrics](http-and-metrics.md).

## WAL forwarding and delivery

The forwarder batches entries by `BATCH_SIZE` or `FLUSH_INTERVAL_MS`, and attempts a final
partial batch during shutdown. It advances the commit cursor only after a successful sink
write. Both retryable and permanent failures retain the batch without advancing the cursor.
A crash after a successful sink write but before cursor commit can replay the write; this is
not exactly-once delivery. InfluxDB identifies points by measurement, tag set, and
timestamp, so same-identity writes merge fields and may overwrite matching fields. The
route cutover does not change delivery-outcome behavior tracked separately in #32.

## Shutdown

On Ctrl+C, HTTP failure, or fatal source error, the service stops accepting work, drains
queued worker jobs, closes the WAL writer, and gives the forwarder up to five seconds to
flush its final partial batch. An uncommitted WAL batch remains available for replay if the
service is restarted and the WAL files are preserved.
