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

The loader accepts v1 inline routes or expands explicit v2 family files into the same flat
route model. V2 shares a mapping among short topic bindings; all files are loaded and
validated before activation, with no runtime configuration lookup or file access.
`main.rs` compiles the route bundle once. MQTT subscriptions come from that compiled set,
and each message selects a route by its configured topic filter. The route's schemas,
identity mapping, field mappings, calculations, measurement, sink target, output validation, and cache
opt-in determine processing. There is no message-type inference from topic suffixes or
publisher origins.

## Pipeline and record

Each worker carries the raw payload, frozen MQTT receive timestamp and retain flag, selected
route, and mapped record in a `PipelineContext`. The pipeline stages are:

1. **decode** — reject inputs over 64 KiB before UTF-8/JSON decoding.
2. **validate_raw** — select the configured route and validate its raw JSON schema.
3. **transform** — map configured JSON pointers, apply configured defaults, and run the
   route's resolved calculations into a typed `TelemetryRecord`. Mapping bounds copied
   data so repeated pointers cannot amplify one input without limit.
4. **validate_output** — enforce the selected route's tags and field types, timestamps,
   values and control-character rules, optional output schema, and the encoder's
   conservative WAL-size bound before caching or encoding.
5. **cache_update** — update latest state only when the route enables caching.
6. **persist** — synchronously encode the record, check its actual WAL size, and await
   `Wal::append_durable`.
7. **observe** — record success metrics labeled with configured route ID.
8. **dlq_publish** — publish rejected input through the active source's DLQ publisher.

Deterministic invalid input calls `ctx.mark_rejection(reason)` and reaches the durable
quarantine failure stage. Operational stage errors and `ctx.mark_deferred(reason)` defer
the input; they are not converted into rejection. `StageFlow::Stop` only stops remaining
stages, and an unmarked stop is deferred. A supplementary DLQ publication failure after
quarantine is logged, not recursively republished.

The canonical record contains a configured measurement, typed tags and fields, device ID,
source topic, frozen timestamp in epoch milliseconds, and MQTT retained metadata. The
reserved `device_id` tag and `mqtt_retained` boolean field are added for every route.
Calculations are resolved during route compilation; there are no firmware-only transform
branches. Static output names, control characters, and tag/field/calculation collisions
are checked when routes compile.

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
Its allocation-free size bound must cover every byte it will append and saturate on
overflow; an underestimate can cause excessive allocation or a cache update before
persistence rejects the actual output.
The Influx encoder uses the existing point builder for sorting and escaping, handles every
configured scalar type, and always writes the record timestamp with `precision=ms`.

The payload is encoded once before WAL append. Output WAL records also freeze the resolved
destination, using the [targeted frame format](wal-and-reliability.md#data-stored).
The forwarder keeps one bounded batch, flushes before a destination change, and writes
serially so the cursor never skips a failed target. A WAL append receipt follows a buffer flush and file sync; segment
creation and cursor replacement also sync their directory metadata. These guarantees depend
on the filesystem and storage honoring sync. For QoS 1 input, PUBACK is eligible only after
the output WAL or quarantine append is durable. The WAL reader consumes only the published
synced head and fails closed at read/corruption faults; readiness remains false while those
faults are active. Broker session expiry/capacity and the local journal budgets bound the
delivery guarantee. This is not a promise of lossless delivery or power-loss durability on
storage that does not honor sync. See [WAL and Reliability](wal-and-reliability.md).

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

On a stop signal or fatal task failure, the service stops accepting work and joins the source.
It releases persistence pressure waits and attempts to drain queued worker jobs and the
forwarder's final partial batch within a shared five-second deadline. Workers still attempt
durable appends when capacity is available; unfinished workers are aborted at the deadline.
Dropping WAL senders closes the writer. Unfinished input stays unacknowledged, and uncommitted
WAL entries remain available for replay if the service is restarted with the WAL files preserved.
