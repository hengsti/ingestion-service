# Messages and Routing

The route bundle is the contract between MQTT publishers and the ingest pipeline.
Set `INGEST_ROUTES_FILE` to the v1 JSON bundle; the shipped configuration is
[`config/routes.json`](../config/routes.json). MQTT subscribes to the bundle's exact
filters. Every subscribed topic, including firmware sensor and status topics, is a
configured route. Route selection does not infer a message type from a topic suffix or
publisher. Schema and schema-resource paths are relative to the bundle file.

## Synthetic climate walkthrough

The tracked [`config/examples/climate`](../config/examples/climate/README.md) bundle is the
copyable synthetic example. Its README includes a payload; `routes.json` selects
`example/climate/+`, extracts the device ID from topic segment 2, freezes receive time,
maps `/temp_c` and `/rel_hum_perc` as floats, and selects `dew_point`. Missing
`/site_label` maps to the default `"unassigned"`; missing `/sensor_label` is omitted.
`cache: false` is explicit. The raw and output schemas are in that bundle's `schemas/`
directory, and the same bundle is covered by configuration-preflight tests.

Run the same static preflight used for startup after changing the example:

```bash
INGEST_ROUTES_FILE=./config/examples/climate/routes.json ./target/release/smarthome-ingest --check-config
```

The shipped firmware examples are in [`config/routes.json`](../config/routes.json):
`firmware-sensor` subscribes to `smarthome/+/bme680`, writes `bme680`, and opts into
cache; `firmware-status` subscribes to `smarthome/+/status`, writes `device_status`,
and opts out. Their raw and output schemas live under `config/schemas/`.

## Route processing

For each subscribed message, the service:

1. Enforces the 64 KiB input limit, decodes UTF-8 JSON, and selects the matching route.
2. Validates the raw JSON against that route's schema.
3. Maps configured JSON pointers into typed tags and fields, applies configured defaults,
   and runs the route's named calculations.
4. Adds the reserved `device_id` tag and `mqtt_retained` boolean field, validates the
   normalized record against its output schema and runtime constraints, then encodes it.
5. Appends the resulting line protocol to the WAL before forwarding it to InfluxDB.

At startup, routes with duplicate IDs, reserved or colliding tag/field names, collisions
with calculated fields, or control characters in static output names or values are
rejected. Mapping also limits copied data, including repeated uses of the same JSON
pointer, to prevent a small input from expanding without bound.

`device_id` may also be checked against an explicitly configured topic segment. This
behavior is part of the selected route, not a global MQTT setting. The service captures
the MQTT receive timestamp and retain flag once at intake; a configured timestamp mapping
can use that frozen receive time when the payload timestamp is not valid.

## Shipped firmware routes

The default bundle keeps the existing firmware subscriptions and Influx measurement names:

| Route | MQTT filter | Measurement | Cache |
|---|---|---|---:|
| `firmware-sensor` | `smarthome/+/bme680` | `bme680` | Yes |
| `firmware-status` | `smarthome/+/status` | `device_status` | No |

Both routes use raw and output schemas from `config/schemas/`. Sensor fields map from
`data.*`; status fields map from the root payload. The status route's configured defaults
remain `0` for missing `uptime`/`free_mem` and `""` for missing `ssid`. Optional fields
remain absent if the publisher omits them. The firmware sensor route calculates dew point,
heat index, and IAQ. It emits `iaq_score` and `iaq_text` together unless `warmed_up` is
explicitly false.

The same pipeline accepts routes for other device formats and topic layouts. A Zigbee or
other JSON payload needs only a route entry with a filter, raw schema, identity, mappings,
optional calculations/output schema, and measurement. There are no firmware-only runtime
branches.

## Firmware compatibility and migration

The unified route model replaces `MQTT_TOPIC_SENSOR`, `MQTT_TOPIC_STATUS`, other
non-DLQ `MQTT_TOPIC_*` routing variables, and `ENFORCE_TOPIC_DEVICE_MATCH`; these
settings are rejected with a migration hint. Move every desired filter and identity rule
into the versioned route bundle, including the firmware routes, then follow the
[stopped-ingest filter/session migration procedure](operations.md#route-changes-and-subscription-migration).
Do not clear a persistent broker session to force a route update. A resumed MQTT 3.1.1
session cannot enumerate unknown old filters, and reconnect does not reconcile them
without trusted filter history. Already queued or in-flight publications for a removed
filter can still arrive and be rejected.
Where the previous firmware deployment used the default `smarthome/+/sensor` filter,
the shipped sensor route now uses `smarthome/+/bme680`; verify the actual old filters
rather than assuming every deployment used that default.

The firmware routes preserve the `bme680` and `device_status` measurement names and
existing tag/field names and types. Documented additions/deltas are the reserved
`device_id` tag and `mqtt_retained` field on every route, `altitude_m` on `bme680`,
`time_iso` as a string field on both firmware measurements, present optional booleans
(including `replayed: false`), and the frozen receive time when `time_valid` is false.
A valid payload timestamp of zero is rejected rather than replaced with server time.
When `warmed_up` is false, both IAQ outputs are omitted. The validated payload `time_iso`
is copied unchanged and also appears in the sensor cache; point timestamps still follow
the configured `time_ms`/`time_valid` policy, including receive-time fallback.
See the canonical [unified-route compatibility decision in issue #19](https://github.com/hengsti/ingestion-service/issues/19).

HomeKit continues to read cached `device_id`, `last_seen_ms`, and
`value.{temp_c, rel_hum_perc, iaq_score, iaq_text}`. A retained sensor publication
delivered by the broker can seed the process-local cache after subscription; cache
contents are not persisted across process restarts and do not prove WAL or InfluxDB
delivery. The `route` metric label replaces the old `kind=sensor|status` label; see
[HTTP API and Metrics](http-and-metrics.md#dashboard-and-alert-compatibility).

## InfluxDB output

One encoder renders every normalized record. It writes the configured measurement, sorted
tags and fields, and the record's validated epoch-millisecond timestamp using
`precision=ms`. Values retain their configured types: floats, signed integers (`i`),
unsigned integers (`u`), booleans, and quoted strings. Optional unmapped values are not
fabricated. Existing Influx series and field names are preserved by the shipped route
bundle; the `device_id` tag and `mqtt_retained` field are emitted for every route.

Output validation rejects fields or tags that do not match the route, invalid timestamps,
non-finite numbers, unsupported control characters, and an encoder size bound that exceeds
the WAL reader's record limit. The bound is conservative and is checked before caching
or encoding, so an output near the limit can be rejected even if its actual bytes would
fit. Persistence checks the actual encoded size again before WAL append. Since each point
has a frozen timestamp, re-encoding a record or replaying its saved WAL payload does not
depend on InfluxDB server time.

InfluxDB merges writes that have the same measurement, tag set, and timestamp; fields in
subsequent writes at that point can overwrite matching field keys. WAL replay can cause
at-least-once write attempts, not exactly-once end-to-end delivery: a crash after a
successful sink write but before cursor commit can replay the same point. A permanent
database row per accepted input is not guaranteed. This route cutover does not change the
separate delivery-outcome work tracked by #32.

## Cache

Only routes with `cache: true` update the latest-state cache. Entries are keyed by the
normalized `device_id`, replaced by newer records, and expose the typed record field map as
`value`. The HTTP response envelopes and SSE `sensor` event name remain stable for existing
HomeKit clients. Cache state is not a persistence or delivery receipt.

## DLQ message

Rejected messages are published to the configured `MQTT_TOPIC_DLQ` with QoS
`AtLeastOnce`:

```json
{
  "received_at": "2026-06-20T12:00:00+00:00",
  "src_topic": "smarthome/esp32-1/bme680",
  "error": "raw validation failed: ...",
  "payload_raw": "{\"device_id\":\"esp32-1\"}"
}
```

Non-UTF-8 payloads use `"<non-utf8>"` as `payload_raw`.
