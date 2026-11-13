# Messages and Routing

The route bundle is the contract between MQTT publishers and the ingest pipeline.
Set `INGEST_ROUTES_FILE` to a v1 inline JSON bundle or v2 manifest; the shipped configuration is
[`config/routes.json`](../config/routes.json). MQTT subscribes to the bundle's exact
filters. Every subscribed topic, including firmware sensor and status topics, is a
configured route. Route selection does not infer a message type from a topic suffix or
publisher. Schema and schema-resource paths are relative to the bundle file.

## Bundle files

The shipped v2 manifest lists active files under [`config/routes/`](../config/routes/).
Each file groups routes with exactly the same mapping:

```json
{
  "version": 2,
  "route_files": ["routes/climate.json", "routes/health.json"]
}
```

A family file has `routes` (bindings) followed by `mapping`. Every binding supplies
`id`, `topic`, and optionally `sink_target`. The mapping supplies the remaining existing
route properties: raw/output schemas, identity, measurement, tags, fields, timestamp,
calculations and cache policy. Add a device with the same format by adding a short binding;
the field mapping stays in one place. Different mappings or fixed identities need separate
files. Mapping overrides, inheritance and deep merging are unsupported. Unknown properties
and binding keys inside a mapping are rejected.

See the runnable [family example](../config/examples/families/README.md), which uses two
climate bindings with different destinations and a separate health family. The manifest
may also register shared schemas with the existing `schema_resources` URI-to-file map.

All relative paths resolve from the **manifest directory**, including schema paths inside
nested family files. V2 requires referenced files to exist inside that directory; absolute
paths, traversal/symlink escapes and repeated family-file references fail. Only listed
files are loaded, in manifest order and then binding order. Unlisted files are inactive;
there is no directory discovery or recursive include mechanism. CD additionally disallows
all symlinks in the runtime configuration.

At startup the loader expands the files into ordinary flat routes, then the existing
compiler validates the entire set. A failure prevents activation of the whole bundle.
Duplicate IDs, overlapping filters and incompatible field types are checked across files.
The same checks run with `--check-config`. There are extra file reads at startup; the
compiled per-message path and runtime route storage model are unchanged.

### Migrate v1 to v2

V1 inline bundles remain supported without rewriting them. Migration is optional:

1. Preserve the complete validated v1 directory and its compatible image as a rollback pair.
   Prepare v2 in a separate directory with copies of the same schemas.
2. For each route, move `id`, `topic` and its existing optional `sink_target` into a binding.
   Move every other property unchanged into `mapping`. Group bindings only when their
   mappings are identical; singleton families are valid.
3. Replace the root inline `routes` array with an explicit `route_files` array, set
   `version` to `2`, and keep `schema_resources` unchanged. Keep every schema path relative
   to the manifest directory. Preserve binding order, route IDs and exact MQTT filters.
4. With the normal runtime environment, validate the new directory using the candidate
   binary: `INGEST_ROUTES_FILE=/path/to/candidate/routes.json smarthome-ingest --check-config`.
   Confirm identical targets, types, timestamps, defaults, calculations and cache policy.
5. Stop ingest before switching the selected configuration directory and compatible image,
   then restart. Preserve MQTT identity/session and both journals. A layout-only migration
   keeps subscriptions unchanged; actual filter changes follow the existing subscription
   migration procedure below.

Binaries predating v2 cannot read the new manifest. Rolling back to one requires explicitly
selecting its compatible v1 directory as well as its image; CD never rewrites operator files
from a snapshot. Pending WAL records keep their encoded payload and destination. The separate
[WAL downgrade restrictions](wal-and-reliability.md#sink-target-migration) still apply.

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

## Zigbee2MQTT bridge health

[`zigbee2mqtt-bridge-health.json`](../config/routes/zigbee2mqtt-bridge-health.json)
selects only `zigbee2mqtt/bridge/health` and writes `zigbee2mqtt_bridge_health` to the
`metrics` bucket. Its fixed `device_id` and `device_class` are `zigbee2mqtt-bridge`;
`cache: false` keeps this operational record out of the latest sensor-state cache.

The mapping follows [Zigbee2MQTT health](https://www.zigbee2mqtt.io/guide/usage/health.html)
and its [implementation](https://github.com/Koenkk/zigbee2mqtt/blob/master/lib/extension/health.ts):

- `response_time` supplies the event timestamp in epoch milliseconds; it is not latency.
  Retained MQTT delivery preserves that original time.
- `os_load_1m`, `os_load_5m`, and `os_load_15m` are load averages, not CPU percentages.
- `os_memory_used_mib` is total minus free host memory. `process_memory_rss_mib` is
  Zigbee2MQTT's resident memory. The publisher divides bytes by 1024 twice despite its
  `_mb` names. Both `*_memory_perc` fields are percentages of total host memory.
- `process_uptime_sec` is Zigbee2MQTT process uptime. MQTT connection state is boolean;
  queued, published, and received counts remain integers. Published/received counts
  reset on restart, and also on each check when `health.reset_on_check` is enabled.
- For the two configured IEEE addresses, `device_<address>_messages`,
  `device_<address>_messages_per_sec`, `device_<address>_leave_count`, and
  `device_<address>_network_address_changes` remain numeric fields. The rate is an
  average since the first counted message in the statistics window. Device counters
  also reset on restart or `reset_on_check`.

An absent device contributes no fields; zero readings remain zero. Other device
addresses pass raw validation but are not stored until their optional field mappings
and output-schema properties are added. This route emits one bridge record per message.

## Route processing

For each subscribed message, the service:

1. Enforces the 64 KiB input limit, decodes UTF-8 JSON, and selects the matching route.
2. Validates the raw JSON against that route's schema.
3. Maps configured JSON pointers into typed tags and fields, applies configured defaults,
   and runs the route's named calculations.
4. Adds the reserved `device_id` tag and `mqtt_retained` boolean field, validates the
   normalized record against its output schema and runtime constraints, then encodes it.
5. Appends the resulting line protocol and resolved `sink_target` to the WAL before
   forwarding it to that InfluxDB bucket.

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
heat index, IAQ and the four climate quantities below. It emits `iaq_score` and `iaq_text`
together unless `warmed_up` is explicitly false. Warm-up does not suppress other calculations.

The same pipeline accepts routes for other device formats and topic layouts. A Zigbee or
other JSON payload needs only a route entry with a filter, raw schema, identity, mappings,
optional calculations/output schema, and measurement. There are no firmware-only runtime
branches.

### Message-local climate quantities

The firmware sensor route enables these independently selectable calculations. All
outputs are float **fields**, never tags, in the normalized record, `bme680` Influx
output and opt-in cache/API/SSE `value` map. Measurements, identity, timestamps and
HTTP/SSE envelopes keep their existing meaning; publisher payloads need no additions.

| Calculation name | Output field | Meaning and unit |
|---|---|---|
| `absolute_humidity` | `absolute_humidity_g_m3` | Water-vapor mass per air volume, g/m³ |
| `humidity_ratio` | `humidity_ratio_g_kg` | Water-vapor mass per dry-air mass, g/kg |
| `surface_temp_at_80rh` | `surface_temp_at_80rh_c` | Hypothetical temperature at which this air's vapor pressure corresponds to 80% RH, °C |
| `dew_point_depression` | `dew_point_depression_c` | Measured air temperature minus its existing dew point, °C |

For mapped temperature `T = temp_c` in °C, `RH = rel_hum_perc` in percent and total
pressure `p = pressure_hpa` in hPa:

```text
es = 6.112 * exp(17.67 * T / (T + 243.5))
e = es * RH / 100
absolute_humidity_g_m3 = 216.7 * e / (T + 273.15)
humidity_ratio_g_kg = 621.98 * e / (p - e)
x = ln(e / (0.8 * 6.112))
surface_temp_at_80rh_c = 243.5 * x / (17.67 - x)
dew_point_depression_c = T - dew_point_c(T, RH)
```

The existing dew-point formula is unchanged. Only humidity ratio needs pressure;
each calculation works alone from mapped floats. All require finite inputs,
`T > -273.15` and `0 < RH <= 100`. Vapor pressure must be positive and finite;
the singularity at `T = -243.5`, underflow to zero, overflow and non-finite
results are rejected. Humidity ratio also requires `p > e`; the surface inverse
rejects a non-finite result at `x = 17.67`. Dew-point depression retains the
existing dew-point singularity check through finite-result validation.

These are approximations intended for ordinary indoor/ambient temperatures.
The vapor-pressure approximation describes saturation over liquid water, not ice;
below-freezing and extreme temperatures are outside the intended indoor use.
Domain/finiteness checks do not certify accuracy over every accepted temperature.
The surface threshold is neither a measured wall/window temperature nor a mould
probability. Dew-point depression describes how close the measured air is to
saturation; it is not a wall/window condensation margin.

At `T=20`, `RH=50`, `p=1013.25`, the four outputs are approximately `8.63750 g/m³`,
`7.25631 g/kg`, `12.62770 °C` and `10.74571 °C`. Calculation failures use the
existing deterministic rejection/quarantine path. The extra fields increase
record, cache, WAL and storage size; no throughput or latency improvement is claimed.
Existing WAL payloads replay unchanged and do not acquire these fields retroactively.
Older binaries cannot load the new calculation names; rollback needs its previous
route/schema directory. Grafana, monitoring rollup allowlists and Advisor consumers
need separate updates to use the new persisted fields.

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

`sink_target` selects a literal bucket on the configured server and organization.
Older v1 bundles can omit it to inherit `INFLUX_BUCKET`. Changing a route target only
affects newly admitted records: pending WAL output retains its saved destination.
Field-type compatibility is checked across routes sharing a bucket and measurement;
independent buckets may use different field types. The
[synthetic sink-target example](../config/examples/sink-targets/README.md) demonstrates
separate sample and health destinations without production device assignments.

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
