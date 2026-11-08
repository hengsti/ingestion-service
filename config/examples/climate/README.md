# Synthetic climate route example

This version-1 bundle is a copyable example of one configured route. Its filter,
schemas, and mappings are in this directory; schema paths are relative to
`routes.json`.

| Route setting | Example behavior |
|---|---|
| Subscription | `example/climate/+` |
| Identity | MQTT topic segment 2, the device part of `example/climate/<device>` |
| Timestamp | Frozen receive time |
| Required fields | `/temp_c` and `/rel_hum_perc` map to float fields |
| Calculation | `dew_point` produces `dew_point_c` |
| Default | Missing `/site_label` produces `site_label: "unassigned"` |
| Optional | Missing `/sensor_label` omits that field |
| Cache | `false`; this example does not populate latest state |

For example, publishing this JSON to `example/climate/living-room` produces the
typed temperature/humidity fields, `dew_point_c`, and the default
`site_label`. `sensor_label` is absent because the payload omits it:

```json
{
  "temp_c": 22.5,
  "rel_hum_perc": 45.0
}
```

If `sensor_label` is present, it is included as a string. A default is used only
when its pointer is absent; `optional: true` omits an absent field. These two
policies cannot be set on the same mapping.

The shipped firmware examples are separate routes in
[`config/routes.json`](../../routes.json), with schemas under `config/schemas/`.
They preserve the `bme680` and `device_status` measurements and participate in
the same configured route pipeline.

The bundle is validated by `smarthome-ingest --check-config` and the
`config_preflight` integration tests. Static preflight validates configuration,
not broker access, credentials, writable runtime storage, or hardware capacity.
Keep this example synthetic; do not add real device identities, broker
credentials, or database tokens.

## Environment contract

The preflight reads the same environment variables as service startup. Required
variables are `INPUT_SOURCE`, `MQTT_HOST`, `MQTT_PORT`, `MQTT_CLIENT_ID`,
`MQTT_TOPIC_DLQ`, `INGEST_ROUTES_FILE`, `OUTPUT_SINK`, `INFLUX_URL`, `INFLUX_ORG`,
`INFLUX_BUCKET`, `INFLUX_TOKEN`, `BATCH_SIZE`, `FLUSH_INTERVAL_MS`, `WAL_DIR`,
`WAL_MAX_BYTES`, `QUARANTINE_DIR`, `QUARANTINE_MAX_BYTES`, `METRICS_BIND`,
`CACHE_TTL_MS`, `CACHE_BIND`, and `CACHE_BUFFER`. Optional variables and defaults
are listed in the [configuration reference](../../../docs/configuration.md).
