# V2 route families

This synthetic bundle uses JSON only. Set `INGEST_ROUTES_FILE` to this directory's
`routes.json` and run the binary with the normal runtime environment and `--check-config`.
It validates offline and does not create journals or connect to MQTT/InfluxDB.
See the [environment reference](../../../docs/configuration.md).

The manifest explicitly selects two family files. `routes/climate.json` lists two
bindings sharing one mapping, writing to `example-sensors` and `example-metrics`.
`routes/health.json` uses a separate mapping and inherits `INFLUX_BUCKET`.
Schemas, including a globally registered resource and climate output validation, live
in `schemas/`. All their paths are relative to this directory, not to `routes/`.

Publish to `example/climate-a/unit-a` or `example/climate-b/unit-b`:

```json
{"temp_c":22.5,"rel_hum_perc":45,"time_ms":1700000000000,"time_valid":true}
```

The route derives `unit-a`/`unit-b` from topic segment 2, maps floats, supplies the default
`site_label="unassigned"`, omits absent `sensor_label`, calculates dew point and opts into
cache. `time_valid=false` selects the frozen receive timestamp instead of `time_ms`.
Add another binding with a unique ID and nonoverlapping topic to reuse the same mapping.

Publish `{"code":-2}` to `example/health/unit-c` for an uncached health point timestamped
at receipt. Bucket existence and permissions are not checked offline.

There are no mapping overrides, recursive includes or automatic file discovery.
For v1 compatibility and rollback, see [migration guidance](../../../docs/messages-and-routing.md#migrate-v1-to-v2).
