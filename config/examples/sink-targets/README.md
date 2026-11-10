# Synthetic sink targets

Set `INGEST_ROUTES_FILE` to this directory's `routes.json`. The two synthetic routes
write to `example-sensors` and `example-metrics` on the configured InfluxDB server
and organization. Create the buckets separately and give `INFLUX_TOKEN` write access
to both. Route JSON contains bucket names only; connection settings and credentials
remain in the environment.

Example input:

```text
example/samples/unit {"temperature":21.5,"count":7,"active":false,"label":"lab","time":1700000000123}
example/health/unit {"code":-2,"active":true,"time":1700000000124}
```

Omitting `sink_target` uses the required `INFLUX_BUCKET` fallback. Both explicit and
fallback destinations are frozen into new WAL records, independently of later route
edits. See [migration instructions](../../../docs/wal-and-reliability.md#sink-target-migration).
