# Synthetic climate route example

This bundle uses route-bundle version `1` and JSON Schema Draft 7. It maps the
canonical `temp_c` and `rel_hum_perc` fields and calculates `dew_point_c`. Device
identity is taken from the third topic segment in the synthetic
`example/climate/<device>` filter.

The bundle is validated by `smarthome-ingest --check-config` and the
`config_preflight` integration tests. Keep this example synthetic; do not add real
device identities, broker credentials, or database tokens.

## Environment contract

The preflight reads the same environment variables as service startup. Required
variables are `INPUT_SOURCE`, `MQTT_HOST`, `MQTT_PORT`, `MQTT_CLIENT_ID`,
`MQTT_TOPIC_DLQ`, `INGEST_ROUTES_FILE`, `OUTPUT_SINK`, `INFLUX_URL`, `INFLUX_ORG`,
`INFLUX_BUCKET`, `INFLUX_TOKEN`, `BATCH_SIZE`, `FLUSH_INTERVAL_MS`, `WAL_DIR`,
`WAL_MAX_BYTES`, `QUARANTINE_DIR`, `QUARANTINE_MAX_BYTES`, `METRICS_BIND`,
`CACHE_TTL_MS`, `CACHE_BIND`, and `CACHE_BUFFER`. Optional variables and defaults
are listed in the [configuration reference](../../../docs/configuration.md).
