# Broker prerequisites for deployment

This is an operator checklist, not a broker configuration managed by this
repository. Keep real device addresses and credentials in the Server deployment,
not in this example.

## Publish selected devices at QoS 1

For each Zigbee2MQTT device whose state is ingested, explicitly configure MQTT
publish QoS 1:

```yaml
devices:
  '<synthetic-device-address>':
    friendly_name: example_sensor
    qos: 1
```

Replace the placeholder locally; do not commit a real device address. The
ingestion service subscribes at QoS 1 and rejects QoS 0 as unrecoverable. A
persistent subscriber session cannot make a QoS 0 publication durable.

## Configure broker persistence and bounded queues

The isolated EMQX fixture uses these explicit settings:

```yaml
environment:
  EMQX_DURABLE_SESSIONS__ENABLE: "true"
  EMQX_MQTT__SESSION_EXPIRY_INTERVAL: 1h
  EMQX_MQTT__MAX_INFLIGHT: "16"
  EMQX_MQTT__MAX_MQUEUE_LEN: "1000"
volumes:
  - emqx-data:/opt/emqx/data
```

These are **integration-test values**, not production defaults or sizing
recommendations. Production must enable broker persistence, set session expiry,
in-flight limits, and per-client queued-message limits explicitly, and retain
the broker data across container restarts. Keep a stable, unique
`MQTT_CLIENT_ID` and `MQTT_CLEAN_SESSION=false` for the ingest subscriber.

Size the queue and expiry to the outage the deployment intends to tolerate.
Estimate queued messages from the measured peak publish rate multiplied by the
outage duration; account for payload bytes and broker storage headroom as well.
If the session expires or the configured queue/storage limit is exceeded,
messages may not be replayed. A durable session is bounded, not a lossless
delivery guarantee.

## Verify the deployed assumptions

`tests/compose.integration.yaml` pins the EMQX test version, enables durable
sessions, sets expiry and queue limits, and uses persistent test volumes.
`tests/broker_integration.rs` exercises queued and unacknowledged QoS 1 messages
across broker restart, as well as the explicit QoS 0 policy. These tests are
evidence for the isolated fixture only; verify the exact production EMQX
version and settings before the first production release. The service
repository does not edit or deploy the surrounding Server stack.
