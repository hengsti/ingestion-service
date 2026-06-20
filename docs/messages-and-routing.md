# Messages and Routing

This document is the integration contract for MQTT publishers.

## Topics

Recommended topic layout:

```text
smarthome/<device_id>/sensor
smarthome/<device_id>/status
smarthome/_dlq/ingest
```

Recommended environment:

```text
MQTT_TOPIC_SENSOR=smarthome/+/sensor
MQTT_TOPIC_STATUS=smarthome/+/status
MQTT_TOPIC_DLQ=smarthome/_dlq/ingest
```

The route matcher supports `+` and `#` MQTT wildcards. `#` must be the final segment. The first `+` segment is treated as the device id position for `ENFORCE_TOPIC_DEVICE_MATCH=true`.

## Raw Sensor Message

Raw sensor messages are validated against `schema/sensor.schema.json` plus `schema/base.schema.json`.

Required base fields:

| Field | Type | Notes |
|---|---|---|
| `device_id` | string | Non-empty |
| `device_class` | string | Non-empty |
| `fw_version` | string | Non-empty |
| `time_ms` | integer | Minimum `0` |
| `time_iso` | string | Non-empty and parsed as RFC3339 by the router |
| `time_valid` | boolean | Controls whether `time_ms` is used as the InfluxDB timestamp |

Required sensor fields:

| Field | Type | Notes |
|---|---|---|
| `room` | string | Room name |
| `data.temp_c` | number | Temperature in Celsius |
| `data.rel_hum_perc` | number | Relative humidity percentage |
| `data.pressure_hpa` | number | Atmospheric pressure in hPa |
| `data.gas_ohm` | number | Gas resistance in Ohms |
| `data.altitude_m` | number | Approximate altitude in meters |

Example:

```json
{
  "device_id": "esp32-1",
  "room": "living_room",
  "device_class": "esp32p4-bme680",
  "fw_version": "1.0.0",
  "time_ms": 1700000000000,
  "time_iso": "2023-11-14T22:13:20Z",
  "time_valid": true,
  "data": {
    "temp_c": 22.5,
    "rel_hum_perc": 45.0,
    "pressure_hpa": 1013.25,
    "gas_ohm": 50000.0,
    "altitude_m": 500.0
  }
}
```

## Canonical Sensor Message

The transform stage adds these fields under `data`:

| Field | Type | Source |
|---|---|---|
| `dew_point_c` | number | Calculated from temperature and humidity |
| `heat_index_c` | number | Calculated from temperature and humidity |
| `iaq_score` | number | Calculated from gas resistance and humidity |
| `iaq_text` | string | Human-readable air quality classification |

Transform bounds:

| Field | Valid range |
|---|---|
| `data.rel_hum_perc` | `(0, 100]` |
| `data.gas_ohm` | `> 0` |
| `data.pressure_hpa` | `[300, 1200]` |

After transform, `schema/sensor.business.schema.json` requires the derived fields and disallows additional properties in the canonical object sections.

## Raw Status Message

Raw status messages are validated against `schema/status.schema.json` plus `schema/base.schema.json`.

Required status fields:

| Field | Type | Notes |
|---|---|---|
| `ip` | string | Current IP address |
| `rssi` | integer | RSSI in dBm |

The raw schema defines `uptime`, `free_mem`, and `ssid`, but does not require them. The transform stage fills missing values:

| Field | Default |
|---|---|
| `uptime` | `0` |
| `free_mem` | `0` |
| `ssid` | `""` |

Example:

```json
{
  "device_id": "esp32-1",
  "device_class": "esp32p4-bme680",
  "fw_version": "1.0.0",
  "ip": "192.168.1.42",
  "rssi": -65,
  "time_ms": 1700000000000,
  "time_iso": "2023-11-14T22:13:20Z",
  "time_valid": true,
  "uptime": 3600,
  "free_mem": 200000,
  "ssid": "HomeNet"
}
```

## Canonical Status Message

After transform, `schema/status.business.schema.json` requires:

- `ip`
- `rssi`
- `uptime`
- `free_mem`
- `ssid`

It also requires `uptime >= 0`, `free_mem >= 0`, and a non-empty `ip`.

## String Normalization

The transform stage trims whitespace around these root string fields:

| Message | Fields |
|---|---|
| Sensor | `device_id`, `room`, `device_class`, `fw_version`, `time_iso` |
| Status | `device_id`, `device_class`, `fw_version`, `ip`, `time_iso`, `ssid` |

The cache lowercases and trims sensor `device_id` keys for lookup.

## InfluxDB Mapping

Sensor messages become measurement `bme680`.

Tags:

- `device_id`
- `room`
- `device_class`
- `fw_version`

Fields:

- `temp_c`
- `rel_hum_perc`
- `pressure_hpa`
- `gas_ohm`
- `iaq_score`
- `iaq_text`
- `dew_point_c`
- `heat_index_c`
- `altitude_m`
- `time_valid`

Status messages become measurement `device_status`.

Tags:

- `device_id`
- `device_class`
- `fw_version`
- `ip`

Fields:

- `time_iso`
- `time_valid`
- `uptime`
- `free_mem`
- `ssid`
- `rssi`

For both message types, `time_ms` is written as the InfluxDB timestamp only when `time_valid=true` and `time_ms > 0`. Otherwise the timestamp is omitted and InfluxDB uses server time.

## DLQ Message

When a pipeline stage rejects a message, the failure stage publishes JSON to `MQTT_TOPIC_DLQ` with QoS `AtLeastOnce`:

```json
{
  "received_at": "2026-06-20T12:00:00+00:00",
  "src_topic": "smarthome/esp32-1/sensor",
  "error": "raw validation failed: schema validation failed: ...",
  "payload_raw": "{\"device_id\":\"esp32-1\"}"
}
```

Non-UTF-8 payloads use `"<non-utf8>"` as `payload_raw`.
