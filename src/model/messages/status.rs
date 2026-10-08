use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct StatusMessage {
    pub device_id: String,
    pub device_class: String,
    pub fw_version: String,
    pub ip: String,
    pub rssi: i64,
    pub time_ms: i64,
    pub time_iso: String,
    pub time_valid: bool,
    pub uptime: i64,
    pub free_mem: i64,
    pub ssid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped_total: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples_failed_total: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mqtt_connects_total: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensor_read_failures_total: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<String>,
}
