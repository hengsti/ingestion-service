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
}
