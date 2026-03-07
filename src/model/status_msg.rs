use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct StatusMessage {
    pub device_id: String,
    pub device_class: String,
    pub fw_version: String,
    pub ip: String,
    pub rssi: i64,
    pub time_ms: i64,
    pub time_iso: String,
    pub time_valid: bool,
}