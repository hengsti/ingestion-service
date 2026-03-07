use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SensorMessage {
    pub device_id: String,
    pub room: String,
    pub device_class: String,
    pub fw_version: String,
    pub time_ms: i64,
    pub time_iso: String,
    pub time_valid: bool,
    pub data: SensorData,
    pub status: SensorStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SensorData {
    pub temp_c: f64,
    pub rel_hum_perc: f64,
    pub pressure_hpa: f64,
    pub gas_ohm: f64,
    pub iaq_score: f64,
    pub iaq_text: String,
    pub dew_point_c: f64,
    pub heat_index_c: f64,
    pub altitude_m: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SensorStatus {
    pub uptime: i64,
    pub free_mem: i64,
    pub rssi: i64,
    pub ssid: String,
}
