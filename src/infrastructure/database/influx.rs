use super::point::Point;
use crate::model::messages::{sensor::SensorMessage, status::StatusMessage};

pub fn sensor_to_point(msg: &SensorMessage) -> Point {
    let mut b = Point::build("bme680")
        .tag("device_id", &msg.device_id)
        .tag("room", &msg.room)
        .tag("device_class", &msg.device_class)
        .tag("fw_version", &msg.fw_version)
        .field_f64("temp_c", msg.data.temp_c)
        .field_f64("rel_hum_perc", msg.data.rel_hum_perc)
        .field_f64("pressure_hpa", msg.data.pressure_hpa)
        .field_f64("gas_ohm", msg.data.gas_ohm)
        .field_f64("iaq_score", msg.data.iaq_score)
        .field_str("iaq_text", &msg.data.iaq_text)
        .field_f64("dew_point_c", msg.data.dew_point_c)
        .field_f64("heat_index_c", msg.data.heat_index_c)
        .field_f64("altitude_m", msg.data.altitude_m)
        .field_bool("time_valid", msg.time_valid);

    // Timestamp nur verwenden, wenn valid und > 0 – sonst server time.
    if msg.time_valid && msg.time_ms > 0 {
        b = b.timestamp_ms(msg.time_ms);
    }

    b.build()
}

pub fn status_to_point(msg: &StatusMessage) -> Point {
    let mut b = Point::build("device_status")
        .tag("device_id", &msg.device_id)
        .tag("device_class", &msg.device_class)
        .tag("fw_version", &msg.fw_version)
        .tag("ip", &msg.ip)
        .field_str("time_iso", &msg.time_iso)
        .field_bool("time_valid", msg.time_valid)
        .field_i64("uptime", msg.uptime)
        .field_i64("free_mem", msg.free_mem)
        .field_str("ssid", &msg.ssid)
        .field_i64("rssi", msg.rssi);

    if msg.time_valid && msg.time_ms > 0 {
        b = b.timestamp_ms(msg.time_ms);
    }

    b.build()
}
