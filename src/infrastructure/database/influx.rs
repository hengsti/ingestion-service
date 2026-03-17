use super::point::Point;
use crate::model::messages::{sensor::SensorMessage, status::StatusMessage};
use anyhow::{Context, Result};
use metrics::{counter, histogram};
use reqwest::Client;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct InfluxWriter {
    client: Client,
    write_url: String,
    token: String,
}

impl InfluxWriter {
    pub fn new(influx_url: &str, org: &str, bucket: &str, token: &str) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build reqwest client")?;

        // InfluxDB v2 write endpoint
        let write_url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ms",
            influx_url.trim_end_matches('/'),
            urlencoding::encode(org),
            urlencoding::encode(bucket)
        );

        Ok(Self {
            client,
            write_url,
            token: token.to_string(),
        })
    }

    pub async fn run_batcher(
        self,
        mut rx: mpsc::Receiver<String>,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Result<()> {
        let mut buf: Vec<String> = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(Duration::from_millis(flush_interval_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        self.flush(&mut buf).await?;
                    }
                }
                maybe = rx.recv() => {
                    match maybe {
                        None => {
                            if !buf.is_empty() {
                                self.flush(&mut buf).await?;
                            }
                            return Ok(());
                        }
                        Some(line) => {
                            buf.push(line);
                            if buf.len() >= batch_size {
                                self.flush(&mut buf).await?;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn flush(&self, buf: &mut Vec<String>) -> Result<()> {
        let lines = buf.len();
        let body = buf.join("\n");
        buf.clear();

        let start = Instant::now();

        let resp = self
            .client
            .post(&self.write_url)
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await
            .context("Influx write request failed")?;

        histogram!("influx_write_duration_seconds").record(start.elapsed().as_secs_f64());
        counter!("influx_lines_written_total").increment(lines as u64);

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            counter!("influx_write_errors_total").increment(1);
            anyhow::bail!("Influx write failed: status={} body={}", status, text);
        }

        counter!("influx_write_success_total").increment(1);
        Ok(())
    }
}

pub fn sensor_to_point(msg: &SensorMessage) -> Point {
    let mut b = Point::build("bme680")
        .tag("device_id", &msg.device_id)
        .tag("room", &msg.room)
        .tag("device_class", &msg.device_class)
        .tag("fw_version", &msg.fw_version)
        .tag("ssid", &msg.status.ssid)
        .field_f64("temp_c", msg.data.temp_c)
        .field_f64("rel_hum_perc", msg.data.rel_hum_perc)
        .field_f64("pressure_hpa", msg.data.pressure_hpa)
        .field_f64("gas_ohm", msg.data.gas_ohm)
        .field_f64("iaq_score", msg.data.iaq_score)
        .field_str("iaq_text", &msg.data.iaq_text)
        .field_f64("dew_point_c", msg.data.dew_point_c)
        .field_f64("heat_index_c", msg.data.heat_index_c)
        .field_f64("altitude_m", msg.data.altitude_m)
        .field_i64("uptime", msg.status.uptime)
        .field_i64("free_mem", msg.status.free_mem)
        .field_i64("rssi", msg.status.rssi)
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
        .field_i64("rssi", msg.rssi)
        .field_str("time_iso", &msg.time_iso)
        .field_bool("time_valid", msg.time_valid);

    if msg.time_valid && msg.time_ms > 0 {
        b = b.timestamp_ms(msg.time_ms);
    }

    b.build()
}

// fn esc_measurement(s: &str) -> String {
//     // measurement: escape commas and spaces
//     s.replace(',', "\\,").replace(' ', "\\ ")
// }

// fn esc_tag(s: &str) -> String {
//     // tags: escape commas, equals, spaces
//     s.replace('\\', "\\\\")
//         .replace(',', "\\,")
//         .replace('=', "\\=")
//         .replace(' ', "\\ ")
// }

// fn esc_string(s: &str) -> String {
//     // field string: escape backslash and quotes
//     s.replace('\\', "\\\\").replace('"', "\\\"")
// }
