use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use crate::model::messages::sensor::{SensorData, SensorMessage};

#[derive(Clone, Debug, Serialize)]
pub struct SensorState {
    pub last_seen_ms: u64,
    pub value: SensorData,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", content = "lowercase")]
pub enum CacheEvent {
    Sensor {
        device_id: String,
        last_seen_ms: u64,
        value: SensorData,
    },
}

#[derive(Clone)]
pub struct CacheState {
    sensors: Arc<RwLock<HashMap<String, SensorState>>>,
    ttl_ms: u64,
    max_sensors: usize,
    event_tx: broadcast::Sender<CacheEvent>,
}

impl CacheState {
    pub fn new(ttl_ms: u64, buffer_size: usize) -> Self {
        // Lagging subscribers may miss older updates once the broadcast buffer wraps.
        let (event_tx, _) = broadcast::channel::<CacheEvent>(buffer_size);

        Self {
            sensors: Arc::new(RwLock::new(HashMap::new())),
            ttl_ms,
            max_sensors: buffer_size,
            event_tx,
        }
    }

    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<CacheEvent> {
        self.event_tx.subscribe()
    }

    pub fn update_sensor(&self, sensor_msg: &SensorMessage) {
        let device_id = normalize_device_id(&sensor_msg.device_id);
        let last_seen_ms = now_ms();

        {
            let mut map = self.sensors.write().expect("sensors lock poisoned");

            // Evict the stalest entry only when a new device would exceed the cap.
            // Updates to existing devices never change the map size.
            if self.max_sensors > 0
                && !map.contains_key(&device_id)
                && map.len() >= self.max_sensors
            {
                if let Some(oldest_key) = map
                    .iter()
                    .min_by_key(|(_, v)| v.last_seen_ms)
                    .map(|(k, _)| k.clone())
                {
                    map.remove(&oldest_key);
                }
            }

            map.insert(
                device_id.clone(),
                SensorState {
                    last_seen_ms,
                    value: sensor_msg.data.clone(),
                },
            );
        }

        let _ = self.event_tx.send(CacheEvent::Sensor {
            device_id,
            last_seen_ms,
            value: sensor_msg.data.clone(),
        });
    }

    pub fn snapshot_all_sensors(&self) -> Vec<(String, SensorState, bool)> {
        let now = now_ms();
        let ttl = self.ttl_ms();

        let map = self.sensors.read().expect("sensors lock poisoned");
        map.iter()
            .map(|(k, v)| {
                let stale = now.saturating_sub(v.last_seen_ms) > ttl;
                (k.clone(), v.clone(), stale)
            })
            .collect()
    }

    pub fn snapshot_sensor(&self, device_id: &str) -> Option<(SensorState, bool)> {
        let device_id = normalize_device_id(device_id);
        let now = now_ms();
        let ttl = self.ttl_ms();

        let map = self.sensors.read().ok()?;
        let val = map.get(&device_id)?.clone();
        let stale = now.saturating_sub(val.last_seen_ms) > ttl;
        Some((val, stale))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn normalize_device_id(device_id: &str) -> String {
    device_id.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sensor_msg(device_id: &str, last_seen_override_ms: Option<u64>) -> SensorMessage {
        SensorMessage {
            device_id: device_id.to_string(),
            room: "room".to_string(),
            device_class: "cls".to_string(),
            fw_version: "1.0".to_string(),
            time_ms: last_seen_override_ms.unwrap_or(0) as i64,
            time_iso: "2024-01-01T00:00:00Z".to_string(),
            time_valid: true,
            data: SensorData {
                temp_c: 20.0,
                rel_hum_perc: 50.0,
                pressure_hpa: 1013.0,
                gas_ohm: 10_000.0,
                iaq_score: 80.0,
                iaq_text: "Good".to_string(),
                dew_point_c: 9.0,
                heat_index_c: 20.0,
                altitude_m: 300.0,
            },
        }
    }

    #[test]
    fn update_sensor_stays_within_max_sensors_cap() {
        let cache = CacheState::new(60_000, 2);

        cache.update_sensor(&sensor_msg("dev-a", None));
        cache.update_sensor(&sensor_msg("dev-b", None));
        cache.update_sensor(&sensor_msg("dev-c", None));

        let all = cache.snapshot_all_sensors();
        assert_eq!(all.len(), 2, "cache must not exceed max_sensors");
        assert!(
            all.iter().any(|(id, _, _)| id == "dev-c"),
            "newly inserted device must be present"
        );
    }

    #[test]
    fn update_sensor_evicts_stalest_entry() {
        // Seed two entries with known last_seen_ms values by manipulating the map directly.
        let cache = CacheState::new(60_000, 2);

        {
            let mut map = cache.sensors.write().unwrap();
            map.insert(
                "dev-old".to_string(),
                SensorState {
                    last_seen_ms: 1_000,
                    value: sensor_msg("dev-old", None).data,
                },
            );
            map.insert(
                "dev-new".to_string(),
                SensorState {
                    last_seen_ms: 2_000_000_000_000,
                    value: sensor_msg("dev-new", None).data,
                },
            );
        }

        cache.update_sensor(&sensor_msg("dev-c", None));

        assert!(
            cache.snapshot_sensor("dev-old").is_none(),
            "stalest entry must be evicted"
        );
        assert!(
            cache.snapshot_sensor("dev-new").is_some(),
            "newer entry must be retained"
        );
        assert!(
            cache.snapshot_sensor("dev-c").is_some(),
            "newly inserted device must be present"
        );
    }

    #[test]
    fn update_sensor_does_not_evict_on_existing_device_update() {
        let cache = CacheState::new(60_000, 2);

        cache.update_sensor(&sensor_msg("dev-a", None));
        cache.update_sensor(&sensor_msg("dev-b", None));
        cache.update_sensor(&sensor_msg("dev-a", None));

        let all = cache.snapshot_all_sensors();
        assert_eq!(
            all.len(),
            2,
            "update of existing device must not change map size"
        );
        assert!(all.iter().any(|(id, _, _)| id == "dev-a"));
        assert!(all.iter().any(|(id, _, _)| id == "dev-b"));
    }
}
