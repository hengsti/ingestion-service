use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use crate::model::sensor_msg::SensorData;
use crate::validate::HandledMessage;

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
    event_tx: broadcast::Sender<CacheEvent>,
}

impl CacheState {
    pub fn new(ttl_ms: u64, buffer_size: usize) -> Self {
        // Buffer for buffer_size events; if the buffer is full, old events will be dropped.
        let (event_tx, _) = broadcast::channel::<CacheEvent>(buffer_size);

        Self {
            sensors: Arc::new(RwLock::new(HashMap::new())),
            ttl_ms,
            event_tx,
        }
    }

    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<CacheEvent> {
        self.event_tx.subscribe()
    }

    pub fn update(&self, msg: &HandledMessage) {
        match msg {
            HandledMessage::Sensor(sensor_msg) => {
                let device_id = normalize_device_id(&sensor_msg.device_id);
                let last_seen_ms = now_ms();

                let state = SensorState {
                    last_seen_ms,
                    value: sensor_msg.data.clone(),
                };

                {
                    let mut map = self.sensors.write().expect("sensors lock poisoned");
                    map.insert(device_id.clone(), state);
                }

                let _ = self.event_tx.send(CacheEvent::Sensor {
                    device_id,
                    last_seen_ms,
                    value: sensor_msg.data.clone(),
                });
            }
            _ => {
                // For now, we only cache sensor messages. Status messages could be cached similarly if needed.
            }
        }
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
