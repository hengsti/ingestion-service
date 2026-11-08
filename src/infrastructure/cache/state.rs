use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use crate::model::messages::telemetry::{TelemetryRecord, TelemetryValue};

#[derive(Clone, Debug, Serialize)]
pub struct DeviceState {
    pub last_seen_ms: u64,
    pub value: Arc<BTreeMap<String, TelemetryValue>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", content = "lowercase")]
pub enum CacheEvent {
    Telemetry {
        device_id: String,
        last_seen_ms: u64,
        value: Arc<BTreeMap<String, TelemetryValue>>,
    },
}

#[derive(Clone)]
pub struct CacheState {
    devices: Arc<RwLock<HashMap<String, CacheEntry>>>,
    ttl_ms: u64,
    max_devices: usize,
    event_tx: broadcast::Sender<CacheEvent>,
}

struct CacheEntry {
    state: DeviceState,
    receive_sequence: u64,
}

impl CacheState {
    pub fn new(ttl_ms: u64, buffer_size: usize) -> Self {
        // Lagging subscribers may miss older updates once the broadcast buffer wraps.
        let (event_tx, _) = broadcast::channel::<CacheEvent>(buffer_size);

        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            ttl_ms,
            max_devices: buffer_size,
            event_tx,
        }
    }

    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<CacheEvent> {
        self.event_tx.subscribe()
    }

    /// Stores and broadcasts a record only when its receive order is newer.
    ///
    /// Returns `true` when the record is accepted.
    pub fn update_record(
        &self,
        record: &TelemetryRecord,
        last_seen_ms: u64,
        receive_sequence: u64,
    ) -> bool {
        let device_id = normalize_device_id(&record.device_id);
        let value = Arc::new(record.fields.clone());
        {
            let mut map = self.devices.write().expect("devices lock poisoned");
            if map
                .get(&device_id)
                .is_some_and(|entry| receive_sequence <= entry.receive_sequence)
            {
                return false;
            }

            // Updates to existing devices never change the map size.
            if self.max_devices > 0
                && !map.contains_key(&device_id)
                && map.len() >= self.max_devices
            {
                if let Some((oldest_key, oldest_entry)) =
                    map.iter().min_by_key(|(_, entry)| entry.receive_sequence)
                {
                    if receive_sequence <= oldest_entry.receive_sequence {
                        return false;
                    }
                    let oldest_key = oldest_key.clone();
                    map.remove(&oldest_key);
                }
            }

            map.insert(
                device_id.clone(),
                CacheEntry {
                    state: DeviceState {
                        last_seen_ms,
                        value: value.clone(),
                    },
                    receive_sequence,
                },
            );

            let _ = self.event_tx.send(CacheEvent::Telemetry {
                device_id,
                last_seen_ms,
                value,
            });
        }
        true
    }

    pub fn snapshot_all_devices(&self) -> Vec<(String, DeviceState, bool)> {
        let now = now_ms();
        let ttl = self.ttl_ms();

        let map = self.devices.read().expect("devices lock poisoned");
        map.iter()
            .map(|(k, v)| {
                let stale = now.saturating_sub(v.state.last_seen_ms) > ttl;
                (k.clone(), v.state.clone(), stale)
            })
            .collect()
    }

    pub fn snapshot_device(&self, device_id: &str) -> Option<(DeviceState, bool)> {
        let device_id = normalize_device_id(device_id);
        let now = now_ms();
        let ttl = self.ttl_ms();

        let map = self.devices.read().ok()?;
        let val = map.get(&device_id)?.state.clone();
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
    use std::sync::Barrier;

    fn record(device_id: &str) -> TelemetryRecord {
        TelemetryRecord {
            route_id: "test-route".to_owned(),
            device_id: device_id.to_owned(),
            measurement: "sensor".to_owned(),
            timestamp_ms: 1_700_000_000_000,
            tags: BTreeMap::new(),
            fields: BTreeMap::from([
                ("enabled".to_owned(), TelemetryValue::Boolean(true)),
                ("temp_c".to_owned(), TelemetryValue::Float(20.0)),
            ]),
            source_topic: "home/device/sensor".to_owned(),
            retained: false,
        }
    }

    #[test]
    fn update_record_stays_within_max_devices_cap() {
        let cache = CacheState::new(60_000, 2);

        cache.update_record(&record("dev-a"), 1_000, 1);
        cache.update_record(&record("dev-b"), 2_000, 2);
        cache.update_record(&record("dev-c"), 3_000, 3);

        let all = cache.snapshot_all_devices();
        assert_eq!(all.len(), 2, "cache must not exceed max_devices");
        assert!(
            all.iter().any(|(id, _, _)| id == "dev-c"),
            "newly inserted device must be present"
        );
    }

    #[test]
    fn update_record_evicts_stalest_entry() {
        // Seed two entries with known last_seen_ms values by manipulating the map directly.
        let cache = CacheState::new(60_000, 2);

        {
            let mut map = cache.devices.write().unwrap();
            map.insert(
                "dev-old".to_string(),
                CacheEntry {
                    state: DeviceState {
                        last_seen_ms: 1_000,
                        value: Arc::new(record("dev-old").fields),
                    },
                    receive_sequence: 1,
                },
            );
            map.insert(
                "dev-new".to_string(),
                CacheEntry {
                    state: DeviceState {
                        last_seen_ms: 2_000_000_000_000,
                        value: Arc::new(record("dev-new").fields),
                    },
                    receive_sequence: 2,
                },
            );
        }

        cache.update_record(&record("dev-c"), 2_000_000_000_001, 3);

        assert!(
            cache.snapshot_device("dev-old").is_none(),
            "stalest entry must be evicted"
        );
        assert!(
            cache.snapshot_device("dev-new").is_some(),
            "newer entry must be retained"
        );
        assert!(
            cache.snapshot_device("dev-c").is_some(),
            "newly inserted device must be present"
        );
    }

    #[test]
    fn update_record_does_not_evict_on_existing_device_update() {
        let cache = CacheState::new(60_000, 2);

        cache.update_record(&record("dev-a"), 1_000, 1);
        cache.update_record(&record("dev-b"), 2_000, 2);
        cache.update_record(&record("dev-a"), 3_000, 3);

        let all = cache.snapshot_all_devices();
        assert_eq!(
            all.len(),
            2,
            "update of existing device must not change map size"
        );
        assert!(all.iter().any(|(id, _, _)| id == "dev-a"));
        assert!(all.iter().any(|(id, _, _)| id == "dev-b"));
    }

    #[test]
    fn update_record_rejects_delayed_evicted_device_without_broadcasting() {
        let cache = CacheState::new(60_000, 2);
        assert!(cache.update_record(&record("dev-a"), 2_000, 2));
        assert!(cache.update_record(&record("dev-b"), 4_000, 4));
        assert!(cache.update_record(&record("dev-c"), 6_000, 6));
        let mut events = cache.subscribe_events();

        assert!(!cache.update_record(&record("dev-a"), 1_000, 1));

        assert!(cache.snapshot_device("dev-a").is_none());
        assert!(cache.snapshot_device("dev-b").is_some());
        assert!(cache.snapshot_device("dev-c").is_some());
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn update_record_evicts_only_for_newer_receive_sequence() {
        let cache = CacheState::new(60_000, 2);
        assert!(cache.update_record(&record("dev-a"), 2_000, 2));
        assert!(cache.update_record(&record("dev-b"), 4_000, 4));

        assert!(!cache.update_record(&record("dev-c"), 9_000, 2));
        assert!(cache.update_record(&record("dev-c"), 1_000, 3));

        assert!(cache.snapshot_device("dev-a").is_none());
        assert!(cache.snapshot_device("dev-b").is_some());
        assert!(cache.snapshot_device("dev-c").is_some());
    }

    #[test]
    fn update_record_orders_by_receive_sequence_when_clock_moves_backwards() {
        let cache = CacheState::new(60_000, 2);
        let mut events = cache.subscribe_events();

        assert!(cache.update_record(&record("dev-a"), 2_000, 2));
        assert!(cache.update_record(&record("dev-a"), 1_000, 3));
        assert!(!cache.update_record(&record("dev-a"), 3_000, 1));

        let (state, _) = cache.snapshot_device("dev-a").expect("device is cached");
        assert_eq!(state.last_seen_ms, 1_000);
        assert!(matches!(
            events.try_recv(),
            Ok(CacheEvent::Telemetry {
                last_seen_ms: 2_000,
                ..
            })
        ));
        assert!(matches!(
            events.try_recv(),
            Ok(CacheEvent::Telemetry {
                last_seen_ms: 1_000,
                ..
            })
        ));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn update_record_broadcasts_concurrent_events_in_receive_order() {
        let count = 64;
        let cache = CacheState::new(60_000, count);
        let mut events = cache.subscribe_events();
        let barrier = Arc::new(Barrier::new(count + 1));
        let workers = (1..=count)
            .map(|sequence| {
                let cache = cache.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.update_record(&record("dev-a"), sequence as u64, sequence as u64);
                })
            })
            .collect::<Vec<_>>();

        barrier.wait();
        for worker in workers {
            worker.join().expect("cache update thread should finish");
        }

        let received = std::iter::from_fn(|| events.try_recv().ok())
            .map(|event| match event {
                CacheEvent::Telemetry { last_seen_ms, .. } => last_seen_ms,
            })
            .collect::<Vec<_>>();

        assert!(
            received.windows(2).all(|pair| pair[0] < pair[1]),
            "accepted SSE updates must be ordered by receive sequence: {received:?}"
        );
        assert_eq!(received.last(), Some(&(count as u64)));
    }

    #[test]
    fn update_record_uses_receive_sequence_to_break_timestamp_ties() {
        let cache = CacheState::new(60_000, 2);

        assert!(cache.update_record(&record("dev-a"), 2_000, 2));
        assert!(!cache.update_record(&record("dev-a"), 2_000, 1));
        assert!(cache.update_record(&record("dev-a"), 2_000, 3));
    }
}
