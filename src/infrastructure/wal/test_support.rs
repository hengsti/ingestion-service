use crate::infrastructure::wal::types::WalEvent;

pub(crate) fn sample_event(seq: u64) -> WalEvent {
    WalEvent {
        topic: format!("smarthome/dev-{seq}/status"),
        ts_ms: 1_700_000_000_000 + seq as i64,
        payload: format!(
            "device_status,device_id=dev-{seq},device_class=test rssi=-50i {}",
            1_700_000_000_000 + seq as i64
        ),
    }
}
