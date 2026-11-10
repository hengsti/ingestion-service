use std::{
    io,
    sync::{Arc, Mutex},
};

use crate::infrastructure::wal::types::WalEvent;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestIoStep {
    Write,
    Flush,
    Sync,
    CursorFileSync,
    CursorDirectorySync,
    SegmentFileSync,
    SegmentDirectorySync,
    ReclaimDirectorySync,
}

pub(crate) type TestIoHook = Arc<dyn Fn(TestIoStep) -> io::Result<()> + Send + Sync>;
pub(crate) type TestIoHookSlot = Arc<Mutex<Option<TestIoHook>>>;

pub(crate) fn run_test_io_hook(hook: &TestIoHookSlot, step: TestIoStep) -> io::Result<()> {
    let hook = hook
        .lock()
        .expect("WAL test I/O hook lock poisoned")
        .clone();
    match hook {
        Some(hook) => hook(step),
        None => Ok(()),
    }
}

pub(crate) fn sample_event(seq: u64) -> WalEvent {
    WalEvent {
        sink_target: None,
        topic: format!("smarthome/dev-{seq}/status"),
        ts_ms: 1_700_000_000_000 + seq as i64,
        payload: format!(
            "device_status,device_id=dev-{seq},device_class=test rssi=-50i {}",
            1_700_000_000_000 + seq as i64
        ),
    }
}
