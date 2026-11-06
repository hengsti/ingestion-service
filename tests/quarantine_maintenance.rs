use std::process::Command;

use smarthome_ingest::infrastructure::{quarantine::Quarantine, wal::types::WalOptions};

#[tokio::test]
async fn maintenance_export_refuses_a_journal_owned_by_another_process() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let journal = root.path().join("quarantine");
    let export = root.path().join("export.jsonl");
    let (_owner, _subscription) = Quarantine::open(WalOptions {
        dir: journal.clone(),
        max_bytes: 16 * 1024 * 1024,
        segment_bytes: 1024 * 1024,
        queue_capacity: 16,
    })
    .await?;

    let output = Command::new(env!("CARGO_BIN_EXE_smarthome-ingest"))
        .arg("--quarantine-export")
        .arg(&export)
        .env("QUARANTINE_DIR", journal)
        .env("QUARANTINE_MAX_BYTES", "16777216")
        .env("WAL_SEGMENT_BYTES", "1048576")
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("opening quarantine journal"));
    Ok(())
}
