use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

static TRACE_LOCK: Mutex<()> = Mutex::new(());

pub(crate) async fn trace(event: String) -> Result<()> {
    let Some(dir) = std::env::var_os("INGEST_TEST_HOOK_DIR").map(PathBuf::from) else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || {
        let _guard = TRACE_LOCK
            .lock()
            .map_err(|_| anyhow::anyhow!("test trace lock poisoned"))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("source.trace"))?;
        ensure!(
            file.metadata()?.len() + (event.len() as u64) < 64 * 1024,
            "test trace limit reached"
        );
        writeln!(file, "{event}")?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("test trace task failed")?
}

pub(crate) async fn barrier(name: &'static str) -> Result<()> {
    let Some(dir) = std::env::var_os("INGEST_TEST_HOOK_DIR").map(PathBuf::from) else {
        return Ok(());
    };
    if std::env::var("INGEST_TEST_BARRIER").as_deref() != Ok(name) {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        fs::write(dir.join(format!("{name}.reached")), [])?;
        let deadline = Instant::now() + Duration::from_secs(90);
        while !dir.join(format!("{name}.release")).exists() {
            ensure!(
                Instant::now() < deadline,
                "test barrier {name} release timed out"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("test barrier task failed")?
}
