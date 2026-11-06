use std::{
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::infrastructure::wal::{
    codec::wal_record_frame_size,
    cursor, segment,
    subscription::WalSubscription,
    types::{AppendDurableError, WalEvent, WalOffset, WalOptions},
    wal::Wal,
};

const MAX_QUARANTINE_REASON_BYTES: usize = 4096;
const ENVELOPE_FIXED_OVERHEAD_BYTES: usize = 256;
const MAX_EXPORT_LINE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_QUEUE_CAPACITY: usize = 16_384;

#[derive(Serialize)]
struct RejectionEnvelope<'a> {
    topic: &'a str,
    received_at_ms: i64,
    raw_payload: &'a [u8],
    reason: &'a str,
    route_id: Option<&'a str>,
    retained: bool,
    qos: u8,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExportLine {
    Header {
        version: u8,
        journal_id: String,
        from: String,
    },
    Record {
        offset: String,
        offset_after: String,
        event: ExportEvent,
    },
    Footer {
        through: String,
        records: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct ExportEvent {
    topic: String,
    ts_ms: i64,
    rejection: serde_json::Value,
}

pub(crate) struct Rejection<'a> {
    pub topic: &'a str,
    pub received_at_ms: i64,
    pub raw_payload: &'a [u8],
    pub reason: &'a str,
    pub route_id: Option<&'a str>,
    pub retained: bool,
    pub qos: u8,
}

/// Separate durable journal for rejected input messages.
pub struct Quarantine {
    wal: Wal,
    dir: PathBuf,
}

impl Quarantine {
    /// Opens an exclusively owned quarantine journal.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be opened, recovered, or exclusively locked.
    pub async fn open(options: WalOptions) -> Result<(Self, WalSubscription)> {
        let dir = options.dir.clone();
        let (wal, subscription) = Wal::open(options)
            .await
            .context("opening quarantine journal")?;
        Ok((Self { wal, dir }, subscription))
    }

    /// Returns the live journal capacity snapshot for monitoring.
    pub fn capacity_handle(&self) -> Arc<super::wal::capacity::Capacity> {
        self.wal.capacity_handle()
    }

    /// Persists one rejection before reporting durable success.
    ///
    /// # Errors
    ///
    /// Returns an error if the record exceeds bounds, the journal is full, or its
    /// writer cannot complete the sync-backed append.
    pub(crate) async fn append_durable(&self, rejection: Rejection<'_>) -> Result<()> {
        let payload = encode_envelope(
            rejection.topic,
            rejection.received_at_ms,
            rejection.raw_payload,
            rejection.reason,
            rejection.route_id,
            rejection.retained,
            rejection.qos,
        )?;
        let payload = String::from_utf8(payload).context("quarantine envelope was not UTF-8")?;
        let event = WalEvent {
            topic: rejection.topic.to_owned(),
            ts_ms: rejection.received_at_ms,
            payload,
        };
        match self.wal.append_durable(event).await {
            Ok(()) => {
                metrics::counter!("quarantine_appends_total").increment(1);
                Ok(())
            }
            Err(AppendDurableError::Full(_)) => {
                bail!("quarantine writer queue is full; rejection was not persisted")
            }
            Err(AppendDurableError::Closed(_)) => {
                bail!("quarantine writer is closed; rejection was not persisted")
            }
            Err(AppendDurableError::Capacity(_)) => {
                bail!("quarantine journal is full; rejection was not persisted")
            }
            Err(AppendDurableError::TooLarge(_)) => {
                bail!("quarantine record exceeds the WAL record limit")
            }
            Err(AppendDurableError::Durability(reason)) => {
                bail!("quarantine append was not durable: {reason}")
            }
        }
    }

    async fn export(&self, subscription: &mut WalSubscription, output_path: &Path) -> Result<u64> {
        let parent = export_parent(output_path, &self.dir)?;
        let from = journal_start_offset(&self.dir)?;
        let through = subscription.durable_head();
        let journal_id = self.journal_id()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path)
            .with_context(|| format!("creating quarantine export {}", output_path.display()))?;
        let mut writer = BufWriter::new(file);

        let result: Result<u64> = async {
            let count = self
                .write_export_contents(&mut writer, subscription, from, through, journal_id)
                .await?;
            writer.flush().context("flushing quarantine export")?;
            writer
                .get_ref()
                .sync_all()
                .context("syncing quarantine export")?;
            segment::sync_directory(&parent)
                .with_context(|| format!("syncing export directory {}", parent.display()))?;
            Ok(count)
        }
        .await;
        drop(writer);

        match result {
            Ok(count) => Ok(count),
            Err(error) => {
                if let Err(cleanup) = fs::remove_file(output_path) {
                    if cleanup.kind() != std::io::ErrorKind::NotFound {
                        return Err(error.context(format!(
                            "also failed to remove incomplete export: {cleanup}"
                        )));
                    }
                }
                Err(error)
            }
        }
    }

    async fn write_export_contents(
        &self,
        writer: &mut BufWriter<File>,
        subscription: &mut WalSubscription,
        from: WalOffset,
        through: WalOffset,
        journal_id: String,
    ) -> Result<u64> {
        write_export_line(
            writer,
            &ExportLine::Header {
                version: 1,
                journal_id,
                from: format_offset(from),
            },
        )?;

        let mut current = from;
        let mut records = 0u64;
        while current < through {
            let Some(entry) = subscription.next_until(through).await? else {
                current = through;
                break;
            };
            let rejection = serde_json::from_str(&entry.event.payload)
                .context("decoding quarantine record for export")?;
            write_export_line(
                writer,
                &ExportLine::Record {
                    offset: format_offset(entry.offset),
                    offset_after: format_offset(entry.offset_after),
                    event: ExportEvent {
                        topic: entry.event.topic,
                        ts_ms: entry.event.ts_ms,
                        rejection,
                    },
                },
            )?;
            current = entry.offset_after;
            records = records
                .checked_add(1)
                .context("quarantine export record count overflowed")?;
        }
        if current != through {
            bail!("quarantine export did not end at a record boundary");
        }

        write_export_line(
            writer,
            &ExportLine::Footer {
                through: format_offset(current),
                records,
            },
        )?;
        Ok(records)
    }

    async fn resolve_prefix(
        &self,
        subscription: &mut WalSubscription,
        target: WalOffset,
        export_path: &Path,
    ) -> Result<()> {
        let file = File::open(export_path)
            .with_context(|| format!("opening quarantine export {}", export_path.display()))?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let from = journal_start_offset(&self.dir)?;
        let snapshot = subscription.durable_head();
        let journal_id = self.journal_id()?;

        if !read_export_line(&mut reader, &mut line)? {
            bail!("quarantine export is empty");
        }
        match serde_json::from_slice::<ExportLine>(&line)? {
            ExportLine::Header {
                version,
                journal_id: exported_journal_id,
                from: exported_from,
            } if version == 1
                && exported_journal_id == journal_id
                && exported_from == format_offset(from) => {}
            ExportLine::Header { .. } => {
                bail!("quarantine export has the wrong journal or starting offset")
            }
            _ => bail!("quarantine export is missing its header"),
        }

        let mut through = from;
        let mut records = 0u64;
        let mut target_found = false;
        loop {
            if !read_export_line(&mut reader, &mut line)? {
                bail!("quarantine export is missing its completion footer");
            }
            match serde_json::from_slice::<ExportLine>(&line)? {
                ExportLine::Record {
                    offset,
                    offset_after,
                    event,
                } => {
                    let entry = subscription
                        .next_until(snapshot)
                        .await?
                        .context("quarantine export extends beyond the journal")?;
                    if offset != format_offset(entry.offset)
                        || offset_after != format_offset(entry.offset_after)
                        || event.topic != entry.event.topic
                        || event.ts_ms != entry.event.ts_ms
                        || event.rejection
                            != serde_json::from_str::<serde_json::Value>(&entry.event.payload)
                                .context("decoding quarantine record during resolution")?
                    {
                        bail!("quarantine export record does not match the journal");
                    }
                    through = entry.offset_after;
                    target_found |= through == target;
                    records = records
                        .checked_add(1)
                        .context("quarantine export record count overflowed")?;
                }
                ExportLine::Footer {
                    through: exported_through,
                    records: exported_records,
                } => {
                    if exported_through != format_offset(through) || exported_records != records {
                        bail!("quarantine export footer does not match its records");
                    }
                    break;
                }
                _ => bail!("quarantine export contains an unexpected header"),
            }
        }

        if read_export_line(&mut reader, &mut line)? {
            bail!("quarantine export contains data after its footer");
        }
        if target == from || !target_found {
            bail!("resolution offset is not an exported record boundary");
        }

        subscription.commit(target).await?;
        Ok(())
    }

    fn journal_id(&self) -> Result<String> {
        Ok(fs::canonicalize(&self.dir)
            .with_context(|| format!("resolving quarantine journal {}", self.dir.display()))?
            .to_string_lossy()
            .into_owned())
    }
}

fn encode_envelope(
    topic: &str,
    received_at_ms: i64,
    raw_payload: &[u8],
    reason: &str,
    route_id: Option<&str>,
    retained: bool,
    qos: u8,
) -> Result<Vec<u8>> {
    let reason = bounded_reason(reason);
    let max_payload_bytes = raw_payload
        .len()
        .checked_mul(4)
        .and_then(|size| size.checked_add(json_string_bound(topic.len())?))
        .and_then(|size| size.checked_add(json_string_bound(reason.len())?))
        .and_then(|size| size.checked_add(json_string_bound(route_id.unwrap_or("").len())?))
        .and_then(|size| size.checked_add(ENVELOPE_FIXED_OVERHEAD_BYTES))
        .context("quarantine envelope size estimate overflowed")?;
    wal_record_frame_size(topic.len(), max_payload_bytes)
        .context("quarantine envelope exceeds the WAL record limit")?;

    let mut encoded = Vec::with_capacity(max_payload_bytes);
    serde_json::to_writer(
        &mut encoded,
        &RejectionEnvelope {
            topic,
            received_at_ms,
            raw_payload,
            reason,
            route_id,
            retained,
            qos,
        },
    )
    .context("serializing quarantine envelope")?;
    wal_record_frame_size(topic.len(), encoded.len())
        .context("serialized quarantine envelope exceeds the WAL record limit")?;
    Ok(encoded)
}

fn json_string_bound(bytes: usize) -> Option<usize> {
    bytes.checked_mul(6)
}

fn bounded_reason(reason: &str) -> &str {
    if reason.len() <= MAX_QUARANTINE_REASON_BYTES {
        return reason;
    }

    let mut end = MAX_QUARANTINE_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

enum MaintenanceCommand {
    Export { output: PathBuf },
    Resolve { target: WalOffset, export: PathBuf },
}

/// Runs an offline quarantine export or resolution command, if requested.
///
/// # Errors
///
/// Returns an error for invalid command arguments or failed maintenance operations.
pub async fn run_maintenance_command() -> Result<bool> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let Some(command) = parse_maintenance_command(&args)? else {
        return Ok(false);
    };
    let options = maintenance_options()?;
    let (quarantine, mut subscription) = Quarantine::open(options).await?;

    match command {
        MaintenanceCommand::Export { output } => {
            let count = quarantine.export(&mut subscription, &output).await?;
            println!("Exported {count} unresolved quarantine records.");
        }
        MaintenanceCommand::Resolve { target, export } => {
            quarantine
                .resolve_prefix(&mut subscription, target, &export)
                .await?;
            println!(
                "Resolved quarantine prefix through {}.",
                format_offset(target)
            );
        }
    }

    Ok(true)
}

fn parse_maintenance_command(args: &[std::ffi::OsString]) -> Result<Option<MaintenanceCommand>> {
    match args {
        [flag, output] if flag == OsStr::new("--quarantine-export") => {
            Ok(Some(MaintenanceCommand::Export {
                output: PathBuf::from(output),
            }))
        }
        [flag, target, export_flag, export]
            if flag == OsStr::new("--quarantine-resolve")
                && export_flag == OsStr::new("--export") =>
        {
            let target = target
                .to_str()
                .context("resolution offset must be valid UTF-8")?;
            Ok(Some(MaintenanceCommand::Resolve {
                target: parse_offset(target)?,
                export: PathBuf::from(export),
            }))
        }
        [] => Ok(None),
        _ => bail!(
            "usage: --quarantine-export FILE | --quarantine-resolve SEGMENT:OFFSET --export FILE"
        ),
    }
}

fn maintenance_options() -> Result<WalOptions> {
    let dir = env::var_os("QUARANTINE_DIR")
        .map(PathBuf::from)
        .context("QUARANTINE_DIR must be set for quarantine maintenance")?;
    let max_bytes = env::var("QUARANTINE_MAX_BYTES")
        .context("QUARANTINE_MAX_BYTES must be set for quarantine maintenance")?
        .parse::<u64>()
        .context("QUARANTINE_MAX_BYTES must be a valid u64")?;
    if max_bytes == 0 {
        bail!("QUARANTINE_MAX_BYTES must be greater than zero");
    }
    let segment_bytes = match env::var("WAL_SEGMENT_BYTES") {
        Ok(value) => value
            .parse::<u64>()
            .context("WAL_SEGMENT_BYTES must be a valid u64")?,
        Err(env::VarError::NotPresent) => DEFAULT_SEGMENT_BYTES,
        Err(error) => return Err(error).context("reading WAL_SEGMENT_BYTES"),
    };
    let queue_capacity = match env::var("WAL_QUEUE_CAPACITY") {
        Ok(value) => value
            .parse::<usize>()
            .context("WAL_QUEUE_CAPACITY must be a valid usize")?,
        Err(env::VarError::NotPresent) => DEFAULT_QUEUE_CAPACITY,
        Err(error) => return Err(error).context("reading WAL_QUEUE_CAPACITY"),
    };
    if queue_capacity == 0 {
        bail!("WAL_QUEUE_CAPACITY must be greater than zero");
    }

    Ok(WalOptions {
        dir,
        max_bytes,
        segment_bytes,
        queue_capacity,
    })
}

fn export_parent(output_path: &Path, journal_dir: &Path) -> Result<PathBuf> {
    let file_name = output_path
        .file_name()
        .context("quarantine export path must include a file name")?;
    let parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("resolving export directory {}", parent.display()))?;
    let journal = fs::canonicalize(journal_dir)
        .with_context(|| format!("resolving quarantine journal {}", journal_dir.display()))?;
    if parent.join(file_name).starts_with(journal) {
        bail!("quarantine export must be outside the journal directory");
    }
    Ok(parent)
}

fn journal_start_offset(dir: &Path) -> Result<WalOffset> {
    if let Some(offset) = cursor::read_cursor(dir)? {
        return Ok(offset);
    }
    let segments = segment::list_segments(dir)?;
    Ok(WalOffset {
        segment_id: segments.first().copied().unwrap_or(1),
        byte_offset: 0,
    })
}

fn format_offset(offset: WalOffset) -> String {
    format!("{}:{}", offset.segment_id, offset.byte_offset)
}

fn parse_offset(raw: &str) -> Result<WalOffset> {
    let (segment, byte_offset) = raw
        .split_once(':')
        .context("resolution offset must use SEGMENT:OFFSET format")?;
    if byte_offset.contains(':') {
        bail!("resolution offset must use SEGMENT:OFFSET format");
    }
    Ok(WalOffset {
        segment_id: segment
            .parse()
            .context("resolution segment must be a u64")?,
        byte_offset: byte_offset
            .parse()
            .context("resolution byte offset must be a u64")?,
    })
}

fn write_export_line(writer: &mut BufWriter<File>, line: &ExportLine) -> Result<()> {
    let encoded = serde_json::to_vec(line).context("serializing quarantine export line")?;
    if encoded.len() > MAX_EXPORT_LINE_BYTES {
        bail!("quarantine export line exceeds its size limit");
    }
    writer.write_all(&encoded)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn read_export_line<R: BufRead>(reader: &mut R, line: &mut Vec<u8>) -> Result<bool> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(false);
            }
            bail!("quarantine export ends with an incomplete line");
        }
        let amount = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(amount) > MAX_EXPORT_LINE_BYTES {
            bail!("quarantine export line exceeds its size limit");
        }
        let has_newline = available[amount - 1] == b'\n';
        line.extend_from_slice(&available[..amount]);
        reader.consume(amount);
        if has_newline {
            line.pop();
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::{Seek, SeekFrom, Write},
        path::Path,
        sync::Arc,
        time::Duration,
    };

    use super::*;
    use crate::infrastructure::wal::{
        capacity::minimum_capacity, test_support::TestIoStep, types::WalOptions,
    };

    fn options(dir: &Path) -> WalOptions {
        WalOptions {
            dir: dir.to_path_buf(),
            max_bytes: 16 * 1024 * 1024,
            segment_bytes: 1024 * 1024,
            queue_capacity: 16,
        }
    }

    async fn reopen_quarantine(dir: &Path) -> anyhow::Result<(Quarantine, WalSubscription)> {
        let mut last_error = None;
        for _ in 0..50 {
            match Quarantine::open(options(dir)).await {
                Ok(opened) => return Ok(opened),
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("quarantine journal did not reopen")))
    }

    async fn append_test_record(quarantine: &Quarantine, topic: &str) -> anyhow::Result<()> {
        quarantine
            .append_durable(Rejection {
                topic,
                received_at_ms: 123,
                raw_payload: &[0x00, 0x80, 0xff],
                reason: "schema validation failed",
                route_id: Some("firmware-sensor"),
                retained: false,
                qos: 1,
            })
            .await
    }

    #[tokio::test]
    async fn test_quarantine_append_is_sync_backed_and_lossless() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (quarantine, mut subscription) = Quarantine::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes: minimum_capacity(1024)?,
            segment_bytes: 1024,
            queue_capacity: 4,
        })
        .await?;

        quarantine
            .append_durable(Rejection {
                topic: "smarthome/device/bme680",
                received_at_ms: 123,
                raw_payload: &[0x00, 0x80, 0xff],
                reason: "payload not utf8",
                route_id: Some("firmware-sensor"),
                retained: true,
                qos: 1,
            })
            .await?;
        let entry = subscription
            .next()
            .await?
            .expect("durable record is readable");
        let envelope: serde_json::Value = serde_json::from_str(&entry.event.payload)?;

        assert_eq!(envelope["raw_payload"], serde_json::json!([0, 128, 255]));
        assert_eq!(entry.event.topic, "smarthome/device/bme680");
        assert_eq!(entry.event.ts_ms, 123);
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_persists_recoverable_oversized_payload() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (quarantine, mut subscription) = Quarantine::open(options(dir.path())).await?;
        let raw_payload = vec![u8::MAX; 65 * 1024];

        quarantine
            .append_durable(Rejection {
                topic: "topic",
                received_at_ms: 123,
                raw_payload: &raw_payload,
                reason: "payload too large",
                route_id: None,
                retained: false,
                qos: 1,
            })
            .await?;
        let entry = subscription
            .next()
            .await?
            .context("recoverable oversized rejection was not persisted")?;
        let envelope: serde_json::Value = serde_json::from_str(&entry.event.payload)?;

        assert_eq!(
            envelope["raw_payload"].as_array().map(Vec::len),
            Some(raw_payload.len())
        );
        Ok(())
    }

    #[test]
    fn test_quarantine_reason_is_capped_on_utf8_boundary() -> anyhow::Result<()> {
        let reason = "é".repeat(3000);
        let encoded = encode_envelope("topic", 1, b"x", &reason, None, false, 1)?;
        let envelope: serde_json::Value = serde_json::from_slice(&encoded)?;

        assert_eq!(envelope["reason"].as_str().map(str::len), Some(4096));
        Ok(())
    }

    #[test]
    fn test_quarantine_envelope_bounds_worst_case_payload_and_rejects_oversize(
    ) -> anyhow::Result<()> {
        let topic = "\"".repeat(8 * 1024);
        let payload = vec![u8::MAX; 120 * 1024];

        let encoded = encode_envelope(&topic, 1, &payload, &"é".repeat(2048), None, false, 1)?;
        assert!(encoded.len() < 1024 * 1024);
        assert!(encode_envelope(
            "topic",
            1,
            &vec![u8::MAX; 300 * 1024],
            "reason",
            None,
            false,
            1
        )
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_full_journal_rejects_append() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let max_bytes = minimum_capacity(1024)?;
        let (quarantine, _subscription) = Quarantine::open(WalOptions {
            dir: dir.path().to_path_buf(),
            max_bytes,
            segment_bytes: 1024,
            queue_capacity: 4,
        })
        .await?;
        let payload = vec![u8::MAX; 120 * 1024];
        let mut saw_full = false;

        for _ in 0..10 {
            match quarantine
                .append_durable(Rejection {
                    topic: "topic",
                    received_at_ms: 1,
                    raw_payload: &payload,
                    reason: "invalid",
                    route_id: None,
                    retained: false,
                    qos: 1,
                })
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    assert!(error.to_string().contains("full"));
                    saw_full = true;
                    break;
                }
            }
        }

        assert!(
            saw_full,
            "bounded journal must reject once its budget is exhausted"
        );
        Ok(())
    }

    #[test]
    fn test_quarantine_export_line_fits_maximum_packet_rejection() -> anyhow::Result<()> {
        let topic = "\u{0001}".repeat(65_500);
        let payload = vec![u8::MAX; 65_500];
        let reason = "\u{0001}".repeat(MAX_QUARANTINE_REASON_BYTES);
        let envelope = encode_envelope(&topic, 1, &payload, &reason, None, false, 1)?;
        let event = ExportEvent {
            topic,
            ts_ms: 1,
            rejection: serde_json::from_slice(&envelope)?,
        };
        let line = ExportLine::Record {
            offset: "1:0".to_owned(),
            offset_after: "1:1".to_owned(),
            event,
        };
        let dir = tempfile::tempdir()?;
        let file = File::create(dir.path().join("export.jsonl"))?;
        let mut writer = BufWriter::new(file);

        write_export_line(&mut writer, &line)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_sync_failure_is_not_reported_as_persisted() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (quarantine, _subscription) = Quarantine::open(options(dir.path())).await?;
        let hook = Arc::clone(&quarantine.wal.test_io_hook);
        *hook.lock().expect("lock test I/O hook") = Some(Arc::new(|step| {
            if step == TestIoStep::Sync {
                return Err(std::io::Error::other("injected quarantine sync failure"));
            }
            Ok(())
        }));

        let error = append_test_record(&quarantine, "topic")
            .await
            .expect_err("sync failure must fail the append");

        assert!(error.to_string().contains("not durable"));
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_export_and_resolution_preserve_unresolved_suffix() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, mut subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic/first").await?;
        append_test_record(&quarantine, "topic/second").await?;

        assert_eq!(quarantine.export(&mut subscription, &export_path).await?, 2);
        let export = fs::read_to_string(&export_path)?;
        let lines: Vec<_> = export.lines().collect();
        let first: ExportLine = serde_json::from_str(lines[1])?;
        let target = match first {
            ExportLine::Record {
                offset_after,
                event,
                ..
            } => {
                assert_eq!(
                    event.rejection["raw_payload"],
                    serde_json::json!([0, 128, 255])
                );
                parse_offset(&offset_after)?
            }
            _ => anyhow::bail!("expected an exported record"),
        };
        assert!(matches!(
            serde_json::from_str::<ExportLine>(lines[0])?,
            ExportLine::Header { .. }
        ));
        assert!(matches!(
            serde_json::from_str::<ExportLine>(lines[3])?,
            ExportLine::Footer { records: 2, .. }
        ));
        drop(subscription);
        drop(quarantine);

        let (quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        quarantine
            .resolve_prefix(&mut subscription, target, &export_path)
            .await?;
        assert_eq!(cursor::read_cursor(&journal_dir)?, Some(target));
        drop(subscription);
        drop(quarantine);

        let (_quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        let unresolved = subscription
            .next()
            .await?
            .expect("unresolved record remains after restart");
        assert_eq!(unresolved.event.topic, "topic/second");
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_export_finishes_at_empty_trailing_segment() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic").await?;
        drop(subscription);
        drop(quarantine);

        File::create(segment::segment_path(&journal_dir, 2))?.sync_all()?;
        let (quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        let exported = tokio::time::timeout(
            Duration::from_secs(1),
            quarantine.export(&mut subscription, &export_path),
        )
        .await??;

        assert_eq!(exported, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_resolution_rejects_record_after_snapshot_without_waiting(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, mut subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic").await?;
        quarantine.export(&mut subscription, &export_path).await?;
        let lines: Vec<_> = fs::read_to_string(&export_path)?
            .lines()
            .map(str::to_owned)
            .collect();
        let record: ExportLine = serde_json::from_str(&lines[1])?;
        let target = match record {
            ExportLine::Record { offset_after, .. } => parse_offset(&offset_after)?,
            _ => anyhow::bail!("expected an exported record"),
        };
        let mut tampered = lines[..3].to_vec();
        tampered.insert(2, lines[1].clone());
        fs::write(&export_path, tampered.join("\n") + "\n")?;
        drop(subscription);
        drop(quarantine);

        let (quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            quarantine.resolve_prefix(&mut subscription, target, &export_path),
        )
        .await?;

        assert!(result.is_err());
        assert!(cursor::read_cursor(&journal_dir)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_resolution_rejects_tampered_export() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, mut subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic").await?;
        quarantine.export(&mut subscription, &export_path).await?;
        let export = fs::read_to_string(&export_path)?;
        let tampered = export.replace("schema validation failed", "modified");
        fs::write(&export_path, tampered)?;
        let lines: Vec<_> = fs::read_to_string(&export_path)?
            .lines()
            .map(str::to_owned)
            .collect();
        let record: ExportLine = serde_json::from_str(&lines[1])?;
        let target = match record {
            ExportLine::Record { offset_after, .. } => parse_offset(&offset_after)?,
            _ => anyhow::bail!("expected an exported record"),
        };
        drop(subscription);
        drop(quarantine);

        let (quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        assert!(quarantine
            .resolve_prefix(&mut subscription, target, &export_path)
            .await
            .is_err());
        assert!(cursor::read_cursor(&journal_dir)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_resolution_rejects_non_boundary_offset() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, mut subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic").await?;
        quarantine.export(&mut subscription, &export_path).await?;
        let lines: Vec<_> = fs::read_to_string(&export_path)?
            .lines()
            .map(str::to_owned)
            .collect();
        let record: ExportLine = serde_json::from_str(&lines[1])?;
        let target = match record {
            ExportLine::Record { offset_after, .. } => {
                let mut offset = parse_offset(&offset_after)?;
                offset.byte_offset -= 1;
                offset
            }
            _ => anyhow::bail!("expected an exported record"),
        };
        drop(subscription);
        drop(quarantine);

        let (quarantine, mut subscription) = reopen_quarantine(&journal_dir).await?;
        assert!(quarantine
            .resolve_prefix(&mut subscription, target, &export_path)
            .await
            .is_err());
        assert!(cursor::read_cursor(&journal_dir)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_export_read_error_leaves_no_resolvable_archive() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let journal_dir = root.path().join("quarantine");
        let export_path = root.path().join("quarantine.jsonl");
        let (quarantine, mut subscription) = Quarantine::open(options(&journal_dir)).await?;
        append_test_record(&quarantine, "topic").await?;

        let segment_path = segment::segment_path(&journal_dir, 1);
        let mut segment_file = OpenOptions::new().write(true).open(segment_path)?;
        segment_file.seek(SeekFrom::Start(4))?;
        segment_file.write_all(&[u8::MAX])?;
        segment_file.sync_all()?;

        assert!(quarantine
            .export(&mut subscription, &export_path)
            .await
            .is_err());
        assert!(!export_path.exists());
        assert!(quarantine
            .resolve_prefix(
                &mut subscription,
                WalOffset {
                    segment_id: 1,
                    byte_offset: 1,
                },
                &export_path,
            )
            .await
            .is_err());
        assert!(cursor::read_cursor(&journal_dir)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_quarantine_open_refuses_a_live_journal_owner() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (_quarantine, _subscription) = Quarantine::open(options(dir.path())).await?;

        assert!(Quarantine::open(options(dir.path())).await.is_err());
        Ok(())
    }

    #[test]
    fn test_quarantine_envelope_preserves_invalid_utf8_bytes() -> anyhow::Result<()> {
        let raw_payload = [0x00, 0x80, 0xff];

        let encoded = encode_envelope(
            "smarthome/device/bme680",
            1_700_000_123_456,
            &raw_payload,
            "payload not utf8",
            Some("firmware-sensor"),
            true,
            1,
        )?;
        let envelope: serde_json::Value = serde_json::from_slice(&encoded)?;

        assert_eq!(envelope["raw_payload"], serde_json::json!([0, 128, 255]));
        assert_eq!(envelope["topic"], "smarthome/device/bme680");
        assert_eq!(envelope["received_at_ms"], 1_700_000_123_456_i64);
        assert_eq!(envelope["route_id"], "firmware-sensor");
        assert_eq!(envelope["retained"], true);
        assert_eq!(envelope["qos"], 1);
        Ok(())
    }
}
