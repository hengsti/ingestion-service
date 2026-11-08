use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::infrastructure::wal::{capacity::METADATA_RESERVE_BYTES, segment::sync_directory};

const MANIFEST_FILES: [&str; 2] = [".mqtt-subscriptions.0.json", ".mqtt-subscriptions.1.json"];
// Two slots together consume at most half of the WAL metadata reserve.
const MAX_MANIFEST_BYTES: usize = (METADATA_RESERVE_BYTES / 4) as usize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct BrokerIdentity {
    pub host: String,
    pub port: u16,
    pub client_id: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManifestRecord {
    generation: u64,
    identity: BrokerIdentity,
    filters: Vec<String>,
    #[serde(default)]
    trusted: bool,
}

pub(super) struct SubscriptionManifest {
    dir: PathBuf,
    identity: BrokerIdentity,
    current: Option<ManifestRecord>,
}

pub(super) struct FilterPlan {
    pub pending: Vec<String>,
    pub subscribe: Vec<String>,
    pub unsubscribe: Vec<String>,
}

pub(super) fn filter_plan(
    known: Option<&[String]>,
    session_present: bool,
    desired: &[String],
) -> Result<FilterPlan> {
    let known = if session_present {
        known.context(
            "MQTT resumed a persistent session without a trusted subscription manifest; \
             stop ingestion, verify and supply the previous filters in WAL_DIR, then restart",
        )?
    } else {
        &[]
    };

    let desired_set: BTreeSet<_> = desired.iter().cloned().collect();
    let pending = known
        .iter()
        .chain(desired)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let known_set: BTreeSet<_> = known.iter().cloned().collect();
    let unsubscribe = known_set.difference(&desired_set).cloned().collect();

    Ok(FilterPlan {
        pending,
        subscribe: desired.to_vec(),
        unsubscribe,
    })
}

impl SubscriptionManifest {
    pub(super) fn open(dir: &Path, identity: BrokerIdentity) -> Result<Self> {
        let paths = MANIFEST_FILES.map(|name| dir.join(name));
        let mut records = Vec::new();
        let mut invalid = Vec::new();

        for path in &paths {
            if let Some(bytes) = read_manifest(path)? {
                match serde_json::from_slice::<ManifestRecord>(&bytes) {
                    Ok(record) => records.push(record),
                    Err(error) => invalid.push((path, error)),
                }
            }
        }

        if records.is_empty() {
            if let Some((path, error)) = invalid.first() {
                return Err(anyhow::anyhow!(error.to_string()).context(format!(
                    "invalid MQTT subscription manifest {}; remove it only after verifying the broker session",
                    path.display()
                )));
            }
        } else if let Some((path, error)) = invalid.first() {
            warn!(
                path = %path.display(),
                error = %error,
                "ignoring incomplete MQTT subscription manifest slot"
            );
        }

        records.sort_by_key(|record| record.generation);
        if records
            .windows(2)
            .any(|pair| pair[0].generation == pair[1].generation && pair[0] != pair[1])
        {
            bail!("conflicting MQTT subscription manifest generations");
        }
        let current = records.pop();
        if current
            .as_ref()
            .is_some_and(|record| record.identity != identity)
        {
            bail!(
                "MQTT subscription manifest identity does not match the configured broker/client; \
                 stop ingestion and explicitly migrate the verified filter set"
            );
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            identity,
            current,
        })
    }

    pub(super) fn known_filters(&self) -> Option<&[String]> {
        self.current
            .as_ref()
            .and_then(|record| record.trusted.then_some(record.filters.as_slice()))
    }

    pub(super) async fn persist(&mut self, filters: Vec<String>) -> Result<()> {
        self.persist_state(filters, true).await
    }

    pub(super) async fn initialize_if_missing(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.persist_state(Vec::new(), false).await?;
        }
        Ok(())
    }

    pub(super) fn validate_filters(&self, filters: &[String]) -> Result<()> {
        self.encode_record(filters.to_vec(), true).map(|_| ())
    }

    async fn persist_state(&mut self, filters: Vec<String>, trusted: bool) -> Result<()> {
        let (record, bytes) = self.encode_record(filters, trusted)?;
        let generation = record.generation;

        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || write_record(&dir, generation, &bytes))
            .await
            .context("MQTT subscription manifest write task failed")??;
        self.current = Some(record);
        Ok(())
    }

    fn encode_record(
        &self,
        filters: Vec<String>,
        trusted: bool,
    ) -> Result<(ManifestRecord, Vec<u8>)> {
        let generation = self
            .current
            .as_ref()
            .map_or(Some(1), |record| record.generation.checked_add(1))
            .context("MQTT subscription manifest generation exhausted")?;
        let record = ManifestRecord {
            generation,
            identity: self.identity.clone(),
            filters,
            trusted,
        };
        let bytes = serde_json::to_vec(&record).context("encoding MQTT subscription manifest")?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            bail!(
                "MQTT subscription manifest exceeds the {}-byte WAL metadata allowance",
                MAX_MANIFEST_BYTES
            );
        }
        Ok((record, bytes))
    }
}

fn read_manifest(path: &Path) -> Result<Option<Vec<u8>>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("opening MQTT subscription manifest {}", path.display()))
        }
    };
    let size = file.metadata()?.len();
    if size > MAX_MANIFEST_BYTES as u64 {
        bail!(
            "MQTT subscription manifest {} exceeds the {}-byte WAL metadata allowance",
            path.display(),
            MAX_MANIFEST_BYTES
        );
    }

    let mut bytes = Vec::with_capacity(size as usize);
    Read::by_ref(&mut file)
        .take(MAX_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading MQTT subscription manifest {}", path.display()))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        bail!(
            "MQTT subscription manifest {} exceeds the {}-byte WAL metadata allowance",
            path.display(),
            MAX_MANIFEST_BYTES
        );
    }
    Ok(Some(bytes))
}

fn write_record(dir: &Path, generation: u64, bytes: &[u8]) -> Result<()> {
    let path = dir.join(MANIFEST_FILES[(generation % 2) as usize]);
    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("removing prior MQTT manifest slot {}", path.display()))
        }
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("creating MQTT subscription manifest {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing MQTT subscription manifest {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing MQTT subscription manifest {}", path.display()))?;
    sync_directory(dir)
        .with_context(|| format!("syncing WAL directory after writing {}", path.display()))?;
    Ok(())
}
