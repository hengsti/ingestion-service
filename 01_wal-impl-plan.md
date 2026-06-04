# WAL Implementation Plan

A lightweight, append-only, replayable Write-Ahead Log between the pipeline and the output sink. Replaces the in-memory `mpsc<String>` channel currently feeding `InfluxWriter::run_batcher`. Decouples the pipeline from the sink so Influx **or** Kafka can be plugged in via a `Sink` trait.

> **Guiding rule:** keep it small, keep it fast, no over-engineering. No fsync. No CRC. No schema versioning. OS page cache is the durability layer.

---

## High-level Architecture

```
worker → PersistStage → Wal::try_append (bounded mpsc, non-blocking)
                                    │
                              ┌─────▼─────┐
                              │  Writer   │  single task, owns BufWriter<File>
                              │   task    │  bincode-encodes [len][bytes]
                              └─────┬─────┘
                                    │  rotates at WAL_SEGMENT_BYTES
                                    │  notifies subscription via Notify
                              ┌─────▼─────┐
                              │  segments │  <dir>/00000000000000000001.log …
                              │  on disk  │  (OS page cache; no fsync)
                              └─────┬─────┘
                                    │
                              ┌─────▼──────────┐
                              │ WalSubscription│  tail-reads segments,
                              │   (reader)     │  resumes from commit.cursor
                              └─────┬──────────┘
                                    │
                              ┌─────▼─────┐
                              │ Forwarder │  batches WalEntries
                              │   task    │  → sink.write(batch).await
                              └─────┬─────┘
                                    │  on success: sub.commit(highest_offset)
                              ┌─────▼─────┐
                              │   Sink    │  trait — InfluxSink today,
                              │  (Influx) │  KafkaSink later
                              └───────────┘
```

## On-disk Layout

```
<WAL_DIR>/
  00000000000000000001.log    ← rolling segment files, 64 MiB each
  00000000000000000002.log
  …
  commit.cursor               ← 16 bytes: u64 segment_id LE | u64 byte_offset LE
```

**Record format** (no header, no CRC):
```
[u32 LE  payload_len][payload_len bytes  bincode(WalEvent)]
```

Recovery rule: read `[len]`; if fewer than `len` payload bytes are available (short read at EOF), the trailing record is treated as torn — stop reading there. The next append truncates and overwrites it.

## Sink Trait

```rust
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch. On Ok, the WAL forwarder commits the highest offset.
    /// On Err, the forwarder logs + metrics and (matching today's behavior)
    /// advances anyway after retry exhaustion inside the sink itself.
    async fn write(&self, batch: &[WalEvent]) -> anyhow::Result<()>;
}
```

---

# Step-by-step Implementation

Each step is small enough to implement and verify on its own. Run `cargo check` after each step. Run the full verification suite (`fmt` + `clippy` + `test`) at Step 14.

---

## Step 1 — Add dependencies

**File:** `Cargo.toml`

- `[dependencies]`
  - `bincode = "1.3"` — chosen over 2.x for stable, zero-config API.
  - `async-trait = "0.1"` — needed for the `Sink` trait.
- `[dev-dependencies]`
  - `tempfile = "3"` — for WAL tests using a temporary directory.

**Verify:** `cargo check`

---

## Step 2 — Make `HandledMessage` serde-compatible

**File:** `src/model/messages/message.rs`

- Add `use serde::{Deserialize, Serialize};`
- Derive `Serialize, Deserialize` on `HandledMessage` and `MessageType`.
- `SensorMessage` / `StatusMessage` already derive both — no change.

**Verify:** `cargo check`

---

## Step 3 — Define WAL public types

**New file:** `src/infrastructure/wal/types.rs`

Types only — no behavior yet:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct WalOffset {
    pub segment_id: u64,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEvent {
    pub topic: String,
    pub ts_ms: i64,
    pub message: HandledMessage,
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub offset: WalOffset, // offset *of this record's start*
    pub event: WalEvent,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub dir: PathBuf,
    pub segment_bytes: u64,    // rotation threshold
    pub queue_capacity: usize, // bounded mpsc into the writer
}

#[derive(Debug, thiserror::Error)] // OR plain enum, no thiserror needed if we keep it simple
pub enum TryAppendError {
    Full(WalEvent),
    Closed(WalEvent),
}
```

> No `thiserror` — match the existing codebase style with a hand-rolled enum + `Display` impl, or just expose the variants. Prefer keeping it simple: a plain enum, no `Display`, callers `match` on it.

**Update:** `src/infrastructure/wal/mod.rs` — re-export the types.
**Update:** `src/infrastructure/mod.rs` — `pub mod wal;` (already exists).

**Verify:** `cargo check`

---

## Step 4 — Segment file helpers

**New file:** `src/infrastructure/wal/segment.rs`

Pure helpers, no async:

- `segment_filename(id: u64) -> String` — formats `"{id:020}.log"`.
- `parse_segment_id(name: &str) -> Option<u64>`.
- `list_segments(dir: &Path) -> Result<Vec<u64>>` — sorted ascending; ignores non-segment files.
- `segment_path(dir: &Path, id: u64) -> PathBuf`.

Inline unit tests for each helper.

**Verify:** `cargo test wal::segment`

---

## Step 5 — Commit cursor read/write

**New file:** `src/infrastructure/wal/cursor.rs`

- `pub fn read_cursor(dir: &Path) -> Result<Option<WalOffset>>` — returns `None` if file missing or zero-length.
- `pub fn write_cursor(dir: &Path, offset: WalOffset) -> Result<()>` — write `commit.cursor.tmp` then `rename` over `commit.cursor`. **No fsync.**

Inline tests:
- write then read returns same offset
- missing file returns `None`
- corrupt (wrong length) file returns error

**Verify:** `cargo test wal::cursor`

---

## Step 6 — Record codec

**New file:** `src/infrastructure/wal/codec.rs`

- `pub fn encode_into(buf: &mut Vec<u8>, event: &WalEvent) -> Result<()>`
  - clears `buf`, reserves space, writes `[u32 LE len placeholder]`, bincode-serializes into `buf`, then patches the `len` prefix.
- `pub fn decode_from<R: Read>(r: &mut R) -> Result<Option<WalEvent>>`
  - reads 4 bytes; if EOF → `Ok(None)` (clean tail).
  - reads `len` bytes; if short → `Ok(None)` (torn tail).
  - bincode-deserializes; on serde error → `Err`.

Inline tests:
- encode → decode roundtrip
- decode on empty reader returns `None`
- decode on truncated payload returns `None`
- decode on garbage bincode returns `Err`

**Verify:** `cargo test wal::codec`

---

## Step 7 — WAL writer task

**New file:** `src/infrastructure/wal/writer.rs`

Internal API used only by `Wal::open`:

```rust
pub(super) struct WriterHandle {
    pub tx: mpsc::Sender<WalEvent>,
    pub notify: Arc<Notify>,
    pub head: Arc<AtomicWalOffset>, // see below
}

pub(super) fn spawn_writer(
    dir: PathBuf,
    start_segment_id: u64,
    start_byte_offset: u64,
    segment_bytes: u64,
    queue_capacity: usize,
) -> Result<WriterHandle>;
```

Behavior of the spawned task:
1. Open active segment file at `start_segment_id` in `OpenOptions::append`. Wrap in `BufWriter::with_capacity(64 KiB)`.
2. Track `current_segment_id` and `current_byte_offset` (= file length).
3. Loop on `rx.recv().await`:
   - bincode-encode event into a reusable `Vec<u8>`.
   - if `current_byte_offset + 4 + payload_len > segment_bytes`: flush, close, increment segment id, open next file.
   - write `[len][bytes]`, advance `current_byte_offset`.
   - update `head` (atomic) to the offset *just after* this record.
   - `notify.notify_waiters()` — coalescing wake.
4. On a periodic 5 ms ticker, `BufWriter::flush()` (push to OS). Avoids latency spikes when traffic is bursty.
5. On `rx.recv()` returning `None` → flush + drop file handle + exit.

`AtomicWalOffset` is just `Arc<AtomicU64>` × 2 (segment_id, byte_offset) wrapped in a small struct with `load`/`store` methods. Subscription uses it to know how far it can read without blocking.

**Inline tests:** none here directly — covered by integration tests in Step 9.

**Verify:** `cargo check`

---

## Step 8 — WAL subscription (reader)

**New file:** `src/infrastructure/wal/subscription.rs`

```rust
pub struct WalSubscription {
    dir: PathBuf,
    head: Arc<AtomicWalOffset>,
    notify: Arc<Notify>,
    cur_segment_id: u64,
    cur_byte_offset: u64,
    cur_reader: Option<BufReader<File>>,
}

impl WalSubscription {
    pub async fn next(&mut self) -> Option<WalEntry>;
    pub async fn commit(&mut self, up_to: WalOffset) -> Result<()>;
}
```

`next()`:
1. If `cur_reader` is `None`, open `cur_segment_id` and seek to `cur_byte_offset`.
2. Try `codec::decode_from(reader)`.
   - `Ok(Some(event))` → record offset = (cur_segment_id, cur_byte_offset_before_read), update cursor to position after, return `Some(WalEntry)`.
   - `Ok(None)` → at EOF or torn tail. Check head:
     - If head's segment > cur_segment_id → close current reader, advance to next segment id, byte_offset 0, retry.
     - Else → `notify.notified().await`, retry.
   - `Err(_)` → log, treat as EOF (defensive).

`commit(up_to)`:
1. `cursor::write_cursor(dir, up_to)`.
2. Delete every segment file whose id < `up_to.segment_id`.
3. Update internal "last committed" if needed (only used for metrics).

**Verify:** `cargo check`

---

## Step 9 — `Wal::open` (assembly + recovery)

**File:** `src/infrastructure/wal/wal.rs` (replace placeholder)

```rust
pub struct Wal { tx: mpsc::Sender<WalEvent> }

impl Wal {
    pub async fn open(opts: WalOptions) -> Result<(Self, WalSubscription)>;
    pub fn try_append(&self, ev: WalEvent) -> Result<(), TryAppendError>;
}
```

`open()` recovery flow:
1. `fs::create_dir_all(&opts.dir)`.
2. `let segments = list_segments(&opts.dir)?;`
3. Determine **active segment id** for writer:
   - If `segments.is_empty()` → create `00…01.log`, active id = 1, file len = 0.
   - Else → active id = max(segments). File len = its size.
4. Determine **read start**:
   - `let cursor = read_cursor(&opts.dir)?;`
   - If `Some(off)` → start subscription there.
   - If `None` → start at `(min(segments), 0)` so we replay everything still on disk.
5. `spawn_writer(...)` with active id + active file length.
6. Build `WalSubscription` from `dir`, `head` (the atomic shared with the writer), `notify`, and the read start.
7. Return `(Wal { tx }, subscription)`.

`try_append()`:
- `self.tx.try_send(ev).map_err(|e| match e { Full(v) => Full(v), Closed(v) => Closed(v) })`.

**Inline integration tests** in `wal.rs`:
- `append → next` returns the same event
- write 100 records, drop the wal, reopen, ensure all 100 still visible (no commit) and offsets monotonic
- commit halfway, reopen, ensure subscription resumes at committed offset
- segment rotation: set segment_bytes=512, write enough records to roll, assert two files exist
- after `commit` past a segment boundary, the older segment file is deleted
- partial trailing record: append a record, manually corrupt the file by appending `[len=999]` with no payload, reopen, ensure replay yields exactly the one good record and stops cleanly
- `try_append` returns `Full` when queue is saturated (use `queue_capacity = 1` and don't drain)

**Verify:** `cargo test wal::`

---

## Step 10 — Sink trait + InfluxSink

**New file:** `src/infrastructure/sink/mod.rs`
- Declare module `influx`.
- Define `Sink` trait (see High-level Architecture above).

**New file:** `src/infrastructure/sink/influx.rs`
- `pub struct InfluxSink { client: reqwest::Client, write_url: String, token: String }`
- `pub fn new(url, org, bucket, token) -> Result<Self>` — same body as today's `InfluxWriter::new`.
- `impl Sink for InfluxSink { async fn write(&self, batch: &[WalEvent]) -> Result<()> }`
  - Convert each event:
    - `HandledMessage::Sensor(s) → sensor_to_point(s).to_line_protocol()`
    - `HandledMessage::Status(s) → status_to_point(s).to_line_protocol()`
  - `body = lines.join("\n")`
  - Reuse the existing 3-attempt retry loop (extracted from `InfluxWriter::flush`) with the same metrics and log lines.
  - On terminal failure, return `Err` so the forwarder can record an error metric. (We still *advance the cursor* in the forwarder to match today's drop-after-3-retries semantics — see Step 11.)

**Refactor:** `src/infrastructure/database/influx.rs`
- Drop `run_batcher` (no longer used).
- Keep `sensor_to_point` / `status_to_point` (used by `InfluxSink`).
- Either delete `InfluxWriter` entirely or shrink it to a constructor returning the http client + url + token. Cleanest: remove it and inline the 3 fields into `InfluxSink`.

**Update:** `src/infrastructure/mod.rs` — `pub mod sink;`

**Inline tests** (`sink::influx::tests`):
- `write` produces correct line-protocol body for a mixed sensor+status batch (use `mockito` or simply assert on the body string built before the HTTP call — refactor `flush` to make the body buildable independently, e.g. `fn build_body(batch: &[WalEvent]) -> String`).

**Verify:** `cargo test sink::`

---

## Step 11 — Forwarder task

**New file:** `src/infrastructure/wal/forwarder.rs`

```rust
pub async fn run_forwarder(
    mut sub: WalSubscription,
    sink: Arc<dyn Sink>,
    batch_size: usize,
    flush_interval_ms: u64,
) -> Result<()>;
```

Loop:
```rust
let mut batch: Vec<WalEntry> = Vec::with_capacity(batch_size);
let mut ticker = tokio::time::interval(Duration::from_millis(flush_interval_ms));
ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

loop {
    tokio::select! {
        biased;
        _ = ticker.tick() => {
            if !batch.is_empty() { flush(&sink, &mut batch, &mut sub).await; }
        }
        maybe = sub.next() => {
            match maybe {
                Some(entry) => {
                    batch.push(entry);
                    if batch.len() >= batch_size {
                        flush(&sink, &mut batch, &mut sub).await;
                    }
                }
                None => return Ok(()), // wal closed
            }
        }
    }
}
```

`flush`:
- `let events: Vec<WalEvent> = batch.iter().map(|e| e.event.clone()).collect();`
- `let highest = batch.last().unwrap().offset_after_record();`
  - Since `WalEntry.offset` is the *start* of the record, expose `offset_after_record()` via subscription bookkeeping (or store the after-offset on the entry directly — pick one and document).
- `match sink.write(&events).await`:
  - `Ok(_)` → `sub.commit(highest).await?` — counter `wal_forwarder_committed_total += batch.len()`.
  - `Err(e)` → log error, counter `wal_forwarder_drop_total += batch.len()`, **still** `sub.commit(highest).await?` (matches existing drop-on-failure behavior).
- `batch.clear()`.

**Decision to lock in:** `WalEntry` carries `offset_after: WalOffset` (cleaner than computing post-hoc). Update Step 3 + Step 8 + Step 9 accordingly when you reach this step.

**Verify:** `cargo check`

---

## Step 12 — Pipeline `PersistStage` rewire

**File:** `src/pipeline/stages/persist.rs`

- Replace `tx: mpsc::Sender<String>` with `wal: Arc<Wal>`.
- New constructor: `pub fn new(wal: Arc<Wal>) -> Self`.
- In `run`:
  - `let msg = ctx.handled_message()?.clone();`
  - `let ev = WalEvent { topic: ctx.topic().to_string(), ts_ms: chrono::Utc::now().timestamp_millis(), message: msg };`
  - `match self.wal.try_append(ev) { Ok(()) => Continue, Err(Full(_)) => mark_dlq("wal queue full") + Stop, Err(Closed(_)) => mark_dlq("wal queue closed") + Stop }`
- **Drop** `ctx.set_line_protocol(line)` — line protocol is now an internal detail of `InfluxSink`. If `ctx.line_protocol()` is unused elsewhere, remove the field from `PipelineContext`. (Verify with `grep`.)
- Rewrite tests to use a real `Wal` over a `tempfile::TempDir`. Verify:
  - happy path: append succeeds → `Continue`, subscription receives the event.
  - queue full: `WAL_QUEUE_CAPACITY=1`, pre-fill, ensure DLQ marked.
  - missing handled_message → returns Err (unchanged).

**Verify:** `cargo test persist::`

---

## Step 13 — Config + main wiring

**File:** `src/config.rs`
- Add fields:
  - `pub wal_dir: PathBuf`
  - `pub wal_segment_bytes: u64`     (default `64 * 1024 * 1024`)
  - `pub wal_queue_capacity: usize`  (default `16_384`)
- Remove `influx_queue_capacity` field, parsing, and Debug entry.
- Add parsing in `from_env`:
  - `WAL_DIR` is **required** (matches the no-defaults policy for required infra).
  - `WAL_SEGMENT_BYTES` and `WAL_QUEUE_CAPACITY` allowed defaults (these are tunables, not hard requirements).
- Update `Debug` impl.

**File:** `src/main.rs`
- Replace lines 119–138 (the influx mpsc + run_batcher block) with:
  ```rust
  let (wal, wal_sub) = Wal::open(WalOptions {
      dir: cfg.wal_dir.clone(),
      segment_bytes: cfg.wal_segment_bytes,
      queue_capacity: cfg.wal_queue_capacity,
  }).await?;
  let wal = Arc::new(wal);

  let sink: Arc<dyn Sink> = Arc::new(InfluxSink::new(
      &cfg.influx_url,
      &cfg.influx_org,
      &cfg.influx_bucket,
      cfg.influx_token.expose_secret(),
  )?);

  let _forwarder_task = tokio::spawn(run_forwarder(
      wal_sub,
      sink,
      cfg.batch_size,
      cfg.flush_interval_ms,
  ));
  ```
- Update `PersistStage::new(...)` call site to `PersistStage::new(wal.clone())`.

**Verify:** `cargo build`

---

## Step 14 — Integration tests + helper updates

**File:** `tests/common/mod.rs`
- Add a helper `pub fn build_pipeline_with_tempwal() -> (PipelineRunner, Arc<Wal>, WalSubscription, TempDir)` for any integration tests that exercise persist.
- Update existing helpers and tests that referenced the old `mpsc::Sender<String>`-based persist stage; route them through the new WAL helper.

Keep behavioral assertions unchanged — anything that previously asserted on the line-protocol channel either:
- moves to `sink::influx::tests` (asserting on `build_body`), or
- is replaced with an assertion on the `WalSubscription` receiving the expected `WalEvent`.

**Verify:** `cargo test --all-features --locked`

---

## Step 15 — Final verification

```powershell
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```

All three must pass. Update `README.md` env-var table with `WAL_DIR`, `WAL_SEGMENT_BYTES`, `WAL_QUEUE_CAPACITY`; remove `INFLUX_QUEUE_CAPACITY`. Update `Dockerfile` only if it sets any of these env vars (it shouldn't).

---

# File-Change Summary

| File | Change |
|---|---|
| `Cargo.toml` | + `bincode`, `async-trait`; dev: + `tempfile` |
| `src/model/messages/message.rs` | derive `Serialize, Deserialize` on `HandledMessage`, `MessageType` |
| `src/infrastructure/wal/mod.rs` | re-exports |
| `src/infrastructure/wal/types.rs` | **new** — `WalOptions`, `WalEvent`, `WalEntry`, `WalOffset`, `TryAppendError` |
| `src/infrastructure/wal/segment.rs` | **new** — segment file naming + listing |
| `src/infrastructure/wal/cursor.rs` | **new** — atomic commit cursor |
| `src/infrastructure/wal/codec.rs` | **new** — `[len][bincode]` encoder/decoder |
| `src/infrastructure/wal/writer.rs` | **new** — single-task writer with rotation |
| `src/infrastructure/wal/subscription.rs` | **new** — tail reader + commit |
| `src/infrastructure/wal/wal.rs` | **new** — `Wal::open`, `try_append` |
| `src/infrastructure/wal/forwarder.rs` | **new** — batch + sink.write + commit |
| `src/infrastructure/sink/mod.rs` | **new** — `Sink` trait |
| `src/infrastructure/sink/influx.rs` | **new** — `InfluxSink` |
| `src/infrastructure/database/influx.rs` | drop `run_batcher` and `InfluxWriter` (or shrink) |
| `src/infrastructure/mod.rs` | `pub mod sink;` (and confirm `wal`) |
| `src/pipeline/stages/persist.rs` | switch to `Arc<Wal>`, drop `set_line_protocol`, rewrite tests |
| `src/pipeline/context.rs` | (if unused elsewhere) remove `line_protocol` field |
| `src/config.rs` | + `wal_dir`, `wal_segment_bytes`, `wal_queue_capacity`; − `influx_queue_capacity` |
| `src/main.rs` | rewire WAL + forwarder + sink |
| `tests/common/mod.rs` | tempdir WAL helper |
| `README.md` | env-var table updates |

# Non-Goals (explicit)

- **No fsync.** OS page cache only. Documented behavior.
- **No CRC.** Trailing torn record is detected via short read; previous records are intact.
- **No schema versioning.** If the `WalEvent` shape ever changes incompatibly, drain the WAL during deploys (or wipe `WAL_DIR`).
- **No per-event ack.** Commits are batch-grained.
- **No multi-subscriber.** Exactly one `WalSubscription` per `Wal`.
- **No segment compaction.** Segments are append-only and deleted whole when fully acked.

# Performance Notes

- Single writer task = no lock contention; sequential disk writes = OS-friendly.
- `BufWriter<File>` with 64 KiB buffer; 5 ms periodic flush keeps tail latency bounded.
- `tokio::sync::Notify` for reader wake-up — coalescing, no per-record allocation.
- Reader re-reads from page cache, not disk (Kafka-style hot-tail pattern).
- Bincode + `Vec<u8>` reuse on the writer path → zero per-record allocation beyond the bincode internal buffer growth.
