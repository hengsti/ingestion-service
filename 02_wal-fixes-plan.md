# WAL Production-Readiness — Implementation Plan

Fixes from the WAL review. Ordered by priority. Each step is small, independently
verifiable (`cargo test` / `clippy`), and respects the project rule: **no
over-engineering — high performance, low latency, low memory.**

The guiding non-goals from `wal-impl-plan.md` still hold: **no fsync, no CRC, no
schema versioning.** These fixes add *crash-recovery correctness* and *hot-path
efficiency* without changing that contract.

Legend: 🔴 blocker · 🟠 should-fix · 🟢 efficiency · ⚪ nit

---

## Phase 1 — Crash-recovery correctness (🔴 blocking)

### Step 1 — Recovery truncation: heal a torn tail on `open`  🔴

**Problem (B1):** `Wal::open` resumes the active segment at
`fs::metadata(path).len()` and the writer reopens with `append(true)`, never
truncating torn trailing bytes. After a crash with a partial record at EOF, the
next append lands *after* the torn bytes → reader mis-frames every subsequent
record → permanent stall or silent data loss on the next rotation.

**Fix:** add a one-time scan of the active segment during recovery that returns
the byte offset of the end of the **last complete record**, then physically
truncate the file to that length before the writer opens it.

**New file:** `src/infrastructure/wal/recover.rs`
- `pub fn last_valid_offset(path: &Path) -> Result<u64>`
  - Open the segment, wrap in `BufReader`.
  - Loop `codec::decode_from(&mut reader)`:
    - `Ok(Some((_, consumed)))` → `valid_len += consumed`, continue.
    - `Ok(None)` → clean EOF or torn tail → stop, return `valid_len`.
    - `Err(_)` → corrupt record mid-file → stop, return `valid_len` (treat the
      rest as torn; defensive — see Step 2 for the *active-tail* poison case).
  - Returns `0` for a missing/empty file.
- `pub fn truncate_to(path: &Path, len: u64) -> Result<()>`
  - `OpenOptions::write(true).open(path)?.set_len(len)?`. No fsync (matches the
    page-cache durability contract). Only call when `len < metadata().len()`.

**Wire into** `src/infrastructure/wal/wal.rs::open` (replaces the
`fs::metadata(...).len()` branch at `wal.rs:26-34`):
- For the active (last) segment: `let valid = last_valid_offset(&path)?;`
  - if `valid < file_len` → `truncate_to(&path, valid)` and log a `warn!` with
    bytes reclaimed (visible crash-recovery signal).
  - use `valid` as the writer's `start_byte_offset`.

**Why a full scan is acceptable:** segments are ≤ `WAL_SEGMENT_BYTES` (64 MiB
default), runs once at startup, sequential read from page cache. Negligible vs.
the cost of corrupt replay.

**Tests** (inline in `recover.rs` + one integration test in `wal.rs`):
- `last_valid_offset` on clean N-record file returns full length.
- torn length-prefix at EOF → returns offset before the prefix.
- full prefix + partial payload → returns offset before the prefix.
- **regression for B1:** write 1 good record, append torn `[len=999]`, `open`
  (truncates), append a 2nd good record, reopen, assert replay yields **exactly
  two** good records with monotonic offsets and then blocks cleanly. This is the
  case the existing `torn_trailing_record_stops_replay_cleanly` test misses.

**Verify:** `cargo test wal::recover wal::wal`

---

### Step 2 — Poison-record handling in the subscription  🟠

**Problem (B2):** `WalSubscription::next` treats every `decode_from` `Err` as a
torn tail (`subscription.rs:141-147`): drop reader → `wait_or_advance` → retry
the **same offset**. A corrupt record that is *not* the active tail (e.g. valid
data after it) re-reads every 5 ms forever (cursor frozen, log spam), or skips
the segment remainder after the next rotation (data loss).

After Step 1, a true torn tail is healed on restart, so the only remaining
in-flight `Err` is a genuinely corrupt record with committed data after it —
which should be **skipped, not retried forever.**

**Fix:** make the decode-error branch distinguish "tail" from "poison":
- If `head.segment_id == cur_segment_id` **and** the bad offset is at/just-below
  the flushed head → torn active tail → wait (current behavior).
- Otherwise the bytes are durable-but-corrupt → **skip the record**, advance
  `cur_byte_offset` past it, increment a `wal_subscription_corrupt_skipped_total`
  counter, and continue.
- Keep it simple: to advance past an undecodable record we need its length. Read
  the 4-byte len prefix directly; if present, skip `4 + len`; if the prefix
  itself is unreadable, fall back to advancing to the next segment.

> Keep this minimal. Do **not** add CRCs or per-record validation. One counter,
> one bounded skip. The common case (clean data) is untouched.

**Tests:**
- corrupt record followed by a valid record in a sealed (rotated) segment →
  subscription skips the bad one, yields the good one, increments the metric.
- torn active tail still waits (existing behavior preserved).

**Verify:** `cargo test wal::subscription wal::wal`

---

## Phase 2 — Sink-outage policy (🟠 conscious decision required)

### Step 3 — Don't drop batches on *retryable* sink failure  🟠

**Problem (B3):** `forwarder.rs::flush` commits the cursor even when
`sink.write` returns `Err` after its 3 internal retries (`forwarder.rs:82-84`).
An InfluxDB outage > ~3 s drops batches and advances the cursor — the WAL does
**not** buffer across sink downtime, which undercuts its purpose.

**DECISION (confirmed by maintainer):** hold-and-retry. On a retryable sink
error, keep the batch and do **not** advance the cursor so the WAL buffers
across an InfluxDB outage. Minimal change that preserves "no poison stall":

- Split sink errors into two kinds via a lightweight return type:
  - `SinkError::Retryable` (HTTP/network/5xx, timeouts) → **do not commit**;
    keep the batch, back off, and retry the *same* batch next loop. The cursor
    stays put, so a crash during the outage replays from disk.
  - `SinkError::Permanent` (4xx malformed line protocol) → drop + metric +
    commit (poison can't block the pipeline forever).
- `InfluxSink::write` already classifies: non-success HTTP status vs. request
  error. Map 4xx → Permanent, everything else → Retryable.
- Add a bounded in-forwarder backoff (reuse `flush_interval_ms` cadence; cap the
  retry sleep, e.g. ≤ 5 s) so we don't hot-loop while Influx is down.

**Guard rails (low-memory):** because the batch is held and the cursor doesn't
advance during an outage, the WAL on disk grows — that's the intended buffer.
The in-memory batch stays a single `Vec`; we are **not** accumulating unbounded
batches in RAM. Disk growth is bounded by segment GC resuming once commits flow.

**Tests** (use a mock `Sink`):
- retryable error → cursor **not** advanced, batch retried, succeeds on 2nd call,
  commits once.
- permanent error → batch dropped, cursor advanced, `wal_forwarder_drop_total`
  incremented.

**Verify:** `cargo test wal::forwarder sink::`

---

## Phase 3 — Graceful shutdown (🟠 nice-to-have for clean deploys)

### Step 4 — Drain the WAL forwarder on shutdown  🟠

**Problem (B4):** on ctrl-c, `main` returns and the writer + `_forwarder_task`
are dropped mid-flight. No data loss (uncommitted records replay next start), but
the final buffered batch never reaches Influx, and there's a benign
`strong_count == 1` race in `wait_or_advance` if the forwarder is ever awaited
for a clean drain.

**Fix (minimal):**
- Hold the `JoinHandle` for the forwarder (drop the `_` prefix at `main.rs:141`).
- On shutdown: signal the writer to finish (drop the `Wal`/sender so `rx.recv()`
  returns `None`), then `await` the forwarder handle with a **5 s** bounded
  timeout (confirmed by maintainer) so it flushes its final batch and commits.
- Confirm `wait_or_advance` terminates cleanly when the writer is gone (the
  `Notify` `strong_count` check already covers this; add a comment + a drain test).

**Tests:**
- end-to-end: append N, trigger shutdown, assert the sink received all N and the
  cursor is fully advanced.

**Verify:** `cargo test wal:: pipeline_end_to_end`

---

## Phase 4 — Hot-path efficiency (🟢 directly serves perf/latency/memory)

Small, self-contained, measurable allocation/IO reductions. No API churn.

### Step 5 — Stop cloning every event per flush  🟢
`forwarder.rs:65` clones the whole batch (`map(|e| e.event.clone())`) just to
pass `&[WalEvent]` to the sink. **Fix:** change `Sink::write` to accept
`&[WalEntry]` (or drain the batch and move events out). Removes one full-batch
deep clone (String topic + message) on every flush.
**Verify:** `cargo test sink:: wal::forwarder`

### Step 6 — Reuse the line-protocol buffer in `build_body`  🟢
`influx.rs:54` allocates `Vec<String>` + join per flush. **Fix:** write each
point into one reused `String` (`write!`/`push_str`), separated by `\n`.
Optionally make the body buffer a field reused across calls.
**Verify:** `cargo test sink::`

### Step 7 — Skip the per-commit `readdir`  🟢
`subscription.rs:175` runs `list_segments` (a `readdir`) on **every** batch
commit even when the committed segment is unchanged. **Fix:** track
`last_gc_segment_id`; only scan + GC when `up_to.segment_id` advances past it.
The cursor write still happens every commit (cheap, 16 bytes).
**Verify:** `cargo test wal::subscription wal::wal`

### Step 8 — Reuse the reader payload buffer  🟢
`codec.rs:37` does `vec![0u8; len]` per record. **Fix:** add a `decode_into`
variant that reads into a caller-owned reusable `Vec<u8>`, used by the
subscription's read loop. Keep the existing `decode_from` for tests/recovery.
**Verify:** `cargo test wal::codec wal::subscription`

### Step 9 — Don't flush/notify when there's nothing new  🟢
The 5 ms `flush_tick` flushes + `notify_waiters()` 200×/s even when idle
(`writer.rs:171-178`). **Fix:** track a `dirty` flag set on write, cleared on
flush; skip the flush + notify when not dirty. Cuts idle syscalls and wakeups.
**Verify:** `cargo test wal::writer`

---

## Phase 5 — Readability / convention nits (⚪ low risk)

### Step 10 — Misc cleanups  ⚪
- `codec.rs:24-26` doc comment claims a torn tail "will be overwritten on next
  append" — only true *after* Step 1. Update the comment to describe truncation.
- `WalEntry.offset` (`types.rs:41`, `#[allow(dead_code)]`) is test-only while the
  forwarder commits `offset_after`. Either document why it's retained or drop it
  and update tests to use `offset_after`.
- `InfluxSink.token` is a plain `String` (`influx.rs:23`); wrap in
  `secrecy::SecretString` to match the codebase convention (never logged today,
  but aligns with `Config`). Access via `.expose_secret()` at the header build.

**Verify:** `cargo clippy --all-targets --all-features --locked -- -D warnings`

---

## Final verification (run after each phase, mandatory at the end)
```powershell
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
```
Update `README.md` only if Step 3 introduces a new env var (e.g. a sink-retry
cap). No Docker changes expected.

---

## Open questions
1. ~~Sink-outage policy~~ — **RESOLVED: hold-and-retry** (Step 3).
2. ~~Shutdown drain timeout~~ — **RESOLVED: 5 s** (Step 4).

## Suggested sequencing
- **Merge 1:** Steps 1–2 (the blocker + its companion). Ship-ready gate.
- **Merge 2:** Step 3 (after Q1 sign-off).
- **Merge 3:** Step 4.
- **Merge 4:** Steps 5–9 (efficiency) — independent, can land anytime.
- **Merge 5:** Step 10 (nits).
