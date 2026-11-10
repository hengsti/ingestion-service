# WAL and Reliability

The WAL is the service boundary between ingestion and output-sink availability. It lets ingestion continue while the active sink is unavailable, subject to local disk capacity and bounded input queues.

## Data Stored

The persist stage stores already-rendered sink payloads, not raw JSON. With the current `InfluxEncoder`, that payload is a single InfluxDB line-protocol line.

Each WAL record has a bincode body with a four-byte prefix:

```text
[u32 little-endian length and format flag][bincode body]
```

The low 31 bits encode the body length, limited to 1 MiB. Bit 31 marks targeted
output: the body is `(topic: String, ts_ms: i64, payload: String, sink_target: String)`.
Unflagged legacy/quarantine bodies remain `(topic: String, ts_ms: i64, payload: String)`.
All strings use bincode's fixed-width length encoding. Targeted frames account for
eight length bytes plus the target bytes in the existing size and capacity limits.
Older binaries reject the flagged prefix as an oversized frame rather than sending
targeted records to their global bucket.

`WalEvent` fields:

| Field | Meaning |
|---|---|
| `topic` | Source topic string (MQTT today) |
| `ts_ms` | Ingest time when the WAL event was created |
| `payload` | Complete sink payload (InfluxDB line protocol today) |
| `sink_target` | Frozen bucket name for output; absent on legacy/quarantine records |

## Sink target migration

Older route bundles remain valid: an omitted `sink_target` inherits `INFLUX_BUCKET`.
New output records always save that resolved bucket. Route edits and fallback-bucket
changes never reinterpret pending records. Keep the InfluxDB server and organization
unchanged while output is pending; these connection settings are not stored per record.

Legacy pending output has no bucket metadata. Before upgrading:

1. Keep the old binary, original routes, and original `INFLUX_BUCKET` active. Quiesce
   publishers or otherwise stop new intake without clearing the persistent MQTT session.
2. Let the old forwarder drain, then stop ingest cleanly. Verify its committed cursor
   reaches the durable WAL head, including any final partial batch. Back up both journals
   and the route bundle while stopped. Do not delete WAL files to force an upgrade.
3. Start the new binary with the existing journal directories. Startup scans pending
   output as part of recovery and refuses any legacy record without changing its bytes
   or cursor. If refused, run the old version with the original bucket and finish draining.
   Committed legacy records and existing quarantine records remain readable.

For binary rollback, an old binary cannot reuse a WAL containing targeted frames,
including committed frames retained in the active segment. Drain with the new binary,
stop ingest, verify cursor equals durable head, and preserve that journal. A downgrade
requires a separate fresh output WAL and the old-compatible route bundle; preserve the
quarantine journal and trusted MQTT subscription history using the
[stopped-ingest migration procedure](operations.md#route-changes-and-subscription-migration).
Never point an older binary at pending targeted output or clear the broker session.
Rolling back only route targets while staying on the new binary does not require a drain.

## Files

WAL files live in `WAL_DIR`.

| File | Purpose |
|---|---|
| `00000000000000000001.log`, `00000000000000000002.log`, ... | Segment files |
| `commit.cursor` | 16-byte committed offset, encoded as big-endian segment id and byte offset |
| `commit.cursor.tmp` | Temporary cursor file used before atomic rename |
| `.journal.lock` | Persistent lock file held open while the journal is in use |

Segment names use 20 decimal digits plus `.log`.

## Writer

The WAL writer runs on a dedicated OS thread because it performs blocking file I/O.

Append flow:

1. Pipeline creates a `WalEvent`.
2. `append_durable` rejects bodies over 1 MiB, reserves journal capacity, and sends the event
   through a bounded sync channel. Capacity rejection returns the event to the caller.
3. The writer encodes and writes the record to a `BufWriter`.
4. The writer batches records for up to 5 ms, or until pending receipts reach the queue capacity,
   then flushes the buffer and calls `File::sync_data`.
5. Only after a successful sync does the writer advance the in-memory head and resolve the
   group's append receipts. A write, flush, sync, or rotation failure stops the writer, fails
   affected receipts, and leaves the head at the last successful sync.

Sync adds storage latency to each batch; 5 ms is the batching deadline, not a bound on total
append latency. The head uses a mutex-free, single-writer atomic snapshot: readers retry when
its version is odd or changes and may spin during publication. It represents the last
successfully synced offset. `Wal::writer_failure()` exposes a terminal writer failure
independently of thread liveness.

When capacity is full, the persist stage retains its current event and waits for a successful
reclaim rather than treating storage pressure as a schema rejection. If the writer queue is
full on the first attempt or during a retry, that event remains pending until the writer progresses.
Worker queues remain bounded. Shutdown releases pressure waits and gives workers and the forwarder
a shared five-second drain deadline, so sink recovery is not required for shutdown. Unfinished
workers are aborted at the deadline without acknowledging their input or advancing the WAL cursor.
QoS 1 messages are acknowledged only after durable output WAL or quarantine persistence. Queue
admission failures and failed quarantine appends leave messages unacknowledged; persistent broker
sessions are required for redelivery after a disconnect.

On Linux, file sync uses `fdatasync` and directory sync uses `fsync`. These calls are meaningful
only when the filesystem, volume driver, and storage device honor flush requests; they cannot
protect against lost media or unsupported network/virtual storage guarantees. On Windows the
code also attempts real file and directory-handle syncs and reports failures, but Windows'
directory-entry crash guarantees are not claimed. Linux/ARM64 is the production target;
Windows tests do not establish its power-loss behavior.

## Segment Rotation

The writer rotates when appending the next record would exceed `WAL_SEGMENT_BYTES` and the current segment is not empty.

Rotation flow:

1. Flush and sync any pending records in the current segment.
2. Publish the synced head and acknowledge those records.
3. Create and sync the next numbered segment, then sync its directory entry.
4. Continue writing at byte offset `0`. Failure leaves the new record unacknowledged.

Fatal writer failures are counted by `wal_writer_fatal_total` with reasons:

- `flush_or_sync`
- `flush_before_rotation`
- `open_next_segment`
- `write_all`

## Recovery

On startup, `Wal::open`:

1. Creates `WAL_DIR` if needed, syncs relevant parent entries, acquires exclusive ownership,
   and probes file create/write/sync/rename/read capability.
2. Lists segment files.
3. Reads `commit.cursor` and validates its segment and record boundary before changing files.
4. Syncs retained older segments before publishing a recovered head.
5. Scans the active segment for the last complete offset.
6. Truncates and syncs an incomplete trailing record only after the committed cursor is validated.
7. Reclaims segments older than the validated committed cursor and syncs their removal.
8. Reconciles existing segment sizes against the byte limit, starts the subscription at the
   committed cursor (or the first segment offset `0`), and starts the writer at the recovered
   offset after syncing its active segment and directory.

If the active segment is empty after an interrupted rotation, recovery accounts for the
preceding retained segment and preserves enough headroom to admit a maximum-sized record.

A corrupt or inconsistent committed cursor is a startup error, including an existing empty
cursor file; recovery preserves the segment instead of truncating through it. Complete corrupt
records and missing or unreadable segments within the known durable range also fail closed.
Only the documented incomplete trailing crash tail may be truncated, and only after the
committed cursor has been validated. Complete corruption is preserved and reported with its
offset; it is not skipped or automatically repaired.

## Subscription

The forwarder reads through `WalSubscription`.

The subscription:

- Reads records from the committed cursor forward, but never beyond the last synced head
  published by the WAL writer.
- Uses blocking file reads on `spawn_blocking`.
- Keeps a reusable payload buffer to reduce allocation.
- Waits for writer notifications when it reaches the synced head or an expected future
  segment.
- Advances to the next segment only when it is within the durable range.
- Returns a read/integrity error at the offending durable offset for invalid frames,
  unexpected short reads, or open/seek failures; it does not skip records or roll forward.

The commit operation rejects offsets beyond the synced head. This prevents a cursor or segment
reclamation from passing unread data. A read or cursor-commit/reclaim fault is exposed to readiness
and retains uncommitted output. Stop the service and preserve the files for diagnosis; never
advance the cursor or delete/truncate data to bypass a read failure. See the
[reader fail-closed contract](https://github.com/hengsti/ingestion-service/issues/39).

On commit, the subscription:

1. Writes and syncs `commit.cursor.tmp`.
2. Renames it to `commit.cursor` and syncs the directory.
3. Only after those steps succeed, deletes segment files with ids strictly lower than the
   committed segment and keeps the segment containing the committed offset.

Segment deletion or directory-sync failures fail the commit operation and are retried at the
same cursor without repeating the successful sink write. Deleted bytes remain accounted until
the directory sync succeeds. Startup also reclaims older segments left behind after a crash.

`WAL_MAX_BYTES` and `QUARANTINE_MAX_BYTES` are independent logical budgets. Each counts segment
lengths, recovered journal metadata, pending reservations, a per-segment allowance, and fixed
metadata headroom. Rejections are stored in the separate quarantine journal and are not sent to
the valid-output forwarder. See [Configuration](configuration.md#wal) for the exact limits and
minimum.

## Forwarder Guarantees

The forwarder flushes when:

- Batch length reaches `BATCH_SIZE`.
- `FLUSH_INTERVAL_MS` ticks with a non-empty batch.
- The next record has a different saved sink target.
- The WAL closes and a final partial batch remains.

Sink outcomes:

| Sink result | Forwarder action | Cursor action |
|---|---|---|
| Success | Clear batch | Commit through highest `offset_after` |
| Permanent failure | Retain batch and retry with backoff until repaired | Do not commit |
| Retryable failure | Retain batch and retry with backoff | Do not commit |

Retry backoff:

- Initial retry delay is `min(max(FLUSH_INTERVAL_MS, 1), 1000)` ms.
- Delay doubles up to 5000 ms.

Cursor commit failures after a successful sink write are retried in-loop. The sink write is not repeated while the cursor commit is being retried.

Only consecutive records with the same saved destination share a batch. The forwarder
retains at most `BATCH_SIZE` events plus one lookahead entry while flushing at a target
boundary, and sends one HTTP request at a time. Alternating targets produce smaller
batches and more requests; no per-target queues or forwarding tasks are added. A failed
target blocks later destinations until repaired, preserving the single ordered cursor.

## Delivery Semantics

The service retains WAL batches and retries delivery after both retryable and permanent sink
failures. For QoS 1 input, PUBACK follows durable output WAL or quarantine persistence. If
neither durable append completes, the message remains unacknowledged. QoS 0 input cannot be
replayed through this contract; a persistent MQTT session is required for redelivery after
disconnect. Broker expiry/queue limits and the local journal budgets bound the guarantee.
Sync receipts depend on the filesystem and storage honoring sync, and do not provide
exactly-once sink delivery or protection from lost media.

With the current `InfluxSink`, cases to understand are:

- If InfluxDB is temporarily unavailable, the forwarder holds the batch and does not advance the cursor.
- If the process crashes before committing a successfully written batch, the batch can be replayed after restart.
- If InfluxDB returns a permanent 4xx error, except `408` or `429`, the batch remains pending and blocks later writes until the destination or pending output is repaired.
- Every encoded point carries a validated epoch-millisecond timestamp before WAL append.
  Replayed writes use the same measurement, tag set, fields, and timestamp; InfluxDB merges
  same-identity writes and may overwrite matching fields. A crash before cursor commit can
  replay a successful write, so this is not exactly-once delivery and InfluxDB server time
  is never substituted.

The WAL is not a general event archive. Committed older segments are deleted.

## Operational Implications

- Put `WAL_DIR` on persistent storage with enough capacity for the expected sink outage duration.
- Alert on `wal_forwarder_retry_outage_active == 1`.
- Alert on `wal_accounted_bytes` approaching `wal_capacity_bytes`, `wal_capacity_full_total`,
  and underlying filesystem free space.
- Investigate permanent sink-error logs and repair the cause; the forwarder retains the failed batch and does not advance the cursor.
- Investigate `wal_writer_fatal_total`; it means the WAL writer exited and subsequent appends will fail.
- Investigate `wal_forwarder_read_failed` and cursor commit/reclaim failures without
  modifying the cursor or segment files. Stop the process and preserve the journal for
  diagnosis; complete corruption is fail-closed, not a supported repair workflow.
