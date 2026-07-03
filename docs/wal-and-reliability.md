# WAL and Reliability

The WAL is the service boundary between ingestion and InfluxDB availability. It lets MQTT ingestion continue while InfluxDB is unavailable, subject to local disk capacity and bounded input queues.

## Data Stored

The persist stage stores already-rendered InfluxDB line protocol, not raw JSON.

Each WAL record is a bincode-serialized `WalEvent` framed as:

```text
[u32 little-endian payload length][bincode WalEvent payload]
```

Maximum WAL record payload length is 1 MiB.

`WalEvent` fields:

| Field | Meaning |
|---|---|
| `topic` | Source MQTT topic |
| `ts_ms` | Ingest time when the WAL event was created |
| `payload` | Complete InfluxDB line protocol line |

## Files

WAL files live in `WAL_DIR`.

| File | Purpose |
|---|---|
| `00000000000000000001.log`, `00000000000000000002.log`, ... | Segment files |
| `commit.cursor` | 16-byte committed offset, encoded as big-endian segment id and byte offset |
| `commit.cursor.tmp` | Temporary cursor file used before atomic rename |

Segment names use 20 decimal digits plus `.log`.

## Writer

The WAL writer runs on a dedicated OS thread because it performs blocking file I/O.

Append flow:

1. Pipeline creates a `WalEvent`.
2. `append_durable` sends it through a bounded sync channel.
3. The writer encodes and writes the record to a `BufWriter`.
4. The writer flushes buffered bytes at least every 5 ms while dirty.
5. The writer advances the in-memory head offset and notifies readers.
6. `append_durable` returns after the writer acknowledges the flush boundary.

Important durability detail: `append_durable` waits for `BufWriter::flush`, not for a storage-media `fsync`. This means bytes have left process memory and are readable from the WAL file, but the code does not claim a power-loss-safe fsync boundary.

## Segment Rotation

The writer rotates when appending the next record would exceed `WAL_SEGMENT_BYTES` and the current segment is not empty.

Rotation flow:

1. Flush current segment.
2. Advance the head offset.
3. Notify readers and acknowledge pending durable appends.
4. Open the next numbered segment.
5. Continue writing at byte offset `0`.

Fatal writer failures are counted by `wal_writer_fatal_total` with reasons:

- `flush_before_rotation`
- `open_next_segment`
- `write_all`

## Recovery

On startup, `Wal::open`:

1. Creates `WAL_DIR` if needed.
2. Lists segment files.
3. Scans the active segment for the last valid offset.
4. Truncates a torn tail if the file contains incomplete trailing bytes.
5. Reads `commit.cursor` if present.
6. Starts the subscription at the committed cursor, or at the first segment offset `0` when no cursor exists.
7. Starts the writer at the recovered active segment and valid byte offset.

A corrupt cursor file is a startup error. For example, a cursor length other than 16 bytes is rejected.

## Subscription

The forwarder reads through `WalSubscription`.

The subscription:

- Reads records from the committed cursor forward.
- Uses blocking file reads on `spawn_blocking`.
- Keeps a reusable payload buffer to reduce allocation.
- Waits for writer notifications when it reaches EOF.
- Advances to the next segment when the writer rotates.
- Skips durable corrupt records that would otherwise stall replay.

On commit, the subscription:

1. Writes `commit.cursor` through temp file and rename.
2. Deletes segment files with ids strictly lower than the committed segment.
3. Keeps the segment that contains the committed offset because it can still contain uncommitted records after that byte offset.

Segment deletion failures are logged but do not fail the commit once the cursor has been written.

## Forwarder Guarantees

The forwarder flushes when:

- Batch length reaches `BATCH_SIZE`.
- `FLUSH_INTERVAL_MS` ticks with a non-empty batch.
- The WAL closes and a final partial batch remains.

Sink outcomes:

| Sink result | Forwarder action | Cursor action |
|---|---|---|
| Success | Clear batch | Commit through highest `offset_after` |
| Permanent failure | Drop poison batch | Commit through highest `offset_after` |
| Retryable failure | Hold batch and retry with backoff | Do not commit |

Retry backoff:

- Initial retry delay is `min(FLUSH_INTERVAL_MS, 1000)`.
- Delay doubles up to 5000 ms.

Cursor commit failures after a successful or permanent sink result are retried in-loop. The sink write is not repeated while the cursor commit is being retried.

## Delivery Semantics

The service aims for at-least-once delivery from WAL to InfluxDB for retryable failures.

Cases to understand:

- If InfluxDB is temporarily unavailable, the forwarder holds the batch and does not advance the cursor.
- If the process crashes before committing a successfully written batch, the batch can be replayed after restart.
- If InfluxDB returns a permanent 4xx error, except `408` or `429`, the batch is dropped and the cursor advances.
- If payload timestamps are valid, replayed writes use the same line protocol timestamp. If timestamps are invalid and omitted, replayed writes can create separate points with InfluxDB server time.

The WAL is not a general event archive. Committed older segments are deleted.

## Operational Implications

- Put `WAL_DIR` on persistent storage with enough capacity for expected InfluxDB outage duration.
- Alert on `wal_forwarder_retry_outage_active == 1`.
- Alert on sustained growth of WAL segment files.
- Investigate `wal_forwarder_drop_total` immediately; it means data was discarded as permanently unwritable.
- Investigate `wal_writer_fatal_total`; it means the WAL writer exited and subsequent appends will fail.
