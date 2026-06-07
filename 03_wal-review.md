# WAL Deep Review Findings (`smarthome-ingest`)

Collected from parallel sub-agent reviews using different models/agent types:
- `wal-correctness-reviewer` (`code-review`, `gpt-5.5`)
- `wal-performance-reviewer` (`performance-benchmarker`, `gemini-3.1-pro-preview`)
- `wal-test-observability-reviewe` (`superpowers:code-reviewer`, `claude-sonnet-4.6`)
- `wal-reliability-reviewer` (`general-purpose`, `claude-opus-4.7`) — returned only blockers summary

---

## A) Correctness findings

### HIGH — WAL append success is not a durability boundary
- **Evidence:** `src/pipeline/stages/persist.rs` (`PersistStage::run`), `src/infrastructure/wal/wal.rs` (`Wal::try_append`), `src/infrastructure/wal/writer.rs` (`writer_loop`)
- **Weak spot:** `try_append` success only means enqueue to in-memory channel, not durable WAL flush. Crash window can lose data while pipeline considers it persisted.
- **Fix direction:** Add durability ack/barrier from writer after flush + head advance, or redefine persistence guarantee explicitly.

### HIGH — Same-segment notification race can stall replay
- **Evidence:** `src/infrastructure/wal/subscription.rs` (`WalSubscription::wait_or_advance`)
- **Weak spot:** Logic advances only when segment ID increases. If same segment offset advanced and notify was raced/missed, reader can park until another write.
- **Fix direction:** Also treat `head.segment_id == cur_segment_id && head.byte_offset > cur_byte_offset` as progress.

### HIGH — Recovery truncates after first decodable-corrupt record
- **Evidence:** `src/infrastructure/wal/recover.rs` (`last_valid_offset`), `src/infrastructure/wal/wal.rs` (`Wal::open`), `src/infrastructure/wal/subscription.rs` skip behavior
- **Weak spot:** Recovery conflates torn tail vs fully-framed corrupt payload, potentially deleting valid records after a poison record.
- **Fix direction:** Truncate only incomplete tail; keep framed corrupt records and skip/commit poison safely.

### MEDIUM — Corrupt length prefix can trigger huge allocation
- **Evidence:** `src/infrastructure/wal/codec.rs` (`decode_into`), `src/infrastructure/wal/recover.rs` (`last_valid_offset`), `src/infrastructure/wal/subscription.rs` (`next`)
- **Weak spot:** Unbounded length from disk is trusted before allocation.
- **Fix direction:** Enforce max WAL record size before allocation.

### MEDIUM — Cursor commit error can stop forwarder after successful sink write
- **Evidence:** `src/infrastructure/wal/forwarder.rs` (`flush`, `run_forwarder`), `src/main.rs` forwarder task error handling
- **Weak spot:** If sink write succeeds but commit fails, forwarder exits; duplicates on restart and WAL growth while stopped.
- **Fix direction:** Retry commit and preserve successful batch boundary until cursor durable.

### MEDIUM — Shutdown can drop pre-WAL worker-queued messages
- **Evidence:** `src/main.rs` worker queueing/shutdown order and post-shutdown WAL drain
- **Weak spot:** Worker queue items not yet appended to WAL can be dropped on shutdown path.
- **Fix direction:** Stop intake first, drain worker queues into WAL before worker exit (or WAL-before-queue architecture).

---

## B) Performance/latency/memory findings

### CRITICAL — Blocking file I/O in Tokio async paths ✅ DONE
- **Evidence:** `src/infrastructure/wal/writer.rs` (`writer_loop`), `src/infrastructure/wal/subscription.rs` (`WalSubscription::next`)
- **Weak spot:** `std::fs` blocking I/O inside async loops can starve runtime workers under disk pressure.
- **Fix direction:** Move disk I/O to dedicated blocking thread(s) or `spawn_blocking`.

### HIGH — Torn read risk in `AtomicWalOffset` ✅ DONE
- **Evidence:** `src/infrastructure/wal/wal.rs` (`AtomicWalOffset::load` / `store`)
- **Weak spot:** Two atomics can be observed inconsistently (`segment_id`, `byte_offset`).
- **Fix direction:** Use one atomic packed value or lock-protected struct.

### HIGH — Allocation churn from cloning and (de)serializing full `HandledMessage`
- **Evidence:** `src/pipeline/stages/persist.rs` clone path, `src/infrastructure/wal/codec.rs` decode path
- **Weak spot:** Deep clone + nested serialization/deserialization increases allocator pressure.
- **Fix direction:** Store flat line protocol payload in WAL event instead of full message object graph.

### MEDIUM — Reopen-on-EOF causes syscall churn
- **Evidence:** `src/infrastructure/wal/subscription.rs` (`WalSubscription::next`)
- **Weak spot:** Drops/reopens file repeatedly on EOF at tail.
- **Fix direction:** Keep descriptor/reader open and re-read after notify.

### MEDIUM — `flush()` comment/semantics mismatch durability expectations
- **Evidence:** `src/infrastructure/wal/writer.rs` flush tick handling
- **Weak spot:** `BufWriter::flush` writes to OS cache, not guaranteed persisted media.
- **Fix direction:** If required, call `sync_data`; otherwise document weaker durability explicitly.

---

## C) Tests and observability findings

### CRITICAL — Missing test for `TryAppendError::Closed` in persist stage ✅ DONE
- **Evidence:** `src/pipeline/stages/persist.rs` closed branch untested
- **Weak spot:** No proof that DLQ route/Stop semantics are correct when WAL channel is closed.
- **Fix direction:** Add targeted unit test asserting `StageFlow::Stop`, DLQ flag, reason string.

### CRITICAL — No tests for `is_permanent_status` ✅ DONE
- **Evidence:** `src/infrastructure/sink/influx.rs` classification function untested
- **Weak spot:** Misclassification can either silently drop retryable data or stall WAL indefinitely.
- **Fix direction:** Add unit tests for key status codes (400/401/404/408/429/500/503) + integration assertions.

### HIGH — No full-path outage retry-hold integration test via `run_forwarder`
- **Evidence:** `forwarder.rs` unit tests rely on `flush`; `tests/batcher.rs` mostly happy-path 204
- **Weak spot:** Core invariant (hold cursor on retryable outage) not validated end-to-end.
- **Fix direction:** Add integration sequence (503, 503, 204) validating retries and delayed commit.

### HIGH — Influx sink non-204 failure paths not integration-tested
- **Evidence:** mock server returns only 204 in existing integration coverage
- **Weak spot:** Permanent/retryable branches and backoff behavior not covered.
- **Fix direction:** Extend mock server to scripted response sequences and assert error mapping.

### MEDIUM — No metric for WAL writer fatal exits
- **Evidence:** fatal returns in `src/infrastructure/wal/writer.rs` only log
- **Weak spot:** No alertable signal for writer death.
- **Fix direction:** Add `wal_writer_fatal_total{reason=...}` counter.

### MEDIUM — Segment rotation is not observable
- **Evidence:** increment of `current_segment_id` has no counter/log metric
- **Weak spot:** Hard to separate normal throughput from WAL growth incidents.
- **Fix direction:** Add `wal_segment_rotations_total`.

### MEDIUM — Retry outage duration is not directly observable
- **Evidence:** only retry count counter exists
- **Weak spot:** Count alone does not show active outage duration.
- **Fix direction:** Add retry-state gauge and/or elapsed duration log on recovery.

### MEDIUM — Retry log misses error/status context
- **Evidence:** `src/infrastructure/sink/influx.rs` retry warn log without error field
- **Weak spot:** Hard to diagnose root cause from logs.
- **Fix direction:** Include `error = %last_err` (and status where available) in warn logs.

### LOW — `sample_event` helper duplicated across WAL test modules
- **Evidence:** duplicated helper in multiple WAL test files
- **Weak spot:** Maintenance drift risk.
- **Fix direction:** Centralize in WAL test support helper.

### LOW — Non-atomic offset load semantics undocumented
- **Evidence:** `AtomicWalOffset` two-load behavior lacks rationale comment
- **Weak spot:** Subtle concurrency assumption not documented.
- **Fix direction:** Add explicit invariant/safety note.

### LOW — WAL test names diverge from repo naming convention
- **Evidence:** descriptive names not in `test_<function>_<scenario>_<expected>` pattern
- **Weak spot:** Consistency/filterability impact.
- **Fix direction:** Rename gradually.

### LOW — No end-to-end test for corrupt cursor file startup behavior
- **Evidence:** unit coverage in cursor module exists; startup policy path lacks E2E assertion
- **Weak spot:** Recovery behavior under corrupt cursor not validated at service-open boundary.
- **Fix direction:** Add WAL open test with malformed cursor and explicit expected policy.

---

## D) Reliability review blockers (from dedicated reliability agent)

The reliability agent marked its todo as **blocked** pending these product decisions:
1. Target durability contract (exactly what boundary counts as durable)
2. Disk-cap / WAL growth policy under extended outage
3. Multi-instance protection policy for WAL directory ownership
4. Shutdown contract for pre-WAL in-flight messages

It referenced unresolved reliability buckets (`C1–C6`, `H1–H5`) but did not return their detailed list in the captured output.

---

## E) Prioritization shortlist (highest impact first)

1. **Blocking async I/O on runtime workers** (critical perf/latency)
2. **Durability boundary mismatch (`try_append` vs flush)** (high correctness/reliability)
3. **`is_permanent_status` untested + sink error-path coverage gaps** (critical test/risk)
4. **Same-segment wake race in subscription** (high correctness/stall risk)
5. **Crash/recovery truncation semantics for corrupt-but-framed records** (high data-loss risk)