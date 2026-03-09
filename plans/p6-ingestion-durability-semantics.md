# P6: Ingestion Durability Semantics

**Status**: In Progress (`2026-03-09T22:05:11Z`)
**Type**: Correctness + durability
**Impact**: Make ingestion job success mean verified ClickHouse materialization instead of "queued for write".

## Problem Statement

The current ingestion path is durable at the **raw-event blob** layer, but not at the **worker acknowledgement** layer.

Today the flow is:

1. API ingest uploads the raw event batch to the event-upload bucket.
2. BullMQ enqueues an ingestion job keyed by `projectId`, `eventBodyId`, and `fileKey`.
3. The worker downloads the raw event blob and merges it into the latest entity shape.
4. The worker enqueues ClickHouse writes into the in-process `ClickhouseWriter` buffer.
5. The BullMQ job can complete before those writes are durably flushed and reconciled.

That creates three correctness hazards:

- **False terminal failure after commit** — the client can time out after ClickHouse has already committed the insert.
- **Pre-commit dedupe** — the Redis `recently-processed` cache is set before durable materialization, so a retry can skip work that is not yet verified.
- **True drop on exhaustion** — the in-process writer still drops records after max attempts instead of writing them to a dead-letter queue.

This is a semantics bug, not just a tuning bug. Raising timeouts or lowering concurrency can reduce the frequency, but they do not fix the contract.

## Implementation Status (`2026-03-09T22:05:11Z`)

The first production-oriented slice is now implemented in `lyfegame/langfuse#13`:

- ingestion-critical ClickHouse writes (`traces`, `observations`, `scores`, `dataset_run_items_rmt`, `blob_storage_file_log`, staging rows) now use an awaited writer path instead of fire-and-forget buffering;
- ambiguous timeout / network insert errors reconcile by querying ClickHouse for the expected record key and `event_ts` watermark before treating the write as failed;
- the ingestion queue now writes the Redis `recently-processed` cache only after `mergeAndWrite()` and the awaited `blob_storage_file_log` side effect both succeed;
- awaited writer failures now reject back to BullMQ instead of silently staying inside the in-memory writer retry loop.

**Still open:** the generic fire-and-forget writer path can still drop records after max attempts. The remaining `Phase 6d` DLQ work is still required before calling the durability story fully complete.

## Design Goal

Adopt **at-least-once ingestion with verified acknowledgement**:

- The raw event blob in object storage is the source of truth.
- BullMQ is the durable work item.
- ClickHouse tables are a materialized projection.
- A BullMQ job is only acknowledged when the required ClickHouse side effects are verified.

This does **not** aim for distributed exactly-once semantics. It aims for a stronger and more realistic contract:

> If a job is marked successful, the intended ClickHouse entity state exists (or is provably already present).

## Non-Goals

- Building cross-table SQL transactions in ClickHouse.
- Eliminating all duplicate writes at the transport layer.
- Replacing the raw-event blob store as the replay source of truth.
- Redesigning the Langfuse ingestion schema.

## Current Failure Modes

### 1. Job completion is decoupled from durable write completion

`IngestionService` enqueues `traces`, `observations`, `scores`, and `blob_storage_file_log` records into `ClickhouseWriter`, but does not await a durable flush barrier before returning to BullMQ.

### 2. Dedupe marker is written before durable verification

The Redis `recently-processed` cache is set before the merged entity write is durably verified. That can convert an ambiguous retry into a skip.

### 3. Ambiguous timeout is treated as failure

`Timeout error`, `ECONNRESET`, and `ECONNREFUSED` can happen after the insert already committed. Without read-after-write verification, BullMQ can record a terminal failure even though the trace row already exists.

### 4. Final writer exhaustion still drops data

The in-process writer retries a bounded number of times and then drops records. The raw event blob is still available for replay, but there is no first-class dead-letter queue or automatic reconciliation path.

## Proposed Architecture

## Phase 6a: Durable write barrier per BullMQ job

Add a new job-scoped write API so ingestion code can await durable completion.

### Contract

```ts
await clickhouseWriter.enqueueAndAwait({
  table: TableName.Traces,
  record: finalTraceRecord,
  verification: {
    kind: "trace",
    projectId,
    entityId: finalTraceRecord.id,
  },
});
```

### Requirements

- `addToQueue()` remains available for low-risk/background paths.
- Ingestion jobs use a stronger API that resolves only when:
  - the relevant insert batch has been flushed, and
  - post-write verification succeeds or the row is proven already present.
- Buffering is still allowed, but the caller receives a promise/future tied to that record's flush outcome.

### Files

- `worker/src/services/ClickhouseWriter/index.ts`
- `worker/src/services/IngestionService/index.ts`

## Phase 6b: Post-verify dedupe cache

Move the Redis `recently-processed` cache write so it only happens after successful durable verification.

### Requirements

- The cache must never suppress a retry before the entity write is verified.
- If verification is ambiguous or fails, the cache must not be set.
- The cache becomes a best-effort optimization, not part of the durability boundary.

### Files

- `worker/src/queues/ingestionQueue.ts`

## Phase 6c: Ambiguous-timeout reconciliation

When a write attempt fails with a retryable network/timeout error, perform an existence check before rethrowing.

### Verification strategy

- `trace-create` → verify `traces.id`
- observation writes → verify `observations.id`
- score writes → verify `scores.id`
- file-log side effect → verify `blob_storage_file_log.event_id=fileKey`

### Result

- If the row exists, treat the write as success.
- If the row does not exist, continue retry/backoff.

### Why this matters

This specifically fixes the March 9 behavior where BullMQ recorded terminal failures for jobs whose `traces` rows and `blob_storage_file_log` rows had already committed.

### Files

- `worker/src/services/ClickhouseWriter/index.ts`
- repository helpers in `packages/shared/src/server/repositories/`

## Phase 6d: DLQ instead of drop

Replace the current “max attempts reached, dropped N record(s)” behavior with a Redis-backed dead-letter queue.

### DLQ payload

Each DLQ item should include:

- `projectId`
- `table`
- `entityId`
- `eventBodyId`
- `fileKey` / bucket path
- last error string
- attempt count
- first-seen and last-attempt timestamps

### Requirements

- No record is silently dropped from the writer path.
- DLQ entries are replayable from the raw event bucket.
- Alerting can page on DLQ growth.

### Files

- `worker/src/services/ClickhouseWriter/index.ts`
- `packages/shared/src/server/redis/`
- `worker/src/scripts/replayIngestionEvents/`

## Phase 6e: Explicit ingestion write timeout + telemetry

Make ingestion write timeouts explicit and observable.

### Additions

- `LANGFUSE_INGESTION_CLICKHOUSE_REQUEST_TIMEOUT_MS`
- histograms for write latency and verification latency
- counters for:
  - `verify_after_timeout.success`
  - `verify_after_timeout.miss`
  - `dlq.enqueued`
  - `writer.ack_timeout`

### Why

This prevents the client default timeout from silently defining the durability boundary.

## Rollout Plan

### Step 1 — Ambiguous timeout verification

Lowest-risk first change:

- add explicit timeout env
- add post-timeout existence verification
- keep current batching model

This should eliminate the false-negative terminal failures we saw on March 9.

### Step 2 — Move dedupe marker post-verify

Once verification exists, move `recently-processed` cache writes behind the durable success path.

### Step 3 — Add durable writer acknowledgements

Introduce `enqueueAndAwait()` / record-scoped flush futures and migrate ingestion callers to it.

### Step 4 — Replace drop with DLQ

Land the DLQ and replay tooling so true exhaustion becomes repairable, observable debt instead of loss.

## Acceptance Criteria

A change is not complete until all of the following are true:

- A forced client-side timeout after a successful ClickHouse insert no longer leaves a terminal BullMQ failed job.
- The Redis dedupe cache is never written before durable verification.
- The writer no longer logs dropped records on retry exhaustion; it emits DLQ entries instead.
- A worker crash before flush causes BullMQ retry/replay from the raw event blob without a false dedupe skip.
- Replay documentation covers DLQ-based recovery, not just manual S3 log reconstruction.

## Why this belongs in the fork

This is application-level ingestion semantics:

- BullMQ worker acknowledgement
- Redis dedupe keys
- ClickhouseWriter behavior
- post-timeout verification logic
- DLQ/replay plumbing

Those all live in `lyfegame/langfuse`, not in the infrastructure repo.

The infrastructure repo should track this as a pre-cutover gate, but the implementation ownership belongs here.
