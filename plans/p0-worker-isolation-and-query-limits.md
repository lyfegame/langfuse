# P0: Worker Role System + Per-Query Resource Limits

**Status**: Implemented in PR #2
**Branch**: `shuyingl/fork-improvements`
**Impact**: Breaks the cascading failure chain at two independent points

## Worker Role System

### Problem

Running a dedicated export worker pod required setting 15+ `QUEUE_CONSUMER_*` env vars. This was error-prone and fragile — miss one flag and an export pod accidentally runs ingestion queues.

### Solution

Single env var `LANGFUSE_WORKER_ROLE` with three values:

| Value | Behavior |
|---|---|
| `all` (default) | Runs all queues — no behavior change for existing deployments |
| `ingestion` | Runs all queues EXCEPT blob storage export |
| `export` | Runs ONLY blob storage export queues |

### Files Changed

- `worker/src/env.ts` — Zod schema for `LANGFUSE_WORKER_ROLE`
- `worker/src/app.ts` — Gate all 29+ queue registrations with `isIngestion` / `isExport`

### Deployment

```yaml
# Export worker pod
env:
  - name: LANGFUSE_WORKER_ROLE
    value: "export"

# Ingestion worker pod
env:
  - name: LANGFUSE_WORKER_ROLE
    value: "ingestion"
```

Existing per-queue `QUEUE_CONSUMER_*` flags remain as overrides within each role.

## Per-Query ClickHouse Resource Limits

### Problem

Export queries ran with default ClickHouse settings — unlimited threads and normal priority. A single export query could consume all CPU threads and starve interactive queries.

### Solution

Two new env vars applied to all export functions:

| Env var | Default | Effect |
|---|---|---|
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_MAX_THREADS` | 10 | Caps CPU threads per export query |
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_PRIORITY` | 2 | Lower priority than interactive (default 0) |

### Files Changed

- `packages/shared/src/env.ts` — Zod schemas for both env vars
- `packages/shared/src/server/repositories/traces.ts` — `clickhouseSettings` on `getTracesForBlobStorageExportParquet()`
- `packages/shared/src/server/repositories/observations.ts` — `clickhouseSettings` on `getObservationsForBlobStorageExportParquet()`
- `packages/shared/src/server/repositories/scores.ts` — `clickhouseSettings` on `getScoresForBlobStorageExportParquet()`
- `packages/shared/src/server/repositories/events.ts` — `clickhouseSettings` on `getEventsForBlobStorageExport()`

### Note

`priority` is cast to `String()` because the ClickHouse client expects string-typed settings. `max_threads` is passed as a number (also accepted).

## Export Observability

### Problem

Export pipeline had bare string logs (`logger.info("message")`) with no structured fields for filtering or dashboarding.

### Solution

- Structured log fields on success/error paths: `table`, `projectId`, `windowStart`, `windowEnd`, `durationMs`, `mode`
- Two OTel histograms for Grafana dashboards:
  - `langfuse.blob_export.table_duration_ms` — per-table export latency
  - `langfuse.blob_export.job_duration_ms` — total job latency with `mode` label (catchup/steady)

### Files Changed

- `worker/src/features/blobstorage/handleBlobStorageIntegrationProjectJob.ts`

### Follow-up

~14 remaining log statements in the file still use string format. These can be converted incrementally (see P4).

## Verification

After deploying with `LANGFUSE_WORKER_ROLE=export` on the export pod:

1. Confirm only blob storage queues register: check worker startup logs for `Worker role: export`
2. Confirm ingestion pod doesn't process exports: set `LANGFUSE_WORKER_ROLE=ingestion`, verify no blob storage queue registration
3. Monitor ClickHouse `system.query_log` for export queries: verify `Settings['max_threads']` = 10 and `Settings['priority']` = 2
4. Check OTel metrics: `langfuse.blob_export.table_duration_ms` and `langfuse.blob_export.job_duration_ms` appear in your metrics backend
