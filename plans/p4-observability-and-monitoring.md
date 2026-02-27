# P4: Observability and Monitoring

**Status**: Partially started (P0 added basic structured logs + OTel histograms)
**Type**: Incremental code changes + dashboard setup
**Impact**: Detect issues before they become firefights

## Current State

PR #2 added:
- Structured logging on export success/error paths (4 log statements)
- OTel histograms: `langfuse.blob_export.table_duration_ms`, `langfuse.blob_export.job_duration_ms`

Remaining: ~14 log statements in the blob storage handler still use string format.

## Phase 4a: Complete Structured Logging

Convert remaining string-style logs in `handleBlobStorageIntegrationProjectJob.ts` to structured format:

```typescript
// Before
logger.info(`[BLOB INTEGRATION] Processing ${table} export for project ${projectId}`);

// After
logger.info({
  message: "[BLOB INTEGRATION] Processing export",
  table,
  projectId,
  windowStart: minTimestamp.toISOString(),
  windowEnd: maxTimestamp.toISOString(),
});
```

### Files

- `worker/src/features/blobstorage/handleBlobStorageIntegrationProjectJob.ts` — all remaining logger calls

## Phase 4b: Additional OTel Metrics

Add metrics for the ingestion pipeline and active query latency:

```typescript
// Ingestion throughput
recordIncrement("langfuse.ingestion.events_processed", batchSize, {
  queue: queueName,
});

// ClickHouse query latency (per service type)
recordHistogram("langfuse.clickhouse.query_duration_ms", durationMs, {
  service: preferredService,  // ReadWrite, ReadOnly, EventsReadOnly
  feature: tag.feature,
});
```

### Files

- `packages/shared/src/server/repositories/clickhouse.ts` — wrap query functions with timing
- Ingestion queue processors in `worker/src/queues/`

## Phase 4c: Export Progress API

New endpoint for programmatic export status monitoring.

**Route**: `GET /api/public/integrations/blob-storage/status`

**Response**:
```json
{
  "projects": [
    {
      "projectId": "...",
      "tables": {
        "traces": {
          "lastExportAt": "2026-02-26T12:00:00Z",
          "lastExportStatus": "success",
          "catchupLagMinutes": 15,
          "lastDurationMs": 4500
        }
      }
    }
  ]
}
```

Reads from `blob_storage_file_log` ClickHouse table + BullMQ job state.

### Files

- New: `web/src/pages/api/public/integrations/blob-storage/status.ts`
- Uses `withMiddlewares` + `createAuthedProjectAPIRoute` pattern

## Phase 4d: Dashboards and Alerting

Create Grafana dashboards for:

1. **Export Health**: `langfuse.blob_export.table_duration_ms` by table, alert on > 10min
2. **Export Lag**: time since last successful export per project, alert on > 1hr
3. **ClickHouse Resource Usage**: `system.metrics` for memory, threads, active queries
4. **Ingestion Throughput**: events/sec by queue, alert on sustained drops
5. **BullMQ Queue Depth**: jobs waiting by queue name, alert on growing backlog

### Alert Rules

| Metric | Threshold | Severity |
|---|---|---|
| Export table duration | > 10 min | Warning |
| Export table duration | > 30 min | Critical |
| Export lag (time since last success) | > 1 hour | Warning |
| ClickHouse memory usage | > 80% of limit | Warning |
| BullMQ blob storage queue depth | > 100 jobs | Warning |
| Web pod 5xx rate | > 5% for 5 min | Critical |
