# Operating the Langfuse Fork for Three Workloads

This guide covers deploying and operating `lyfegame/langfuse` to support three concurrent workloads without interference:

1. **Bulk Export** — ClickHouse to GCS/S3 Parquet for ML training pipelines
2. **Bulk Ingestion** — SDK data into ClickHouse (live user data + synthetic data)
3. **Active Queries** — Langfuse UI + programmatic API

## Architecture

```
┌─────────────────┐   ┌───────────────────┐   ┌───────────────────┐
│  Web (Next.js)  │   │ Worker: ingestion  │   │ Worker: export    │
│  UI + API       │   │ Evals, deletions,  │   │ Blob storage      │
│                 │   │ ingestion queues   │   │ export only       │
└────────┬────────┘   └─────────┬──────────┘   └─────────┬─────────┘
         │                      │                         │
         └──────────────────────┴─────────────────────────┘
                                │
                          ClickHouse
```

Four layers of isolation prevent workloads from interfering:

| Layer | What it does | How |
|---|---|---|
| **Process** | Separates export from ingestion at the OS level | `LANGFUSE_WORKER_ROLE` env var |
| **Query** | Limits each export query's ClickHouse resources | `max_memory_usage`, `max_threads`, `priority` (client-side) |
| **Query size** | Automatically shrinks queries that fail | Adaptive window splitting |
| **Service routing** | Routes exports to dedicated ClickHouse user with server-enforced limits | `CLICKHOUSE_EXPORT_URL` + `docs/clickhouse-users.xml` |

---

## Deployment

### Worker Roles

Deploy separate worker pods for export and ingestion using a single env var:

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

| Value | Queues processed |
|---|---|
| `all` (default) | Everything. Backward-compatible for single-worker deployments. |
| `ingestion` | All queues EXCEPT blob storage export. |
| `export` | Only blob storage export queues (`BlobStorageIntegrationQueue` + `BlobStorageIntegrationProcessingQueue`). |

Per-queue `QUEUE_CONSUMER_*_IS_ENABLED` flags remain as fine-grained overrides within each role.

### ClickHouse Export Service Routing

Export queries can be routed to a dedicated ClickHouse URL (typically a separate ClickHouse user with server-enforced resource profiles):

| Env var | Default | Effect |
|---|---|---|
| `CLICKHOUSE_EXPORT_URL` | (falls back to `CLICKHOUSE_URL`) | ClickHouse URL for export queries. Set to a URL with export-specific user credentials (e.g., `http://export_user:password@clickhouse:8123`) to enforce server-side resource limits. |

When set, all 4 export functions (traces, observations, scores, events) route through this URL. Without it, exports use the default `CLICKHOUSE_URL`.

To set up server-side ClickHouse user profiles, mount `docs/clickhouse-users.xml` into your ClickHouse container at `/etc/clickhouse-server/users.d/custom-users.xml`. This creates three users with server-enforced resource caps:

| User | Profile | max_memory_usage | priority | Use case |
|---|---|---|---|---|
| `interactive_user` | interactive | 16 GiB | 0 (highest) | Web UI + API queries |
| `export_user` | export | 32 GiB | 2 (lowest) | Blob storage export |
| `ingestion_user` | ingestion | 8 GiB | 1 | Data ingestion |

### ClickHouse Export Resource Limits

These env vars control client-side resource limits per export query. They apply regardless of whether `CLICKHOUSE_EXPORT_URL` is set:

| Env var | Default | Type | Effect |
|---|---|---|---|
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_MAX_MEMORY_USAGE` | `"20000000000"` (20 GiB) | string | Max memory per export query. ClickHouse kills the query (not the server) on breach. |
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_MAX_THREADS` | `10` | number | Max CPU threads per export query. |
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_PRIORITY` | `2` | number | Query priority (0 = highest). UI/ingestion queries at default priority (0) preempt exports. |
| `LANGFUSE_CLICKHOUSE_DATA_EXPORT_REQUEST_TIMEOUT_MS` | `600000` (10 min) | number | HTTP request timeout for export queries. |

When both client-side and server-side limits are set, the **stricter** limit wins (ClickHouse takes the minimum).

### Export Chunking

| Env var | Default | Effect |
|---|---|---|
| `LANGFUSE_BLOB_STORAGE_EXPORT_CATCHUP_INTERVAL_MS` | `900000` (15 min) | Chunk size when catching up. Smaller = less memory per query, more queries. |
| `LANGFUSE_BLOB_STORAGE_EXPORT_TRACE_ONLY_PROJECT_IDS` | (empty) | Comma-separated project IDs that only export traces (skip observations/scores/events). |

---

## How Adaptive Window Splitting Works

When an export query fails for ANY reason and the time window is larger than 60 seconds, the system automatically splits the window in half and retries each half:

```
15-min chunk fails (any error: memory limit, timeout, socket hang up, etc.)
  │
  ├─ Window > 60s? YES → Split into two 7.5-min chunks
  │   ├─ First half: succeeds → file uploaded
  │   └─ Second half: fails → split again into two 3.75-min chunks
  │       ├─ First quarter: succeeds
  │       └─ Second quarter: succeeds
  │
  └─ Window ≤ 60s? → Let BullMQ retry with exponential backoff (30s base, 5 attempts)
```

**Why split on ANY error (not just resource errors):** ClickHouse mid-stream errors for binary Parquet format often surface as "socket hang up" with no ClickHouse error text. Trying to detect the specific error type is fragile. Splitting on any error ensures the system never gets permanently stuck. The worst case is extra smaller files, which are harmless — each half covers a disjoint time range and deterministic filenames mean retries overwrite.

**What gets logged:** Every split emits a structured warning:
```json
{
  "message": "blob_export_error_splitting",
  "table": "observations",
  "projectId": "abc123",
  "errorType": "MEMORY_LIMIT",
  "errorMessage": "Failed to upload file to S3: memory limit exceeded...",
  "originalWindowMs": 900000,
  "splitWindowMs": 450000
}
```

The `errorType` is classified as `MEMORY_LIMIT`, `OVERCOMMIT`, `TIMEOUT`, or `unknown` for observability. An OTel histogram `langfuse.blob_export.split_count` is recorded with `table` and `errorType` labels.

---

## Tuning Guide

### max_memory_usage

The most important setting. It determines how much ClickHouse memory a single export query can use before being killed.

| Scenario | Recommended value | Notes |
|---|---|---|
| Small instance (32 GiB RAM) | `"8000000000"` (8 GiB) | Leaves headroom for ingestion + UI |
| Medium instance (64-96 GiB) | `"16000000000"` (16 GiB) | Good balance |
| Large instance (128+ GiB) | `"20000000000"` (20 GiB, default) | Conservative cap |
| Testing adaptive splitting | `"1000000000"` (1 GiB) | Artificially low to verify splits work |

**If exports are splitting frequently:** Increase `max_memory_usage`. Frequent splits mean the cap is too low for your data density.

**If ClickHouse is OOMing:** Decrease `max_memory_usage`. The adaptive splitting will handle the resulting failures by using smaller windows.

### Catch-up chunk interval

Controls how much data each export query processes when catching up (behind by more than one export period).

| Scenario | Recommended value | Notes |
|---|---|---|
| Dense data (large observations) | `300000` (5 min) | Smaller chunks, less memory |
| Normal data | `900000` (15 min, default) | Good balance |
| Sparse data | `1800000` (30 min) | Fewer queries, faster catch-up |

The adaptive splitting provides a safety net: even if the chunk is too large, it will split automatically.

### max_threads and priority

`max_threads: 10` is usually correct. It caps CPU usage per export query, leaving threads for UI/ingestion queries.

`priority: 2` means ClickHouse processes UI queries (priority 0) and ingestion queries (priority 0-1) first. Export queries run when there's spare capacity.

---

## Monitoring

### Key logs to watch

| Log message | Meaning | Action |
|---|---|---|
| `blob_export_error_splitting` | Export chunk failed, splitting into smaller windows | Normal if occasional. Investigate if constant for one table. |
| `blob_export_job_completed` | Full export job completed for a project | Healthy operation. Check `durationMs` and `tablesExported`. |
| `[BLOB INTEGRATION] Error exporting` | Export failed at minimum window (60s), can't split further | BullMQ will retry. If persistent, the data in that time range may need investigation. |

### OTel metrics

| Metric | Labels | Meaning |
|---|---|---|
| `langfuse.blob_export.table_duration_ms` | `table` | How long each table export takes |
| `langfuse.blob_export.split_count` | `table`, `errorType` | How often adaptive splitting triggers |

### ClickHouse query_log

Verify export queries are using the correct settings:

```sql
SELECT
  query_id,
  Settings['max_memory_usage'] AS max_mem,
  Settings['max_threads'] AS max_thr,
  Settings['priority'] AS prio,
  memory_usage,
  query_duration_ms
FROM system.query_log
WHERE log_comment LIKE '%blobstorage%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 10;
```

---

## Troubleshooting

### Export is stuck on the same time window

**Symptom:** Logs show the same `minTimestamp`/`maxTimestamp` window being retried every hour.

**Root cause:** Every chunk smaller than 60s still fails. The data in that time range is too large or has a structural issue.

**Fix:**
1. Check which table and time range: look for `[BLOB INTEGRATION] Error exporting` logs
2. Query ClickHouse directly for that time range to understand the data:
   ```sql
   SELECT count(), sum(length(input) + length(output)) AS total_bytes
   FROM observations
   WHERE project_id = 'xxx'
     AND start_time >= '2026-02-25 12:00:00'
     AND start_time <= '2026-02-25 12:01:00';
   ```
3. If data is genuinely too large for a 60s window, increase `max_memory_usage`
4. If data is reasonable, the issue may be transient (network, ClickHouse restart). Wait for BullMQ retry.

### ClickHouse OOM despite max_memory_usage

**Symptom:** ClickHouse process crashes with OOM killer.

**Root cause:** `max_memory_usage` only limits individual queries. If multiple queries run concurrently (export + UI + ingestion), their combined memory can exceed server RAM.

**Fix:**
1. Lower `max_memory_usage` for exports
2. Ensure only 1 export query runs at a time (BullMQ concurrency is already 1)
3. Consider ClickHouse server-side user profiles (see `plans/p1-clickhouse-server-profiles.md`) for a hard server-level cap

### Export produces duplicate files after splitting

**Symptom:** GCS/S3 has multiple files for overlapping time ranges.

**This is expected and harmless.** When a chunk splits:
- Each half produces its own file with `maxTimestamp` in the filename
- If a retry re-exports a half that already succeeded, the file is overwritten (same filename)
- Downstream consumers should read all files in a project/table directory — smaller disjoint files are equivalent to one larger file

### Ingestion is slow despite worker isolation

**Symptom:** Ingestion latency increases even with separate worker pods.

**Root cause:** Export and ingestion share the same ClickHouse instance. Heavy export queries can starve ClickHouse resources.

**Fix:**
1. Verify `priority: 2` is set on export queries (check `system.query_log`)
2. Lower `max_threads` for exports (default 10, try 4-6)
3. Lower `max_memory_usage` to force smaller queries
4. For complete isolation, consider a ClickHouse read replica for exports (future)

---

## Future Improvements (Recommended)

These are not implemented but recommended based on operational experience:

### ClickHouse Read Replica for Exports

Route export queries to a dedicated ClickHouse replica. This provides complete resource isolation at the database level — export can never affect UI or ingestion performance. This is the ultimate isolation but requires infrastructure changes (replica setup, replication lag handling). The `CLICKHOUSE_EXPORT_URL` routing is already in place, so this would only require pointing it to the replica.

### Export Progress API

A REST endpoint for programmatic export status monitoring (`lastExportAt`, `catchupLagMinutes`, `lastDurationMs`). Currently, operators can query `blob_storage_file_log` and BullMQ state directly. See `plans/p4-observability-and-monitoring.md` for the API design.
