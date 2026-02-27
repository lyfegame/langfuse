# P2: Export-Optimized Materialized View

**Status**: Not started
**Type**: ClickHouse migration + code change
**Impact**: Cuts export memory ~50% by eliminating runtime dedup

## Problem

The current export path runs `LIMIT 1 BY id` on the observations table to deduplicate. For wide time windows this means ClickHouse must:
1. Scan all rows in the window
2. Sort/group by id
3. Keep only the latest version

This peaked at 132 GiB memory before the Parquet export fix. Even with the current chunked approach, large windows still consume significant memory because dedup happens at query time.

## Solution

Create a `ReplacingMergeTree` materialized view that pre-deduplicates observations. Export queries read from the MV instead of the base table, skipping runtime dedup entirely.

### Migration SQL

```sql
-- File: packages/shared/prisma/migrations/clickhouse/NNNN_export_observations_mv.sql

CREATE MATERIALIZED VIEW IF NOT EXISTS observations_export_mv
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (project_id, id)
PARTITION BY toYYYYMM(start_time)
AS SELECT
  id, project_id, trace_id, type, name, start_time, end_time,
  model, input, output, metadata, level, status_message,
  completion_start_time, model_parameters, usage_details, cost_details,
  updated_at, is_deleted
FROM observations;
```

### Code Change

In `packages/shared/src/server/repositories/observations.ts`, update `getObservationsForBlobStorageExportParquet()`:

```diff
- FROM observations
+ FROM observations_export_mv FINAL
```

Using `FINAL` on a `ReplacingMergeTree` is much cheaper than `LIMIT 1 BY` on the base `MergeTree` because:
- Fewer rows to scan (already partially merged)
- No sort/group operation needed
- Memory proportional to unique IDs, not total rows

### Backfill

The MV only captures new inserts after creation. To backfill existing data:

```sql
INSERT INTO observations_export_mv
SELECT
  id, project_id, trace_id, type, name, start_time, end_time,
  model, input, output, metadata, level, status_message,
  completion_start_time, model_parameters, usage_details, cost_details,
  updated_at, is_deleted
FROM observations;
```

Run during a maintenance window — this is a one-time bulk insert.

### Risk

- **Storage**: The MV roughly doubles observation storage. Monitor PVC usage after deployment.
- **Write amplification**: Each insert into `observations` triggers an insert into the MV. This adds ~5-10% write overhead.
- **Schema drift**: If columns are added to `observations`, the MV must be updated to match. Add a comment in the migration noting this dependency.

### Verification

1. Compare row counts: `SELECT count() FROM observations_export_mv` vs `SELECT count(DISTINCT id) FROM observations`
2. Compare export output: run export from both old and new paths, diff the Parquet files
3. Monitor `system.query_log` for export queries: memory usage should drop significantly
4. Monitor PVC usage for the expected storage increase
