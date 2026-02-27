# P3: ClickHouse Projections

**Status**: Not started
**Type**: ClickHouse migration
**Impact**: Speeds up common UI query patterns by providing pre-sorted indexes

## Problem

The base tables are ordered for ingestion (by `project_id`, `timestamp/start_time`, `id`). Common UI queries use different access patterns:
- Session view: `WHERE project_id = X AND session_id = Y ORDER BY timestamp`
- Trace detail waterfall: `WHERE project_id = X AND trace_id = Y ORDER BY start_time`
- Evaluation view: `WHERE project_id = X AND trace_id = Y ORDER BY timestamp`

Without projections, these queries do full scans within the project partition.

## Solution

Add ClickHouse projections as a migration. Projections are lightweight — they're automatically maintained by ClickHouse on insert and the query planner uses them transparently.

### Migration SQL

```sql
-- File: packages/shared/prisma/migrations/clickhouse/NNNN_add_projections.sql

-- Session-based trace lookups (session view)
ALTER TABLE traces ADD PROJECTION IF NOT EXISTS proj_traces_by_session (
    SELECT * ORDER BY (project_id, session_id, timestamp, id)
);
ALTER TABLE traces MATERIALIZE PROJECTION proj_traces_by_session;

-- Trace-based observation lookups (trace detail waterfall)
ALTER TABLE observations ADD PROJECTION IF NOT EXISTS proj_observations_by_trace (
    SELECT * ORDER BY (project_id, trace_id, start_time, id)
);
ALTER TABLE observations MATERIALIZE PROJECTION proj_observations_by_trace;

-- Trace-based score lookups (evaluation view)
ALTER TABLE scores ADD PROJECTION IF NOT EXISTS proj_scores_by_trace (
    SELECT * ORDER BY (project_id, trace_id, timestamp, id)
);
ALTER TABLE scores MATERIALIZE PROJECTION proj_scores_by_trace;
```

### No Code Changes Required

ClickHouse automatically selects projections when the query's `ORDER BY` or `WHERE` clause matches. No application code changes needed.

### Risk

- **Storage**: Each `SELECT *` projection roughly **doubles** table storage. For a 2 TiB PVC this means ~4 TiB needed.
- **Materialization**: `MATERIALIZE PROJECTION` rewrites existing data. This is CPU and I/O intensive — run during low-traffic windows.
- **Write amplification**: Each insert writes to both the base table and all projections. For 3 projections on 3 tables, this is ~2x write amplification total.

### Verification

1. Before/after: Run a representative session lookup query, compare `read_rows` and `elapsed` in `system.query_log`
2. Confirm projection usage: `SELECT query, projections FROM system.query_log WHERE projections != ''`
3. Monitor PVC: Confirm storage increase is within expected range (~2x per table with projection)

### Decision Point

Given the storage cost (~2x per table), consider whether projections are worth it for your workload. If UI query latency is acceptable without projections, skip this and focus on P1/P2 instead. Profile first with:

```sql
SELECT
  query,
  read_rows,
  memory_usage,
  query_duration_ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%session_id%'
ORDER BY event_time DESC
LIMIT 20;
```
