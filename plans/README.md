# Langfuse Fork Improvement Plans

Improvement plans for `lyfegame/langfuse` — the data aggregation backbone for bulk export, ingestion, and active queries.

## Documents

| Document | Status | Scope |
|---|---|---|
| [overview.md](overview.md) | Current | Context, architecture, plan status, key decisions |
| [p0-worker-isolation-and-query-limits.md](p0-worker-isolation-and-query-limits.md) | **Done** | Worker role system + per-query ClickHouse resource limits |
| [p1-clickhouse-server-profiles.md](p1-clickhouse-server-profiles.md) | Recommended | Server-side ClickHouse user profiles and memory caps |
| [p2-export-materialized-view.md](p2-export-materialized-view.md) | **Dropped** | Pre-deduped MV for observations export |
| [p3-clickhouse-projections.md](p3-clickhouse-projections.md) | **Dropped** | ClickHouse projections for common query patterns |
| [p4-observability-and-monitoring.md](p4-observability-and-monitoring.md) | Partial | Full structured logging, OTel metrics, dashboards, alerting |
| [p5-ci-and-fork-hygiene.md](p5-ci-and-fork-hygiene.md) | **Done** | CI pipeline, branch strategy, release tagging |

## What's Done

The fork now has three layers of workload isolation:

1. **Process isolation** — `LANGFUSE_WORKER_ROLE` env var separates export and ingestion into dedicated worker pods
2. **ClickHouse query isolation** — `max_memory_usage`, `max_threads`, `priority` per export query
3. **Adaptive query sizing** — On any export error, automatically splits the time window and retries with smaller chunks. Never gets permanently stuck.

See `../docs/` for the operational guide.
