# Langfuse Fork Improvement Plans

Improvement plans for `lyfegame/langfuse` — the data aggregation backbone for bulk export, ingestion, and active queries.

## Documents

| Document | Status | Scope |
|---|---|---|
| [overview.md](overview.md) | Current | Context, architecture, plan status, key decisions |
| [p0-worker-isolation-and-query-limits.md](p0-worker-isolation-and-query-limits.md) | **Done** | Worker role system + per-query ClickHouse resource limits |
| [p1-clickhouse-server-profiles.md](p1-clickhouse-server-profiles.md) | **Done** | Export service routing + users.xml template for server-side profiles |
| [p4-observability-and-monitoring.md](p4-observability-and-monitoring.md) | **Done** | All logs structured, OTel histograms, dashboards/alerting are ops |
| [p5-ci-and-fork-hygiene.md](p5-ci-and-fork-hygiene.md) | **Done** | CI pipeline, branch strategy, release tagging |

## What's Done

The fork has four layers of workload isolation:

1. **Process isolation** — `LANGFUSE_WORKER_ROLE` env var separates export and ingestion into dedicated worker pods
2. **ClickHouse query isolation** — `max_memory_usage`, `max_threads`, `priority` per export query
3. **Adaptive query sizing** — On any export error, automatically splits the time window and retries with smaller chunks. Never gets permanently stuck.
4. **ClickHouse service routing** — `CLICKHOUSE_EXPORT_URL` routes export queries to a dedicated ClickHouse user with server-enforced resource profiles (`docs/clickhouse-users.xml`)

Plus full structured logging across the entire blob export pipeline for machine-parseable observability.

See `../docs/` for the operational guide.
