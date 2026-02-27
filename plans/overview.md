# Overview: Langfuse Fork Improvements

## Context

The Langfuse fork (`lyfegame/langfuse`) serves as the data aggregation backbone for three workloads:

1. **Bulk Export** — Extract trace/observation/score data to GCS Parquet for ML training pipelines
2. **Bulk Ingestion** — Intake live user data + synthetic data via Langfuse SDK
3. **Active Queries** — Langfuse UI + programmatic API for understanding agent runs

These workloads compete for shared ClickHouse resources. Without isolation, export starves ingestion, which cascades into web pod 502s.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Web (Next.js)│     │ Worker: ingestion │     │ Worker: export   │
│  UI queries   │     │ BullMQ jobs      │     │ Blob storage only│
│  tRPC/API     │     │ Evals, deletions │     │                  │
└──────┬────────┘     └────────┬─────────┘     └────────┬─────────┘
       │                       │                        │
       └───────────────────────┴────────────────────────┘
                               │
                         ClickHouse
```

Four layers of isolation ensure workloads don't interfere:

| Layer | Mechanism | Status |
|---|---|---|
| **Process isolation** | `LANGFUSE_WORKER_ROLE` separates export/ingestion into dedicated pods | Done |
| **Query isolation** | `max_memory_usage`, `max_threads`, `priority` per export query | Done |
| **Query size bounding** | Adaptive window splitting — always splits on error, never gets stuck | Done |
| **Service routing** | `CLICKHOUSE_EXPORT_URL` routes exports to a dedicated ClickHouse user with server-enforced profiles | Done |

## Plan Status

| Plan | Status | Rationale |
|---|---|---|
| **P0**: Worker isolation + query limits | **Done** | Worker roles, max_threads, priority, max_memory_usage, adaptive splitting |
| **P1**: ClickHouse server profiles | **Done** | Export service routing + users.xml template. Deploy users.xml to enforce server-side limits. |
| **P2**: Export materialized view | **Dropped** | Doubles observation storage (~2+ TiB), adds write amplification, schema drift risk. `LIMIT 1 BY` + adaptive splitting already solves the problem. |
| **P3**: ClickHouse projections | **Dropped** | Doubles storage per table. Only worth it if UI latency is a proven problem — profile first. |
| **P4**: Observability | **Done** | All logs structured, OTel histograms for export timing. Dashboards/alerting are ops tasks. |
| **P5**: CI pipeline + fork hygiene | **Done** | CI activated on main. |

## Key Decisions

### Why P2 (Export MV) was dropped

The MV was designed to avoid `LIMIT 1 BY` dedup at query time, reducing export memory by ~50%. However:
- `LIMIT 1 BY` is already proven and working
- Adaptive splitting handles oversized chunks automatically
- MV doubles observation storage (already at 2 TiB PVC)
- Write amplification for every insert
- Schema drift: MV must be kept in sync with base table on every column change

The cost far outweighs the benefit now that adaptive splitting ensures forward progress.

### Why P3 (Projections) was dropped

Projections pre-sort data for UI query patterns (session view, trace detail). However:
- Each `SELECT *` projection roughly doubles table storage
- Write amplification for every insert (~2x total with 3 projections)
- No evidence UI query latency is a problem yet

**Recommendation:** Profile UI queries first. If latency is a problem, add projections for the specific patterns that need it.

### P1 (Server Profiles) — what's done vs. what's deployment config

The code changes are done: `"Export"` service type in `PreferredClickhouseService`, `CLICKHOUSE_EXPORT_URL` env var, routing in `getClickhouseUrl()`, and all 4 export functions pass `preferredClickhouseService: "Export"`.

What remains is deployment config: mount `docs/clickhouse-users.xml` into ClickHouse and set `CLICKHOUSE_EXPORT_URL` pointing to the export user. This is defense-in-depth — server-enforced `max_memory_usage` prevents OOM even from ad-hoc queries.

## Reference

See `docs/` for the operational guide on deploying and operating the fork for all three workloads.
