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

Three layers of isolation ensure workloads don't interfere:

| Layer | Mechanism | Status |
|---|---|---|
| **Process isolation** | `LANGFUSE_WORKER_ROLE` separates export/ingestion into dedicated pods | Done |
| **Query isolation** | `max_memory_usage`, `max_threads`, `priority` per export query | Done |
| **Query size bounding** | Adaptive window splitting — always splits on error, never gets stuck | Done |

## Plan Status

| Plan | Status | Rationale |
|---|---|---|
| **P0**: Worker isolation + query limits | **Done** | Worker roles, max_threads, priority, max_memory_usage, adaptive splitting |
| **P1**: ClickHouse server profiles | **Recommended (not code)** | Server-enforced user profiles add another isolation layer. Infrastructure config only. |
| **P2**: Export materialized view | **Dropped** | Doubles observation storage (~2+ TiB), adds write amplification, schema drift risk. `LIMIT 1 BY` + adaptive splitting already solves the problem. |
| **P3**: ClickHouse projections | **Dropped** | Doubles storage per table. Only worth it if UI latency is a proven problem — profile first. |
| **P4**: Observability | **Partially done, rest recommended** | Structured logs + OTel histograms done. Further metrics and dashboards are incremental. |
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

### Why P1 (Server Profiles) is recommended but not implemented

Server-side ClickHouse user profiles (users.xml) enforce resource limits at the ClickHouse server level, independent of client-side settings. This is valuable defense-in-depth:
- Any query that misses client-side settings still hits the server cap
- Server-enforced `max_memory_usage` prevents OOM even from ad-hoc queries

But this requires ClickHouse server configuration changes (mounting users.xml), not code changes in the Langfuse application.

## Reference

See `docs/` for the operational guide on deploying and operating the fork for all three workloads.
