# Langfuse Fork Improvement Plans

Improvement plans for `lyfegame/langfuse` — the data aggregation backbone for bulk export, ingestion, and active queries.

## Documents

| Document | Scope |
|---|---|
| [overview.md](overview.md) | Context, firefight history, architecture, execution order |
| [p0-worker-isolation-and-query-limits.md](p0-worker-isolation-and-query-limits.md) | Worker role system + per-query ClickHouse resource limits |
| [p1-clickhouse-server-profiles.md](p1-clickhouse-server-profiles.md) | Server-side ClickHouse user profiles and memory caps |
| [p2-export-materialized-view.md](p2-export-materialized-view.md) | Pre-deduped MV for observations export |
| [p3-clickhouse-projections.md](p3-clickhouse-projections.md) | ClickHouse projections for common query patterns |
| [p4-observability-and-monitoring.md](p4-observability-and-monitoring.md) | Full structured logging, OTel metrics, dashboards, alerting |
| [p5-ci-and-fork-hygiene.md](p5-ci-and-fork-hygiene.md) | CI pipeline, branch strategy, release tagging |

## Priority Order

```
P0  Ship immediately    Worker isolation + query limits (PR #2)
P1  Deploy next         ClickHouse server profiles (deployment config, no code)
P2  Code next           Export MV (cuts export memory ~50%)
P3  Code after P2       Projections (speeds UI queries)
P4  Ongoing             Observability improvements
P5  One-time            CI pipeline + fork hygiene
```

## Guiding Principle

Isolate at every layer, observe before optimizing:

```
Layer 1: K8s pods         → LANGFUSE_WORKER_ROLE (P0)
Layer 2: CH queries       → max_threads + priority (P0)
Layer 3: CH server        → User profiles + memory caps (P1)
Layer 4: CH schema        → MVs + projections (P2, P3)
Layer 5: CH routing       → Export read replica (future)
```
