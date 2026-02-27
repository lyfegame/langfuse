# Overview: Langfuse Fork Improvements

## Context

The Langfuse fork (`lyfegame/langfuse`) serves as the data aggregation backbone for three workloads:

1. **Bulk Export** — Extract trace/observation/score data to GCS Parquet for ML training pipelines
2. **Bulk Ingestion** — Intake live user data + synthetic data via Langfuse SDK
3. **Active Queries** — Langfuse UI + programmatic API for understanding agent runs

These workloads compete for shared ClickHouse resources. Without isolation, export starves ingestion, which cascades into web pod 502s.

## Firefight History (Feb 25-26, 2026)

~40 reactive commits across 2 days:

| Area | Iterations | What happened |
|---|---|---|
| ClickHouse memory | 6 | 12Gi → 200Gi (broke Autopilot!) → 64Gi → 96Gi → 160Gi |
| Web pods | 3 | Crash-loop 502s → CPU bumps → probe tuning |
| Export | 5 | OOM at 132 GiB → fork with LIMIT 1 BY + Parquet → row group limits |
| KEDA | 2 | Workers killed mid-export → cooldown 5min → 30min |
| PVC | 1 | 1TiB → 2TiB after disk-full |
| Data loss | 1 | ClickHouse migration never executed during GKE cutover → 4 days lost |

**Root cause**: All three workloads share a single ClickHouse instance with no resource isolation at any layer.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  Langfuse   │     │   Worker     │     │    ClickHouse       │
│  Web (UI)   │────▶│  (BullMQ)    │────▶│  (single instance)  │
│  + API      │     │              │     │                     │
└──────┬──────┘     └──────┬───────┘     └─────────────────────┘
       │                   │                       ▲
       │            ┌──────┴───────┐               │
       │            │  Export pods │───────────────┘
       │            │  (KEDA)     │  ← Full table scans
       │            └─────────────┘
       │
       └──── Active queries (interactive, latency-sensitive)
```

The cascading failure:
1. Export query scans entire table → eats all ClickHouse threads + memory
2. Ingestion async inserts queue up → BullMQ backs up
3. Web queries timeout → 502s
4. KEDA kills export worker mid-query → wasted work → retry → repeat

## Workload Profiles

| Workload | Pattern | Latency | CH pressure | Isolation need |
|---|---|---|---|---|
| Bulk Export | Sequential full-table scans | Hours OK | Very high (memory + CPU) | Must be capped |
| Ingestion | High-throughput async inserts | Seconds | Medium (write buffering) | Must not be starved |
| Active Queries | Interactive filtered reads | < 2s | Low-medium (indexed) | Must stay responsive |

## Fork State

The fork (`lyfegame/langfuse`, branch `main`) contains:
- 7 modified files, +318/-66 lines from upstream
- Fixes for export OOM (LIMIT 1 BY, Parquet stream, row group limits)
- Worker role system and query resource limits (PR #2)

## Execution Order

```
P0  Worker isolation + query limits          ← PR #2 (immediate)
P1  ClickHouse server profiles               ← Deployment config (next deploy)
P2  Export materialized view                  ← Code change (reduces export memory 50%)
P3  ClickHouse projections                   ← Migration (speeds UI queries)
P4  Observability improvements               ← Ongoing
P5  CI pipeline + fork hygiene               ← One-time setup
```

Observability (P4) is listed after schema changes but should be done incrementally alongside each phase to validate impact.
