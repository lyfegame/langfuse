# P1: ClickHouse Server-Side User Profiles

**Status**: Done (code routing) — server-side users.xml is deployment config
**Type**: Code change (Export service type routing) + deployment configuration (users.xml)
**Impact**: Hard memory cap prevents the #1 firefight trigger (132 GiB OOM)

## Problem

Per-query `clickhouseSettings` (P0) are client-side only. They're defense-in-depth but:
- Any query that misses the setting runs unlimited
- There's no server-enforced memory cap — a single query can OOM the entire instance
- The 132 GiB OOM that started the firefight would NOT have been prevented by client-side settings alone

## Solution

Create dedicated ClickHouse users with server-enforced resource profiles.

### users.xml Configuration

```xml
<clickhouse>
  <profiles>
    <!-- Interactive queries: default for web UI + ingestion -->
    <interactive>
      <max_threads>32</max_threads>
      <max_memory_usage>16000000000</max_memory_usage>  <!-- 16 GiB -->
      <max_execution_time>30</max_execution_time>       <!-- 30s timeout -->
      <priority>0</priority>                             <!-- highest priority -->
    </interactive>

    <!-- Export queries: bounded resources, lower priority -->
    <export>
      <max_threads>10</max_threads>
      <max_memory_usage>32000000000</max_memory_usage>  <!-- 32 GiB hard cap -->
      <max_execution_time>600</max_execution_time>      <!-- 10 min timeout -->
      <priority>2</priority>                             <!-- lower priority -->
    </export>

    <!-- Ingestion: write-focused, minimal read resources -->
    <ingestion>
      <max_threads>8</max_threads>
      <max_memory_usage>8000000000</max_memory_usage>   <!-- 8 GiB -->
      <max_execution_time>60</max_execution_time>
      <priority>1</priority>
    </ingestion>
  </profiles>

  <users>
    <interactive_user>
      <password_sha256_hex><!-- hash --></password_sha256_hex>
      <profile>interactive</profile>
      <networks><ip>::/0</ip></networks>
    </interactive_user>

    <export_user>
      <password_sha256_hex><!-- hash --></password_sha256_hex>
      <profile>export</profile>
      <networks><ip>::/0</ip></networks>
    </export_user>

    <ingestion_user>
      <password_sha256_hex><!-- hash --></password_sha256_hex>
      <profile>ingestion</profile>
      <networks><ip>::/0</ip></networks>
    </ingestion_user>
  </users>
</clickhouse>
```

### Code Changes Required

The Langfuse codebase already supports multiple ClickHouse URLs via `PreferredClickhouseService`:
- `ReadWrite` → `CLICKHOUSE_URL`
- `ReadOnly` → `CLICKHOUSE_READ_ONLY_URL`
- `EventsReadOnly` → `CLICKHOUSE_EVENTS_READ_ONLY_URL`

To use separate users per workload, add a new env var for the export connection:

```
CLICKHOUSE_EXPORT_URL=http://export_user:password@clickhouse:8123
```

And route export queries through it. This requires a small code change to the ClickHouse client to add an `Export` service type.

### Deployment

1. Mount `users.xml` to ClickHouse container/pod at `/etc/clickhouse-server/users.d/custom-users.xml`
2. Set env vars per worker pod:
   - Export pod: `CLICKHOUSE_URL` pointing to `export_user`
   - Ingestion pod: `CLICKHOUSE_URL` pointing to `ingestion_user`
   - Web pod: `CLICKHOUSE_URL` pointing to `interactive_user`

### Verification

1. Run export query manually via `clickhouse-client -u export_user` — confirm `max_memory_usage` is enforced
2. Attempt a query exceeding 32 GiB — should fail with `Memory limit exceeded` instead of OOM
3. Check `system.query_log` for user-level isolation: `SELECT user, max_memory_usage FROM system.query_log`

### Risk

- If the export user's 32 GiB cap is too low for large observation tables, exports will fail cleanly instead of OOM-ing the instance. This is the desired behavior — a failed export is better than a crashed ClickHouse.
- Adjust `max_memory_usage` based on actual export query profiling.
