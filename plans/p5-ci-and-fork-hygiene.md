# P5: CI Pipeline and Fork Hygiene

**Status**: Not started
**Type**: One-time setup
**Impact**: Prevents regressions, enables safe merges

## Current CI State

| Workflow | Status | Notes |
|---|---|---|
| `build-custom.yml` | Active | Builds Docker images on `v*-altera.*` tags |
| `ci.yml.template` | Inactive | Template from upstream, needs activation |
| `codeql.yml` | Active | Security analysis |
| `codespell.yml` | Active | Spelling checks |
| `licencecheck.yml` | Active | License compliance |

**Missing**: No automated build/lint/typecheck on PRs. A broken build can be merged without detection.

## Phase 5a: Activate CI Pipeline

Rename `ci.yml.template` to `ci.yml` with updates for the fork:

```yaml
name: CI

on:
  pull_request:
    branches: ["main"]
  push:
    branches: ["main"]

jobs:
  build-lint-typecheck:
    runs-on: ubuntu-latest
    env:
      DATABASE_URL: postgresql://postgres:postgres@localhost:5432/postgres
      DIRECT_URL: postgresql://postgres:postgres@localhost:5432/postgres
    steps:
      - uses: actions/checkout@v4

      - name: Start infrastructure
        run: docker compose -f "docker-compose.yml" up -d --build

      - uses: pnpm/action-setup@v4

      - uses: actions/setup-node@v4
        with:
          node-version-file: '.nvmrc'
          cache: 'pnpm'

      - run: pnpm install --frozen-lockfile

      - run: pnpm turbo db:generate

      - name: Build, lint, typecheck
        run: pnpm turbo build lint typecheck
        env:
          SKIP_ENV_VALIDATION: true
```

Key changes from the template:
- Target `main` branch (not `cloud`)
- Use `.nvmrc` for Node version (v24, not hardcoded v18)
- Use `pnpm/action-setup@v4` (not v2)
- Use `actions/cache` built into `setup-node` (not separate step)
- Add `--frozen-lockfile` to prevent lockfile changes in CI

## Phase 5b: Build Gate on Image Push

Update `build-custom.yml` to run build checks before pushing images:

```yaml
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version-file: '.nvmrc'
          cache: 'pnpm'
      - run: pnpm install --frozen-lockfile
      - run: pnpm turbo db:generate
      - run: pnpm turbo build typecheck
        env:
          SKIP_ENV_VALIDATION: true

  build-web:
    needs: validate
    # ... existing build-web job

  build-worker:
    needs: validate
    # ... existing build-worker job
```

## Phase 5c: Branch and Release Strategy

### Branch Naming

- `main` — stable fork branch, all PRs target this
- Feature branches: `shuyingl/<feature-name>`
- Tags: `v3.155.0-altera.N` (N increments per release)

### Release Process

1. PR merges to `main`
2. Create tag: `git tag v3.155.0-altera.N`
3. Push tag: `git push origin v3.155.0-altera.N`
4. `build-custom.yml` triggers → builds and pushes Docker images
5. Update K8s deployment to reference new image tag

### Upstream Sync

Periodically sync with upstream:
```bash
git fetch upstream
git merge upstream/main --no-edit
# Resolve conflicts, test, push
```

## Phase 5d: Commit Squash

The current 11+ commits on `main` include a commit+revert pair and iterative Parquet fixes. Before the next release, squash into clean semantic commits:

1. `fix: pin turbo@2.8.10 in worker Dockerfile`
2. `perf: lower web worker parsing threshold from 100KB to 10KB`
3. `feat: 15-min catch-up chunks + 30s retry backoff for blob exports`
4. `feat: replace FINAL with LIMIT 1 BY + native Parquet export`
5. `feat: worker role system, export resource limits, and observability`

This makes `git log` readable and `git bisect` useful.
