# MotherDuck Business — Operationalization Reference

**Project:** THYROID_2026  
**Date:** 2026-03-14  
**Scope:** Everything required for a production-grade MotherDuck Business workflow —
environment separation, service-account auth, performance regression, dashboard
hardening, and CI strengthening.

---

## Table of Contents

1. [Environment Architecture](#1-environment-architecture)
2. [Service-Account Authentication](#2-service-account-authentication)
3. [Dev → QA → Prod Promotion Model](#3-dev--qa--prod-promotion-model)
4. [Query Benchmark Suite](#4-query-benchmark-suite)
5. [Dashboard Smoke Tests](#5-dashboard-smoke-tests)
6. [RO Share Publication Check](#6-ro-share-publication-check)
7. [CI Job Graph](#7-ci-job-graph)
8. [Remaining Manual Steps](#8-remaining-manual-steps)
9. [Quick-Reference Command Table](#9-quick-reference-command-table)

---

## 1. Environment Architecture

Three MotherDuck databases map to the standard dev/qa/prod tiers.

| Env  | Database                         | Duckling tier¹ | Write allowed |
|------|----------------------------------|----------------|---------------|
| dev  | `thyroid_research_2026_dev`      | Standard       | ✓             |
| qa   | `thyroid_research_2026_qa`       | Standard       | ✓             |
| prod | `thyroid_research_2026`          | Pulse          | CI/SA only    |

¹ Adjust duckling tiers in `config/motherduck_environments.yml` to match your
  MotherDuck Business plan limits.

**RO share** (prod read-only surface, used by Streamlit dashboard):

```
md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c
```

The dashboard connects via the share first (`thyroid_share` catalog alias), falls
back to the RW prod database, then falls back to local DuckDB.

**Configuration file:** `config/motherduck_environments.yml`

Each environment block records the database name, write permission flag, recommended
duckling size, and (for prod) the share path.  `motherduck_client.resolve_database_for_env(env)`
loads this file at runtime so scripts never hard-code database names.

---

## 2. Service-Account Authentication

### Token Hierarchy

| Priority | Variable / Source           | Intended consumer                  |
|----------|-----------------------------|------------------------------------|
| 1        | `MD_SA_TOKEN` env var       | CI jobs, automation scripts        |
| 2        | `MOTHERDUCK_TOKEN` env var  | Interactive developer sessions     |
| 3        | `.streamlit/secrets.toml`   | Streamlit Cloud deployment         |

Resolution is performed by `motherduck_client.get_token(prefer_service_account=False)`.
Pass `prefer_service_account=True` (or `--sa` CLI flag) to prefer `MD_SA_TOKEN`.

### Creating a Service Account Token

1. Log in to [app.motherduck.com](https://app.motherduck.com) with the team account.
2. Navigate to **Settings → Service Tokens**.
3. Create a token with **read-only** access for the RO share checks and
   **read-write** access for the prod database (promotion gate only).
4. Store as `MD_SA_TOKEN` in GitHub Actions secrets.

### Using `MotherDuckClient.for_env()`

```python
from motherduck_client import MotherDuckClient

# Interactive dev session — uses MOTHERDUCK_TOKEN
con = MotherDuckClient.for_env("dev").connect()

# CI automation — uses MD_SA_TOKEN
con = MotherDuckClient.for_env("prod", use_service_account=True).connect()
```

The `for_env()` factory reads `config/motherduck_environments.yml` automatically,
so callers do not need to know database names.

---

## 3. Dev → QA → Prod Promotion Model

### Promotion Gate: `scripts/91_promotion_gate.py`

A single script enforces 6 gates before allowing promotion.

| Gate | Check                                         | Prod-only |
|------|-----------------------------------------------|-----------|
| G1   | 14 critical tables exist                      | No        |
| G2   | 8 canonical metric bounds (row-count ranges)  | No        |
| G3   | No row multiplication (uniqueness on 3 tables)| No        |
| G4   | Core column null-rate ceilings                | No        |
| G5   | `val_*` validation tables have 0 FAIL rows    | **Yes**   |
| G6   | RO share accessible and >10 000 patients       | **Yes**   |

**Usage:**

```bash
# Promote dev → qa (G5 + G6 skipped)
.venv/bin/python scripts/91_promotion_gate.py --from dev --to qa

# Promote qa → prod (all 6 gates enforced)
.venv/bin/python scripts/91_promotion_gate.py --from qa --to prod --sa

# Dry-run (prints gates, makes no changes)
.venv/bin/python scripts/91_promotion_gate.py --from qa --to prod --dry-run
```

**Exit codes:**

| Code | Meaning                        |
|------|--------------------------------|
| 0    | All gates PASS — safe to promote |
| 1    | One or more gates FAIL — blocked |
| 2    | Connectivity error              |

### Promotion Workflow

```
[local dev] → git push → CI lint+unit+motherduck-ci
                                  ↓ PASS
              git tag v2026.XX.YY-release
                                  ↓
              CI: promotion-gate (qa → prod)
              CI: share-publication-check
                                  ↓ PASS
              Merge and announce release
```

### Rollback

MotherDuck does not have automatic point-in-time restore at the table level.
For rollback: re-run `scripts/26_motherduck_materialize_v2.py --md` against a
previous git commit to rebuild all 131 materialized tables from source views.
The rebuild is idempotent (`CREATE OR REPLACE TABLE`).

---

## 4. Query Benchmark Suite

### Script: `scripts/92_query_benchmark.py`

19 benchmark queries in two tiers:

| Tier   | SLA        | Queries | Source                              |
|--------|------------|---------|-------------------------------------|
| `hot`  | 500 ms     | 10      | Materialized `md_*` tables          |
| `warm` | 5 000 ms   | 9       | Views, multi-join, GROUP BY         |

**How it works:**

- Runs each query 3× and takes the **median** wall-clock time.
- Compares median against `benchmark_baseline.csv` (stored in `exports/md_benchmark_20260314/`).
- Flags any query whose median exceeds **2× baseline** as a regression.
- Saves timestamped CSV results to `exports/md_benchmark_<date>/benchmark_results_<ts>.csv`.

**First-time setup (populate baseline):**

```bash
.venv/bin/python scripts/92_query_benchmark.py --md --update-baseline
```

Commit the resulting `benchmark_baseline.csv`.

**After a planned performance shift** (e.g. duckling upgrade, index change):

```bash
.venv/bin/python scripts/92_query_benchmark.py --md --update-baseline
# commit the new baseline
```

**In CI:** `benchmark-regression` job runs on every push that reaches `motherduck-ci`.
Regressions are **informational** (non-blocking) by default.  Set
`REGRESSION_EXIT_CODE = 1` at the top of `92_query_benchmark.py` to make them blocking.

---

## 5. Dashboard Smoke Tests

### Script: `scripts/93_dashboard_smoke.py`

25 smoke queries covering all 6 dashboard workflow sections + 4 column-presence
spot-checks on critical tables.

| Section                 | Queries |
|-------------------------|---------|
| Overview                | 5       |
| Patient Explorer        | 4       |
| Data Quality            | 4       |
| Linkage & Episodes      | 5       |
| Outcomes & Analytics    | 5       |
| Manuscript & Export     | 2       |

Column-presence checks: `manuscript_cohort_v1`, `thyroid_scoring_py_v1`,
`survival_cohort_enriched`, `patient_analysis_resolved_v1`.

**Modes:**

```bash
# Test via RO share (same path as deployed Streamlit Cloud)
.venv/bin/python scripts/93_dashboard_smoke.py --share

# Test via RW prod database
.venv/bin/python scripts/93_dashboard_smoke.py --md

# Test a specific environment
.venv/bin/python scripts/93_dashboard_smoke.py --env qa --md
```

**When to run:**

- Before deploying a new version to Streamlit Cloud.
- After `scripts/26_motherduck_materialize_v2.py --md` finishes a full refresh.
- As part of the pre-release checklist (see runbook).

---

## 6. RO Share Publication Check

### Script: `scripts/94_share_publication_check.py`

5 check families that guard the RO share before tagging a release:

| Check | Description                                           |
|-------|-------------------------------------------------------|
| C1    | Connectivity — share path resolves and returns rows   |
| C2    | 18 required tables exist with minimum row counts      |
| C3    | 11 PHI column names are absent from all share tables  |
| C4    | `thyroid_share` catalog alias resolves                |
| C5    | Row counts for 5 tables match RW prod (≤ 2% drift)    |

**Usage:**

```bash
# Full check (C1–C5): needs both share access and RW prod token
.venv/bin/python scripts/94_share_publication_check.py

# Fast check (C1–C4 only): suitable for CI where RW prod token may not exist
.venv/bin/python scripts/94_share_publication_check.py --no-count-check

# Service-account mode
.venv/bin/python scripts/94_share_publication_check.py --sa --no-count-check
```

**When all checks PASS**, the script prints a post-pass publication checklist:

```
  ✓ Tag the commit: git tag v2026.XX.YY-release
  ✓ Update RELEASE_NOTES.md
  ✓ Restart Streamlit Cloud app (force cache flush)
  ✓ Verify dashboard Overview tab shows correct patient count
```

---

## 7. CI Job Graph

```
lint-and-syntax
├── unit-tests
└── motherduck-ci (on: MOTHERDUCK secret present)
        ├── benchmark-regression           (on: every push, informational)
        ├── share-publication-check        (on: v* tags or manual dispatch)
        │                                      requires MD_SA_TOKEN
        └── promotion-gate (qa→prod)       (on: v* tags)
                needs: [motherduck-ci, share-publication-check]
                requires: MD_SA_TOKEN
```

### Required GitHub Secrets

| Secret             | Used by                          | Notes                              |
|--------------------|----------------------------------|------------------------------------|
| `MOTHERDUCK`       | `motherduck-ci`, `benchmark-regression` | Personal or team token       |
| `MD_SA_TOKEN`      | `share-publication-check`, `promotion-gate` | Service-account token     |

---

## 8. Remaining Manual Steps

The following items cannot be automated via git commit and require manual action:

1. **Add `MD_SA_TOKEN` to GitHub Actions secrets**  
   Repository → Settings → Secrets and variables → Actions → New repository secret.

2. **Populate the benchmark baseline**  
   After the first successful run with a live MotherDuck connection:  
   ```bash
   .venv/bin/python scripts/92_query_benchmark.py --md --update-baseline
   git add exports/md_benchmark_20260314/benchmark_baseline.csv
   git commit -m "chore: populate initial benchmark baseline"
   git push
   ```

3. **Verify PyYAML is in `requirements.txt`**  
   `motherduck_client.py` now imports `yaml` (for `config/motherduck_environments.yml`).
   Confirm `PyYAML` (or `pyyaml`) is listed.

4. **Create dev and qa databases in MotherDuck**  
   ```sql
   -- run in MotherDuck console or via .venv/bin/python -c "..."
   CREATE DATABASE IF NOT EXISTS thyroid_research_2026_dev;
   CREATE DATABASE IF NOT EXISTS thyroid_research_2026_qa;
   ```

5. **Decide on benchmark regression blocking**  
   Default behaviour is informational (non-blocking).  To block CI on regressions,
   edit the top of `scripts/92_query_benchmark.py`:
   ```python
   REGRESSION_EXIT_CODE = 1   # change from 0 to 1
   ```

---

## 9. Quick-Reference Command Table

| Task                          | Command                                                       |
|-------------------------------|---------------------------------------------------------------|
| Connect to dev                | `MotherDuckClient.for_env("dev").connect()`                   |
| Connect to prod (SA)          | `MotherDuckClient.for_env("prod", use_service_account=True).connect()` |
| Run promotion gate            | `python scripts/91_promotion_gate.py --from qa --to prod --sa` |
| Run benchmark suite           | `python scripts/92_query_benchmark.py --md`                   |
| Update benchmark baseline     | `python scripts/92_query_benchmark.py --md --update-baseline` |
| Run dashboard smoke (share)   | `python scripts/93_dashboard_smoke.py --share`                |
| Run share publication check   | `python scripts/94_share_publication_check.py --sa --no-count-check` |
| Full materialization refresh  | `python scripts/26_motherduck_materialize_v2.py --md`         |
| Full CI locally               | `act push` (requires [act](https://github.com/nektos/act))    |
