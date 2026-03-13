# Streamlit Dashboard — Final Polish Pass

**Date:** 2026-03-13
**Version:** v3.3.0-2026.03.13

## Changes

### 1. Runtime Status Panel

- **New sidebar expander** (`🖥️ Runtime Status`): shows connection mode, catalog, version, timestamp, and review mode status at a glance
- **Compact status banner** at top of main area replaces the old `st.info` box — shows version, colored mode badge (RO SHARE / RW FALLBACK / LOCAL), catalog, and connection time
- **Footer** now includes connection mode alongside version and catalog
- **Diagnostics tab** leads with runtime status panel and system health KPI cards before the view inventory

### 2. Fallback Transparency

- `_get_con()` now stores connection metadata in `_CONNECTION_META` dict (mode, detail message, timestamp)
- All three connection paths tracked: `ro_share`, `rw_fallback`, `local`
- **Prominent warning banner** shown above all content when RO share fails and app falls back to RW — includes link to Connection Help
- **Error banner** for local-only mode (MotherDuck unreachable)
- Session state populated for all connection metadata so child modules can inspect runtime mode
- **Connection Help expander** enhanced with:
  - Current connection status display
  - Troubleshooting table (symptoms → causes → fixes)
  - Sign-in/public-sharing diagnostic guidance for Streamlit Cloud deployments

### 3. Health Tables Integration

- **Diagnostics tab** now has a dedicated "Health & Validation Tables" section showing all 14 `val_*` monitoring tables with existence check and row counts
- **System Health KPI cards** show availability of Integrity, Provenance, Linkage, and Lab Coverage tables
- Lowest provenance fill rates (bottom 5 tables) shown when `val_provenance_completeness_v2` is available
- **Data Build Info** expanded with `manuscript_cohort_v1` and `val_dataset_integrity_summary_v1` checks
- QA Workbench continues to provide the actionable, drill-down view of each health domain

### 4. Query-Cost / Responsiveness Improvements

**Audit findings:**

| Query Pattern | Approach | Status |
|---|---|---|
| Main cohort load | `advanced_features_sorted` (materialized, pre-sorted) | Already optimized |
| Sidebar filters | Cached via `@st.cache_data(ttl=300)` | Already optimized |
| Tab-level queries | `cached_sqdf()` with 1h TTL | Already optimized |
| `tbl_exists()` checks | `information_schema.tables` + 5min cache | Lightweight |
| Health table reads | Small tables (5-50 rows), no optimization needed | OK |
| `streamlit_patient_timeline_v` | Materialized table on MotherDuck | Already optimized |

**No remaining slow views for the app-critical path.** All large scans go through materialized tables. The only potentially slow operation is the initial `advanced_features_sorted` load (~16k rows), which is cached for 1 hour after first load.

**Documented limitations:**
- Health tables (`val_*`) require script 75/77/78/80 deployment to populate
- `imaging_fna_linkage_v3` has 0 rows (source-limited, not a query issue)
- Overview tab `date_rescue_rate_summary` query is fast (single-row summary table)

### 5. Smoke Tests

New file: `scripts/smoke_test_dashboard.py`

**Test coverage:**
1. Token availability (env var or `.streamlit/secrets.toml`)
2. RO share connection (or local DuckDB with `USE_LOCAL_DUCKDB=1`)
3. 10 critical tables (existence + row count)
4. 5 health/validation tables (existence + non-empty, non-critical)
5. Patient timeline query (research_id=1)
6. Overview patient count (>10k assertion)
7. Advanced features load
8. Manuscript cohort (>10k assertion)
9. Scoring systems (non-critical)

**Usage:**
```bash
# Full smoke test against MotherDuck:
.venv/bin/python scripts/smoke_test_dashboard.py

# Quiet mode (exit code only, for CI):
.venv/bin/python scripts/smoke_test_dashboard.py --quiet

# Against local DuckDB:
USE_LOCAL_DUCKDB=1 .venv/bin/python scripts/smoke_test_dashboard.py
```

**Exit codes:** 0 = all pass, 1 = critical failure, 2 = warnings only (non-critical failures)

### 6. Read-Only Safeguards

- Default connection mode is RO share — no writes possible
- Review Mode toggle is explicit (sidebar, off by default)
- RW fallback banner makes write-capability visible
- `write_decision()` only accessible when Review Mode is toggled on
- `_get_con()` cached connection is always the RO path; RW connection is established separately in Review Mode

## Files Changed

| File | Change |
|---|---|
| `dashboard.py` | Connection metadata, status banner, fallback warnings, sidebar runtime expander, footer, connection help |
| `app/helpers.py` | `get_runtime_info()`, `render_runtime_status_panel()`, `render_fallback_warning()`, `render_health_kpis()` |
| `app/diagnostics.py` | Health table inventory, runtime status panel, system health KPIs, provenance summary |
| `scripts/smoke_test_dashboard.py` | New smoke test script |
| `docs/streamlit_final_polish_20260313.md` | This document |

## Acceptance Criteria

- [x] App makes its runtime mode obvious (status banner, sidebar, footer)
- [x] No silent fallback ambiguity (prominent warning on RO→RW fallback)
- [x] Health tables surfaced in Diagnostics tab
- [x] Query paths use materialized tables (audited, documented)
- [x] Smoke test verifiable from CLI
- [x] Demo-readiness improved (clear mode indicators, sign-in diagnostics)
