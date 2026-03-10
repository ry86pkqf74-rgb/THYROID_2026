# THYROID_2026 Release Notes

## v2026.03.10-v3-dashboard (Current)
**Date:** 2026-03-10
**Patients:** 11,673 | **Surgeries:** Multiple per patient handled

### What's New
- V3 dashboard optimizations: updated table references, expanded materializations
- Validation Engine tab expanded with domain-level downloads, runner summary, and reconciliation gap view
- Script 26 now materializes 63 tables (up from 47): adds post-review overlays, manuscript cohorts, adjudication detail views
- Script 30 (`30_readiness_check.py`): MotherDuck/local readiness report with critical/optional table audit
- Dashboard sidebar: expanded Data Build Info (8 health checks), Connection Help tooltip, MOTHERDUCK_TOKEN instructions
- Tab naming cleaned up: "v2" tabs renamed to descriptive labels (Extraction Completeness, Molecular Episodes, etc.)
- QA tab: added V2 QA Validation links (date completeness, domain summary, high priority)
- Adjudication tab: added granular linkage quality section showing per-pair confidence tiers
- Missing critical table warnings surfaced at dashboard startup

### Tests
- `test_enrichment_and_chronology.py`: validates RAI/operative enrichment fields and FNA→molecular / preop→surgery chronology constraints

### Materialization Instructions
```bash
python scripts/26_motherduck_materialize_v2.py --md
python scripts/29_validation_engine.py --md
python scripts/30_readiness_check.py --md
```

### Deploy Order
Phase 6 Adjudication: 15 → 16 → 17 → 18 → 19 → 20
V2 Canonical Pipeline: 22 → 23 → 24 → 25 → 26 → 27
Validation: 29 (engine) → 29 (runner) → 30 (readiness)

---

## v2026.03.10-publication-ready
**Date:** 2026-03-10 05:29
**Patients:** 11,673 | **Surgeries:** Multiple per patient handled

### What's New
- Scripts 13 & 14 executed (performance pack + publication bundle)
- 4 new materialized views for instant dashboard tabs
- Local DuckDB backup created (`thyroid_master_local.duckdb`)
- Publication bundle + manuscript tables ready
- All queries now sub-second on new tabs

### Streamlit Features Live
- Patient Timeline Explorer
- QA Dashboard with laterality & matching checks
- Risk & Survival visuals
- Advanced Features v3 Explorer
- Export buttons everywhere

### Trial Downgrade Ready
All derived objects are materialized and backed up locally.
