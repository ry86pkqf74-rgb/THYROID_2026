# THYROID_2026 Release Notes

## v2026.03.10-v2-pipeline (Latest)
**Date:** 2026-03-10
**Patients:** 11,673 | **Views:** 60+ | **Materialized tables:** 47

### V2 Canonical Episode Pipeline (Scripts 22–27)
- `22_canonical_episodes_v2.py` — 9 canonical episode tables with inline V2 extractors (RAI scan findings, operative flags, Bethesda grades, histology detail)
- `23_cross_domain_linkage_v2.py` — 6 cross-domain linkage tables with 5-tier confidence (exact_match → unlinked); enforces FNA-before-molecular and preop-before-surgery chronology
- `24_reconciliation_review_v2.py` — 5 cross-domain review views (imaging-pathology concordance uses surgery-aware temporal windows)
- `25_qa_validation_v2.py` — `qa_issues_v2`, `qa_date_completeness_v2`, `qa_high_priority_review_v2`; weak linkages routed as QA warnings
- `26_motherduck_materialize_v2.py` — 47 tables materialized in MotherDuck (up from 20): granular linkage, Streamlit-critical views, manual review queues, date_rescue_rate_summary
- `27_date_provenance_formalization.py` — ALTER TABLE adds 4 provenance columns to all 6 `note_entities_*` tables; `enriched_master_timeline` + `date_rescue_rate_summary` KPI views

### V2 Extraction Layer
- `MolecularDetailExtractor`, `RAIDetailExtractor`, `ImagingNoduleExtractor`, `OperativeDetailExtractor`, `HistologyDetailExtractor` in `notes_extraction/extract_*_v2.py`
- Date utilities: `classify_date_status`, `compute_date_confidence`, `resolve_event_date` in `utils/date_utils.py`
- Vocabulary normalization maps in `notes_extraction/vocab.py`

### Validation & Readiness (Scripts 28–30)
- `28_manual_review_export.py` — manual review queue export with `--md` flag
- `29_validation_runner.py` — combined 14-view validation + 6-domain review export + reconciliation gap summary
- `30_readiness_check.py` — MotherDuck/local readiness report with critical/optional table audit

### V2 Dashboard Tabs
- Extraction Completeness, Molecular Episodes, RAI Episodes, Imaging/Nodule, Operative, Adjudication v2
- Pre-adjudication data banners; linkage quality sections with weak-linkage warnings
- Validation Engine tab with domain-level downloads and runner summary
- Date Rescue Rate KPI chart on Overview tab

### Test Suite
- 7 V2 test files in `tests/`: molecular, RAI, imaging, operative, histology parsers; date utils; linkage confidence
- `test_enrichment_and_chronology.py`: chronology constraint validation
- All tests use fixed seeds (random_state=42) for reproducibility

---

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
