# THYROID_2026 — Agent Memory

## Learned User Preferences

- Always stage, commit, and push changes to GitHub when completing a task; explicitly confirm that all three steps completed successfully
- Run typecheck/lint on Python before stage, commit, and push
- Put study results in dedicated folders under `studies/` (e.g. `studies/proposal2_ete_staging/`) rather than at repo root
- When saving outputs, ensure changes are reflected in both DuckDB online (MotherDuck) and GitHub where applicable; analysis artifacts go to GitHub; schema/data changes go to MotherDuck

## Learned Workspace Facts

- Tech stack: Python, pandas, scipy, statsmodels, lifelines, scikit-learn, DuckDB, MotherDuck, Streamlit, Parquet (DVC-tracked), openpyxl for Excel
- Primary key for all tables: `research_id` (int); standardize column names like "Research ID number", "Research_ID#" to `research_id`
- Project layout: `/processed/` (parquet), `/raw/` (source files), `/exports/` (CSV exports), `/scripts/` (ETL), `/studies/` (study outputs)
- Key views/tables: `ptc_cohort`, `recurrence_risk_cohort`, `tumor_pathology`, `master_cohort`, `advanced_features_view`, `advanced_features_v2`, `advanced_features_v3`
- Reconciliation V2 views (script 16): `histology_reconciliation_v2`, `molecular_episode_v2`, `molecular_unresolved_audit_mv`, `rai_episode_v2`, `rai_unresolved_audit_mv`, `timeline_rescue_mv`, `timeline_unresolved_summary_mv`, `validation_failures_v2`, `patient_validation_rollup_mv`, `patient_master_timeline_v2`, `patient_reconciliation_summary_v`, `patient_episode_audit_v`
- Semantic Cleanup V3 views (script 17): replaces all 6 `enriched_note_entities_*` views with date_status taxonomy; adds `validation_failures_v3`, `patient_validation_rollup_v2_mv`, `timeline_rescue_v2_mv`, `timeline_unresolved_summary_v2_mv`
- Date status taxonomy (applied to all enriched views): `exact_source_date` (entity_date, confidence 100), `inferred_day_level_date` (note_date, confidence 70), `coarse_anchor_date` (surgery/FNA/molecular fallback, confidence 35-60), `unresolved_date` (no source, confidence 0)
- All enriched views now have columns: `date_status`, `date_is_source_native_flag`, `date_is_inferred_flag`, `date_requires_manual_review_flag`
- Histology normalization categories: PTC_classic, PTC_follicular_variant, PTC_tall_cell, PTC_hobnail, PTC_diffuse_sclerosing, PTC_columnar, FTC, HCC_oncocytic, NIFTP, MTC, ATC, PDTC, adenoma, hyperplasia, benign
- Validation severity levels: error (manual review required), warning (verify and resolve), info (monitor)
- Validation v3 reclassified coarse anchor dates from error to info; truly unresolvable dates remain errors; use `validation_failures_v3` (not v2) for current state
- Adjudication framework views (script 18): `molecular_episode_v3`, `molecular_analysis_cohort_v`, `molecular_linkage_failure_summary_v`, `rai_episode_v3`, `rai_analysis_cohort_v`, `rai_linkage_failure_summary_v`, `histology_analysis_cohort_v`, `histology_discordance_summary_v`, `histology_manual_review_queue_v`, `molecular_manual_review_queue_v`, `rai_manual_review_queue_v`, `timeline_manual_review_queue_v`, `patient_manual_review_summary_v`, `patient_reconciliation_summary_v` (fixed), `streamlit_patient_header_v`, `streamlit_patient_timeline_v`, `streamlit_patient_conflicts_v`, `streamlit_patient_manual_review_v`, `streamlit_cohort_qc_summary_v`
- Molecular v3 adds multi-dimensional linkage confidence (temporal/platform/pathology concordance), `molecular_analysis_eligible_flag`, `molecular_date_raw_class`
- RAI v3 adds `rai_assertion_status` (definite_received/likely_received/planned/historical/negated/ambiguous), `rai_treatment_certainty`, `rai_interval_class` (replaces single invalid_interval), `rai_eligible_for_analysis_flag`
- Histology analysis cohort adds `final_histology_for_analysis`, `final_t_stage_for_analysis`, `adjudication_needed_flag`, `analysis_eligible_flag`, expanded `discordance_type`
- `patient_reconciliation_summary_v` fixed: uses path_synoptics/molecular/RAI union as patient spine instead of empty `master_cohort`; returns 10,872 rows
- Deployment order: script 15 → 16 → 17 → 18
- Reconciliation V2 depends on Phase 1 views from script 15; deploy script 15 first then script 16; script 17 depends on both; script 18 depends on all three
- Data access: use `motherduck_client.py` or local `thyroid_master.duckdb`; fallback to CSV exports when DuckDB unavailable
- ETE manuscript lives in `studies/proposal2_ete_staging/`
- `.cursor/` and `.venv/` are in `.gitignore`
- Use fixed random seeds (e.g. `np.random.seed(42)`, `random_state=42`) for reproducibility in analyses
- MotherDuck requires DuckDB ≤1.4.4; v1.5.0 is incompatible
- raw_* tables (from read_xlsx) keep original Excel column names (e.g. "Research ID number"); cleaned tables use research_id
