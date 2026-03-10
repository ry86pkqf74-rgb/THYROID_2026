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
- Histology normalization categories: PTC_classic, PTC_follicular_variant, PTC_tall_cell, PTC_hobnail, PTC_diffuse_sclerosing, PTC_columnar, FTC, HCC_oncocytic, NIFTP, MTC, ATC, PDTC, adenoma, hyperplasia, benign
- Validation severity levels: error (manual review required), warning (verify and resolve), info (monitor)
- Reconciliation V2 depends on Phase 1 views from script 15; deploy script 15 first then script 16
- Data access: use `motherduck_client.py` or local `thyroid_master.duckdb`; fallback to CSV exports when DuckDB unavailable
- ETE manuscript lives in `studies/proposal2_ete_staging/`
- `.cursor/` and `.venv/` are in `.gitignore`
- Use fixed random seeds (e.g. `np.random.seed(42)`, `random_state=42`) for reproducibility in analyses
- MotherDuck requires DuckDB ≤1.4.4; v1.5.0 is incompatible
- raw_* tables (from read_xlsx) keep original Excel column names (e.g. "Research ID number"); cleaned tables use research_id
