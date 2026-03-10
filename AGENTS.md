# THYROID_2026 — Agent Memory

## Learned User Preferences

- Always stage, commit, and push changes to GitHub when completing a task; explicitly confirm that all three steps completed successfully
- Put study results in dedicated folders under `studies/` (e.g. `studies/proposal2_ete_staging/`) rather than at repo root
- When saving outputs, ensure changes are reflected in both DuckDB online (MotherDuck) and GitHub where applicable; analysis artifacts go to GitHub; schema/data changes go to MotherDuck

## Learned Workspace Facts

- Tech stack: Python, pandas, scipy, statsmodels, lifelines, scikit-learn, DuckDB, MotherDuck, Streamlit, Parquet (DVC-tracked), openpyxl for Excel
- Primary key for all tables: `research_id` (int); standardize column names like "Research ID number", "Research_ID#" to `research_id`
- Project layout: `/processed/` (parquet), `/raw/` (source files), `/exports/` (CSV exports), `/scripts/` (ETL), `/studies/` (study outputs)
- Key views/tables: `ptc_cohort`, `recurrence_risk_cohort`, `tumor_pathology`, `master_cohort`, `advanced_features_view`
- Data access: use `motherduck_client.py` or local `thyroid_master.duckdb`; fallback to CSV exports when DuckDB unavailable
- ETE manuscript lives in `studies/proposal2_ete_staging/`
- `.cursor/` and `.venv/` are in `.gitignore`
- Use fixed random seeds (e.g. `np.random.seed(42)`, `random_state=42`) for reproducibility in analyses
