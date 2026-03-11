# Manuscript-Ready Checklist — THYROID_2026 v2026.03.10

Use this checklist before submission or Zenodo archive.

## Data & Pipeline

- [ ] Scripts 1–9 executed (ingest, DuckDB build, views, MotherDuck upload)
- [ ] Scripts 13–15 executed (performance MVs, publication prep, final validation)
- [ ] Script 27 executed: `python scripts/27_fix_legacy_episode_compatibility.py` (legacy compatibility layer)
- [ ] Local DuckDB backup present: `thyroid_master_local.duckdb` (optional, for downgrade/reproducibility)
- [ ] MotherDuck database `thyroid_research_2026` has all required views/tables

## Dashboard

- [ ] Streamlit runs without "Missing critical tables" error
- [ ] Overview tab: cohort KPIs, date rescue rate, completeness by year
- [ ] Advanced Features v3 tab: full column set, filters, export
- [ ] Survival tab: Kaplan-Meier from `risk_enriched_mv` (if lifelines installed)
- [ ] Statistical Analysis tab: Table 1 generation, hypothesis testing, regression modeling, forest plots, diagnostics
- [ ] Publication footer visible: `THYROID_2026 v2026.03.10-publication-ready | Local DuckDB backup available`

## Exports & Studies

- [ ] Publication bundle under `exports/` (e.g. `THYROID_2026_PUBLICATION_BUNDLE_*` or `FINAL_RELEASE_*`)
- [ ] Table 1–4 CSVs generated (demographics, risk stratification, complications, timeline)
- [ ] ETE staging outputs in `studies/proposal2_ete_staging/` (if applicable)
- [ ] `RELEASE_NOTES.md` and `docs/QA_report.md` up to date

## Reproducibility

- [ ] `data_dictionary.md` documents legacy compatibility layer and modern table mapping
- [ ] `CITATION.cff` at repo root for attribution
- [ ] Tag `v2026.03.10-publication-ready` on commit to be archived
- [ ] DVC tracked for large parquet exports (if used)

## Next Steps

- **Analytic modeling:** Use `risk_enriched_mv`, `advanced_features_v3`, manuscript export views
- **Zenodo archive:** Create snapshot from tag, upload bundle + code
- **Manuscript figures:** Use `notebooks/01_publication_figures.ipynb` for Kaplan-Meier, ETE staging, timeline explorer
