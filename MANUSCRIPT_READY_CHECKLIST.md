# Manuscript-Ready Checklist — THYROID_2026 v2026.03.10

Use this checklist before submission or Zenodo archive.

## Data & Pipeline

- [x] Scripts 1–9 executed (ingest, DuckDB build, views, MotherDuck upload)
- [x] Scripts 13–15 executed (performance MVs, publication prep, final validation)
- [x] Script 21 executed: `duckdb "md:thyroid_research_2026" < scripts/21_survival_analysis_v3.sql` (builds survival v3 views)
- [x] Script 27 executed: `python scripts/27_fix_legacy_episode_compatibility.py` (legacy compatibility layer)
- [x] Local DuckDB backup present: `thyroid_master_local.duckdb` (optional, for downgrade/reproducibility)
- [x] MotherDuck database `thyroid_research_2026` has all required views/tables

## Dashboard

- [x] Streamlit runs without "Missing critical tables" error
- [x] Overview tab: cohort KPIs, date rescue rate, completeness by year
- [x] Advanced Features v3 tab: full column set, filters, export
- [x] Survival tab: Kaplan-Meier from `risk_enriched_mv` (if lifelines installed)
- [x] Statistical Analysis tab: Table 1 generation, hypothesis testing, regression modeling, forest plots, diagnostics
- [x] Publication footer visible: `THYROID_2026 v2026.03.10-publication-ready | Local DuckDB backup available`

## Exports & Studies

- [x] Publication bundle under `exports/` (e.g. `THYROID_2026_PUBLICATION_BUNDLE_*` or `FINAL_RELEASE_*`)
- [x] Publication bundle includes `time_to_rai_v3_mv`, `recurrence_free_survival_v3_mv`, and `genotype_stratified_outcomes_v3_mv`
- [x] Table 1–4 CSVs generated (demographics, risk stratification, complications, timeline)
- [x] ETE staging outputs in `studies/proposal2_ete_staging/` (if applicable)
- [x] `RELEASE_NOTES.md` and `docs/QA_report.md` up to date

## Reproducibility

- [x] `data_dictionary.md` documents legacy compatibility layer and modern table mapping
- [x] `CITATION.cff` at repo root for attribution
- [x] Tag `v2026.03.10-publication-ready` on commit to be archived
- [x] DVC tracked for large parquet exports (if used)

## Interactive Stats & Modeling

- [x] `utils/statistical_analysis.py` — `ThyroidStatisticalAnalyzer` with all core methods
- [x] `app/statistical_analysis.py` — 7-tab Statistical Analysis & Modeling dashboard tab
- [x] Table 1 generator with SMD (standardized mean difference) when stratifying
- [x] FDR/Bonferroni/Holm multiple comparison correction in hypothesis testing
- [x] Logistic regression with OR table, VIF, AUC, clinical snippet
- [x] Cox PH with HR table, concordance, Schoenfeld test, clinical snippet
- [x] Forest plots (Plotly, publication-ready, log scale)
- [x] Longitudinal Tg/TSH mixed-effects analysis (`longitudinal_summary()`)
- [x] Power/sample-size calculators (two-proportion, logistic OR, log-rank HR)
- [x] NSQIP complication cohort (`any_nsqip_complication`, RLN, hypocalcemia)
- [x] Publication Export sub-tab with LaTeX notes and clinical snippet templates
- [x] `scripts/36_statistical_analysis_examples.py` — 8-phase CLI demo
- [x] `notebooks/36_statistical_analysis_examples.ipynb` — interactive notebook

### To run the statistical analysis examples:
```bash
.venv/bin/python scripts/36_statistical_analysis_examples.py --md
```

### To access the dashboard tab:
```bash
streamlit run dashboard.py
# Navigate to "📊 Statistical Analysis & Modeling"
```

## Next Steps

- **Analytic modeling:** Use `risk_enriched_mv`, `advanced_features_v3`, manuscript export views
- **Zenodo archive:** Create snapshot from tag, upload bundle + code
- **Manuscript figures:** Use `notebooks/01_publication_figures.ipynb` for Kaplan-Meier, ETE staging, timeline explorer

## Manuscript Package V3 — Script 22

- [x] Deploy views: `duckdb "md:thyroid_research_2026" < scripts/22_manuscript_package_v3.sql`
- [x] Run one-click package: `python scripts/22_manuscript_package.py --md`
- [x] Verify ZIP created: `THYROID_2026_MANUSCRIPT_PACKAGE_YYYYMMDD.zip`
- [x] Confirm `studies/manuscript_package_*/` contains tables/, figures/, manifest.json


## ✅ Package Generated — 2026-03-10 23:55

- [x] `22_manuscript_package.py` executed successfully
- [x] LaTeX tables (booktabs) in `studies/manuscript_package_20260310_2354/tables/`
- [x] Publication figures (300 DPI PNG + SVG) in `studies/manuscript_package_20260310_2354/figures/`
- [x] Final zip: `THYROID_2026_MANUSCRIPT_PACKAGE_20260310_2354.zip`
- [x] All checklist items marked complete — **READY FOR SUBMISSION**


## ✅ Package Generated — 2026-03-10 23:55

- [x] `22_manuscript_package.py` executed successfully
- [x] LaTeX tables (booktabs) in `studies/manuscript_package_20260310_2355/tables/`
- [x] Publication figures (300 DPI PNG + SVG) in `studies/manuscript_package_20260310_2355/figures/`
- [x] Final zip: `THYROID_2026_MANUSCRIPT_PACKAGE_20260310_2355.zip`
- [x] All checklist items marked complete — **READY FOR SUBMISSION**
