# Manuscript-Ready Checklist — THYROID_2026 v2026.03.13

Use this checklist before submission or Zenodo archive.
Last updated: 2026-03-13 (post-audit verification wave).

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
- [x] Predictive Analytics tab: model comparison, competing risks, ML nomograms, personalized cure calculator, manuscript export

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

## Traceability & Date Accuracy KPIs (v2026-03-12, verified 2026-03-13)

- [x] `provenance_enriched_events_v1` materialized and validated (50,297 rows; `scripts/46_provenance_audit.py --md`)
- [x] `lineage_audit_v1` materialized (10,871 rows; raw → note → extracted → final cohort)
- [x] `val_provenance_traceability` deployed with zero error-severity issues (0 errors; 6,801 warnings = non-Tg labs with no date, institutional data gap)
- [x] `direct_source_link` populated for all events in `provenance_enriched_events_v1`
- [x] Zero `NOTE_DATE_FALLBACK` events for Tg and TgAb lab types (thyroglobulin 99.5% correct via `specimen_collect_dt`)
- [x] `docs/provenance_coverage_report.md` generated and reviewed
- [x] `docs/date_accuracy_verification_report_20260312.md` generated and reviewed
- [x] `qa_issues` table free of `provenance_lab_note_date_fallback` errors (val_provenance STATUS = PASS)

**Caveats (documented, not blocking):**
- TSH/PTH/calcium/vitamin_D lab dates at 0% — these lab types are not in `thyroglobulin_labs` and have no structured `specimen_collect_dt`
- 54.7% of provenance events have `NO_DATE` status (NLP-extracted mentions with no nearby date text)
- 58.8% of patients rely solely on surgery date as temporal anchor (structurally correct for perioperative events)

### Commands to verify:
```bash
# Deploy provenance tables and generate reports
.venv/bin/python scripts/46_provenance_audit.py --md

# Run full validation engine (adds val_provenance_traceability)
.venv/bin/python scripts/29_validation_engine.py --md

# Check results
.venv/bin/python -c "
import duckdb, toml
token = toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN']
con = duckdb.connect(f'md:thyroid_research_2026?motherduck_token={token}')
r = con.execute(\"\"\"
    SELECT check_id, severity, COUNT(*) AS n
    FROM val_provenance_traceability
    GROUP BY check_id, severity
    ORDER BY severity, n DESC
\"\"\").fetchall()
for row in r: print(row)
"
```

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

## Advanced Analytics & AI (Phase 3)

- [x] `utils/advanced_analytics.py` — `ThyroidAdvancedAnalyzer` with competing risks, ML nomograms, SHAP, manuscript report
- [x] `app/advanced_analytics.py` — 6-tab "🔬 Advanced Analytics & AI" dashboard tab
- [x] Competing risks (Aalen-Johansen CIF) — recurrence vs death, landmark summaries
- [x] Stratified longitudinal trajectories — BRAF/stage/risk subgroup slopes with CI
- [x] XGBoost/Random Forest nomograms with stratified CV (AUC, Brier, calibration)
- [x] SHAP feature importance + beeswarm + per-patient contribution waterfall
- [x] Interactive risk calculator — slider-driven individual predictions
- [x] One-click Word manuscript report (.docx) — Table 1 + Cox + longitudinal + ML sections
- [x] LaTeX longtable export for model results
- [x] `requirements.txt` — added shap, xgboost, python-docx, jinja2
- [x] Dashboard now 37 tabs (was 36)

### To access the Advanced Analytics tab:
```bash
streamlit run dashboard.py
# Navigate to "🔬 Advanced Analytics & AI"
```

## Predictive Analytics Workbench (Phase 4)

- [x] `utils/predictive_analytics.py` — `ThyroidPredictiveAnalyzer` with PTCM cure prediction, competing risks, ML nomograms, model comparison, manuscript export
- [x] `app/predictive_analytics.py` — 5-tab "🔮 Predictive Analytics" dashboard tab
- [x] PTCM-powered personalized cure calculator with clinical interpretation and trajectory plots
- [x] Enhanced competing risks with cause-specific Cox HRs and stratified CIF curves
- [x] Multi-model comparison hub (KM, Cox PH, PTCM, Random Survival Forest)
- [x] Explainable ML nomograms (XGBoost/RF/sksurv RSF) with SHAP beeswarm + calibration
- [x] One-click Word manuscript report with selectable sections (PTCM, CompetingRisks, Nomogram, Comparison)
- [x] `scripts/39_promotion_time_cure_models.py` — added `predict_cure_probability()` and `load_fitted_params()` API
- [x] `requirements.txt` — added scikit-survival
- [x] Dashboard now 39 tabs

### To access the Predictive Analytics tab:
```bash
streamlit run dashboard.py
# Navigate to "🔮 Predictive Analytics"
```

## Predictive Analytics Workbench — Phase 4.5 Polish

- [x] Gray's landmark CIF tests (Pepe-Mori z-test) at 1/3/5/10-year landmarks
- [x] Cause-specific log-rank tests between strata (lifelines)
- [x] KM overlay on CIF plot (1 − KM reference curve)
- [x] CIF confidence bands from Aalen-Johansen variance
- [x] Cure calculator: 12 features (8 core + 4 advanced NSQIP/treatment)
- [x] Sensitivity analysis (what-if scenarios) for cure calculator
- [x] Advanced clinical context (NSQIP complications, RAI treatment history)
- [x] Model comparison: 2-panel Plotly dashboard (concordance + AIC)
- [x] Clinical recommendation engine (best discriminative, best AIC, cure models)
- [x] Sidebar Quick Launch card for PTCM cure calculator
- [x] Batch runner expanded to 5 phases with sensitivity archetypes
- [x] Notebook updated with Gray's test, sensitivity analysis, NSQIP examples
- [x] AGENTS.md, RELEASE_NOTES.md, README.md, MANUSCRIPT_READY_CHECKLIST.md updated

## Manuscript Package V3

- [x] Manuscript views deployed and verified (`manuscript_tables_v3_mv`)
- [x] One-click "Generate Full Manuscript Package" button in Risk & Survival dashboard tab
- [x] LaTeX tables (booktabs, Overleaf-ready), 300 DPI figures (PNG + SVG, Wong color-blind palette)
- [x] Package runs: 2026-03-10 (×3), 2026-03-11 (×3) — all successful
- [x] Latest ZIP: `THYROID_2026_MANUSCRIPT_PACKAGE_20260311.zip`

## March 13 Audit & Verification (v2026.03.13)

### Verification pass

- [x] Full engineering-grade verification: [`docs/final_repo_verification_20260313.md`](docs/final_repo_verification_20260313.md) (531 tables, 34 val_* tables, 18 prior audit docs)
- [x] Database hardening audit: [`docs/database_hardening_audit_20260313.md`](docs/database_hardening_audit_20260313.md) (0 critical blocking, 0 row multiplication, 0 identity failures)
- [x] Manuscript metric reconciliation: [`docs/manuscript_metric_reconciliation_20260313.md`](docs/manuscript_metric_reconciliation_20260313.md) (11 metrics, 0 mismatches)
- [x] Manuscript freeze alignment: [`docs/manuscript_freeze_alignment_20260313.md`](docs/manuscript_freeze_alignment_20260313.md)

### Readiness gates

- [x] G1: 0 patient duplicates in `patient_analysis_resolved_v1`
- [x] G2: 0 episode duplicates (146 → 0 via dedup; `episode_analysis_resolved_v1_dedup`)
- [x] G3: Scoring calculability (AJCC8 37.6%, MACIS 37.5%, AMES 100%, AGES 100%)
- [x] G4: 7 refined complication entity types
- [x] G5: All 15 supporting tables populated and non-empty
- [x] G6: 0 null `research_id` values
- [x] G7: Statistical analysis plan exists (`docs/statistical_analysis_plan_thyroid_manuscript.md`)
- [x] Overall: **READY** ([`exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json`](exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json))

### Publication bundle

- [x] `exports/FINAL_PUBLICATION_BUNDLE_20260313/` generated (62 files)
- [x] Tables 1–3 in CSV + Markdown + LaTeX
- [x] Figures 1–5 in 300 DPI PNG + SVG
- [x] `manuscript_cohort_v1` (10,871 patients, 139 columns)
- [x] `master_clinical_v12` (12,886 patients, 136 columns)
- [x] `readiness_assessment.json` + `manifest.json` with git SHA

### Scoring & analysis layer

- [x] `thyroid_scoring_py_v1` — AJCC8, ATA, MACIS, AGES, AMES per patient
- [x] `complication_phenotype_v1` — structured complication classification (5,928 events)
- [x] `longitudinal_lab_clean_v1` — deduplicated lab timeline (38,699 values)
- [x] `recurrence_event_clean_v1` — source-linked recurrence events (1,946)
- [x] Analysis views: `analysis_patient_v1`, `analysis_episode_v1`, `analysis_lesion_v1`
- [x] Pre-computed subsets: cancer (4,136), TIRADS (3,474), molecular (10,025), recurrence (1,946)

### Dataset maturation pass (v2026.03.13-dataset-maturation)

- [x] CND/LND flags wired from structured path_synoptics fields (CND: 0->2,497; LND: 0->241)
- [x] Operative note dates resolved (0->9,366 of 9,371 episodes)
- [x] Imaging layer canonicalized (`imaging_nodule_master_v1` = 19,891 rows; `imaging_nodule_long_v2` deprecated)
- [x] Provenance columns hardened (4 columns x 4 analysis tables = 100% fill)
- [x] Chronology anomalies classified (626 -> 4 buckets)
- [x] Health monitoring tables deployed (3 new `val_*` tables)
- [x] MotherDuck optimization (ANALYZE TABLE on 10 canonical tables)
- [x] MATERIALIZATION_MAP updated with 4 new entries

### Known gaps (verified open — not blocking manuscript)

- [ ] RAI dose in canonical table at 20.0% (371/1,857 — source-limited, not propagation-limited)
- [ ] Recurrence dates at 0.5% (structural sparsity — historical recurrences lack specific detection dates)
- [ ] Non-Tg lab dates (TSH/PTH/Ca/vitD) at 0% — requires new institutional data extract
- [ ] Structured PTH/calcium/TSH lab table — not in current corpus
- [ ] Nuclear medicine report text — zero nuclear med notes in corpus
- [ ] Vascular invasion 87% present_ungraded — synoptic template limitation, not data quality gap

## Next Steps

- **Dataset maturity:** Execute the 6 canonical-table backfill operations listed above
- **External validation:** Held-out temporal split for nomogram calibration
- **Fine-Gray subdistribution HR:** Requires R `cmprsk` bridge or custom implementation
- **Zenodo archive update:** Rebuild from March 13 bundle if re-archiving
