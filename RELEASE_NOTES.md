# THYROID_2026 Release Notes

## v2026.03.11-advanced-analytics (Latest)
**Date:** 2026-03-11
**Patients:** 11,673 | **Enhancement:** Advanced Analytics & AI — Phase 3

### Advanced Analytics & AI (New Dashboard Tab)

#### `utils/advanced_analytics.py` — `ThyroidAdvancedAnalyzer` class
- `fit_competing_risks()` — Aalen-Johansen cumulative incidence functions for competing risks (recurrence vs death); landmark CIF summary at 1/3/5/10 years; handles combined event indicators; interactive Plotly CIF curves
- `fit_stratified_longitudinal()` — stratified mixed-effects models for Tg/TSH trajectories by clinical subgroups (BRAF, stage, risk band, RAI); per-stratum slope comparison table with CI and p-values; multi-stratum spaghetti/trend plots
- `train_ml_nomogram()` — XGBoost or Random Forest for binary outcomes; stratified k-fold CV (AUC, Brier); class-imbalance handling (scale_pos_weight); native + SHAP feature importance; SHAP beeswarm plot (Plotly); calibration curves
- `predict_individual_risk()` — individualized risk prediction with SHAP-based feature contributions; risk classification (Low/Intermediate/High)
- `compute_feature_ranges()` — min/max/median/percentile ranges for risk calculator slider defaults
- `generate_manuscript_report()` — automated Word document generation (python-docx) with Table 1, Cox HR, longitudinal summary, competing risks, ML nomogram sections; clinical interpretation text auto-included
- `generate_latex_table()` — DataFrame to LaTeX longtable conversion with proper escaping
- Thyroid-specific presets: `COMPETING_RISK_PRESETS`, `NOMOGRAM_PRESETS`, `LONGITUDINAL_STRATIFIERS`

#### `app/advanced_analytics.py` — 6 sub-tabs in "🔬 Advanced Analytics & AI" dashboard tab
- **Competing Risks** — source/time/event/competing selectors; Aalen-Johansen CIF plot; landmark summary table; clinical interpretation
- **Longitudinal Trajectories** — marker selector (Tg/TSH/Anti-Tg); stratification by BRAF/stage/risk/RAI; per-stratum slope comparison; trajectory spaghetti plots
- **ML Nomograms & SHAP** — model type toggle (XGBoost/RF); outcome + predictor selectors; CV folds slider; SHAP importance bar + beeswarm; calibration plot; feature importance table
- **Interactive Risk Calculator** — slider-driven individualized predictions from trained ML model; color-coded risk display (Low/Intermediate/High); SHAP contribution waterfall
- **Manuscript Report** — section picker; Word document (.docx) generation + download; LaTeX table export for stored models
- **Diagnostics** — library badge status (lifelines/statsmodels/xgboost/sklearn/SHAP/python-docx); source availability + row counts; package versions

#### Dependencies
- `requirements.txt` — added `shap`, `xgboost`, `python-docx`, `jinja2`

#### Dashboard
- 37 tabs (was 36); `t_advai` = "🔬 Advanced Analytics & AI" → `render_advanced_analytics(con)`

---

## v2026.03.10-statistical-workbench-v2 (Previous)
**Date:** 2026-03-10
**Patients:** 11,673 | **Enhancement:** Statistical Analysis Workbench — Phase 2 additions

### Statistical Analysis Enhancements

#### `utils/statistical_analysis.py`
- `generate_table_one()` — now passes `smd=True` to `TableOne` when stratifying; SMD (standardized mean difference) displayed for balance assessment
- `longitudinal_summary(marker)` — new method: linear mixed-effects model (`MixedLM`, random intercept by patient) for repeated Tg/TSH measurements; falls back to OLS if convergence fails; returns per-patient slope, cohort CI, and rising% with clinical interpretation string
- `power_two_proportions()` — Fleiss (1981) formula for two-proportion z-test (e.g., BRAF+ vs BRAF− recurrence comparison)
- `power_logistic()` — Hsieh, Block & Larsen (1998) formula for logistic regression sample size
- `sample_size_km()` — Schoenfeld (1981) formula for log-rank events + total n
- `format_clinical_snippet()` — generates plain-English manuscript-ready text for significant regression findings; includes thyroid-specific context for 18 key variables (BRAF, TERT, ETE, LN ratio, AJCC stage, etc.)
- New constants: `THYROID_NSQIP_OUTCOMES`, `THYROID_NSQIP_PREDICTORS`, `NSQIP_COMPLICATION_COLUMNS`, `ETE_SUBTYPES`, `LONGITUDINAL_MARKERS`, `_CLINICAL_CONTEXT`
- `THYROID_OUTCOMES` expanded to include `structural_recurrence`, `rai_need`, `any_nsqip_complication`

#### `app/statistical_analysis.py`
- **7 sub-tabs** (was 5): Table 1 · Hypothesis Testing · Regression Modeling · **Longitudinal Analysis** (new) · Visualizations · Diagnostics · **Publication Export** (new)
- **Longitudinal Analysis tab**: marker selector (Tg/TSH/Anti-Tg), MixedLM summary, per-patient slope histogram, rising/falling % split, per-patient CSV export
- **Publication Export tab**: Table 1 export with LaTeX column notes, clinical snippet template textarea, interpretation guide for key predictors, export checklist
- After every Logistic/Cox result: `format_clinical_snippet()` auto-displayed via `st.info()`
- `nsqip_complication_cohort` virtual source: inline SQL joins `complications + master_cohort + tumor_pathology + path_synoptics`; computes `rln_injury`, `hypocalcemia`, `hypoparathyroidism`, `seroma`, `hematoma`, `any_nsqip_complication` flags
- Data sources expanded: `extracted_clinical_events_v4`, `longitudinal_lab_view`, `nsqip_complication_cohort`

#### `scripts/36_statistical_analysis_examples.py`
- Phase 7: `phase7_longitudinal()` — runs MixedLM for Tg and TSH; exports per-patient CSV and model summary
- Phase 8: `phase8_power_analysis()` — four thyroid-specific hypotheses (BRAF recurrence, ETE logistic, Cox HR=1.8, hypocalcemia NSQIP); exports `power_analysis.csv`

#### `notebooks/36_statistical_analysis_examples.ipynb`
- New notebook (35 cells) covering: connection, Table 1 with SMD, FDR hypothesis tests, logistic OR + forest plot + snippet, Cox HR + forest plot + snippet, longitudinal Tg/TSH mixed-effects, power analysis (3 hypotheses + sensitivity curve), Spearman heatmap, NSQIP complications, missing data summary

---

## v2026.03.10-statistical-workbench (Previous)
**Date:** 2026-03-10
**Patients:** 11,673 | **New capability:** Publication-ready Statistical Analysis Workbench

### Statistical Analysis & Modeling (New Dashboard Tab)
- `utils/statistical_analysis.py` — `ThyroidStatisticalAnalyzer` class (1,042 lines):
  - `generate_table_one()` — auto-detects variable types; uses `tableone` when available with fallback; Shapiro-Wilk normality detection selects Mann-Whitney / t-test / Kruskal / ANOVA automatically; missing % reported
  - `run_hypothesis_tests()` — Fisher exact (sparse cells) vs Chi-square for categoricals; Welch t-test vs Mann-Whitney / ANOVA vs Kruskal-Wallis for continuous; FDR/Bonferroni/Holm correction; Cohen's d / Cramér's V effect sizes
  - `fit_logistic_regression()` — statsmodels Logit with OR table, 95% CI, VIF multicollinearity diagnostics, AUC (sklearn), pseudo-R², AIC/BIC, perfect-separation warnings
  - `fit_cox_ph()` — lifelines CoxPHFitter with HR table, concordance index, Schoenfeld proportionality check, sparse-events warning
  - `create_forest_plot()` — publication-ready Plotly horizontal forest plot with log scale, color-coded significance, annotated CI/p text
  - `correlation_matrix_with_pvalues()` — pingouin pairwise or scipy fallback; significance stars (*/**/***)
  - `create_correlation_heatmap()` — diverging colorscale Plotly heatmap with annotation
  - `missing_data_summary()` — per-column missing %, dtype, sorted by severity
- `app/statistical_analysis.py` — 5 sub-tabs in "📊 Statistical Analysis" dashboard tab:
  - **Table 1** — source selector, group-by, variable multiselects, styled p-value highlighting, CSV/Excel/Parquet export
  - **Hypothesis Testing** — target selector, feature multiselects, correction method, results table with interpretation
  - **Regression Modeling** — Logistic + Cox sub-forms, predictor multiselects, forest plot auto-display, VIF table
  - **Visualizations** — Correlation heatmap, Missing data bar, Distribution comparison with overlay
  - **Diagnostics & Export** — library badge status, available sources with row counts, package versions
- Thyroid-specific presets: `THYROID_TABLE1_PRESET`, `THYROID_OUTCOMES`, `THYROID_PREDICTORS`, `THYROID_SURVIVAL`
- `requirements.txt` — added `scikit-learn` (for AUC computation via `roc_auc_score`)
- `notebooks/05_statistical_workbench_examples.ipynb` — worked examples for Table 1, logistic regression, Cox PH, forest plot

### Clinical Interpretation
All regression results include plain-English interpretation strings, e.g.:
> "BRAF mutation: OR=1.87 indicates 87% increased odds" / "HR=2.12 indicates 112% increased hazard"

---

## v2026.03.10-v2-pipeline
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
