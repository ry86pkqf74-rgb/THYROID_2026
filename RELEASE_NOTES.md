# THYROID_2026 Release Notes

## v2026.03.13-truth-sync (Latest)
**Date:** 2026-03-13

### Repo-Wide Truth Synchronization

Audit and fix pass ensuring all docs, version strings, and status claims tell
one coherent story. No data or code logic changes.

#### Version Reconciliation

| Location | Before | After |
|----------|--------|-------|
| `dashboard.py _APP_VERSION` | v3.2.0-2026.03.13 | **v3.3.0-2026.03.13** |
| README release history | v2026.03.13 post-hardening | **v2026.03.13 (truth-sync)** |
| RELEASE_NOTES latest tag | v2026.03.13-final-manuscript-readiness | **v2026.03.13-truth-sync** |
| CITATION.cff | 2026.03.13 | unchanged |
| docs/REPO_STATUS.md verdict | "Not dataset-mature" | **"Approaching dataset-mature"** |

#### Overclaim Corrections

| Claim | Location | Action |
|-------|----------|--------|
| "Every data point ... traceable to its direct source" | README.md | **Rewritten** — scoped to manuscript cohort and structured domains |
| "Traceability & Date Accuracy Guarantee (v2026.03.12)" | README.md | **Updated** — v2026.03.13, removed "guarantee" framing |
| Imaging "NOT VERIFIED" | RELEASE_NOTES audit-verification | **Annotated** — later maturation pass populated 19,891 rows |
| Operative "NOT VERIFIED" | RELEASE_NOTES audit-verification | **Annotated** — CND/LND flags wired (0→2,497/241); NLP enrichment remains open |
| Imaging nodule master listed as pending | REPO_STATUS.md | **Updated** — marked CLOSED (19,891 rows) |

#### New/Updated Documents

- `docs/FINAL_REPO_STATUS_20260313.md` — single source of truth: readiness, maturity, safe/unsafe claims
- `scripts/check_truth_sync.py` — guardrail checking version string consistency and banned overclaims
- README deployment truth table (local + token / cloud private / cloud public / cloud sign-in)

---

## v2026.03.13-final-manuscript-readiness
**Date:** 2026-03-13

### Final Manuscript-Readiness Hardening Pass

10-workstream audit and fix pass converting the repo to the strongest truthfully
supportable state for manuscript writing.

#### Fixes Applied
- Ran script 71 operative NLP enrichment (13,186 entities extracted; zero delta due to COALESCE guards — documented as pipeline architecture gap)
- Created 3 missing Streamlit tables: `streamlit_patient_conflicts_v` (1,015 rows), `streamlit_patient_manual_review_v` (7,552 rows), `adjudication_progress_summary_v` (0 rows)
- Ran ANALYZE on 17 manuscript-critical MotherDuck tables
- Updated CITATION.cff version to 2026.03.13 with license clarification
- Updated README with honest assessment of limitations

#### Documentation Produced
- `docs/FINAL_MANUSCRIPT_READINESS_VERDICT_20260313.md` — go/no-go verdict (B: Ready with scoped caveats)
- `docs/final_manuscript_readiness_dependency_map_20260313.md` — full pipeline dependency map
- `docs/final_provenance_date_hardening_audit_20260313.md` — provenance coverage matrix
- `docs/operative_v2_materialization_repair_20260313.md` — operative NLP root cause
- `docs/imaging_nodule_master_repair_20260313.md` — imaging status (fully populated)
- `docs/rai_recurrence_propagation_repair_20260313.md` — RAI/recurrence propagation
- `docs/final_notes_coverage_truth_audit_20260313.md` — honest note coverage assessment
- `docs/streamlit_wiring_and_deployment_verification_20260313.md` — dashboard verification
- `docs/final_repo_truth_sync_20260313.md` — version/claim synchronization
- `docs/final_validation_and_optimization_20260313.md` — validation results

#### Key Metrics Verified
- 578 MotherDuck tables (578 distinct in main schema)
- 0 patient-level duplicates in manuscript_cohort_v1 and patient_analysis_resolved_v1
- AJCC8 37.6%, ATA 28.9%, MACIS 37.5%, AGES 100%, AMES 100% calculability
- All Streamlit tabs verified against live MotherDuck tables
- 17 tables ANALYZE'd for query optimization

#### Verdict: READY FOR MANUSCRIPT WRITING WITH DOCUMENTED CAVEATS

---

## v2026.03.13-post-hardening-cleanup
**Date:** 2026-03-13

### Post-Hardening Cleanup

Bounded cleanup pass after final hardening. No broad rewrites, no new
extraction campaigns. Preserves all validated manuscript-ready outputs.

#### Lab Canonical Dedup

| Metric | Before | After |
|--------|--------|-------|
| `longitudinal_lab_canonical_v1` rows | 45,954 | 39,961 |
| Exact duplicate groups | 5,976 | 0 |
| Excess rows removed | — | 5,993 |
| Patient count | 3,349 | 3,349 (preserved) |

Root cause: script 77 ingested threshold values (e.g., "<0.9") as both
censored and uncensored rows. Dedup rule: prefer `is_censored=TRUE`
(conservative threshold interpretation), tiebreak by latest wave.

#### Operative V2 Assessment

8 operative NLP enrichment fields confirmed at 0% fill (Category C — raw
text only, extractor exists but outputs not materialized). No safe landing
without running full extraction campaign. Documented in audit note.

#### Documentation Synchronization

- README: removed stale "sign in" guidance, old flat-tab architecture
  references, duplicated gap section, outdated deploy instructions
- RELEASE_NOTES: added post-hardening cleanup entry
- dashboard.py: updated docstring and version to v3.2.0-2026.03.13
- Audit note: `docs/final_small_cleanup_audit_20260313.md`

#### Validation

- `val_lab_canonical_v1` rebuilt: all 5 analytes PASS (was WARN due to dupes)
- `md_longitudinal_lab_canonical_v1` materialized copy synced
- ANALYZE TABLE run on deduped lab tables

---

## v2026.03.13-final-hardening
**Date:** 2026-03-13

### Final Hardening Pass

Targeted pass after dataset maturation, canonical gap closure, lab scaffold,
and Streamlit refactor. Focuses on closing actionable gaps, root-cause analysis,
and documentation synchronization.

#### Script 78 (`78_final_hardening.py`)

| Phase | Deliverable | Impact |
|-------|-------------|--------|
| A | `recurrence_manual_review_queue_v1` | Prioritized review of 1,764 unresolved recurrence dates |
| A | `val_recurrence_date_resolution_v1` | Tier summary with manuscript-cohort breakdown |
| B | `imaging_fna_linkage_v3` re-run | Relaxed UNION preferring v1 (real features) over v2 (NULL placeholder) |
| C | `dose_missingness_reason` column | RAI dose missingness classified into 4 categories |
| C | `vw_rai_dose_missingness_summary` | Aggregate missingness breakdown |
| D | `val_lab_canonical_v1` | Executable contract validation (plausibility, tiers, dates, dedup) |

#### Root-Cause Conclusions

- **Imaging-FNA linkage 0 rows**: `imaging_nodule_master_v1` was empty when
  script 49 ran. Fixed by relaxed UNION in script 78 Phase B.
- **Molecular-surgery linkage absent**: By design. Molecular tests link through
  `preop_surgery_linkage_v3` with `preop_type='molecular'`. Documented.
- **RAI dose missingness**: ~900 episodes have no nuclear medicine notes in
  corpus (source limitation). Classification field added.
- **Lab contract**: 18 validation rules existed but were never consumed.
  Now wired via `val_lab_canonical_v1` and `tests/test_lab_canonical.py`.

#### Dashboard Polish

- Overview: quick navigation, dataset caveats expander
- QA Workbench: imaging-FNA status, chained molecular metrics, RAI missingness,
  recurrence date resolution
- Manual Review: prioritized recurrence queue with priority scoring
- Source table captions and caveat badges throughout

#### Documentation

- [`docs/final_hardening_audit_20260313.md`](docs/final_hardening_audit_20260313.md)
- Updated [`docs/analysis_resolved_layer.md`](docs/analysis_resolved_layer.md) with linkage semantics
- Updated [`docs/lab_layer_scaffold_plan_20260313.md`](docs/lab_layer_scaffold_plan_20260313.md) with contract validation

---

## v2026.03.13-canonical-gap-closure
**Date:** 2026-03-13

### Canonical Gap Closure + Lab Scaffold + Dashboard Refactor

#### Script 76 (`76_canonical_gap_closure.py`)

5-phase canonical table gap closure:

| Phase | Target | Impact |
|-------|--------|--------|
| A | Operative NLP enrichment | 6 new columns (parathyroid, frozen, berry, EBL) |
| B | RAI dose provenance | 20% -> 41% dose coverage; 3 provenance columns |
| C | Molecular RAS subtype | 325 episodes with ras_subtype filled |
| D | Linkage ID propagation | Surgery/path/FNA linkage IDs across 6 tables |
| E | Recurrence date hardening | 4-tier date classification (exact/biochemical/unresolved/NA) |

#### Script 77 (`77_lab_canonical_layer.py`)

- `longitudinal_lab_canonical_v1`: 45,954 rows, 5 analytes, 3,349 patients
- `val_lab_completeness_v1`: per-analyte coverage with future placeholders
- Forward-compatible schema for institutional lab extract

#### Workflow Dashboard Refactor

39 flat tabs reorganized into 6 workflow-first sections:
1. Overview
2. Patient Explorer
3. Data Quality (new QA Workbench + Manual Review Workbench)
4. Linkage & Episodes
5. Outcomes & Analytics
6. Manuscript & Export

New modules: `app/qa_workbench.py`, `app/manual_review_workbench.py`

---

## v2026.03.13-dataset-maturation
**Date:** 2026-03-13
**Patients:** 10,871 (manuscript cohort) | 4,136 (analysis-eligible cancer subcohort)

### Dataset Maturation Pass

Executed `scripts/75_dataset_maturation.py` — 10-phase post-audit maturation
bringing the repository from manuscript-ready to approaching dataset-mature.

#### Fixes applied

| Fix | Before | After | Delta |
|-----|--------|-------|-------|
| CND flag (operative_episode_detail_v2) | 0 TRUE | 2,497 TRUE | +2,497 (26.6%) |
| LND flag (operative_episode_detail_v2) | 0 TRUE | 241 TRUE | +241 (2.6%) |
| Operative note dates | 0 resolved | 9,366 resolved | +9,366 (99.9%) |
| Provenance columns (4 tables) | partial | 100% filled | 4 columns x 4 tables |
| Chronology anomalies | unclassified | 626 classified | 4 resolution buckets |
| Health monitoring tables | 0 | 3 tables | new deployment |
| ANALYZE TABLE | never run | 10 tables analyzed | MotherDuck optimization |

#### Imaging layer standardization

- `imaging_nodule_master_v1` (19,891 rows) designated canonical
- `imaging_nodule_long_v2` deprecated (schema stub, all data NULL)
- Dashboard modules updated to prefer master_v1
- Design doc: [`docs/imaging_layer_v3_design.md`](docs/imaging_layer_v3_design.md)

#### Chronology anomaly classification

626 temporal anomalies classified into 4 buckets:
- `benign_temporal_offset`: 102 (molecular/FNA post-surgery, late RAI within 2y)
- `source_extraction_error`: 14 (future dates, pre-1990 dates)
- `true_conflict`: 510 (RAI before surgery, very late RAI)

#### New tables

- `val_dataset_integrity_summary_v1` — per-table row counts and coverage
- `val_provenance_completeness_v2` — provenance field fill rates
- `val_episode_linkage_completeness_v1` — linkage type completeness
- `val_temporal_anomaly_resolution_v1` — classified chronology anomalies

#### Documentation

- [`docs/canonical_layer_integrity_report_20260313_addendum.md`](docs/canonical_layer_integrity_report_20260313_addendum.md)
- [`docs/dataset_maturation_report_20260313.md`](docs/dataset_maturation_report_20260313.md)
- [`docs/imaging_layer_v3_design.md`](docs/imaging_layer_v3_design.md)

---

## v2026.03.13-audit-verification
**Date:** 2026-03-13
**Patients:** 10,871 (manuscript cohort) | 4,136 (analysis-eligible cancer subcohort)
**MotherDuck tables:** 531 | **Validation tables:** 34 `val_*`

### Full Engineering Verification Pass

An end-to-end verification audited 531 MotherDuck tables, 34 validation tables,
and 18 prior audit documents. Definitive report:
[`docs/final_repo_verification_20260313.md`](docs/final_repo_verification_20260313.md)

**Verdict:** Manuscript-ready | Not dataset-mature | Extraction pipeline complete

#### Verification matrix (domain-by-domain, at time of initial audit)

> **Note:** Several domains below were subsequently improved by the
> maturation/hardening passes documented in later release entries.
> See `docs/FINAL_REPO_STATUS_20260313.md` for the current status.

| Domain | Verdict (initial) | Updated status | Key evidence |
|--------|---------|----------------|-------------|
| Demographics | VERIFIED | — | 99% age, 93% sex/race |
| Surgery | VERIFIED | — | 100% date coverage |
| Pathology | MOSTLY VERIFIED | — | 90% extraction completeness |
| Molecular | PARTIALLY VERIFIED | RAS flag backfilled (325 rows) | 546 BRAF, 337 RAS, 108 TERT extracted |
| Imaging | NOT VERIFIED | **Resolved:** `imaging_nodule_master_v1` populated (19,891 rows) | Maturation pass fixed schema mismatch |
| RAI | PARTIALLY VERIFIED | Dose coverage 3% → 41% | `scripts/76_canonical_gap_closure.py` Phase B |
| Recurrence | PARTIALLY VERIFIED | Review queue deployed | 1,986 flagged; 1,764 dates unresolved (structural) |
| Complications | MOSTLY VERIFIED | — | 7 entities refined |
| Operative Notes | NOT VERIFIED | CND/LND flags wired (2,497/241); NLP enrichment still 0% | Pipeline architecture gap |
| Manuscript Metrics | VERIFIED | — | 11 metrics pass cross-source consistency |

#### Database hardening audit
[`docs/database_hardening_audit_20260313.md`](docs/database_hardening_audit_20260313.md)
- 0 critical blocking issues for manuscript use
- 0 row multiplication problems
- 0 identity integrity failures
- 1,155 cross-domain consistency flags (benign-procedure patients lacking operative records)

#### Readiness gates — all 7 PASS
- G1: 0 patient duplicates
- G2: 0 episode duplicates (146 resolved via dedup)
- G3: AJCC8 37.6%, MACIS 37.5%, AMES 100%, AGES 100%
- G4: 7 refined complication entity types
- G5: All 15 supporting tables populated
- G6: 0 null research_ids
- G7: SAP exists

#### Analysis-resolved layer (scripts 48–55)
- `patient_analysis_resolved_v1` — 10,871 rows, one per patient
- `episode_analysis_resolved_v1_dedup` — 9,368 rows (146 duplicates resolved)
- `lesion_analysis_resolved_v1` — 11,851 rows
- `manuscript_cohort_v1` — 10,871 rows, 139 columns, frozen
- `thyroid_scoring_py_v1` — AJCC8, ATA, MACIS, AGES, AMES per patient
- `complication_phenotype_v1` — 5,928 phenotyped events
- `longitudinal_lab_clean_v1` — 38,699 clean lab values
- `recurrence_event_clean_v1` — 1,946 clean recurrence events

#### Publication bundle
`exports/FINAL_PUBLICATION_BUNDLE_20260313/` — 62 files:
- Tables 1–3 (CSV + Markdown + LaTeX)
- Figures 1–5 (300 DPI PNG + SVG)
- Cox PH results, KM summary, logistic models
- `master_clinical_v12` (12,886 patients, 136 columns)
- `manuscript_cohort_v1` (10,871 patients, 139 columns)
- `readiness_assessment.json`, `manifest.json`
- Phase 13 final report

#### Manuscript reconciliation
[`docs/manuscript_metric_reconciliation_20260313.md`](docs/manuscript_metric_reconciliation_20260313.md)
- 11 canonical metrics with explicit numerators, denominators, population labels
- 0 metric mismatches across sources
- Metric SQL registry exported to `exports/manuscript_reconciliation_20260313_0708/`

#### Additional March 13 audit documents

| Document | Scope |
|----------|-------|
| [`docs/canonical_backfill_report_20260313.md`](docs/canonical_backfill_report_20260313.md) | 1,988 cells backfilled (RAI dose, RAS flag, linkage IDs) |
| [`docs/provenance_date_audit_20260313.md`](docs/provenance_date_audit_20260313.md) | Provenance + date accuracy audit |
| [`docs/operative_nlp_motherduck_propagation_20260313.md`](docs/operative_nlp_motherduck_propagation_20260313.md) | Operative NLP gap analysis |
| [`docs/operative_note_path_linkage_audit_20260313.md`](docs/operative_note_path_linkage_audit_20260313.md) | Op-note ↔ pathology linkage |
| [`docs/hp_discharge_note_audit_20260313.md`](docs/hp_discharge_note_audit_20260313.md) | H&P / discharge note coverage |
| [`docs/imaging_nodule_materialization_20260313.md`](docs/imaging_nodule_materialization_20260313.md) | Imaging nodule master gap |
| [`docs/manuscript_freeze_alignment_20260313.md`](docs/manuscript_freeze_alignment_20260313.md) | Denominator language alignment |

#### Open backfill items (not blocking manuscript)

1. Operative note NLP enrichment → canonical table (extractor exists)
2. RAI dose propagation from `extracted_rai_dose_refined_v1` (307 doses)
3. RAS flag propagation from `extracted_ras_subtypes_v1` (316+ patients)
4. Linkage ID backfill from V3 linkage tables
5. Imaging nodule master materialization (19,891 TIRADS rows available)
6. Recurrence date enrichment (structural sparsity)

---

## v2026.03.11-predictive-workbench-v2 (Previous)
**Date:** 2026-03-11
**Patients:** 11,673 | **Enhancement:** Phase 4.5 — Predictive Analytics Workbench Polish

### Polished Features (4 high-impact feedback items)

#### Competing Risks Enhancement
- **Gray's landmark CIF tests** — Pepe-Mori z-test approximation comparing AJ CIF at 1/3/5/10-year landmarks between strata pairs; significance stars (\*/\*\*/\*\*\*)
- **Cause-specific log-rank** — lifelines pairwise log-rank on cause-specific hazards (etiology complement to CIF comparison)
- **KM overlay** on CIF plot — dashed gray `1 − KM` curve showing standard overestimate when ignoring competing risks
- **CI bands** — shaded confidence intervals from AJ variance on primary CIF curve
- **Enhanced clinical note** — structured as two complementary questions (etiology via Cox HR vs. patient counseling via CIF); references Pepe-Mori 1993 and Gray 1988

#### Cure Calculator Expansion
- **12 features** (8 core + 4 advanced): added `any_nsqip_complication`, `rln_injury`, `hypocalcemia`, `rai_received` in an **Advanced** expander
- **Sensitivity analysis** — `sensitivity_analysis()` varies each feature independently, returns cure-probability swing sorted by absolute impact
- **Advanced clinical context** — `_advanced_feature_interpretation()` generates NSQIP/RAI surveillance notes (doesn't modify PTCM theta)
- **Smart defaults note** — cohort-derived defaults with explicit reference to most common profile
- **Sensitivity note** — hybrid theta adjustment disclosure (Tuttle 2017, Adam 2015, ATA 2016)

#### Model Comparison Dashboard
- **Two-panel Plotly figure** — concordance bars + AIC bars side-by-side with per-model color coding
- **Clinical recommendation engine** — `_model_comparison_recommendation()` picks best discriminative model, best AIC fit, and available cure models
- All 6 models confirmed: KM, Cox PH, Weibull PTCM, Mixture Cure, Penalized Cox Ridge, Random Survival Forest

#### Dashboard Tab Polish
- **Sidebar Quick Launch card** — reads PTCM `analysis_metadata.json` and displays π̄, N, events, AIC in a compact card; points to Cure Calculator tab
- Tab positioning confirmed: 🔮 Predictive Analytics immediately after 📊 Statistical Analysis

### Updated Deliverables
- `notebooks/40_predictive_analytics_examples.ipynb` — added sensitivity analysis, NSQIP advanced features, Gray's test display, model recommendation cells
- `scripts/40_predictive_analytics_batch.py` — expanded to 5 phases: model comparison, competing risks + Gray's tests, batch scoring, sensitivity archetypes (3 patient profiles), manuscript report

---

## v2026.03.11-predictive-workbench
**Date:** 2026-03-11
**Patients:** 11,673 | **Enhancement:** Integrated Predictive Analytics & Comparative Survival Workbench

### Predictive Analytics & Nomograms (New Dashboard Tab: "🔮 Predictive Analytics")

#### `utils/predictive_analytics.py` — `ThyroidPredictiveAnalyzer` class
- `predict_individual_cure_probability()` — PTCM-powered personalized cure estimation with cure tier (very_high/high/moderate/low), conditional survival at 1/3/5/10/15 years, feature contribution analysis (Δθ per covariate), and clinical interpretation text
- `predict_cure_batch()` — score a DataFrame of patients with PTCM cure_prob and cure_tier columns
- `fit_competing_risks()` — enhanced Aalen-Johansen CIF with stratified curves, cause-specific Cox HRs per event type, landmark summaries at 1/3/5/10 years
- `train_explainable_nomogram()` — XGBoost, Random Forest, or scikit-survival Random Survival Forest with SHAP beeswarm/bar plots, calibration assessment, cross-validated AUC/Brier
- `compare_survival_models()` — unified comparison of KM, Cox PH, PTCM, and RSF: concordance, AIC, event counts, model-specific notes
- `create_interactive_cure_calculator()` — returns feature specs + prediction function for Streamlit widget wiring
- `generate_manuscript_report()` — Word doc (.docx) with PTCM, competing risks, nomogram, and comparison sections
- `plot_individual_cure_trajectory()` — personalized PTCM survival curve vs population average
- Thyroid-specific presets: `PREDICTIVE_PRESETS` (recurrence, death), `CURE_CALCULATOR_FEATURES` (6 clinical inputs), `CLINICAL_INTERPRETATIONS` (per-tier clinical guidance)

#### `app/predictive_analytics.py` — 5 sub-tabs in "🔮 Predictive Analytics" dashboard tab
- **Model Comparison Hub** — one-click comparison of KM/Cox/PTCM/RSF with concordance bar chart
- **Competing Risks Analysis** — preset selector, optional stratification, CIF curves, cause-specific HRs
- **ML Nomograms & SHAP** — model type toggle, SHAP importance + beeswarm, calibration, individual risk prediction
- **Personalized Cure Calculator** — slider/toggle inputs for 6 clinical features → live PTCM cure probability with trajectory plot, clinical interpretation, and feature contribution table
- **Manuscript Export** — section picker, title/author inputs, Word document generation + download

#### `scripts/39_promotion_time_cure_models.py` — new prediction API
- `predict_cure_probability()` — reusable function for single-patient PTCM scoring
- `load_fitted_params()` — loads fitted params from CSV exports for external callers

#### Enhancements (v2 — post-review refinements)
- **Competing risks**: Added sksurv CIF confidence bands, clinical methodology note ("Cause-specific HRs for etiological insight; CIF for real-world probability; Fine-Gray planned for v2")
- **Cure calculator**: Extended to 8 inputs — added tumor size (continuous 0.1–10cm) and lymph node status (N0/N1a/N1b/Nx) with hybrid theta adjustment (literature-derived multipliers from Tuttle 2017, Adam 2015, ATA 2016)
- **Model comparison**: Added Weibull mixture cure model (MLE via scipy, AIC-comparable with PTCM) and Penalized Cox Ridge (sksurv CoxnetSurvivalAnalysis) — now 6 models total
- **Tab position**: Moved "🔮 Predictive Analytics" to directly after "📊 Statistical Analysis" (high-impact-first UX)

#### Additional Deliverables
- `notebooks/40_predictive_analytics_examples.ipynb` — 5-section interactive walkthrough (cure prediction, competing risks, model comparison, SHAP nomogram, manuscript export)
- `scripts/40_predictive_analytics_batch.py` — CLI batch runner (4 phases: comparison, competing risks, batch PTCM scoring, manuscript report); supports `--md`, `--local`, `--dry-run`

#### Dependencies
- `requirements.txt` — added `scikit-survival`

---

## v2026.03.11-advanced-analytics
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

## v2026.03.10-v3-dashboard
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
