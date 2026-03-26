# STROBE / TRIPOD Gap Check for Manuscript Submission

**Date:** 2026-03-26

## STROBE Checklist (Observational — cross-sectional/cohort)

| Item | Description | Status | Location / Gap |
|------|-------------|--------|----------------|
| 1a | Title: study design | ✅ | Title indicates retrospective cohort |
| 1b | Abstract: structured summary | ✅ | `abstract_only.md`, `manuscript_full_draft.md` |
| 2 | Background / rationale | ⚠️ | Brief in analysis_plan.md; needs expansion for submission |
| 3 | Objectives / hypotheses | ⚠️ | Implicit (predictors of total thyroidectomy); should be stated explicitly |
| 4 | Study design | ✅ | Retrospective cohort; stated in methods |
| 5 | Setting | ⚠️ | Single-center; dates/location not explicit in current draft |
| 6a | Eligibility criteria | ✅ | Preop imaging 2–4 cm; exclusions documented in supplement |
| 6b | Sources and methods of selection | ✅ | `cohort_flow.csv`, `cohort_build_log.md` |
| 7 | Variables | ✅ | Predictors + outcome defined; `supplement_exclusions_and_definitions.csv` |
| 8 | Data sources / measurement | ⚠️ | MotherDuck database noted; detailed measurement methods (e.g., nodule sizing protocol) missing |
| 9 | Bias | ❌ **GAP** | No formal bias discussion; selection bias from single-center, exclusion of LN+ not discussed |
| 10 | Study size | ✅ | N=558 documented; no a priori power calculation (common for retrospective) |
| 11 | Quantitative variables | ✅ | Bethesda ≥4 binarized, age continuous, documented |
| 12a | Statistical methods | ✅ | Multivariable logistic regression; `study_pipeline.py` |
| 12b | Subgroup / interaction analyses | ✅ | Molecular subset, platform-specific (clearly exploratory) |
| 12c | Missing data handling | ⚠️ | bethesda_ge4 fillna(0) documented; need explicit statement re: imputation approach |
| 12d | Sensitivity analyses | ✅ | Broad nodal exclusion + bethesda complete-case in `sensitivity_summary.csv` |
| 12e | Loss to follow-up | N/A | Cross-sectional outcome (initial procedure type) |
| 13a | Participants: flow diagram | ✅ | `fig_cohort_flow.png`, `cohort_flow.csv` |
| 13b | Non-participation reasons | ⚠️ | Exclusions counted; reasons for missing data not individually traced |
| 14a | Descriptive data: characteristics | ✅ | `baseline_table_primary.csv`, `baseline_table_broad_nodal.csv` |
| 14b | Missing data by variable | ✅ | `missingness_summary.csv` |
| 15 | Outcome data | ✅ | N events documented in `model_summary_final.csv` |
| 16a | Main results: unadjusted + adjusted | ✅ | `univariable_tests.csv` + `table2_*.csv`, `model_summary_final.csv` |
| 16b | Category boundaries | ✅ | Bethesda ≥4 threshold documented |
| 16c | Relative/absolute measures | ⚠️ | ORs reported; predicted probabilities/marginal effects not in current output |
| 17 | Other analyses | ✅ | Molecular, concordance, completion tables |
| 18 | Key results | ⚠️ | `journal_style_results.md` is skeleton; needs elaboration |
| 19 | Limitations | ❌ **GAP** | Missing dedicated limitations paragraph |
| 20 | Interpretation | ⚠️ | Brief; needs contextual comparison with literature |
| 21 | Generalizability | ❌ **GAP** | Not discussed; single-center academic medical center |
| 22 | Funding | ❌ **GAP** | Not stated |

## TRIPOD Items (applicable to primary/extended models as prediction models)

| Item | Description | Status | Gap |
|------|-------------|--------|-----|
| 1 | Title identifies prediction model | ⚠️ | Title is descriptive; explicitly name "prediction" if intended |
| 2 | Abstract: prediction context | ⚠️ | Abstract focuses on association, not prediction |
| 3a | Background: prediction rationale | ❌ **GAP** | Not framed as a prediction model study |
| 5b | Specify development/validation | ❌ **GAP** | Internal validation (bootstrap optimism) done but not discussed in text |
| 10a | Sample size for EPV | ✅ | EPV = 320/4 = 80 (adequate) |
| 10b | Number of events | ✅ | 320 events for primary models |
| 10d | Model performance measures | ✅ | `model_performance.csv` (AUC, Brier, calibration, optimism-corrected AUC) |
| 15a | Discrimination (AUC) | ✅ | Provided in `model_performance.csv` |
| 15b | Calibration | ✅ | Intercept + slope in `model_performance.csv` |
| 16 | Optimism correction | ✅ | Bootstrap optimism in `model_performance.csv` |
| 19 | Supplementary: full model | ✅ | All coefficients in `logistic_*.csv` and `model_summary_final.csv` |

## Priority gaps for submission

1. **Bias discussion (STROBE 9)** — Describe selection bias, information bias, confounding
2. **Limitations paragraph (STROBE 19)** — Single-center, retrospective, molecular testing rate ~4%
3. **Generalizability (STROBE 21)** — Academic medical center; practice patterns may differ
4. **Missing data statement (STROBE 12c)** — Explicit: bethesda missing coded as non-≥4; sensitivity in complete-case analysis
5. **Funding/COI (STROBE 22)** — Required by most journals
6. **Prediction framing (TRIPOD 3a, 5b)** — If models are presented as predictive, frame explicitly; otherwise label as "association study"
7. **Setting details (STROBE 5)** — Year range, institution type, referral patterns
