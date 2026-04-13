# ETE Manuscript — Master Document

**Working title:** Microscopic extrathyroidal extension (mETE) in papillary thyroid carcinoma: ordinal and structural evidence under AJCC 8.

**Version:** master, branch `ete-remediation-20260413`
**Last revised:** 2026-04-13
**Source of truth:** this file. All quantitative claims trace to the locked artifacts in `artifacts/ete_export_freeze_manifest.json` and the frozen audit bundle keyed by `studies/proposal2_ete_staging/analysis_metadata.yaml` (audit_generated 2026-03-10; random seed 42).

**Status framing (non-negotiable).** This draft uses frozen numerics. It does **not** claim a "fresh fully updated live-database reanalysis". Any promotion of refreshed numerics requires (a) export refresh + lock, (b) AJCC7 unification landed on main (completed on this branch), (c) PSM policy (completed: anchor = 711 pairs, reruns treated as sensitivity), (d) release-governance gates green. See §9 and the companion `artifacts/ete_release_governance_status.md`.

---

## 1. Abstract

**Background.** Microscopic extrathyroidal extension (mETE) has a contested role in AJCC 8th edition T-staging and in risk stratification for papillary thyroid carcinoma (PTC). Prior AJCC 7th-edition staging assigned T3b to T4a; the current analysis corrects this mapping to T3.

**Methods.** We studied 3,278 patients with PTC from a curated institutional cohort; a classic-variant subset comprised 596 patients. Extrathyroidal extension was classified as absent, microscopic, or gross. The primary analysis modeled the institutional recurrence-risk band (ordinal) with mETE as the contrast of interest, adjusted for age, sex, tumor size, and lymph node ratio. A pre-specified secondary analysis used a structural burden endpoint (imaging-based pathologic cervical lymphadenopathy OR reoperation proxy) and compared mETE with no ETE after 1:1 propensity matching (711 pairs; caliper 0.05 on age, sex, tumor size, and N1 status). Two-sided alpha was 0.05; random seed 42 throughout.

**Results.** In the expanded complete-case ordinal model (N=3,269), mETE was associated with **lower** odds of a higher risk category (OR 0.60, 95% CI 0.51-0.72). Older age predicted higher risk category (OR 1.05 per year). In matched analysis (711 pairs), structural burden was more frequent with mETE (14.49% vs 10.55%; risk difference 3.9 percentage points; OR 1.43, Fisher P=0.030) with imperfect post-match balance for nodal disease (SMD -0.58 for N1). Formal interaction testing showed no mETE x age interaction on the structural endpoint (P=0.26) but a significant mETE x N1 interaction (P=0.006). Among patients >=55 years, the ordinal mETE association attenuated toward null (OR 0.87, P=0.35) versus younger patients (OR 0.44, P<0.001).

**Conclusions.** In this cohort, mETE tracks substantially higher baseline nodal burden and, after propensity matching that did not fully neutralize nodal disease, is associated with modestly higher structural burden. The interaction of mETE with N1 status, not with age, is the clinically relevant effect modifier on the structural endpoint. AJCC7 T3b-to-T4a migration was re-examined: under canonical AJCC 7 rules T3b maps to T3, which reclassifies 276 tumor-1-level records in `ptc_full.csv` (346 at the wider audit level).

---

## 2. Introduction

Extrathyroidal extension is a staging criterion in PTC. AJCC 8 re-defined mETE out of T3 and into T1-T2 on the basis that microscopic extension alone has limited prognostic value at 5-10 years. Multiple retrospective cohorts have since re-examined whether mETE confers residual risk once other anatomic features are held constant, and whether age modifies that association. Two reporting issues recur in the literature: (1) conflation of baseline nodal prevalence with effect-modification terms, and (2) incidental propensity-score instability that shifts reported matched-pair counts and odds ratios between implementations. This manuscript separates those issues explicitly and reports frozen primary numerics alongside a deterministic sensitivity frame.

## 3. Methods

### 3.1 Design and cohort

Retrospective single-institution cohort. The expanded analytic file merged deduplicated PTC rows from `recurrence_full.csv`, `ptc_full.csv`, and patient-level imaging flags from `imaging_correlation.csv` via `studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py::load_expanded`. Classic-variant restrictions for the 596-patient primary descriptive and ordinal sensitivity cohort follow `proposal2_ete_analysis.py`.

### 3.2 ETE classification

Gross ETE was defined as `tumor_1_gross_ete = 1`. Any pathologic extrathyroidal extension without gross criteria was classified as microscopic ETE (`tumor_1_extrathyroidal_ext = True` and not gross). Absence of extension was classified as no ETE.

### 3.3 Endpoints

The **primary** ordinal outcome was the three-level recurrence risk band (`risk_ord`: low, intermediate, high). Because the high-risk tier is assigned to all gross-ETE patients by construction, gross-ETE coefficients in ordinal models are not interpreted independently; mETE is the primary contrast. The **secondary** binary structural burden endpoint combined CT/MRI pathologic lymphadenopathy flags (`ct_pathologic_ln_flag` OR `mri_pathologic_ln_flag`) and a reoperation proxy (>1 distinct surgery date in the pathology-linked surgery listing).

### 3.4 AJCC 7th-edition mapping (T-stage)

Canonical AJCC 7 maps T3b to T3 (not T4a). The helper module `studies/proposal2_ete_staging/ajcc7_mapping.py` enforces this mapping across the executable analysis path (`proposal2_ete_analysis.py::derive_ajcc7_t_stage` and `proposal2_expanded_cohort.py::derive_ajcc7`). A regression test (`tests/test_ajcc7_mapping.py::test_no_stale_t3b_to_t4a_in_executable_paths`) guards against re-introduction of the stale mapping.

### 3.5 Propensity score matching

Among patients without gross ETE, mETE was compared to no ETE using logistic-regression propensity scores with covariates age, sex, tumor size, and N1 indicator. 1:1 greedy nearest-neighbour matching without replacement was applied within caliper 0.05 on the propensity scale (random seed 42). Balance was assessed by standardized mean differences before and after matching. Binary structural effects used Fisher's exact test and Haldane-Anscombe-adjusted odds ratios.

**Anchor policy.** The frozen 711-pair manuscript PSM result is the anchor for all PSM numerics reported in this manuscript. Reruns on refreshed exports are sensitivity analyses only. The determinism contract is documented in `artifacts/ete_psm_stability_report.md` and enforced by tests in `tests/test_psm_determinism.py`.

### 3.6 Interactions, stratification, and sensitivity analyses

Logistic interaction terms were tested for mETE x tumor size, mETE x age, and mETE x N1. Pre-specified tumor-size strata were <=1, 1-2, and 2-4 cm. Sensitivity analyses included aggressive-histology subsets, multiple imputation (m=20, predictive mean matching, seed 42, Rubin pooling), and age thresholds.

### 3.7 Software

Python 3.14.2; pandas 2.3.3, numpy 2.4.3, scipy 1.17.1, statsmodels 0.14.6, scikit-learn 1.8.0, lifelines 0.30.3. Two-sided alpha = 0.05. Random seed 42 for stochastic steps.

---

## 4. Results

### 4.1 Cohort description

The expanded PTC cohort comprised 3,278 patients (classic variant subset N=596). Baseline N1 prevalence differed across ETE groups: 56.9% (no ETE), 67.2% (microscopic ETE), 74.7% (gross ETE); P<0.001.

### 4.2 Ordinal regression (primary)

In the expanded complete-case model (N=3,269), mETE was associated with lower odds of a higher recurrence-risk band (OR 0.60, 95% CI 0.51-0.72). Age (OR 1.05 per year, P<0.001) and lymph-node ratio (OR 1.31, P=0.032) retained positive associations. Multiple-imputation pooled mETE OR was 0.602, concordant with complete-case estimates. High-versus-not-high discrimination ROC AUC was 0.851 (base, 5-fold CV) and 0.876 (full model including mETE).

### 4.3 Propensity-matched structural burden (secondary)

After matching (711 pairs), structural burden was 14.49% (mETE) versus 10.55% (no ETE); risk difference 3.94 percentage points; odds ratio 1.434 (Haldane-Anscombe); Fisher P=0.030. Post-match SMD for the N1 indicator was -0.576 (worsened from pre-match 0.22), indicating incomplete nodal balance despite inclusion in the propensity model.

### 4.4 Interactions

Structural logistic interactions on mETE: mETE x age OR 0.99 (95% CI 0.97-1.01, P=0.258); mETE x N1 OR 0.36 (95% CI 0.17-0.74, P=0.006); mETE x tumor size non-significant.

### 4.5 Age attenuation (descriptive)

Expanded ordinal sensitivity: age >=55 mETE OR 0.87 (0.64-1.17, P=0.352) versus age <55 mETE OR 0.44 (0.35-0.56, P<0.001). This is descriptive attenuation, not formal effect modification.

### 4.6 AJCC7 downstream

Under canonical AJCC7 (T3b -> T3), 276 tumor-1-level records shift from T4a to T3 in `ptc_full.csv` (N=2844). The wider audit-level count is 346. No other T-stage cell changes. Crosswalk in `artifacts/ete_ajcc7_diff.md`.

---

## 5. Discussion

We separated three distinct issues. First, **baseline** nodal prevalence is higher in mETE than in no-ETE patients; this is descriptive. Second, formal **interaction** testing on the structural endpoint yielded a significant mETE x N1 term and a non-significant mETE x age term. Third, descriptive attenuation of the mETE association with the composite ordinal risk category in older adults should not be labeled as effect modification without a significant product term.

Propensity matching did not fully neutralize N1 imbalance (SMD -0.58 after matching). Structural comparisons are therefore framed as hypothesis-generating and potentially confounded by residual lymphatic burden. The re-executed propensity match on current exports produced pair counts of 711-712 depending on row order and sklearn version (documented in `artifacts/ete_psm_stability_report.md`); the frozen 711-pair result remains the anchor for reporting. Any divergence outside the +-5-pair sensitivity band triggers a governance review.

The AJCC 7th-edition correction (T3b -> T3) does not alter the primary mETE findings. It does reclassify 276 tumor-1 records and is reported for reproducibility of ancillary tables.

## 6. Limitations

Single-institution retrospective design; imperfect post-match nodal balance; reoperation proxy is indirect; thyroglobulin-based follow-up uses last-observed date as censor; `ln_ratio` is quasi-binary and 84%+ missing at the forensics patient layer; propensity-score implementation is sensitive to row order and sklearn minor version (mitigated by the determinism contract on this branch but historically a source of discrepancy).

## 7. Conclusion

Microscopic ETE marks patients with heavier baseline nodal burden. After imperfect propensity matching, mETE is associated with modestly higher structural burden (OR 1.43, P=0.030); the clinically relevant effect modifier on the structural endpoint is N1 status, not age. AJCC 7 T-stage should apply T3b -> T3; the canonical mapping has been enforced in the executable analysis path.

---

## 8. Key numerics table

| Quantity | Value | Source |
|----------|------:|--------|
| Expanded PTC N | 3278 | `analysis_metadata.yaml` cohort |
| Classic analytic N | 596 | `tables/analytic_cohort.csv` |
| Expanded ordinal mETE OR (CC) | 0.603 | `table3_ordinal_regression.csv` |
| Expanded ordinal mETE 95% CI | 0.505-0.720 | same |
| Expanded ordinal age OR | 1.050 | same |
| Expanded ordinal ln_ratio OR | 1.309 | same |
| MI mETE OR | 0.602 | `analysis_metadata.yaml` MI |
| AUC base CV mean / full | 0.851 / 0.876 | `analysis_metadata.yaml` auc |
| PSM pairs (frozen) | 711 | `table6_propensity_matching_effect.csv` |
| No ETE structural % / mETE % | 10.55 / 14.49 | same |
| Structural OR (frozen) | 1.434 | same |
| Fisher p (frozen) | 0.030 | same |
| SMD N1 after match | -0.576 | `table6_propensity_matching_balance.csv` |
| mETE x age p | 0.258 | `table8_interaction_tests.csv` |
| mETE x N1 p | 0.006 | same |
| AJCC7 T3b->T3 reclassifications (tumor-1) | 276 | `artifacts/ete_ajcc7_diff.md` |
| AJCC7 T3b->T3 reclassifications (audit) | 346 | `studies/proposal2_ete_staging/audit_report.md` |

## 9. Reproducibility and provenance statement

- Export source: Branch A (frozen). Input SHA-256 locked in `artifacts/ete_export_freeze_manifest.json`.
- AJCC 7 mapping: canonical T3b -> T3; enforced by `ajcc7_mapping.py` and `tests/test_ajcc7_mapping.py`.
- PSM anchor: 711 pairs (frozen manuscript result). Rerun policy and instability report in `artifacts/ete_psm_stability_report.md`; determinism tests in `tests/test_psm_determinism.py`.
- Claim boundary: this manuscript does **not** advertise a "fresh fully updated live-database reanalysis". That claim requires governance promotion described in `artifacts/ete_release_governance_status.md`.
- Software, seeds, and environment versions: see §3.7 and `analysis_metadata.yaml`.

## 10. Numeric traceability

All quantitative claims in sections 1-8 trace to entries in `artifacts/ete_manuscript_numeric_manifest.json`, which pins every number to its frozen-artifact source path and SHA-256.
