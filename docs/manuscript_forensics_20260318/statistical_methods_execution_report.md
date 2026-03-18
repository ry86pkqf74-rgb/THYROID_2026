# Statistical Methods Execution Report
## Manuscript Forensics — THYROID_2026 ETE Staging Study
### Generated: 2026-03-18

---

## 1. Executive Summary

The ETE staging manuscript is a retrospective cohort analysis of papillary thyroid carcinoma (PTC) patients evaluating the impact of microscopic extrathyroidal extension (mETE) versus gross ETE on AJCC 8th edition staging, recurrence risk stratification, and structural disease outcomes. The analytical architecture consists of six Python scripts in `studies/proposal2_ete_staging/` that operate on `risk_enriched_mv` (a MotherDuck materialized table joining `recurrence_risk_features_mv` and `survival_cohort_ready_mv`). The primary analysis uses N=596 classic PTC patients; sensitivity analyses expand to N=3,278 all-PTC patients. Analyses include: (1) stage migration quantification (AJCC 7→8, McNemar test), (2) ordinal logistic regression for recurrence risk band with multiple imputation (m=20, Rubin's rules), (3) propensity score matching (1:1, caliper=0.05, N=711 pairs), (4) Cox proportional hazards (L2-penalized, penalizer=0.1), (5) Kaplan-Meier disease-free survival with log-rank tests, (6) tumor-size–stratified logistic models, (7) interaction tests (mETE × size, mETE × age, mETE × nodal status), and (8) aggressive-variant safety analysis. All analyses use seed=42 for reproducibility, Python 3.14.2, and specific package versions documented in `analysis_metadata.yaml`.

---

## 2. Cohort Construction

### 2.1 Starting Population

| Step | Source | N | Script |
|------|--------|---|--------|
| All surgical thyroid patients | `path_synoptics` → `master_cohort` | 10,871 | scripts/03_research_views.py |
| PTC histology filter | `tumor_pathology.histology_1_type = 'PTC'` | ~6,630 | scripts/03_research_views.py (`ptc_cohort`) |
| Risk enrichment join | `recurrence_risk_features_mv` LEFT JOIN `survival_cohort_ready_mv` | ~6,630 | scripts/13_performance_optimizations_pack.py |
| Expanded PTC cohort | Merge ptc_full.csv + recurrence_full.csv + imaging_correlation.csv, deduplicate | **3,278** | proposal2_expanded_cohort.py `load_all_ptc()` |
| Classic PTC only | Filter to classic/unspecified variant | **596** | proposal2_ete_analysis.py `load_data()` |

### 2.2 Analysis-Specific Cohorts

| Analysis | N | Additional Filters | Script |
|----------|---|-------------------|--------|
| **Primary ordinal regression** | 593 (complete-case) | Non-missing: risk_ord, ete_micro, ete_gross, age, sex, tumor_size, ln_ratio | proposal2_ete_analysis.py |
| **Multiple imputation ordinal** | 596 (all) | m=20 imputations for ln_ratio, tumor_size, tg_max | proposal2_recommendations.py |
| **Stage migration** | 593 | Non-missing AJCC7 and AJCC8 stages | proposal2_ete_analysis.py |
| **PSM (mETE vs NoETE)** | 711 matched pairs | Exclude Gross ETE; non-missing age, sex, tumor_size, n_positive_flag | proposal2_endpoint_psm_strata.py |
| **Cox PH survival** | ~5,794 | time_to_event_days > 0, event_occurred not null | proposal2_cox_regression.py |
| **Expanded ordinal (Cohort A)** | 3,269 CC / 3,278 MI | All PTC, relaxed inclusion | proposal2_expanded_cohort.py |
| **Expanded ordinal (Cohort B)** | 2,157 CC / 2,166 MI | Classic + unspecified PTC | proposal2_expanded_cohort.py |
| **Aggressive variant safety** | Variable | is_aggressive = True (tall cell, columnar, hobnail, diffuse sclerosing, solid) | proposal2_expanded_cohort.py |

### 2.3 Key Discrepancy Notes

- The primary analysis (N=596) is a strict classic-PTC subset of the expanded cohort (N=3,278).
- The Cox analysis (N~5,794) queries `risk_enriched_mv` directly, which has broader inclusion than the 3,278-row CSV export.
- The PSM analysis runs on the expanded cohort (3,278) excluding Gross ETE.

---

## 3. Variable Derivation

### 3.1 ETE Classification

**Script:** `proposal2_ete_analysis.py::classify_ete()` (and identically in all other scripts)

```python
# Three-level classification from pathology fields:
# Source columns: tumor_1_extrathyroidal_ext, tumor_1_gross_ete, tumor_1_ete_microscopic_only
def classify_ete(df):
    conditions = [
        df["tumor_1_gross_ete"].fillna("").str.lower().isin(["yes","true","1","x"]),
        df["tumor_1_ete_microscopic_only"].fillna("").str.lower().isin(["yes","true","1","x"]),
        df["tumor_1_extrathyroidal_ext"].fillna("").str.lower().str.contains("yes|minimal|microscopic|present"),
    ]
    # Gross ETE → "Gross ETE"
    # Microscopic only → "Microscopic ETE"  
    # Any other ETE text → "Microscopic ETE"
    # Otherwise → "No ETE"
```

**Result (N=3,278):** No ETE=724 (22.1%), Microscopic ETE=1,736 (53.0%), Gross ETE=818 (25.0%)

### 3.2 AJCC 7th Edition T-Stage Derivation

**Script:** `proposal2_ete_analysis.py::derive_ajcc7_t_stage()` and `proposal2_expanded_cohort.py::derive_ajcc7()`

```python
# AJCC 7: mETE → T3 (unlike AJCC 8 which does NOT upstage for mETE)
# Size rules: ≤1cm → T1a, 1-2cm → T1b, 2-4cm → T2, >4cm → T3a
# Microscopic ETE override: any mETE → T3
# Gross ETE: T3b → T4a (invasion beyond thyroid capsule)
# Overall staging: Age <45 → I (M0) or II (M1); Age ≥45 → T/N/M-dependent
```

**CRITICAL AUDIT NOTE:** T3b→T4a mapping affected 346 patients. The audit script (`audit_reproduce.py`) corrected this to T3b→T3, resulting in 346 T-stage and 46 overall-stage reclassifications.

### 3.3 Recurrence Risk Band

**Source:** `recurrence_risk_features_mv` (script 10)

```sql
CASE
    WHEN overall_stage ILIKE 'III%' OR overall_stage ILIKE 'IV%' 
         OR tumor_1_gross_ete IS NOT NULL 
         OR tg_max >= 10 THEN 'high'
    WHEN overall_stage ILIKE 'II%' OR tg_max >= 2 THEN 'intermediate'
    ELSE 'low'
END AS recurrence_risk_band
```

**AUDIT WARNING:** 100% of gross ETE patients are classified as high risk by construction — the risk band includes gross ETE in its derivation, inflating the gross ETE OR. The **mETE OR is the clinically meaningful coefficient.**

### 3.4 Structural Disease Endpoint

**Script:** `proposal2_endpoint_psm_strata.py::derive_core_vars()`

```python
structural_recurrence = (
    (ct_pathologic_ln_flag == 1)  # CT/MRI pathologic lymphadenopathy
    OR
    (count of surgery_dates > 1)  # reoperation proxy
)
```

**Counts (N=3,278):** 504 structural events (497 imaging proxy, 7 reoperation proxy)

### 3.5 DFS (Disease-Free Survival)

**Script:** `proposal2_endpoint_psm_strata.py::derive_core_vars()`

```python
dfs_years = (tg_last_date - surgery_date).days / 365.25
# If tg_last_date missing → use surgery_date (0 follow-up)
dfs_event = structural_recurrence  # 0/1
```

**Censoring:** At last thyroglobulin measurement date. Patients without Tg labs are censored at surgery (dfs_time=0).

### 3.6 Stage Migration Variables

```python
stage7_num = mapping(overall_stage_ajcc7)  # I→1, II→2, III→3, IVA→4, IVB/IVC→5
stage8_num = mapping(overall_stage_ajcc8)
downstaged = (stage8_num < stage7_num)
upstaged = (stage8_num > stage7_num)
```

### 3.7 Histology Subgroup Flags

```python
classic_variant = variant_standardized contains "classic" or "unspecified"
aggressive_variant = variant_label in [
    "Tall cell", "Columnar cell", "Solid variant", "Diffuse sclerosing"
]
```

### 3.8 Nodal Status / LN Ratio

```python
ln_ratio = ln_positive / ln_examined  # continuous, 0-1
n_positive_flag = (ln_positive > 0)   # binary
```

**AUDIT WARNING:** `ln_examined` is effectively binary for 83% of patients (583/3,278). LN ratio may function as a binary indicator rather than continuous.

### 3.9 M-Stage Handling

```python
m_stage_final = m_stage_ajcc8 if available else "M0"
# Missing M-stage defaulted to M0 (available for only 18.0% of expanded cohort)
```

---

## 4. Primary Composite Endpoint Model (Ordinal Logistic Regression)

### 4.1 Identification

| Item | Value |
|------|-------|
| **Script** | `proposal2_ete_analysis.py::adverse_features_analysis()` |
| **Model class** | `statsmodels.miscmodels.ordinal_model.OrderedModel` |
| **Distribution** | Logistic (`distr="logit"`) |
| **Optimizer** | BFGS (default) |
| **Output** | `tables/table4_ordinal_regression.csv` |

### 4.2 Formula

```
recurrence_risk_band_ordinal ~ ete_micro + ete_gross + age_at_surgery + female + largest_tumor_cm + ln_ratio
```

Where `recurrence_risk_band_ordinal` = {0: low, 1: intermediate, 2: high}

### 4.3 Covariates

| Variable | Type | Coding |
|----------|------|--------|
| `ete_micro` | Binary | 1 = microscopic ETE |
| `ete_gross` | Binary | 1 = gross ETE |
| `age_at_surgery` | Continuous | Years |
| `female` | Binary | 1 = female |
| `largest_tumor_cm` | Continuous | cm |
| `ln_ratio` | Continuous | positive/examined ratio |

### 4.4 Complete-Case Rule

Drop rows where ANY of: `risk_ord`, `ete_micro`, `ete_gross`, `age_at_surgery`, `female`, `largest_tumor_cm`, `ln_ratio` is missing. Primary N=593 (from 596).

### 4.5 Multiple Imputation

**Script:** `proposal2_recommendations.py::run_multiple_imputation()` and `proposal2_expanded_cohort.py::run_mi_pipeline()`

- **Method:** Predictive mean matching (PMM-lite) with 5% jitter
- **m:** 20 imputed datasets
- **Imputed variables:** `ln_ratio`, `largest_tumor_cm`, `tg_max`
- **Pooling:** Rubin's rules — `pooled_var = within_var + (1 + 1/m) * between_var`
- **Seed:** 42

### 4.6 AUC Calculation

```python
# Binary outcome: high risk (risk_ord >= 2) vs not
# Three model specifications:
#   Base: ete_gross + age + female + tumor_size + ln_ratio
#   Full: + ete_micro
# 5-fold stratified CV with seed=42
# Reported: AUC_Base_CV = 0.851 (SD 0.020), AUC_Full_CV = 0.876 (SD 0.010)
# delta_AUC_CV = 0.025
```

### 4.7 Proportional Odds Check

**From `analysis_metadata.yaml`:** "Proportional odds assumption may be violated: largest coefficient difference across cut-points is 8.12 for 'ete_gross'. Consider partial proportional odds model."

### 4.8 Primary Result

| Variable | OR | 95% CI | p-value |
|----------|---|--------|---------|
| **ete_micro** | **0.42** | **(0.28–0.64)** | **<0.001** |
| ete_gross | 340.72 | (114.21–1016.43) | <0.001 |
| age_at_surgery | 1.05 | (1.03–1.06) | <0.001 |
| female | 0.95 | (0.61–1.49) | 0.835 |
| largest_tumor_cm | 0.99 | (0.91–1.07) | 0.760 |
| ln_ratio | 2.65 | (1.75–4.01) | <0.001 |

---

## 5. Propensity Score Matching Analysis

### 5.1 Script

`proposal2_endpoint_psm_strata.py::propensity_match()`

### 5.2 Pool Definition

- Start: Expanded PTC cohort (N=3,278)
- Exclude: Gross ETE patients
- Remaining: mETE (N=1,736) vs No ETE (N=724)
- Drop: rows missing age, sex, tumor_size, n_positive_flag

### 5.3 PS Estimation

```python
from sklearn.linear_model import LogisticRegression
lr = LogisticRegression(random_state=42, max_iter=1000)
covariates = ["age_at_surgery", "female", "largest_tumor_cm", "n_positive_flag"]
lr.fit(X[covariates], treatment)
ps = lr.predict_proba(X)[:, 1]
logit_ps = log(ps / (1 - ps))
```

### 5.4 Matching Algorithm

```python
from sklearn.neighbors import NearestNeighbors
nn = NearestNeighbors(n_neighbors=1, metric="euclidean")
nn.fit(control_logit_ps)
distances, indices = nn.kneighbors(treated_logit_ps)
# Accept if distance <= caliper
caliper = 0.05
# Without replacement (used_control set tracks consumed controls)
```

### 5.5 Results

| Metric | Value |
|--------|-------|
| **Matched pairs** | **711** |
| NoETE structural recurrence % | 10.55% |
| mETE structural recurrence % | 14.49% |
| Risk difference | 3.94% |
| **OR (Fisher exact)** | **1.434** |
| **Fisher p-value** | **0.030** |

### 5.6 Balance Diagnostics (SMD Before → After)

| Variable | SMD Before | SMD After |
|----------|-----------|----------|
| age_at_surgery | 0.141 | -0.213 |
| female | 0.053 | -0.116 |
| largest_tumor_cm | -0.009 | 0.032 |
| n_positive_flag | 0.218 | **-0.576** |

**NOTE:** Post-match SMD for `n_positive_flag` exceeds ±0.1 threshold. Balance on nodal status is inadequate. This is documented in `analysis_metadata.yaml`.

### 5.7 Matched DFS

Kaplan-Meier on matched cohort with log-rank test comparing mETE vs No ETE. Output: `audit_figures/fig10_matched_dfs.png`.

---

## 6. Kaplan-Meier / Survival Analysis

### 6.1 Cox PH Survival

**Script:** `proposal2_cox_regression.py::run_cox()`

```python
from lifelines import CoxPHFitter
cph = CoxPHFitter(penalizer=0.1)  # L2 penalized
cph.fit(df[covariates + ["time_to_event_days", "event_occurred"]],
        duration_col="time_to_event_days",
        event_col="event_occurred")
```

**Covariates:**
- Gross ETE (binary), Micro ETE (binary), Age ≥55 (binary)
- Tumor >4cm (binary), LN ratio (standardized), BRAF positive (binary)

**Cohort:** ~5,794 from `risk_enriched_mv` (all PTC with time_to_event_days > 0)

### 6.2 KM Curves

Three KM figures generated:
- **Figure 10:** KM by AJCC risk band (low/intermediate/high) — `proposal2_cox_regression.py::fig10_km_risk_band()`
- **Figure 11:** KM by ETE status (No/Micro/Gross) — `proposal2_cox_regression.py::fig11_km_ete()`
- **Figure 10 (matched):** KM on PSM matched cohort — `proposal2_endpoint_psm_strata.py::plot_matched_dfs()`
- **Figure 7:** KM by ETE group using Tg follow-up time — `proposal2_recommendations.py::try_kaplan_meier()`

### 6.3 Censoring

- **Cox/KM from risk_enriched_mv:** `time_to_event_days` is days from surgery to recurrence event or last follow-up. Censoring at last known follow-up.
- **Matched DFS:** Censored at `tg_last_date` (last thyroglobulin measurement). Patients without Tg labs censored at surgery date (dfs_time=0).

### 6.4 Log-Rank Test

```python
from lifelines.statistics import multivariate_logrank_test, logrank_test
# Multivariate for 3-group comparisons
# Pairwise for 2-group comparisons (No ETE vs Gross ETE)
```

---

## 7. Interaction and Subgroup Analyses

### 7.1 Interaction Models

**Script:** `proposal2_endpoint_psm_strata.py::interaction_tests()`

```python
from statsmodels.api import Logit
# Base variables: ete_micro, age_at_surgery, female, largest_tumor_cm, n_positive_flag
# Three interaction terms tested separately:
#   1. ete_micro * largest_tumor_cm
#   2. ete_micro * age_at_surgery
#   3. ete_micro * n_positive_flag
```

| Interaction | OR | 95% CI | p-value |
|-------------|---|--------|---------|
| mETE × tumor_size | 1.043 | (0.94–1.15) | 0.412 |
| mETE × age | 0.990 | (0.97–1.01) | 0.258 |
| **mETE × nodal status** | **0.360** | **(0.17–0.74)** | **0.006** |

### 7.2 Tumor-Size Stratified Models

**Script:** `proposal2_endpoint_psm_strata.py::stratified_models()`

```python
# Outcome: structural_recurrence (binary)
# Model: Logit(structural ~ ete_micro + age + female + n_positive_flag)
# Strata: ≤1 cm, 1-2 cm, 2-4 cm (>4 cm excluded due to small N)
```

| Stratum | N | Events | mETE OR | 95% CI | p |
|---------|---|--------|---------|--------|---|
| ≤1 cm | 641 | 78 | 1.324 | (0.74–2.37) | 0.346 |
| 1-2 cm | 628 | 73 | 1.456 | (0.80–2.67) | 0.224 |
| 2-4 cm | 501 | 86 | 1.574 | (0.92–2.68) | 0.095 |

### 7.3 Classic Variant Subgroup

**Script:** `proposal2_ete_analysis.py::subgroup_analyses()`

Ordinal regression restricted to classic PTC (N=596). Chi-square tests for ETE × risk band within age and tumor-size strata.

### 7.4 Aggressive Variant Safety Analysis

**Script:** `proposal2_expanded_cohort.py::aggressive_variant_check()`

Separate ordinal regression for aggressive (tall cell/columnar/hobnail/diffuse sclerosing/solid) vs non-aggressive variants. Flags if mETE OR ≥ 1.5 in aggressive subgroup (potential safety concern).

---

## 8. CT Timing / Structural Endpoint Characterization

### 8.1 Source

- `ct_pathologic_ln_flag` derived from imaging data (CT/MRI showing pathologic lymphadenopathy)
- Joined via `risk_enriched_mv` which includes imaging proxies

### 8.2 Structural Endpoint Counts (Expanded Cohort)

| ETE Group | N | Events | Rate | Imaging | Reoperation |
|-----------|---|--------|------|---------|-------------|
| No ETE | 724 | 76 | 10.5% | 72 | 4 |
| Microscopic ETE | 1,736 | 270 | 15.6% | 267 | 3 |
| Gross ETE | 818 | 158 | 19.3% | 158 | 0 |

### 8.3 CT Timing Intervals

CT timing intervals (≤30d, 31-365d, >365d) are NOT separately characterized in the source data. The structural endpoint is a patient-level binary flag without temporal decomposition of imaging events.

---

## 9. Missing Data and Sensitivity Analyses

### 9.1 Missingness by Variable

| Variable | N Available | N Missing | % Missing |
|----------|-----------|-----------|-----------|
| risk_ord (risk band) | 3,278 | 0 | 0% |
| age_at_surgery | 3,278 | 0 | 0% |
| sex | 3,278 | 0 | 0% |
| tumor_size_cm | 3,267 | 11 | 0.3% |
| ln_ratio | 3,267 | 11 | 0.3% |
| m_stage_ajcc8 | 590 | 2,688 | **82.0%** |
| tg_max | ~2,800 | ~478 | ~14.6% |

### 9.2 Multiple Imputation

- **Method:** Predictive mean matching (PMM-lite) with 5% Gaussian jitter
- **Imputed:** `ln_ratio`, `largest_tumor_cm`, `tg_max`
- **m = 20** imputed datasets
- **Pooling:** Rubin's rules
- **Results match complete-case direction:** MI mETE OR=0.60 vs CC mETE OR=0.42

### 9.3 M-Stage Default

M-stage missing for 82% of cohort. All missing values defaulted to M0. Sensitivity analysis excluding patients with confirmed distant metastasis does not change primary findings.

### 9.4 Sensitivity Runs

| Analysis | Label | N | mETE OR | 95% CI | p |
|----------|-------|---|---------|--------|---|
| Primary (CC) | Complete-case | 593 | 0.42 | (0.28–0.64) | <0.001 |
| MI pooled | m=20 | 596 | 0.60 | (0.51–0.72) | <0.001 |
| Age ≥55 | Subgroup | 249 | 0.30 | (0.16–0.57) | <0.001 |
| Tumor ≤4 cm | Subgroup | 448 | 0.31 | (0.19–0.52) | <0.001 |
| Age <55 | Subgroup | 344 | 0.57 | (0.32–1.02) | 0.056 |
| Expanded All PTC (MI) | Cohort A | 3,278 | 0.60 | (0.50–0.72) | <0.001 |
| Expanded Classic (MI) | Cohort B | 2,166 | 0.52 | (0.42–0.64) | <0.001 |
| Original Classic (MI) | Cohort C | 589 | 0.50 | (0.33–0.74) | <0.001 |

---

## 10. Software / Packages / Runtime

### 10.1 Environment

From `analysis_metadata.yaml`:

```
Python: 3.14.2 (main, Dec 5 2025, 16:49:16) [Clang 17.0.0]
numpy: 2.4.3
pandas: 2.3.3
scipy: 1.17.1
statsmodels: 0.14.6
scikit-learn: 1.8.0
matplotlib: 3.10.8
lifelines: 0.30.3
```

### 10.2 DuckDB / MotherDuck

- DuckDB 1.4.4 (MotherDuck compatible)
- Database: `thyroid_research_2026` on MotherDuck
- Key view: `risk_enriched_mv` = `recurrence_risk_features_mv` LEFT JOIN `survival_cohort_ready_mv`
- Fallback CSV exports used when MotherDuck unavailable

### 10.3 Random Seed

**SEED = 42** used consistently across all scripts for:
- Multiple imputation (PMM sampling)
- Propensity score matching (LogisticRegression random_state)
- Cross-validation folds (StratifiedKFold)
- NumPy random state

### 10.4 Runtime

Total execution: 56.6 seconds (per `analysis_metadata.yaml`)

---

## 11. Result-to-Code Crosswalk

See `final_metric_crosswalk.csv` for the complete mapping. Key results:

| Manuscript Result | Value | Script | Output File | Verified |
|-------------------|-------|--------|-------------|----------|
| Primary cohort N | 596 | proposal2_ete_analysis.py | analytic_cohort.csv | ✓ |
| Expanded cohort N | 3,278 | proposal2_expanded_cohort.py | analytic_cohort_expanded.csv | ✓ |
| mETE OR (primary) | 0.42 (0.28–0.64) | proposal2_ete_analysis.py | table4_ordinal_regression.csv | ✓ |
| mETE OR (MI pooled) | 0.60 (0.51–0.72) | proposal2_recommendations.py | table5_sensitivity.csv | ✓ |
| PSM matched pairs | 711 | proposal2_endpoint_psm_strata.py | table6_propensity_matching_effect.csv | ✓ |
| PSM OR structural | 1.434 (p=0.030) | proposal2_endpoint_psm_strata.py | table6_propensity_matching_effect.csv | ✓ |
| mETE×nodal interaction | 0.360 (p=0.006) | proposal2_endpoint_psm_strata.py | table8_interaction_tests.csv | ✓ |
| mETE T-stage downstaged | 71.5% (1,241) | proposal2_expanded_cohort.py | analysis_metadata.yaml | ✓ |
| Overall downstaged | 57.3% (1,872) | proposal2_expanded_cohort.py | analysis_metadata.yaml | ✓ |
| AUC Base (CV) | 0.851 (SD 0.020) | proposal2_ete_analysis.py | analysis_metadata.yaml | ✓ |
| AUC Full (CV) | 0.876 (SD 0.010) | proposal2_ete_analysis.py | analysis_metadata.yaml | ✓ |
| Cox concordance | 0.853 | proposal2_cox_regression.py | table3_cox_regression.csv | ✓ |

---

## Appendix A: Audit Findings (from analysis_metadata.yaml)

1. **CRITICAL — LN_RATIO_QUALITY:** `ln_examined` is effectively binary for 83% of available values. LN ratio acts as binary, not continuous.
2. **CRITICAL — AJCC7_T3b_MAP:** T3b→T4a mapping affected 346 patients. Corrected to T3b→T3 in audit.
3. **IMPORTANT — OUTCOME_CIRCULARITY:** 100% of gross ETE patients are high risk by construction. The mETE OR is the clinically meaningful coefficient.
4. **IMPORTANT — PROP_ODDS:** Proportional odds may be violated for `ete_gross` (coefficient difference 8.12 across cut-points).
5. **MINOR — M_STAGE_MISSING:** M-stage available for only 18% of expanded cohort.

---

## Appendix B: Data File Hashes

| File | SHA256 (first 16) |
|------|-------------------|
| analytic_cohort.csv | 27b54f5045cc995c |
| analytic_cohort_expanded.csv | (see provenance.json) |
| ptc_full.csv | d4892ff454df9b0d |
| recurrence_full.csv | 3c8ffd687a8a6761 |
| imaging_correlation.csv | 6b595ffd0643ea7b |
