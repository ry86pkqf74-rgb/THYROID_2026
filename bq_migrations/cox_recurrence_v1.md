# Cox PH Recurrence Model — Report

**DFL:** DFL-20260506-086  
**Migration log:** mig_086_cox_recurrence_v1  
**Model ID in bqml_eval_log_v1:** `cox_recurrence_v1`  
**Script:** `THYROID_2026/scripts/cox_recurrence_v1.py`  
**Run:** 2026-05-06

---

## Summary

This is a proper time-to-event model (Cox Proportional Hazards) on the thyroid recurrence cohort. It complements `recurrence_5y_baseline_v1` (BQML logistic regression, AUC 0.738) by correctly handling right-censoring and providing hazard ratios interpretable for the manuscript.

---

## Cohort

| Parameter | Value |
|---|---|
| Source | `pub_workspace.cohort_m044_ajcc_ete_v1` |
| N patients | 2,580 |
| N recurrence events | 428 (16.6%) |
| Right-censored | 2,152 (83.4%) |
| Duration variable | `followup_years * 365.25` (days) |
| Event variable | `any_recurrence_flag` |

---

## Model

- **Method:** CoxPHFitter (lifelines 0.30.3)
- **Penalizer:** 0.1 (L2 ridge regularization)
- **Baseline hazard:** Breslow estimator

---

## Performance

| Metric | Value |
|---|---|
| **C-index (Harrell)** | **0.674** |
| Brier score @ 1y | 0.174 |
| Brier score @ 3y | 0.175 |
| Brier score @ 5y | 0.160 |
| Log-likelihood | −2769.92 |
| LLR test (9 df) | 142.73, p < 0.001 |
| Partial AIC | 5557.83 |

**Comparison to BQML logistic baseline (AUC 0.738):**  
The C-index (0.674) is numerically lower than the logistic AUC (0.738). Note these measure different things on different cohorts — the AUC is a 5-year binary classifier; the C-index is time-to-event concordance over all follow-up. Direct comparison is informative but not apples-to-apples.

---

## Hazard Ratios (Top Predictors)

| Feature | HR | 95% CI | p |
|---|---|---|---|
| `tumor_size_cm` | 1.08 | 1.05–1.12 | <0.005 |
| `ata_high_risk` | 1.39 | 1.16–1.66 | <0.005 |
| `ata_intermediate_risk` | 0.62 | 0.51–0.75 | <0.005 |
| `histology_ptc` | 0.71 | 0.58–0.85 | <0.005 |
| `stage_iii_iv` | 1.32 | 0.97–1.80 | 0.08 |
| `ln_positive` | 1.01 | 1.00–1.02 | 0.20 |
| `gross_ete` | 1.07 | 0.90–1.26 | 0.44 |
| `age_at_surgery` | 1.00 | 0.99–1.00 | 0.24 |
| `sex_male` | 1.03 | 0.87–1.21 | 0.76 |

**Key findings:**
- Tumor size (HR 1.08/cm) and ATA high-risk (HR 1.39) are the strongest predictors of earlier recurrence
- Non-PTC histology has lower recurrence rate (HR 0.71 for PTC vs. other) — counter-intuitive; may reflect cohort structure (MTC, ATC have different staging patterns)
- ATA intermediate risk shows lower HR than low risk (reference) — likely a staging/treatment interaction
- Gross ETE is non-significant after adjusting for stage (HR 1.07, p=0.44) — collinear with stage

---

## Features

9 covariates: `age_at_surgery`, `sex_male`, `gross_ete`, `stage_iii_iv`, `ata_high_risk`, `ata_intermediate_risk`, `histology_ptc`, `ln_positive`, `tumor_size_cm`  
(BRAF not available in M044 cohort; braf_positive absent from `cohort_m044_ajcc_ete_v1`)

---

## Files

- Coefficients: `THYROID_2026/studies/cox_recurrence_v1/cox_recurrence_v1_coefficients.csv`
- Results JSON: `THYROID_2026/studies/cox_recurrence_v1/cox_recurrence_v1_results.json`
- Script: `THYROID_2026/scripts/cox_recurrence_v1.py`

---

## Governance

- **DFL-20260506-086** logged in Airtable Data Feedback Log before this run
- **bqml_eval_log_v1** row inserted: model_id=`cox_recurrence_v1`, c_index=0.674 in `notes`
- No per-patient predictions stored — aggregate metrics only
- Source model is not a BQML model; entry flagged in notes field accordingly
