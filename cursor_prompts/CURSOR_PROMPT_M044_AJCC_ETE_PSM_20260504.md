# Cursor Prompt: M044 AJCC 8th Edition ETE Staging & Propensity Score Analysis

**Agent:** GPT 5.5 (Composer 2.0) — complex propensity score matching + survival modeling; GPT 5.5 handles multi-step causal inference pipelines efficiently  
**Estimated time:** 2.5–3 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, analytic view `manuscript_workspace.m044_ajcc_ete_analytic_v1` (N=4,013). This view contains all malignant patients with ETE data (excludes 6 patients missing ETE entirely from the 4,019 malignant cohort).

### ETE Distribution:
| ETE Grade | N | % |
|---|---|---|
| microscopic | 2,518 | 62.7% |
| gross | 1,262 | 31.4% |
| none | 200 | 5.0% |
| present_ungraded | 33 | 0.8% |

### ETE × AJCC T-Stage Cross-Tab:
| T-Stage | ETE Grade | N |
|---|---|---|
| T1a | microscopic | 935 |
| T1a | none | 53 |
| T1b | microscopic | 700 |
| T1b | none | 47 |
| T2 | microscopic | 628 |
| T2 | none | 46 |
| T3a | microscopic | 249 |
| T3a | none | 54 |
| T3b | gross | 1,262 |
| T3b | present_ungraded | 8 |

**Key observation:** All gross ETE maps to T3b (by definition in AJCC 8th). Microscopic ETE spans T1a–T3a. The clinical question is whether microscopic ETE independently predicts recurrence beyond what T-stage captures.

### Outcomes:
- Recurrence: 499 (12.4%)
- Complications: 154 (3.8%)

### Available Columns (31):
`research_id`, `age_at_surgery`, `sex`, `race`, `bmi_combined`, `histology_final`, `histology_pub_category`, `tumor_size_cm_dominant`, `multifocal_flag_path`, `is_malignant`, `ete_grade_final`, `ete_grade_clean`, `gross_ete_flag`, `ajcc8_stage_group`, `ajcc8_t_stage`, `ajcc8_n_stage`, `vascular_invasion_final`, `vascular_vessel_count`, `margin_status_final`, `margin_r_classification`, `ln_positive_final`, `ln_rollup_total_examined`, `ln_rollup_total_positive`, `braf_positive_final`, `ras_positive_final`, `molecular_risk_tier`, `surg_procedure_type`, `any_recurrence_flag`, `any_confirmed_complication_flag`, `rai_received_reconciled`, `ata_risk_category`

## Task

### 1. ETE Impact on Recurrence — Univariate

For each ETE grade (none, microscopic, gross, present_ungraded):
- Recurrence rate with 95% Wilson CI
- Kaplan-Meier curves (if time-to-event data available; otherwise use simple proportions)
- Pairwise comparisons: none vs microscopic, microscopic vs gross, none vs gross

### 2. ETE Impact Stratified by T-Stage

Within each T-stage (T1a, T1b, T2, T3a):
- Compare recurrence rate WITH vs WITHOUT microscopic ETE
- This is the key clinical question: does microscopic ETE add prognostic information within the same T-stage?
- For T3b (all gross ETE), report recurrence rate as reference

### 3. Propensity Score Matching: Microscopic ETE vs No ETE

**Treatment:** microscopic ETE (N=2,518)  
**Control:** no ETE (N=200)  

Covariates for PSM:
- `age_at_surgery` (continuous)
- `sex` (binary)
- `tumor_size_cm_dominant` (continuous)
- `histology_pub_category` (categorical — use PTC vs non-PTC or top categories)
- `multifocal_flag_path` (binary)
- `ln_rollup_total_positive` (continuous or binary: any vs none)
- `vascular_invasion_final` (binary: any vs none)
- `surg_procedure_type` (total vs hemi)

Methods:
- Nearest-neighbor matching with caliper = 0.2 × SD of logit propensity score
- 1:1 matching (will be limited by N=200 control group)
- Assess balance: standardized mean differences (SMD < 0.1 for all covariates)
- If balance is poor, try inverse probability of treatment weighting (IPTW) as alternative

Outcomes in matched cohort:
- Recurrence rate comparison (chi-square or McNemar's)
- Odds ratio with 95% CI
- Complication rate comparison

### 4. Multivariate Logistic Regression

Model: `any_recurrence_flag ~ ete_grade_clean + age_at_surgery + sex + tumor_size_cm_dominant + histology_pub_category + ln_rollup_total_positive + vascular_invasion_final + surg_procedure_type + rai_received_reconciled`

- Report adjusted OR with 95% CI for each ETE grade (reference: none)
- Test for interaction: ETE × tumor size, ETE × LN status
- C-statistic for model discrimination
- Hosmer-Lemeshow goodness of fit

### 5. AJCC Staging Reclassification Analysis

Simulate what would happen if microscopic ETE were removed from staging (as some have proposed):
- How many patients would be reclassified? (T3a → T2 for those with microscopic ETE as sole T3a criterion)
- Compare recurrence in reclassified-down patients vs those who remain T3a
- Net reclassification improvement (NRI)

### 6. "present_ungraded" ETE Subgroup

For the 33 patients with `ete_grade_clean = 'present_ungraded'`:
- Demographics, tumor characteristics
- Recurrence rate
- Are these more similar to microscopic or gross ETE patients?
- Recommendation: classify as microscopic for main analysis, sensitivity analysis both ways

### 7. Output

Save to `studies/m044_ajcc_ete_analysis/`:
- `ete_recurrence_by_grade.csv` — recurrence rates with CI
- `ete_by_tstage_recurrence.csv` — within-stage ETE analysis
- `psm_balance_table.csv` — covariate balance before/after matching
- `psm_outcomes.csv` — matched cohort outcomes
- `multivariable_regression.csv` — logistic regression results
- `reclassification_analysis.csv` — staging reclassification impact
- `ete_analysis_summary.tex` — LaTeX tables

### 8. Upload to MotherDuck

Create `manuscript_workspace.m044_ete_analysis_v1` with patient-level analytic fields (PSM weights, matched pairs, predicted probabilities).

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- `ete_grade_clean` is the cleaned column (4 values: none/microscopic/gross/present_ungraded); do NOT use `ete_grade_final` (7 raw values)
- The small N=200 for "no ETE" limits PSM power — consider IPTW as primary method if matching yields poor balance
- `present_ungraded` (N=33) should be excluded from primary PSM, included in sensitivity analysis
- Gross ETE = T3b by definition — the interesting analysis is microscopic ETE within T1–T3a
- Sex: lowercase `female`/`male`
