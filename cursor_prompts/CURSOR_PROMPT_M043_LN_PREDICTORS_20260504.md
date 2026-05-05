# Cursor Prompt: M043 Lymph Node Metastasis Predictors — Multivariate Analysis

**Agent:** Sonnet 4.6 (Composer 2.0) — multivariate logistic regression with standard predictive modeling; Sonnet handles this efficiently  
**Estimated time:** 2–2.5 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, analytic view `manuscript_workspace.m043_ln_predictors_analytic_v1` (N=4,019). All malignant patients.

### LN Metastasis Distribution:
- LN positive: 1,120 (27.9%)
- LN negative: 2,899 (72.1%)
- Recurred: 502 (12.5%)

### Lymph Node Dissection (LND) Data:
| LND Type | N | LN+ | Recurred |
|---|---|---|---|
| NULL (no formal LND) | 3,563 | 750 | 399 |
| lateral_neck_dissection | 203 | 164 (80.8%) | 32 |
| mrnd (modified radical) | 186 | 164 (88.2%) | 56 |
| lateral_neck_dissection_unspecified | 67 | 42 (62.7%) | 15 |

### CND Data:
- CND performed: 1,831 (45.5%)
- CND has confidence tiers from `canonical_cnd_resolved_v1`

### Available Columns (39):
`research_id`, `age_at_surgery`, `sex`, `race`, `bmi_combined`, `histology_final`, `histology_pub_category`, `tumor_size_cm_dominant`, `multifocal_flag_path`, `is_malignant`, `ajcc8_stage_group`, `ajcc8_t_stage`, `ajcc8_n_stage`, `ete_grade_clean`, `gross_ete_flag`, `vascular_invasion_final`, `vascular_vessel_count`, `ln_positive_final`, `ln_rollup_total_examined`, `ln_rollup_total_positive`, `ln_rollup_has_per_level_data`, `ln_positive_flag`, `lnd_type`, `lnd_side`, `cnd_performed`, `cnd_confidence`, `braf_positive_final`, `ras_positive_final`, `molecular_risk_tier`, `mol_has_fusion`, `tirads_resolved`, `imaging_nodule_size_cm`, `bethesda_final`, `surg_procedure_type`, `any_recurrence_flag`, `any_confirmed_complication_flag`, `rai_received_reconciled`, `ata_risk_category`, `margin_r_classification`

## Task

### 1. Univariate Predictors of LN Metastasis

For each candidate predictor, calculate the association with `ln_positive_flag`:
- `age_at_surgery` — continuous (also categorize: <45, 45–65, >65)
- `sex` — female vs male
- `tumor_size_cm_dominant` — continuous and categorical (<1, 1–2, 2–4, >4 cm)
- `histology_pub_category` — PTC vs FTC vs other
- `multifocal_flag_path` — yes vs no
- `ete_grade_clean` — none/microscopic/gross
- `vascular_invasion_final` — none/microscopic/extensive
- `braf_positive_final` — yes vs no (among tested patients only)
- `ras_positive_final` — yes vs no
- `mol_has_fusion` — yes vs no
- `molecular_risk_tier` — high/intermediate/low/wild_type
- `tirads_resolved` — TR1–TR5
- `bethesda_final` — 1–6
- `imaging_nodule_size_cm` — continuous

Report: OR (95% CI), p-value for each. Use chi-square for categorical, t-test/Wilcoxon for continuous.

### 2. Multivariate Logistic Regression

**Primary model:** Predict `ln_positive_flag` using:
- `age_at_surgery` (continuous)
- `sex`
- `tumor_size_cm_dominant` (continuous)
- `histology_pub_category` (PTC vs non-PTC)
- `multifocal_flag_path`
- `ete_grade_clean` (categorical: none/microscopic/gross)
- `vascular_invasion_final` (binary: any vs none)
- `braf_positive_final` (binary, among tested)

**Secondary model (molecular-enriched):** Add molecular features:
- `molecular_risk_tier`
- `mol_has_fusion`
- `ras_positive_final`
- (Run on subset with molecular testing data)

Report:
- Adjusted OR (95% CI) for each predictor
- C-statistic (AUC) for model discrimination
- Hosmer-Lemeshow goodness of fit
- Likelihood ratio test comparing primary vs molecular-enriched model

### 3. LN Burden Analysis

Among LN-positive patients (N=1,120):
- Distribution of `ln_rollup_total_positive` (1, 2–4, 5–9, ≥10)
- Predictors of HIGH LN burden (≥5 positive) — repeat logistic regression
- LN ratio: `ln_rollup_total_positive / ln_rollup_total_examined` — distribution and prognostic significance

### 4. Impact of CND on LN Detection

Compare patients WITH vs WITHOUT CND:
- LN positive rate (higher in CND group expected — detection bias)
- Mean number of LN examined
- Mean number of LN positive
- Stage migration effect — does CND lead to upstaging?
- Recurrence rate comparison: CND vs no CND (crude and adjusted)

### 5. LND Type and Outcomes

For patients with formal LND (lateral neck dissection or MRND, N=456):
- Indications (clinical N1b vs prophylactic)
- LN yield by LND type
- Recurrence rate by LND type
- Complications by LND type
- Compare lateral neck dissection vs MRND outcomes

### 6. Predictive Nomogram

Build a clinical prediction model for LN metastasis:
- Use the final multivariate model
- Generate predicted probabilities for each patient
- Create risk groups (low: <15%, moderate: 15–40%, high: >40%)
- Calibration plot: predicted vs observed LN+ rates by decile
- Internal validation: bootstrap (1000 replicates) for optimism-corrected C-statistic

### 7. Recurrence Analysis by LN Status

- Recurrence rate: LN+ vs LN− with 95% CI
- Recurrence by LN burden category (0, 1–4, ≥5 positive)
- Multivariate model: `any_recurrence_flag ~ ln_positive_flag + age + sex + tumor_size + ETE + vascular_invasion + rai_received_reconciled`
- Does LN ratio predict recurrence better than absolute count?

### 8. Output

Save to `studies/m043_ln_predictors/`:
- `univariate_predictors.csv` — OR, CI, p-value for each predictor
- `multivariate_model.csv` — adjusted OR from logistic regression
- `ln_burden_analysis.csv` — distribution and predictors of high burden
- `cnd_impact.csv` — CND vs no CND comparison
- `lnd_outcomes.csv` — LND type outcomes
- `nomogram_predictions.csv` — patient-level predicted probabilities
- `recurrence_by_ln.csv` — recurrence stratified by LN status
- `ln_predictors_summary.tex` — LaTeX tables

### 9. Upload to MotherDuck

Create `manuscript_workspace.m043_ln_analysis_v1` with patient-level predicted probabilities and risk groups.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR in CPM but BIGINT in `canonical_lnd_resolved_v2` and `canonical_cnd_resolved_v1` — the analytic view already handles this join, so use the view directly
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- `ln_positive_flag` is the primary outcome (binary); `ln_rollup_total_positive` is the count
- 3,563 patients have NULL `lnd_type` — these had central compartment sampling or incidental LN in thyroidectomy specimen, NOT formal neck dissection
- CND (`cnd_performed`) = central neck dissection (level VI); LND = lateral neck dissection (levels II–V)
- Molecular data is available for ~1,286 patients — the molecular-enriched model runs on this subset only
- `tirads_resolved` and `bethesda_final` have significant missingness — handle appropriately in univariate (complete case) and note limitations
- Sex: lowercase `female`/`male`
