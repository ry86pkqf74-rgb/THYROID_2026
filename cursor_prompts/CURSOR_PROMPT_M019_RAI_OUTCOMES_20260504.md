# Cursor Prompt: M019 Radioactive Iodine (RAI) Outcomes Analysis

**Agent:** Sonnet 4.6 (Composer 2.0) — outcomes analysis with standard regression methods; Sonnet handles this efficiently  
**Estimated time:** 2.5–3 hours  
**Date:** 2026-05-04

## Context

MotherDuck database `thyroid_canonical_publication_v1_0`, analytic view `manuscript_workspace.m019_rai_outcomes_analytic_v1` (N=862). All patients who received RAI (`rai_received_reconciled IS TRUE`).

### Cohort Summary:
- Total: 862 patients
- Has dose data: 214 (24.8%) — **only 1 in 4 has actual dose values**
- Has Tg data: 566 (65.7%)
- Recurred: 155 (18.0%)
- Has ATA response category: 35 (4.1%)
- TgAb interference: 58 (6.7%)

### RAI Intent:
| Intent | N |
|---|---|
| NULL | 613 |
| unknown | 150 |
| ablation | 82 |
| remnant_ablation | 10 |
| therapeutic | 4 |
| adjuvant | 3 |

**NOTE:** 88.5% of patients have NULL or unknown RAI intent. This is a major data limitation — address prominently.

### Dose Data:
- Patients with any dose: 214
- Average max dose: 42.3 mCi (median 0 — most are missing/zero)
- Average cumulative dose: 464.8 mCi (inflated by multi-treatment patients)
- Dose confidence is poor — only 35 patients are `confirmed_with_dose`

### Thyroglobulin Trajectory:
| Trajectory Class | N |
|---|---|
| suppressed | 301 |
| low_stable | 106 |
| detectable_stable | 84 |
| rising | 71 |
| insufficient_data | 6 |

### ATA Response Categories (N=35 only):
| Response | N |
|---|---|
| structural_incomplete | 23 |
| biochemical_incomplete | 6 |
| indeterminate | 4 |
| excellent | 1 |
| insufficient_data | 1 |

### Available Columns (61):
`research_id`, `age_at_surgery`, `sex`, `race`, `bmi_combined`, `histology_final`, `histology_pub_category`, `tumor_size_cm_dominant`, `multifocal_flag_path`, `is_malignant`, `ajcc8_stage_group`, `ajcc8_t_stage`, `ajcc8_n_stage`, `ajcc8_m_stage`, `ete_grade_clean`, `vascular_invasion_final`, `ln_positive_final`, `ln_rollup_total_positive`, `braf_positive_final`, `ras_positive_final`, `molecular_risk_tier`, `rai_received_reconciled`, `rai_validation_tier`, `n_rai_episodes`, `confirmed_rai_episodes`, `rai_max_dose_mci`, `rai_min_dose_mci`, `rai_total_cumulative_dose_mci`, `rai_dose_data_available`, `rai_dose_confidence_worst`, `rai_first_days_from_surg`, `rai_intent_v9`, `rai_intent_list`, `rai_avid_flag`, `rai_avidity`, `rai_scan_findings_v9`, `rai_stimulated_tg`, `rai_stimulated_tsh`, `tg_data_available`, `tg_nadir`, `tg_peak`, `tg_last_value`, `tg_rising_flag`, `tg_trajectory_class`, `tg_n_measurements`, `tg_below_threshold_ever`, `post_rai_tg_nadir`, `post_rai_tg_last`, `post_rai_tg_count`, `max_stimulated_tg`, `n_stimulated_tg_measurements`, `tgab_interference_flag`, `tgab_last_value`, `tgab_nadir`, `tgab_peak`, `ata_risk_category`, `ata_response_category`, `any_recurrence_flag`, `any_confirmed_complication_flag`, `surg_procedure_type`, `margin_r_classification`

## Task

### 1. RAI Utilization Patterns

- RAI receipt rate in the full cohort: 862/10,871 (7.9%) — but calculate among malignant only: 862/4,019
- RAI by ATA risk category
- RAI by AJCC stage
- RAI by histology
- RAI by time period (to show de-escalation trends if present — join `date_of_surgery` or `surgery_year` from CPM if needed)
- RAI intent distribution (acknowledge 88.5% missing)
- Number of RAI episodes distribution (`n_rai_episodes`)

### 2. Thyroglobulin Response to RAI

**Primary analysis using Tg trajectory (N=566):**
- Recurrence rate by `tg_trajectory_class`:
  - suppressed (N=301): expect lowest recurrence
  - low_stable (N=106): expect low recurrence
  - detectable_stable (N=84): intermediate
  - rising (N=71): expect highest recurrence
- Tg trajectory as predictor of recurrence: OR (95% CI) for each class vs suppressed
- Multivariate model: `any_recurrence_flag ~ tg_trajectory_class + age + sex + tumor_size + ajcc8_stage_group + ata_risk_category`

**Secondary analysis using Tg values:**
- `post_rai_tg_nadir`: distribution, threshold analysis (ROC for predicting recurrence)
- `tg_rising_flag`: recurrence rate if rising vs not
- `max_stimulated_tg`: association with recurrence
- Optimal Tg cutoff for predicting recurrence (Youden's J)

### 3. TgAb Interference Analysis

For 58 patients with `tgab_interference_flag IS TRUE`:
- How many had unreliable Tg due to TgAb?
- Were these patients managed differently?
- Recurrence rate compared to TgAb-negative patients
- Note: TgAb positivity itself may indicate autoimmune thyroid disease or residual thyroid tissue

### 4. Dose-Response Analysis (N=214 with dose data)

**Caveat:** Only 214 patients have dose data, and quality is variable. Interpret with caution.

- Recurrence rate by dose category: <30 mCi (low dose/remnant ablation), 30–100 mCi (standard), 100–150 mCi (high dose), >150 mCi (therapeutic)
- Cumulative dose and recurrence (for multi-treatment patients)
- Logistic regression: `recurrence ~ dose_category + ata_risk + ajcc_stage`
- **Sensitivity analysis:** Restrict to `rai_dose_confidence_worst` = high-confidence doses

### 5. RAI vs No RAI Comparison (requires joining full cohort)

Compare RAI recipients (N=862) vs non-recipients among malignant patients:
```sql
-- Join back to full malignant cohort
SELECT * FROM canonical_patient_master 
WHERE is_malignant IS TRUE
```

- Demographics comparison (Table 1)
- Disease characteristics (tumor size, stage, ETE, LN, vascular invasion)
- Recurrence rate: RAI vs no RAI (crude)
- **Propensity score analysis:** RAI receipt is confounded by disease severity
  - Covariates: age, sex, tumor size, AJCC stage, ATA risk, ETE, LN status, vascular invasion, histology
  - IPTW or PSM to estimate treatment effect
  - Report ATT (average treatment effect on treated)
- Subgroup analysis: RAI benefit in low-risk vs intermediate vs high-risk patients

### 6. Timing of RAI

Using `rai_first_days_from_surg`:
- Distribution of time from surgery to first RAI
- Early (<90 days) vs late (>90 days) RAI: recurrence comparison
- Is there an optimal timing window?

### 7. ATA Dynamic Risk Assessment

For the 35 patients with `ata_response_category`:
- Cross-tabulate response category × recurrence
- Report descriptively only (N too small for inference)
- Compare Tg trajectory classification vs formal ATA response (concordance)

### 8. Output

Save to `studies/m019_rai_outcomes/`:
- `rai_utilization_patterns.csv` — receipt rates by risk/stage/histology
- `tg_response_analysis.csv` — recurrence by Tg trajectory
- `tg_threshold_roc.csv` — ROC data for Tg cutoff
- `tgab_interference.csv` — TgAb impact analysis
- `dose_response.csv` — recurrence by dose category
- `rai_vs_no_rai.csv` — propensity-adjusted comparison
- `rai_timing.csv` — time to RAI analysis
- `rai_outcomes_summary.tex` — LaTeX tables

### 9. Upload to MotherDuck

Create `manuscript_workspace.m019_rai_analysis_v1` with patient-level response classifications and propensity scores.

## Connection
```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```
Use MotherDuck token from `.env.motherduck` or `MOTHERDUCK_TOKEN`.

## Important Notes
- `research_id` is VARCHAR
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`
- **Data sparsity is the dominant challenge:** 75% lack dose data, 35% lack Tg data, 96% lack ATA response category, 88% lack RAI intent. Every analysis section must acknowledge these limitations.
- `rai_received_reconciled` (862 TRUE) is the correct RAI flag — NOT `rai_received_flag` (583 TRUE, undercounts)
- `tg_trajectory_class` is the most analytically useful Tg variable (N=566) — prefer this over raw Tg values
- For RAI vs no-RAI comparison, the non-RAI group must come from the full CPM: `SELECT * FROM canonical_patient_master WHERE is_malignant IS TRUE AND (rai_received_reconciled IS NOT TRUE OR rai_received_reconciled IS NULL)`
- Median max dose of 0 indicates most `rai_max_dose_mci` values are NULL/zero — filter to `rai_dose_data_available IS TRUE` for dose analyses
- TgAb interference affects Tg reliability — exclude or flag `tgab_interference_flag IS TRUE` patients in Tg-based analyses
- Sex: lowercase `female`/`male`
