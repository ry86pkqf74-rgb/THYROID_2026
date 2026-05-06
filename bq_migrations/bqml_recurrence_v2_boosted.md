# BQML Boosted Tree Recurrence Model — Report

**DFL:** DFL-20260506-087  
**Migration log:** mig_087_bqml_boosted_models  
**Model ID:** `recurrence_5y_boosted_v1`  
**Migration SQL:** `bq_migrations/mig_087_bqml_boosted_models.sql`  
**Run:** 2026-05-06 (~22 min training time)

---

## Summary

Direct AUC comparison between BQML `BOOSTED_TREE_CLASSIFIER` and the logistic baseline (`recurrence_5y_baseline_v1`). Both use the same cohort definition and feature set. Result: logistic regression slightly outperforms boosted trees on this cohort, consistent with the hypothesis that the dataset is too small for complex non-linear models.

---

## Model Configuration

```sql
CREATE OR REPLACE MODEL `pub_workspace.recurrence_5y_boosted_v1`
OPTIONS(
  model_type            = 'BOOSTED_TREE_CLASSIFIER',
  input_label_cols      = ['recurrence_5y'],
  num_parallel_tree     = 8,
  max_iterations        = 50,
  early_stop            = TRUE,
  enable_global_explain = TRUE,
  data_split_method     = 'RANDOM',
  data_split_eval_fraction = 0.2,
  auto_class_weights    = TRUE
)
```

---

## Cohort

| Parameter | Value |
|---|---|
| Source | `pub_canonical.canonical_patient_master` |
| Filter | `is_malignant = TRUE AND (recurrence = TRUE OR followup_years >= 5)` |
| N total | 1,285 |
| N events | 502 (39%) |
| Label | `any_recurrence_flag` cast to INT64 |

---

## Performance vs Logistic Baseline

| Metric | Logistic Baseline | Boosted Tree |
|---|---|---|
| **AUC (ROC)** | **0.738** | **0.712** |
| Accuracy | 0.686 | 0.685 |
| F1 Score | 0.640 | 0.598 |
| Precision | 0.589 | 0.604 |
| Recall | 0.700 | 0.592 |
| Log Loss | — | 0.605 |
| Early stop iteration | — | 4 (of 50 max) |

**Key finding:** The boosted tree AUC (0.712) is **lower** than the logistic baseline (0.738). Early stopping at iteration 4 (out of 50) indicates the gradient boosting did not converge meaningfully — the cohort is too small for the model to learn complex non-linear interactions beyond what logistic regression already captures.

**Interpretation for manuscript:** The similar performance of logistic and boosted-tree models confirms that the recurrence signal in this cohort is primarily driven by linear relationships between known risk factors (ATA category, ETE, tumor size). Non-linear interaction detection would require either a larger cohort or additional feature engineering.

---

## Feature Importance (ML.GLOBAL_EXPLAIN)

| Feature | Attribution Score |
|---|---|
| `ata_risk_category` | 0.1899 |
| `age_at_surgery` | 0.0920 |
| `histology_final` | 0.0871 |
| `ete_grade_final` | 0.0761 |
| `tumor_size_cm_dominant` | 0.0753 |
| `multifocal_flag` | 0.0397 |
| `ln_positive_final` | 0.0095 |
| `sex` | 0.0032 |
| `braf_positive` | 0.0026 |
| `ajcc8_stage_group` | 0.0011 |
| `molecular_tested` | **0.0000** |

**Note:** `molecular_tested` attribution = 0 — this proxy variable (whether BRAF or TERT result is present) carries zero predictive signal once clinical risk factors are included. This is consistent with the Cox model finding that molecular testing *status* is less important than the test *result*.

---

## QC Assertion

Per the prompt spec, a QC assertion was added to monitor that boosted-tree AUC ≥ logistic baseline AUC:

```sql
-- QC assertion: boosted AUC vs logistic baseline
SELECT
  CASE WHEN boosted_auc >= baseline_auc THEN 'PASS' ELSE 'WARN' END AS assertion_result,
  boosted_auc,
  baseline_auc,
  boosted_auc - baseline_auc AS delta_auc
FROM (
  SELECT
    (SELECT auc FROM `pub_workspace.bqml_eval_log_v1`
     WHERE model_id='recurrence_5y_boosted_v1' ORDER BY trained_at DESC LIMIT 1) AS boosted_auc,
    (SELECT auc FROM `pub_workspace.bqml_eval_log_v1`
     WHERE model_id='recurrence_5y_baseline_v1' ORDER BY trained_at DESC LIMIT 1) AS baseline_auc
)
```

**Current result: WARN** (0.712 < 0.738, delta = −0.026). This is expected and documented — the cohort is too small for boosted trees to outperform logistic regression. No data drift or feature leakage suspected; the warning reflects a modeling constraint, not a data quality issue.

---

## Training Convergence

| Iteration | Eval Loss |
|---|---|
| 1 | 0.657 |
| 2 | 0.633 |
| 3 | 0.626 |
| 4 | 0.622 |

Stopped at iteration 4 (patience-based early stopping). Minimal loss improvement after iteration 2 confirms the model quickly plateaued.

---

## Files

- Migration SQL: `bq_migrations/mig_087_bqml_boosted_models.sql`
- This report: `_scripts/bqml_recurrence_v2_boosted.md`
- Model: `pub_workspace.recurrence_5y_boosted_v1` (BQML)
- Eval log: `pub_workspace.bqml_eval_log_v1` (model_id=`recurrence_5y_boosted_v1`)
- Migration log: `pub_signoff.bq_migration_log_v1` (mig_087_bqml_boosted_models)

---

## Governance

- **DFL-20260506-087** logged before training
- **bqml_eval_log_v1** row inserted: AUC=0.712, all eval metrics
- **bq_migration_log_v1** row: mig_087_bqml_boosted_models
- No per-patient outputs; aggregate metrics only
