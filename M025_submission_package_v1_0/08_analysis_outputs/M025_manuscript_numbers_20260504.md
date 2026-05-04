# M025 — manuscript number helper (20260504)

**Cohort (`cohort_m025_tirads_performance_v1`):** n = 3,375
**Patients with ordinal TR (`tirads_resolved` + worst-score fallback):** n = 3,375
**Pathologic malignancies (gold: `is_malignant`):** 1,479

## Primary operative threshold headline (TR ≥ TR4)

- Sensitivity 0.713; specificity 0.559.
- TP=1,054 FP=837 FN=425 TN=1,059

## ROC reminder

- AUC (ordinal TI-RADS rank classifier) ≈ **0.6478**.

**Operative caveat:** malignancy enrichment at **every TI-RADS stratum vs ACR-illustrative ROM** (`snowflake_trial/reports/m025_tirads_performance.md`).

## Regenerate

```bash
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_tables.py
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_figures.py
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_manuscript_md.py
```
