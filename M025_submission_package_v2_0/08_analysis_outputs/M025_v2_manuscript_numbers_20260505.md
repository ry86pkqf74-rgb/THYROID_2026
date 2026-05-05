# M025 v2.0 — manuscript number helper (20260505)

> **Working title:** Patient-level versus nodule-level TI-RADS calibration in a 25-year operative thyroid cohort

**Nodule spine:** `cohort_m025_nodule_level_v1` — total rows = 37,438
**Strict ACR analytic-eligible nodules:** n = 3,687
**Strict nodules with known TR rank:** n = 3,687
**Path-proven malignant nodules (strict):** 631
**Patient comparator cohort:** `cohort_m025_tirads_performance_v1` — n = 3375

## Headline (TR ≥ TR4, strict nodule grain)

- Sensitivity 0.769; specificity 0.471.
- TP=485 FP=1,616 FN=146 TN=1,440

## ROC

- Nodule-level strict AUC (ordinal TR) ≈ **0.6399**.
- Patient-level comparator AUC ≈ **0.6478**.

**Framing:** Patient-level v1.0 analysis (`M025_submission_package_v1_0/`) remains frozen as sister manuscript; v2.0 recovers ACR-expected ROM at TR4/TR5 at nodule grain — see Table 3 / Fig 3b in this package.

## Regenerate

```bash
.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_tables.py
.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_figures.py
.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_manuscript_md.py
```
