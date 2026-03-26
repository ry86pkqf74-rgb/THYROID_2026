# Findings Note — Manuscript Readiness Bundle

**Study:** Preoperative predictors of initial total thyroidectomy among 2.0–4.0 cm thyroid nodules
**Date:** 2026-03-26

## Key results

- **Primary cohort:** N=558 (lobectomy 238, total 320).
- **Bethesda ≥ 4** is the dominant predictor of initial total thyroidectomy (OR 2.74, 95% CI 1.81–4.15, p < 0.001).
- **Age** has a modest inverse association (OR 0.986/year, p = 0.026).
- **Bilateral nodules** independently associated in the extended model (OR 2.00, 95% CI 1.28–3.13, p = 0.002).
- **Molecular testing** was rare (~4% tested) and not significantly associated with extent of surgery.
- **Broad nodal exclusion sensitivity** (N=635) preserves all direction and significance patterns — results are robust.

## Model performance

- Primary parsimonious AUC, Brier, and calibration are in `model_performance.csv`.
- Bootstrap optimism correction applied (200 resamples).

## Critical caveats

1. **Zero completion thyroidectomies** in this cohort → completion model has complete separation (unreliable).
2. **Pathology-defined size cohort is empty** (N=0) → no path sensitivity analysis possible.
3. **Molecular subset models** show extreme coefficients and separation — treat as hypothesis-generating only.
4. **Missing bethesda** ~27% — coded as "not ≥4"; complete-case sensitivity in `sensitivity_summary.csv`.

## QA verdict

All primary reconciliation checks pass. See `qa_reconciliation.md`.

## Deliverables written

1. `qa_reconciliation.md`
2. `baseline_table_primary.csv`, `baseline_table_broad_nodal.csv`
3. `model_summary_final.csv`
4. `missingness_summary.csv` (enhanced: both cohorts)
5. `model_performance.csv` (AUC + bootstrap CI, Brier, calibration, optimism-corrected AUC)
6. `sensitivity_summary.csv` (nodal exclusion robustness + bethesda complete-case)
7. `analysis_freeze.md`
8. `strobe_tripod_gap_check.md`
