# M025 v2.0 — validation / reconciliation (draft)

## Sources

| Artifact | Expected |
|---------|----------|
| Nodule spine | `manuscript_workspace.cohort_m025_nodule_level_v1` |
| Patient comparator | `manuscript_workspace.cohort_m025_tirads_performance_v1` |
| Primary subset | `analytic_eligible_strict_acr_pernodule = TRUE` |

## Row-count gates (from mig_306)

- Total N rows (nodule): 37,438  
- Distinct patients: 6,523  
- Strict analytic-eligible nodules: 3,687  

## Checks after `build_m025_tables.py`

1. `08_analysis_outputs/m025v2_run_snapshot.json` reflects live pulls (timestamps refresh).
2. `m025v2_per_tr_rom_with_ci.csv` — patient-level ROM columns should track v1.0 operative cohort; nodule ROM TR4 ≈ 18.7%, TR5 ≈ 26.1% (Wilson CIs per live run).
3. `m025v2_threshold_metrics_per_nodule.csv` — TP/FP/TN/FN reconcile to strict subset only.
4. Bethesda × TIRADS table matches ad hoc SQL in `M025_v2_tirads_analysis.sql` §3.

## Sister package

- `M025_submission_package_v1_0/` remains the frozen **patient-level** package (`mig_292`).
