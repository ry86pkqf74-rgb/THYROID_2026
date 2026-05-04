# M025 submission package — validation report (automated scaffold)

## Live MotherDuck pull (builders)

Executed `build_m025_tables.py` / `build_m025_figures.py` against `thyroid_canonical_publication_v1_0`.

| Artifact | Evidence |
|---------|----------|
| Cohort view row count (`cohort_m025_tirads_performance_v1`) | **3,375** (`m025_run_snapshot.json`) |
| Ordinal TR (`tirads_resolved` + worst-score fallback complete) | **3,375** / **3,375** |
| Pathologic malignant (`canonical_patient_master.is_malignant` join) | **1,479** |
| ROC AUC ordinal rank | See `m025_supp_ROC_summary.csv` (**~0.648** live run). |

## Reconciliation checkpoints

1. Tier counts for TR1–TR5 in workbook **Table 1** should reconcile to **3,375** row spine (exclusive unknown).
2. **Table 2** threshold metrics use same evaluable denominator as cohort view `tr_rank` non-null (**3,375** this run).
3. Cross-strat Fig 4 CSV vs **Table 3** crosstab: row/column totals must agree with analytic spine parquet.

## Signoff migration

`qc_framework_v1/migrations/292_m025_submission_package_20260504.sql` inserts `signoff_migration.mig_292` idempotently (applied in this scaffold session).
