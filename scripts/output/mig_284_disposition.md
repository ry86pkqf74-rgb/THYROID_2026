# mig_284 consumer disposition

- Applied at UTC: 2026-05-04T04:14:06.462769+00:00
- Strategy: drop legacy recurrence *tables* and replace with compatibility *views* backed by new SSOT.
- Legacy `BASE TABLE` objects remaining at legacy names: 0
- Legacy names now exposed as `VIEW`: 3 (`canonical_recurrence_v1`, `canonical_recurrence_resolved_v1`, `recurrence_event_clean_v1`)
- Legacy compatibility views present: 3
- `canonical_recurrence_events_v1` rows: 1946
- `canonical_recurrence_patient_rollup_v1` rows: 10871
- Compat `main.canonical_recurrence_v1` rows: 10871
- Compat `main.canonical_recurrence_resolved_v1` rows: 10871
- Compat `main.recurrence_event_clean_v1` rows: 1946

## Repointed repo consumers
- `M044_submission_package_v1_0/08_analysis_code/M044_ETE_analysis.sql`
- `M044_submission_package_v1_0/08_analysis_code/m044_ete_fit_models.py`
- `M044_submission_package_v1_0/08_analysis_code/build_m044_master_excel.py`