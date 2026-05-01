# Project Memory: semantic_publication Layer (mig_223)

Date: 2026-05-01
Batch: `mig_223_semantic_publication_layer_20260430`
Release: `pub_v1_0_20260430`

## Closeout

Lane G / mig_223 created the `semantic_publication` schema in MotherDuck with one release manifest table and eight manuscript-safe views. The runner is `qc_framework_v1/scripts/apply_mig223_semantic_publication_layer.py`.

## Objects

- `semantic_publication.release_manifest_v1`: 1 row
- `semantic_publication.vw_patient_master_safe_VIEW_v1`: 10,871 rows
- `semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1`: 5,944 rows
- `semantic_publication.vw_recurrence_safe_VIEW_v1`: 10,739 rows
- `semantic_publication.vw_molecular_safe_VIEW_v1`: 1,384 rows
- `semantic_publication.vw_fna_safe_VIEW_v1`: 8,050 rows
- `semantic_publication.vw_us_nodule_safe_VIEW_v1`: 29,504 rows
- `semantic_publication.vw_labs_long_safe_VIEW_v1`: 44,124 rows
- `semantic_publication.vw_cohort_membership_safe_VIEW_v1`: 10,871 rows

## Registry And Gates

- Registered semantic objects: 9 table rows.
- Registered semantic columns: 281 column rows.
- Final verification gate: `(208, 0, 0, 0, 0)`.
- `qc_open_issue_count`: 0.

## Artifacts

- Report: `qc_framework_v1/reports/mig_223_semantic_publication_layer_20260430.md`
- Export summary: `exports/mig223_semantic_publication_20260430/run_summary.json`