# mig_223 Semantic Publication Layer Closeout

Generated: 2026-05-01T04:08:13.312877+00:00
Batch: `mig_223_semantic_publication_layer_20260430`

## Objects Created

| Object | Rows |
|---|---:|
| `semantic_publication.release_manifest_v1` | 1 |
| `semantic_publication.vw_patient_master_safe_VIEW_v1` | 10871 |
| `semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1` | 5944 |
| `semantic_publication.vw_recurrence_safe_VIEW_v1` | 10739 |
| `semantic_publication.vw_molecular_safe_VIEW_v1` | 1384 |
| `semantic_publication.vw_fna_safe_VIEW_v1` | 8050 |
| `semantic_publication.vw_us_nodule_safe_VIEW_v1` | 29504 |
| `semantic_publication.vw_labs_long_safe_VIEW_v1` | 44124 |
| `semantic_publication.vw_cohort_membership_safe_VIEW_v1` | 10871 |

## Release Manifest Metrics

| Metric | Value |
|---|---:|
| `n_patients` | 10871 |
| `n_surgeries` | 4022 |
| `n_malignant_patients` | 4022 |
| `n_pathology_events` | 5944 |
| `n_fna_events` | 8050 |
| `n_molecular_events` | 1384 |
| `n_us_exams` | 8843 |
| `n_recurrence_path_proven` | 143 |
| `n_recurrence_imaging_only` | 585 |
| `qc_open_issue_count` | 0 |

## Verification

5-gate result: `(208, 0, 0, 0, 0)`

Acceptance checks passed: schema/table/views created, 9 semantic objects registered as verified, path safe view row count is 5,944, cohort membership safe view row count is 10,871, and gates 2-5 are clean.
