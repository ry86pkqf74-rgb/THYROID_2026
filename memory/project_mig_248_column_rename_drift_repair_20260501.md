# mig_248 Closeout — Column-Rename Drift Repair

**Date:** 2026-05-01
**Batch ID:** `mig_248_column_rename_drift_repair_20260501`
**Migration file:** `qc_framework_v1/migrations/248_column_rename_drift_repair_20260501.sql`
**Scan artifact:** `exports/mig_248_column_rename_drift_repair_20260501/view_queryability_scan.csv`

## Preflight QC

```text
gate1_verified_tables  gate1_distinct_objects  gate2_missing_signoff  gate3_count_mismatch  gate4_verified_cols_missing_metadata  gate5_clinical_date_violations  cpm_pts  us_gland_v2_pts  us_ln_v2_pts  cohort_parity_ok        release_id  sem_patient_master_rows  sem_cohort_membership_rows  sem_path_malignant_tumor_rows  sem_recurrence_rows  sem_fna_rows  sem_us_nodule_rows  sem_molecular_rows  sem_labs_long_rows  path_borderline_count  recurrence_implausible_date_count  us_nodule_size_outlier_count  us_nodule_multi_attr_unresolved_count  us_nodule_nlp_backfill_pending_count  us_ln_nlp_backfill_pending_count  us_gland_nlp_backfill_pending_count                                  latest_col_registry_batch_id  verified_main_objects_missing_comment     most_recent_signoff_ts                                             most_recent_signoff_migration publication_qc_status_built_at
                   218                     218                      0                     0                                     0                               0    10871            10871         10871              True pub_v1_0_20260430                    10871                       10871                           5944                10739          8050               29504                1384               44124                     27                                132                            15                                  10570                                  2061                              6793                                13578 mig_239_semantic_research_id_varchar_standardization_20260501                                      2 2026-05-01 05:54:23.434531 qc_framework_v1/migrations/238_publication_qc_status_VIEW_v1_20260501.sql     2026-05-01 04:18:06.853854
```

## Per-view scan summary

- Cohort views scanned: 82 (82 OK, 0 error).
- Adjacent manuscript_workspace views scanned: 75 (68 OK, 7 error).
- Scan CSV: `exports/mig_248_column_rename_drift_repair_20260501/view_queryability_scan.csv`.

## Repairs applied

- `manuscript_workspace.cohort_m049_pyramidal_lobe_v1`: `syn_isthmus_size_cm` -> `syn_isthmus_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_m058_thyroid_size_weight_v1`: `syn_right_lobe_size_cm` -> `syn_right_lobe_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_m058_thyroid_size_weight_v1`: `syn_left_lobe_size_cm` -> `syn_left_lobe_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_m058_thyroid_size_weight_v1`: `syn_isthmus_size_cm` -> `syn_isthmus_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_descriptive_full_cohort_v1`: `syn_right_lobe_size_cm` -> `syn_right_lobe_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_descriptive_full_cohort_v1`: `syn_left_lobe_size_cm` -> `syn_left_lobe_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.
- `manuscript_workspace.cohort_descriptive_full_cohort_v1`: `syn_isthmus_size_cm` -> `syn_isthmus_size_cm_legacy_raw`; post-repair rows = 10871. Semantic treatment: preserve original raw VARCHAR view semantics via mig_173 `_legacy_raw`; typed axis/volume columns remain available for numeric analysis.

## Unresolved / out-of-scope failures

- `manuscript_workspace.canonical_detail_pointer_v1`: Binder Error: Referenced column "feeds_master_columns_normalized" not found in FROM clause! | Candidate bindings: "feeds_master_columns_secondary", "feeds_master_columns", "feeds_master_columns_array", "needs_manual_review", "domain"
- `manuscript_workspace.ete_manuscript_analytic_v1`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
- `manuscript_workspace.ete_manuscript_analytic_v2`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
- `manuscript_workspace.ete_manuscript_analytic_v3`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
- `manuscript_workspace.ete_manuscript_analytic_v4`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
- `manuscript_workspace.ete_manuscript_analytic_v6`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?
- `manuscript_workspace.ete_manuscript_analytic_v7`: Catalog Error: Table with name path_malignant_event_fingerprint_v1 does not exist! | Did you mean "path_tumor_size_invariant_v1"?

## dive_cohort_size_v1

- Built successfully with 63 rows.

## Post-apply QC

```text
gate1_verified_tables  gate1_distinct_objects  gate2_missing_signoff  gate3_count_mismatch  gate4_verified_cols_missing_metadata  gate5_clinical_date_violations  cpm_pts  us_gland_v2_pts  us_ln_v2_pts  cohort_parity_ok        release_id  sem_patient_master_rows  sem_cohort_membership_rows  sem_path_malignant_tumor_rows  sem_recurrence_rows  sem_fna_rows  sem_us_nodule_rows  sem_molecular_rows  sem_labs_long_rows  path_borderline_count  recurrence_implausible_date_count  us_nodule_size_outlier_count  us_nodule_multi_attr_unresolved_count  us_nodule_nlp_backfill_pending_count  us_ln_nlp_backfill_pending_count  us_gland_nlp_backfill_pending_count                                  latest_col_registry_batch_id  verified_main_objects_missing_comment     most_recent_signoff_ts                                             most_recent_signoff_migration publication_qc_status_built_at
                   218                     218                      0                     0                                     0                               0    10871            10871         10871              True pub_v1_0_20260430                    10871                       10871                           5944                10739          8050               29504                1384               44124                     27                                132                            15                                  10570                                  2061                              6793                                13578 mig_239_semantic_research_id_varchar_standardization_20260501                                      2 2026-05-01 05:54:23.434531 qc_framework_v1/migrations/238_publication_qc_status_VIEW_v1_20260501.sql     2026-05-01 04:18:53.122476
```

## Notes

- This lane is manuscript_workspace view-DDL only; it does not update `canonical_patient_master`, canonical registry tables, or signoff tables.
- `gate1` is expected to remain unchanged because these cohort views are not registered publication objects.
- No PHI-bearing notes/entity text was queried; the scan used `information_schema` and `COUNT(*)` only.
