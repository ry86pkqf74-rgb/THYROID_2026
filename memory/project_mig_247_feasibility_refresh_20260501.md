# mig_247 — manuscript_feasibility_v1 refresh (2026-05-01)

## Summary

- Rows refreshed in MotherDuck: **83**
- **`canonical_version_at_scoring`**: `v1_0_post_mig_246` for all 83 rows
- **Feasibility color distribution (after refresh)**:
  - GREEN: **27** (was 46 before refresh)
  - YELLOW: **5** (was 17 before refresh)
  - RED: **51** (was 20 before refresh)
- **Manuscript dashboard** (`manuscript_dashboard_VIEW_v1`): `READY_TO_DRAFT` = **3** (post mig_246 commentary listed 16 stale—this drop reflects stricter MISSING-column handling + fractional coverage thresholds on live CPM at 10871-row denominator).
- **semantic_publication.vw_publication_qc_status_VIEW_v1**: gate1 **218** unchanged (feasibility is not gate1-listed).
- **SQL + apply**: regeneration and optional MotherDuck apply via `scripts/_mig247_build_feasibility_refresh.py` (`--apply` updates live DB).

## Interpretation notes

Coverage percentages are **`non-null COUNT / 10,871` on full `canonical_patient_master`** (not restricted to manuscript-specific cohort VIEW rows), matching the documented 04-16 pattern and keeping `variable_coverage_pct[]` comparable across manuscripts with different `candidate_n`.

Any **`key_variables` name absent from live CPM** forces **RED** (per mig_247 heuristic), including analytic aliases like `tirads_best_category_v12` / `tumor_size_cm` that may still exist behind cohort VIEW SQL but **not as CPM bare columns**.

## Color transitions (old→new counts)

| GREEN->GREEN | 26 |
| RED->RED | 20 |
| GREEN->RED | 17 |
| YELLOW->RED | 14 |
| GREEN->YELLOW | 3 |
| YELLOW->YELLOW | 2 |
| YELLOW->GREEN | 1 |

## Manuscript IDs that transitioned to GREEN (rank improved to GREEN)

[39]

## Manuscript IDs with worsened rank vs pre-refresh

[1, 4, 6, 7, 9, 11, 16, 18, 19, 23, 25, 28, 29, 30, 31, 33, 35, 36, 37, 40, 42, 43, 44, 45, 46, 47, 67, 72, 73, 75, 78, 80, 81, 82]

## Variables MISSING from canonical_patient_master (key_variables unchanged)

['heavy_metal_exposure', 'metabolomics_panel', 'quality_of_life', 'tirads_best_category_v12', 'tirads_nodules_scored_combined', 'tirads_worst_category_v12', 'tumor_size_cm', 'zip_code']

## Rename hints documented in gating (not promoted into key_variables[])

{'tumor_size_cm': 'path_tumor_size_cm or tumor_size_cm_max (verify manuscript intent)', 'tirads_best_category_v12': 'CPM lacks v12 alias; NLP rollup: nlp_tirads_max_category; cohort views alias cupm.max_tirads_category_ever', 'tirads_nodules_scored_combined': 'no direct CPM column; use canonical_us / imaging rollup', 'tirads_worst_category_v12': 'see tirads_best_category_v12 / cupm rollup'}

## Cohort views broken during refresh (mig_248 scope)

- m048 `cohort_m048_tnm_multifocal_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m049 `cohort_m049_pyramidal_lobe_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m050 `cohort_m050_tumor_size_volume_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m051 `cohort_m051_ete_ln_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m052 `cohort_m052_mrlnd_ln_count_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m053 `cohort_m053_nondiagnostic_fna_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m054 `cohort_m054_niftp_reclass_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m055 `cohort_m055_recurrence_rai_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m056 `cohort_m056_age_epidemiology_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m057 `cohort_m057_risk_stratification_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m058 `cohort_m058_thyroid_size_weight_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m059 `cohort_m059_prognostic_scoring_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m060 `cohort_m060_adenoma_ftump_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m061 `cohort_m061_thyroiditis_outcomes_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m062 `cohort_m062_incidental_frozen_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m063 `cohort_m063_frozen_false_neg_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m064 `cohort_m064_frozen_decision_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m065 `cohort_m065_frozen_tt_vs_lob_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m066 `cohort_m066_parathyroid_id_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m068 `cohort_m068_mutation_labs_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m069 `cohort_m069_graves_hashimoto_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m070 `cohort_m070_hereditary_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m071 `cohort_m071_immunologic_meds_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...
- m076 `cohort_m076_ln_surveillance_v1`: Binder Error: Table "p" does not have a column named "syn_isthmus_size_cm"  Candidate bindings: : "syn_isthmus_size_cm_legacy_raw", "syn_isthmus_size_parse_stat...

## Distinct cohort views throwing Binder (mig_248)

All 24 failures share **`syn_isthmus_size_cm` stale reference** → replace with **`syn_isthmus_size_cm_legacy_raw`** / parse-status guard per `syn_isthmus` rename policy.

Distinct `cohort_view_name` values (*n*=24):

`cohort_m048_tnm_multifocal_v1`, `cohort_m049_pyramidal_lobe_v1`, `cohort_m050_tumor_size_volume_v1`, `cohort_m051_ete_ln_v1`, `cohort_m052_mrlnd_ln_count_v1`, `cohort_m053_nondiagnostic_fna_v1`, `cohort_m054_niftp_reclass_v1`, `cohort_m055_recurrence_rai_v1`, `cohort_m056_age_epidemiology_v1`, `cohort_m057_risk_stratification_v1`, `cohort_m058_thyroid_size_weight_v1`, `cohort_m059_prognostic_scoring_v1`, `cohort_m060_adenoma_ftump_v1`, `cohort_m061_thyroiditis_outcomes_v1`, `cohort_m062_incidental_frozen_v1`, `cohort_m063_frozen_false_neg_v1`, `cohort_m064_frozen_decision_v1`, `cohort_m065_frozen_tt_vs_lob_v1`, `cohort_m066_parathyroid_id_v1`, `cohort_m068_mutation_labs_v1`, `cohort_m069_graves_hashimoto_v1`, `cohort_m070_hereditary_v1`, `cohort_m071_immunologic_meds_v1`, `cohort_m076_ln_surveillance_v1`
