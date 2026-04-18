# Script 273 — Thin-Wrapper PI Review (24 rows)

## Heuristic

Script 272 used the data-based heuristic from the coworker's Prompt 21 §3 method: a cohort view is classified as `thin_wrapper` when its row count equals the full cohort (10,871) and its column set is a subset of `manuscript_workspace.cohort_descriptive_full_cohort_v1` (164 cols).

## Why provisional

Heuristic agreement does NOT prove a cohort view is intended as a thin wrapper — some manuscripts may legitimately use the full cohort with no filter; others may be `dedicated_filtered` whose filter happens to be a no-op for the current data state. Each row therefore needs PI confirmation.

## What PIs should do

For each row in `manuscript_workspace.thin_wrapper_pi_review_v273`, populate `pi_confirmation` with one of:

- `confirmed_thin_wrapper` — manuscript intentionally consumes the full cohort (no patient filter); Script 274 will un-set `filter_type_provisional`.
- `reclass_dedicated_filtered` — there *is* an intended filter; the view should be rebuilt with that filter and the row reclassified by Script 274.
- `reclass_dedicated_full_cohort` — manuscript explicitly wants the full cohort but tracked separately (e.g. for differing column projection); Script 274 will reclassify and clear `filter_type_provisional`.

## Script 274 hand-off

After PI sign-off, Script 274 will read `pi_confirmation` from `manuscript_workspace.thin_wrapper_pi_review_v273` and: (a) clear `filter_type_provisional=true` on rows confirmed as `thin_wrapper`; (b) update `filter_type` and clear the provisional flag for rows reclassified.

## Snapshot of provisional rows

| manuscript_id | cohort_view_name | row_count | col_count | jaccard |
|---|---|---|---|---|
| 48 | `cohort_m048_tnm_multifocal_v1` | 10871 | 25 | 0.152 |
| 49 | `cohort_m049_pyramidal_lobe_v1` | 10871 | 16 | 0.098 |
| 50 | `cohort_m050_tumor_size_volume_v1` | 10871 | 18 | 0.110 |
| 51 | `cohort_m051_ete_ln_v1` | 10871 | 26 | 0.159 |
| 52 | `cohort_m052_mrlnd_ln_count_v1` | 10871 | 23 | 0.140 |
| 53 | `cohort_m053_nondiagnostic_fna_v1` | 10871 | 18 | 0.110 |
| 54 | `cohort_m054_niftp_reclass_v1` | 10871 | 19 | 0.116 |
| 55 | `cohort_m055_recurrence_rai_v1` | 10871 | 22 | 0.134 |
| 56 | `cohort_m056_age_epidemiology_v1` | 10871 | 17 | 0.104 |
| 57 | `cohort_m057_risk_stratification_v1` | 10871 | 24 | 0.146 |
| 58 | `cohort_m058_thyroid_size_weight_v1` | 10871 | 21 | 0.128 |
| 59 | `cohort_m059_prognostic_scoring_v1` | 10871 | 27 | 0.165 |
| 60 | `cohort_m060_adenoma_ftump_v1` | 10871 | 22 | 0.134 |
| 61 | `cohort_m061_thyroiditis_outcomes_v1` | 10871 | 23 | 0.140 |
| 62 | `cohort_m062_incidental_frozen_v1` | 10871 | 19 | 0.116 |
| 63 | `cohort_m063_frozen_false_neg_v1` | 10871 | 20 | 0.122 |
| 64 | `cohort_m064_frozen_decision_v1` | 10871 | 22 | 0.134 |
| 65 | `cohort_m065_frozen_tt_vs_lob_v1` | 10871 | 21 | 0.128 |
| 66 | `cohort_m066_parathyroid_id_v1` | 10871 | 20 | 0.122 |
| 68 | `cohort_m068_mutation_labs_v1` | 10871 | 27 | 0.165 |
| 69 | `cohort_m069_graves_hashimoto_v1` | 10871 | 21 | 0.128 |
| 70 | `cohort_m070_hereditary_v1` | 10871 | 19 | 0.116 |
| 71 | `cohort_m071_immunologic_meds_v1` | 10871 | 20 | 0.122 |
| 76 | `cohort_m076_ln_surveillance_v1` | 10871 | 25 | 0.152 |
