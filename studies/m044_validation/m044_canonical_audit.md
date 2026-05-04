# M044 canonical cohort validation

- **Generated (UTC):** 2026-05-04T03:09:21Z
- **Cohort:** `manuscript_workspace.cohort_m044_ajcc_ete_v1`
- **SQL:** `scripts/m044_validate_canonical_v1.sql`

## Summary

| Status | `FAIL` |
| Errors | 23 |

## Main audit (`main_audit`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `n_rows` | 4128 | 4012 | no |
| `distinct_research_id` | 4128 | 4012 | no |
| `duplicate_extra_rows` | 0 | 0 | yes |
| `ete_microscopic` | 2576 | 2517 | no |
| `ete_gross` | 1266 | 1262 | no |
| `ete_no_negative` | 192 | 190 | no |
| `ete_present_ungraded` | 29 | 29 | yes |
| `ete_missing_other` | 65 | 14 | no |
| `recurrence_path_proven_raw_n` | 228 | 227 | no |
| `recurrence_path_proven_quarantined_n` | 24 | 24 | yes |
| `recurrence_path_proven_n` | 204 | 203 | no |
| `recurrence_imaging_only_n` | 24 | 14 | no |
| `recurrence_composite_n` | 228 | 217 | no |
| `primary_quarantined_n` | 0 | 0 | yes |
| `primary_negative_days_n` | 0 | 0 | yes |
| `recurrence_path_proven_positive_fu_n` | 199 | 198 | no |
| `recurrence_path_proven_zero_fu_n` | 5 | 5 | yes |
| `fu_zero_n` | 1400 | 1338 | no |
| `fu_positive_n` | 2728 | 2674 | no |

## Cohort membership vs CPM filter

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `cohort_rows_not_in_cpm_filter` | 0 | 0 | yes |
| `cpm_filter_missing_from_cohort` | 0 | 0 | yes |
| `cpm_malignant_staged_n` | — | 4012 | — |
| `cohort_n` | — | 4012 | — |

## CPM ETE consistency (`cpm_ete_consistency`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `ete_mismatch_n` | 178 | 176 | no |
| `n_joined` | — | 4012 | — |
| `ete_match_n` | — | 3836 | — |

## Surgery-date lineage (`surgery_date_lineage`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `n_cohort` | 4128 | 4012 | no |
| `surg_first_nonmissing` | 4128 | 4012 | no |
| `surg_first_missing` | 0 | 0 | yes |
| `surg_date_pre_1999_n` | 3 | 3 | yes |
| `surg_date_1999_2024_n` | 4090 | 3976 | no |
| `surg_date_post_2024_n` | 35 | 33 | no |
| `surg_date_after_2024_06_04_n` | 245 | 236 | no |
| `calendar_partition_violations` | 0 | 0 | yes |

## Table 1B — total thyroidectomy × ETE (`table1b_tt_ete_audit`)

Union rule: `surg_total_thyroidectomy IS TRUE` OR `surg_procedure_type` normalized to `total_thyroidectomy` on `main.canonical_patient_master` (M044 eligibility filter).

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `tt_n_total` | 2798 | 2748 | no |
| `tt_n_noneg` | 59 | 59 | yes |
| `tt_n_microscopic` | 1732 | 1708 | no |
| `tt_n_gross` | 956 | 954 | no |
| `tt_n_present_ungraded` | 23 | 23 | yes |
| `tt_n_missing_other` | 28 | 4 | no |

## Surgery date vs operative v2 (`surgery_date_vs_operative_v2_optional`, informational)

| Metric | Actual |
|--------|--------|
| `n_compared` | 4012 |
| `n_mismatch_vs_first_surgery_date_v2` | 88 |
| `cohort_rows_with_null_first_surgery_date_v2` | 0 |

## Recurrence coherence (`recurrence_coherence`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `v_path_status_missing_bool` | 0 | 0 | yes |
| `v_imaging_only_incoherent` | 0 | 0 | yes |
| `v_none_but_evidence_bool` | 0 | 0 | yes |

## Legacy recurrence audit (`legacy_recurrence_audit`)

Live counts from `manuscript_workspace.m044_legacy_recurrence_flag_audit_v1` (mig_257/258). Legacy flags are **not** analytic endpoints.

| Metric | Actual |
|--------|--------|
| `m044_cohort_n` | 4012 |
| `legacy_any_recurrence_true_n` | 498 |
| `legacy_any_true_canonical_status_none_n` | 314 |
| `legacy_structural_recurrence_true_n` | 1816 |
| `legacy_structural_true_canonical_status_none_n` | 1658 |
| `canonical_recurrence_row_missing_n` | 0 |


## Failures

- main_audit.n_rows: expected 4128, got 4012
- main_audit.distinct_research_id: expected 4128, got 4012
- main_audit.ete_microscopic: expected 2576, got 2517
- main_audit.ete_gross: expected 1266, got 1262
- main_audit.ete_no_negative: expected 192, got 190
- main_audit.ete_missing_other: expected 65, got 14
- main_audit.recurrence_path_proven_raw_n: expected 228, got 227
- main_audit.recurrence_path_proven_n: expected 204, got 203
- main_audit.recurrence_imaging_only_n: expected 24, got 14
- main_audit.recurrence_composite_n: expected 228, got 217
- main_audit.recurrence_path_proven_positive_fu_n: expected 199, got 198
- main_audit.fu_zero_n: expected 1400, got 1338
- main_audit.fu_positive_n: expected 2728, got 2674
- cpm_ete_consistency.ete_mismatch_n: expected 178, got 176
- surgery_date_lineage.n_cohort: expected 4128, got 4012
- surgery_date_lineage.surg_first_nonmissing: expected 4128, got 4012
- surgery_date_lineage.surg_date_1999_2024_n: expected 4090, got 3976
- surgery_date_lineage.surg_date_post_2024_n: expected 35, got 33
- surgery_date_lineage.surg_date_after_2024_06_04_n: expected 245, got 236
- table1b_tt_ete_audit.tt_n_total: expected 2798, got 2748
- table1b_tt_ete_audit.tt_n_microscopic: expected 1732, got 1708
- table1b_tt_ete_audit.tt_n_gross: expected 956, got 954
- table1b_tt_ete_audit.tt_n_missing_other: expected 28, got 4

## Raw `ete_grade_final` distribution (diagnostic)

| ete_grade_final_raw | n |
|---------------------|---|
| microscopic | 2517 |
| gross | 1262 |
| false | 174 |
| present_ungraded | 29 |
| absent | 16 |
| None | 10 |
| true | 4 |
