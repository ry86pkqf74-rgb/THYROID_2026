# M044 canonical cohort validation

- **Generated (UTC):** 2026-05-01T14:06:17Z
- **Cohort:** `manuscript_workspace.cohort_m044_ajcc_ete_v1`
- **SQL:** `scripts/m044_validate_canonical_v1.sql`

## Summary

| Status | `PASS` |
| Errors | 0 |

## Main audit (`main_audit`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `n_rows` | 4128 | 4128 | yes |
| `distinct_research_id` | 4128 | 4128 | yes |
| `duplicate_extra_rows` | 0 | 0 | yes |
| `ete_microscopic` | 2576 | 2576 | yes |
| `ete_gross` | 1266 | 1266 | yes |
| `ete_no_negative` | 192 | 192 | yes |
| `ete_present_ungraded` | 29 | 29 | yes |
| `ete_missing_other` | 65 | 65 | yes |
| `recurrence_path_proven_n` | 228 | 228 | yes |
| `recurrence_imaging_only_n` | 24 | 24 | yes |
| `recurrence_composite_n` | 252 | 252 | yes |
| `fu_zero_n` | 1400 | 1400 | yes |
| `fu_positive_n` | 2728 | 2728 | yes |

## Cohort membership vs CPM filter

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `cohort_rows_not_in_cpm_filter` | 0 | 0 | yes |
| `cpm_filter_missing_from_cohort` | 0 | 0 | yes |
| `cpm_malignant_staged_n` | — | 4128 | — |
| `cohort_n` | — | 4128 | — |

## CPM ETE consistency (`cpm_ete_consistency`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `ete_mismatch_n` | 178 | 178 | yes |
| `n_joined` | — | 4128 | — |
| `ete_match_n` | — | 3950 | — |

## Surgery-date lineage (`surgery_date_lineage`)

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `n_cohort` | 4128 | 4128 | yes |
| `surg_first_nonmissing` | 4128 | 4128 | yes |
| `surg_first_missing` | 0 | 0 | yes |
| `surg_date_pre_1999_n` | 3 | 3 | yes |
| `surg_date_1999_2024_n` | 4090 | 4090 | yes |
| `surg_date_post_2024_n` | 35 | 35 | yes |
| `surg_date_after_2024_06_04_n` | 245 | 245 | yes |
| `calendar_partition_violations` | 0 | 0 | yes |

## Table 1B — total thyroidectomy × ETE (`table1b_tt_ete_audit`)

Union rule: `surg_total_thyroidectomy IS TRUE` OR `surg_procedure_type` normalized to `total_thyroidectomy` on `main.canonical_patient_master` (M044 eligibility filter).

| Metric | Expected | Actual | OK |
|--------|---------|--------|-----|
| `tt_n_total` | 2798 | 2798 | yes |
| `tt_n_noneg` | 59 | 59 | yes |
| `tt_n_microscopic` | 1732 | 1732 | yes |
| `tt_n_gross` | 956 | 956 | yes |
| `tt_n_present_ungraded` | 23 | 23 | yes |
| `tt_n_missing_other` | 28 | 28 | yes |

## Surgery date vs operative v2 (`surgery_date_vs_operative_v2_optional`, informational)

| Metric | Actual |
|--------|--------|
| `n_compared` | 4128 |
| `n_mismatch_vs_first_surgery_date_v2` | 87 |
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
| `m044_cohort_n` | 4128 |
| `legacy_any_recurrence_true_n` | 503 |
| `legacy_any_true_canonical_status_none_n` | 316 |
| `legacy_structural_recurrence_true_n` | 1817 |
| `legacy_structural_true_canonical_status_none_n` | 1659 |
| `canonical_recurrence_row_missing_n` | 0 |


## Raw `ete_grade_final` distribution (diagnostic)

| ete_grade_final_raw | n |
|---------------------|---|
| microscopic | 2576 |
| gross | 1266 |
| false | 176 |
| None | 61 |
| present_ungraded | 29 |
| absent | 16 |
| true | 4 |
