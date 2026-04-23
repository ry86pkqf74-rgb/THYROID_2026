# Script 403 — Phase 0 probe (rid 6275 PDTC stage_group apply)

## Halt gates (H1–H10 + malignant pre-check)

| all_pass | True |

- **H1 (rid 6275 state lock):** 1 (expected 1)
- **H2 (6275 in queue, source 399):** 1 (expected 1)
- **H3 (queue total):** 7 (expected 7)
- **H4 (CPM total):** 10871 (expected 10871)
- **H5 (PDTC convention):** staged=46 (expected 46), age<55 M0→I bucket=6 (min 5) → True
- **H6 (static AJCC8 Ch 73 DTC age-stratified):** True
- **H7 (archive unused):** cpm=0, queue=0
- **H8 (CPM SET audit):** True
- **H9 (writes only rid 6275):** True ok
- **H10 (no peer rids in write SQL):** True ok
- **Malignant NULL stage_group (pre):** 7 (expected 7)

## PDTC convention precedent (already-staged cohort)

| bucket | rule | count (live) |
|---|---|---|
| All PDTC-like, staged | `ajcc8_stage_group IS NOT NULL` | 46 |
| age<55, M0, Stage I | DTC age-stratified precedent | 6 |

## Queued rows (pre)

- **12198** (395): ptc_age_61_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review
- **1404** (395): ptc_age_64_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review
- **423** (399): mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_n1a_m0_row
- **6275** (399): other_malignant_staging_rules_undefined_t_null_n_disagreement_n0_vs_n1a
- **6768** (399): angiosarcoma_of_thyroid_per_histology_final_not_ajcc8_thyroid_stageable_soft_tissue_sarcoma_framewor…
- **924** (399): mtc_age_33_primary_t3b_n1a_outlier_vs_v2_ajcc7_dominant_all_t1a_n1b_majority_signal_yields_iva_under…
- **9600** (399): mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudication_needed

## Planned writes

- **A:** CPM UPDATE rid 6275 → `ajcc8_stage_group='I'` (AJCC8 Ch 73 DTC; PDTC grouped with DTC).
- **B:** DELETE queue rid 6275 (`source_script='399'`).
- **C:** `__readme` script_403; snapshots: CPM×1, queue×1 (6275 pre-delete).
- **NOT:** `diagnosis_primary` / classification columns (Script 404).

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T04:58:07.852232+00:00
