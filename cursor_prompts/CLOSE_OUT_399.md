# Script 399 — Close-out (malignant NULL stage_group)

- **Git commit / SHA:** `(pending — Phase 4 commit)`
- **Tag:** `v1_0-malignant-null-stage-group-closeout-20260423_034419`
- **UTC timestamp:** 2026-04-23T03:47:16.624290+00:00
- **Probe SHA256 (consumed):** `57955aec7541a05483b6846256da917ac2c88cbc17083caab7bc3aa3d1079a68`
- **Snapshot FQN (8-row audit):** `thyroid_canonical_publication_v1_0.archive_pub_v1_0.cpm_pre_malignant_null_stage_group_closeout_20260423_034419`

## Schema (queue table)

- `manuscript_workspace.cpm_stage_group_manual_review_v1`: additive `ajcc8_t_stage VARCHAR` (S-1).
- Backfill S-2/S-3: 1404, 12198 T from CPM (both NULL at apply time).

## Halt-gate verdicts (Phase 0) — H1–H10

| gate | result |
|---|---|
| H1 | PASS |
| H2 | PASS |
| H3 | PASS (pre-apply) |
| H4 | PASS |
| H5 | PASS |
| H6 | PASS |
| H7 | PASS |
| H8 | PASS |
| H9 | PASS (queue T column) |
| H10 | PASS (repo grep) |

## Apply summary (CPM — stage_group only)

- **111 / DTC_NOS / T1b N1a M0 / age 28 → Stage I** (DTC age<55 M0; corrected+path I).
- **106 / MTC / T1b N0 M0 / age 60 → Stage I** (MTC AJCC8; path I).

## Queue `ajcc8_t_stage` (8 rows, P10)

- **1404:** `NULL`
- **12198:** `NULL`
- **4015:** `T2`
- **9600:** `T1b`
- **423:** `NULL`
- **924:** `T3b`
- **6275:** `NULL`
- **6768:** `T1a`

## Queue INSERT reasons (`source_script='399'`, 6 rows)

- **4015:** `mtc_t2_n1a_m0_rule_yields_iii_no_builder_or_path_corroboration`
- **9600:** `mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudication_needed`
- **423:** `mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_n1a_m0_row`
- **924:** `mtc_multi_axis_primary_v2_disagreement_t3b_vs_t1a_n1a_vs_n1b_builder_and_path_both_i_no_combination_reconciles`
- **6275:** `other_malignant_staging_rules_undefined_t_null_n_disagreement_n0_vs_n1a`
- **6768:** `other_malignant_staging_rules_undefined_n_disagreement_n1a_vs_n0_path_ii`

## Malignant allowlist status

- Malignant NULL `ajcc8_stage_group` in CPM: **10 → 8** (1404, 12198 + six 399 queue-only rids; 111/106 now staged).
- Full malignant cohort: every row is staged or queued; no orphans.

## CF-399 follow-ups

- **CF-399-1:** MTC vs DTC staging in builder — e.g. rid 423 `_corrected` vs MTC N1a M0; audit MTC row staging rules (potential Script 400).
- **CF-399-2:** MTC M1 stage authority — rid 9600 (IVB vs IVC) — registry / edition adjudication.
- **CF-399-3:** `other_malignant` staging framework (6275, 6768).
- **CF-399-4:** CF-395-1 — chart review for 1404 and 12198 (unchanged).
- **CF-399-5:** Optional v2/dominant columns on queue; v2 for 924/6768 still in `reason` only.

## Phase 3 verification

- **all_pass:** True

## Note (NO-OP materialization)

Database was committed in a prior run; P3 in this script was subsequently corrected to expect **8** malignant CPM NULLs (2×CF-395-1 + 6×Script-399 queue-only rows).
