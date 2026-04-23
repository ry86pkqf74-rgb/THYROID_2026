# Script 401 — Phase 0 probe (manual-review queue sort-out, narrowed)

## Halt gates (H1–H10)

| all_pass | True |

- **H1 (queue total / sources):** count=8, src={'395': 2, '399': 6}
- **H2 (CPM apply WHERE, rid 4015):** 1 (expected 1)
- **H3 (4015 in queue, source 399):** 1 (expected 1)
- **H4 (1404,12198,924,6768 present):** True []
- **H5 (CPM total):** 10871
- **H6 (static MTC T2 N1a M0 → III):** True
- **H7 (423,9600,6275 in queue; writes skip them):** True
- **H8 (no prior archive tables for 401):** cpm=0, q=0
- **H9 (CPM SET audit, stage_group only):** True
- **H10 (write SQL has no 6275):** True
- **Malignant NULL stage_group (pre):** 8

## Queued rows (pre) — reason digest

- **12198** (395): no_T_signal_path_stage_raw_III_ajcc_edition_unknown
- **1404** (395): no_T_signal_path_stage_raw_III_ajcc_edition_unknown
- **4015** (399): mtc_t2_n1a_m0_rule_yields_iii_no_builder_or_path_corroboration
- **423** (399): mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_…
- **6275** (399): other_malignant_staging_rules_undefined_t_null_n_disagreement_n0_vs_n1a
- **6768** (399): other_malignant_staging_rules_undefined_n_disagreement_n1a_vs_n0_path_ii
- **924** (399): mtc_multi_axis_primary_v2_disagreement_t3b_vs_t1a_n1a_vs_n1b_builder_and_path_bo…
- **9600** (399): mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudicat…

## Planned writes

- **A:** CPM 4015 → `ajcc8_stage_group='III'` (AJCC8 Ch 73 MTC).
- **B:** DELETE queue 4015 (source 399).
- **C-1..4:** reason UPDATE 1404, 12198, 924, 6768 (see script constants).
- **D:** `__readme` script_401; dual snapshots (CPM×1, queue×8).
- **NOT applied:** 6275 (PDTC) — Script 402.

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T04:15:34.172113+00:00
