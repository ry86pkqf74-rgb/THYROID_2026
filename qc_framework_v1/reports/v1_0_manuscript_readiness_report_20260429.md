# Lakehouse v1.0 manuscript readiness — coverage report

**Generated:** 2026-04-29 (UTC) — MotherDuck `thyroid_canonical_publication_v1_0` (queries via `scripts._md_connect.connect_locked()`)

---

## Executive summary

- **canonical_patient_master** remains **`in_progress`**. Registry snapshot: **1,441** verified, **13** `na`, **144** `not_started`, **0** failed, **1,598** columns total (**CF-mig162-PM-BLOCKED** until `not_started → 0`).
- **Lakehouse-verified columns (all canon tables):** **2,876** rows in `canonical_column_verification_registry_v1` with `verification_status = 'verified'`.
- **Cohort parity:** `canonical_patient_master` has **10,871** rows and **10,871** distinct `research_id`.
- **5-gate audit (premig_162 — PM not yet verified):** gate1 **88** | gate2 **0** | gate3 **0** | gate4 **0** | gate5 **21** (post mig_127 refinement). After PM flips verified, **expect gate1 = 89** absent other churn.
- Clinical event **DATE** policy and archive timestamp allowlists are unchanged from project standards; §4 summarizes compliance posture referenced in mig_160 / `clinical_date_retype_20260428` family.

---

## §1 Tier-2 canonical inventory

`canonical_table_signoff_registry_v1` rows where `table_name LIKE 'canonical_%'` (**58** rows). Status distribution:

| table_name | table_status | n_columns_total | n_verified | n_na | n_not_started | n_failed |
|-----------|---------------|-----------------|------------|------|----------------|----------|
| canonical_patient_master | in_progress | 1598 | 1441 | 13 | 144 | 0 |
| canonical_cleanup_audit_v1 | not_started | 18 | 0 | 2 | 16 | 0 |
| *(56 additional `canonical_*` tables — predominantly `verified`)* ||||||

Representative **`verified`** tables (sorted after PM / cleanup rows): airway, cervical LN, complications, column registry, ETE/invasion/FNA lines, labs (Tg/TSH/Ca/PTH), frozen section, pathology/invasion feeders, recurrence/RAI-related rollups — full row-level export from Section D §1 SQL in-repo migration **162**.

---

## §2 Verification methodology distribution (top 30, verified cols only)

| verification_method | n_cols |
|----------------------|-------:|
| mechanical_derivation_compare | 244 |
| derivation_re_derivation_post_rollup_rebuild | 173 |
| derivation_re_derivation_against_verified_events | 141 |
| external_registry_nsqip_study_linkage_on_cpm | 102 |
| auto_no_source_counterpart | 96 |
| derivation_re_derivation_post_events_repair | 87 |
| derivation_replay_vs_canonical_operative_events_v1_tri_state_null | 60 |
| patient_level_nlp_aggregate_per_condition | 58 |
| derivation_canonical_labs_rollups_mig115_script347 | 56 |
| multi_source_derivation_plus_domain_sanity | 53 |
| source_lineage_thyroid_operative_sheet_feed_on_cpm | 48 |
| structured_source_compare_with_normalizer | 47 |
| parser_provenance_and_internal_nonregression | 41 |
| mechanical_derivation_compare against verified events table | 37 |
| derivation_cervical_ln_multi_source_mig111_113_path_us | 36 |
| derivation_ln_core_path_malignant_and_level_rollups_mig89 | 34 |
| derivation_ln_rollup_pathology_pair_internal_consistency | 31 |
| derivation_vs_canonical_molecular_genetics_v2 | 30 |
| patient_level_ajcc_overlay_dominant_tumor_mig266b_family | 29 |
| source_family_archive_replay_molecular_test_episode_v2 | 28 |
| rule_based_derivation_with_source_limited_nulls | 28 |
| derivation_replay_etemanuscript_mig61c_v6_plus_inline_closeout | 27 |
| commercial_panel_passthrough_afirma_thyroseq | 25 |
| ctc_equivalence_vs_pre_promotion_archive | 24 |
| derivation_vs_canonical_path_events_and_gm_raw_feed | 23 |
| derivation_vs_canonical_invasion_events_v1 | 23 |
| rule_based_derivation_with_source_limited_shell_rows | 23 |
| tumor_histology_counts_and_size_rollups_path_family | 23 |
| derivation_vs_canonical_recurrence_resolved_v1 | 21 |
| derivation_vs_canonical_complications_events_voice_nerve_mig_98c | 21 |

---

## §3 Carry-forward inventory (CF tag counts on column notes)

Top tags by descending column-hit count (truncated — full table has **104** distinct extracted tags):

| cf_tag | n_cols |
|--------|-------:|
| CF-mig136-DAYS-SEMANTIC | 58 |
| CF-117-US-EXAM-ID-PORTABILITY | 53 |
| CF-GEN07-ROM-OCR | 41 |
| CF-90-DATE-FORMAT | 38 |
| CF-87-AJCC | 36 |
| CF-117-US-GLAND-PARENCHYMA | 28 |
| CF-mig137-PM-MOL-DATE-RETYPE | 25 |
| CF-117-US-LN-SHELL | 23 |
| CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any | 17 |
| CF-mig58 | 15 |
| CF-mig145-CT-AIRWAY-COMMENT-PROXY | 15 |
| CF-mig156-N- | 15 |
| CF-PMH-MULTISOURCE-DISAGREEMENT | 15 |
| CF-119-FROZEN-ROLLUP-DATE-RETYPE | 14 |
| CF-mig151-PROC-NLP-VS-CODES-GRAIN | 14 |

---

## §4 Date-type compliance

Clinical calendar dates are governed by **`CF-100-DATE-RETYPE`** umbrella and scripted remediation **`scripts/413_clinical_date_retype.py`** / **`qc_framework_v1/migrations/clinical_date_retype_20260428.md`**. Survival SSOT (**`canonical_survival_followup_v1`**) uses **`last_known_alive_date`** as **DATE**; layered event tables may retain TIMESTAMP for provenance — compare with **`CAST(... AS DATE)`** or **`DATE_TRUNC('day', ...)`** only.

Gate **5** = **21** pending items on **verified** tables (excluding `na`-tagged provenance pseudodates per mig **127**) — aligns with cowork docs; close with global date-retype batches, not mig_162 alone.

---

## §5 Cohort parity

`canonical_patient_master`: **10,871** rows, **10,871** distinct `research_id` — Parity verified (2026-04-29 query).

---

## §6 Open CFs requiring future work (curated disposition)

Cross-reference §3 frequencies. Project-level placeholders called out in the source prompt remain:

| CF tag | disposition |
|--------|-------------|
| CF-mig151-RADTX-DERIVATION-GAP | Deferred unless manuscript radioactive-therapy arm expands beyond current upstream |
| CF-mig150-PTH-MULTI-SOURCE-DERIVATION | Notes/PTH lineage restoration queued |
| CF-mig150-TP-UPSTREAM-NOT-IN-MAIN | Live lateral-neck canonical attaches pending upstream |
| CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO | RAI NLP / iodine avidity substantive backfill deferred |
| **CF-mig162-PM-BLOCKED** *(this run)* | **144** columns still **`not_started`** on PM — finalize remaining thematic clusters (NLP/longitudinal/molecular residuals per batch_id), then rerun Section A |

---

## §7 Recommended manuscript-pipeline next steps

1. Clear **`canonical_patient_master`** residual **`not_started`** columns and re-run pre-flight (**Section A** in migration **162`).
2. Take archive snapshot (**162** pre-snapshot DDL) immediately before APPLY.
3. APPLY **§B only** after gate PASS + 5-gate sweep; expect **gate1 88 → 89**.
4. Re-export this report after APPLY for immutable zip / Zenodo pinning.
5. Lock **baseline cohort** definition (CPM-derived) and primary outcome derivation scripts against git tag post-signoff.

---

**Report provenance:** `qc_framework_v1/migrations/162_patient_master_finalization_and_lakehouse_audit_20260429.sql` (embedded queries §D). **No RW MotherDuck actions** were executed generating this markdown.
