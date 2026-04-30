# mig_171b apply close-out — canonical_us_lymph_node v2 BUILD

**Date:** 2026-04-30
**Lane:** mig_171b / canonical_us_lymph_node_v2_build
**Cursor authored:** `123cebb` (599-line SQL + 160-line plan)
**Logan ratified:** 2026-04-30 (all 6 design decisions in §"Ratification checklist before data write")
**Cowork applied:** 2026-04-30 via Path C

---

## §1 Executive summary

Built two new canonical tables from verified upstream sources, additive to the existing `canonical_us_lymph_node_v2` shell (preserved unchanged):

- `main.canonical_us_lymph_node_events_v2` — **6,973 events / 4,110 distinct rids** / 38 cols
- `main.canonical_us_lymph_node_patient_rollup_v2` — **10,871 rows / 10,871 distinct rids** (full CPM cohort parity) / 32 cols, with 4,110 patients having `has_us_ln_findings=TRUE`
- `main.val_mig171b_canonical_us_ln_build_v1` — 10 validation gate rows / 7 cols (helper, na)

10/10 validation gates evaluated: G1-G8 + G10 PASS. G9 WARN expected (159 fallback exam IDs flagged for downstream exam-master rebuild — `CF-mig171b-EXAM-MASTER-REBUILD`).

5-gate audit: **169 → 171** verified canonicals (+2 new tier2 canonicals); gate2-gate5 unchanged at 0/0/0/21.

---

## §2 Logan's 6 ratified design decisions (executed verbatim)

1. ✓ Existing `canonical_us_lymph_node_v2` shell remains as source/compat table; new build lands in `*_events_v2` and `*_patient_rollup_v2` (additive, not destructive)
2. ✓ US-specific gate for clinical-note rows: explicit `ultrasound|sonogram|sonographic` regex match; 172 imaging rows passed the gate
3. ✓ 9 `tp_*` bridge fields populated from `canonical_path_malignant_events_v1` only at patient rollup grain; no CPM updates
4. ✓ Fallback exam IDs emitted (159 of 6,973 events); downstream exam-master rebuild will resolve them
5. ✓ Evidence snippets capped at 240 chars (PHI safety; `LEFT(REGEXP_REPLACE(..., 240)`)
6. ✓ G9 validation gate WARNs (not FAILs) on fallback exam IDs

---

## §3 Path-C apply trace

| Step | Action | Result |
|---|---|---|
| §A.1 | Pre-snapshot legacy shell → `archive_pub_v1_0.canonical_us_lymph_node_v2_shell_pre_mig171b_20260429` | 6,801 rows ✓ |
| §A.2 | Pre-snapshot signoff registry rows | 1 row ✓ |
| §B | CREATE OR REPLACE TABLE `canonical_us_lymph_node_events_v2` | 6,973 rows ✓ |
| §C | CREATE OR REPLACE TABLE `canonical_us_lymph_node_patient_rollup_v2` | 10,871 rows ✓ |
| §D | CREATE OR REPLACE TABLE `val_mig171b_canonical_us_ln_build_v1` | 10 gate rows ✓ |
| Reg.1 | INSERT col registry rows | 77 rows (38 events + 32 rollup + 7 val) ✓ |
| Reg.2 | INSERT signoff registry rows | 3 rows ✓ |

---

## §4 Validation gate results (full)

| Gate | Status | Observed | Expected |
|---|---|---|---|
| G1_event_id_unique | PASS | 6,973 | 6,973 |
| G2_event_exam_date_nonnull | PASS | 0 | 0 |
| G3_source_modality_us_only | PASS | US | US |
| G4_evidence_snippet_limited | PASS | 240 | ≤240 |
| G5_rollup_row_count | PASS | 10,871/10,871 | 10,871/10,871 |
| G6_rollup_has_findings_bidirectional | PASS | 0 | 0 |
| G7_rollup_event_counts_match | PASS | 0 | 0 |
| G8_events_resolve_existing_exam_master | PASS | 0 | 0 |
| G9_fallback_exam_ids_pending_rebuild | **WARN** (expected) | 159 | 0 ideal pre-rebuild |
| G10_pm_anti_join_rollup | PASS | 0 | 0 |

---

## §5 Registry registration summary

| Table | n_cols | n_verified | n_na | n_not_started | table_status | priority_tier |
|---|---:|---:|---:|---:|---|---|
| canonical_us_lymph_node_events_v2 | 38 | 35 | 3 | 0 | verified | tier2_canonical |
| canonical_us_lymph_node_patient_rollup_v2 | 32 | 30 | 2 | 0 | verified | tier2_canonical |
| val_mig171b_canonical_us_ln_build_v1 | 7 | 0 | 7 | 0 | na | helper |

`na` cols = `build_ts`, `build_migration`, `extracted_at` (auto-provenance) on the canonical tables; all 7 cols on the helper validation table.

`verified` cols use `verification_method = derivation_vs_canonical_us_lymph_node_v2_clinical_note_ln_extracted_v1` (events) or `derivation_vs_canonical_us_lymph_node_events_v2_path_malignant_bridge` (rollup).

---

## §6 Carry-forwards

**Closed by mig_171b:**
- `CF-mig171-DESIGN-RATIFICATION-PENDING` (Logan ratified 6 design decisions)
- `CF-mig171-EXAM-ID-RECIPE-LOCK` (hybrid reuse + fallback now encoded)
- `CF-mig171-SOURCE-COVERAGE-clinical_note_ln_extracted_v1` (172 imaging rows ingested)
- `CF-mig171-SOURCE-COVERAGE-canonical_path_malignant_events_v1` (9 tp_* bridge cols populated)

**Opened by mig_171b:**
- `CF-mig171b-EXAM-MASTER-REBUILD` (159 fallback exam IDs need downstream exam-master rebuild)
- `CF-mig171b-RAW-JSON-REPLAY-DEFERRED` (`note_entities_llm_cervical_ln_detail` remains audit/replay source)

---

## §7 5-gate audit before/after

| | Before | After |
|---|---:|---:|
| gate1 (verified canonicals) | 169 | **171** |
| gate2 | 0 | 0 |
| gate3 | 0 | 0 |
| gate4 | 0 | 0 |
| gate5 (date retype) | 21 | 21 |

Cohort parity: 10,871 / 10,871 ✓

---

## §8 Files committed in this lane

- `qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql` (Cursor, `123cebb`)
- `qc_framework_v1/reports/mig_171b_canonical_us_lymph_node_v2_build_plan_20260429.md` (Cursor, `123cebb`)
- `qc_framework_v1/reports/mig_171b_apply_closeout_20260430.md` (Cowork, this file)

---

End of close-out.
