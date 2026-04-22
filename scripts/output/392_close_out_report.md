# Script 392 — ETE Boolean-String Normalization: Close-Out Report

**Completed:** 2026-04-22T23:46:57Z
**Run stamp:** 20260422_234621
**Snapshot:** `archive_pub_v1_0.cpm_ete_pre392_20260422_234621`
**Tag:** `v1_0-ete-bool-strings-normalized-20260422_234621`

---

## Summary

| Phase | Operation | Rows Affected |
|-------|-----------|---------------|
| 2A | Archive snapshot (183 junk rows) | 183 |
| 2B | `'false'` → `'none'` (both `ete_grade_final_v2` + `ete_grade`) | 179 |
| 2C | `'true'` → `'gross'` (corroborated, both columns + `ete_ordinal_worst`=2) | 2 |
| 2D | T-stage cascade: T2→T3b for 1 row; stage_group re-derived for 2 rows | 1 T-stage / 2 stage-group |
| 2E | Queue uncorroborated `'true'` rows (`boolean_string_no_corroboration`) | 2 |
| 2F | `__readme` provenance | 1 |

**Note on ordinal scale:** Live scale is `{0,1,2}` where 2=gross (not 3 as originally specified). Applied `GREATEST(COALESCE(ete_ordinal_worst, 0), 2)` — consistent with the 240-builder's terminal ordinal value.

---

## Phase 3 Verification Gates

| Gate | Result |
|------|--------|
| V1: 0 `'false'` literals remain in `ete_grade_final_v2` | ✅ PASS |
| V2: Exactly 2 `'true'` literals remain (queue-routed) | ✅ PASS |
| V3: Queue has 2 rows from `queued_by_script='392'` | ✅ PASS |
| V4: `ete_grade == ete_grade_final_v2` for all 183 original rows | ✅ PASS |
| V5: 0 `'false'` strings in full CPM | ✅ PASS |
| V6: Orphan stage_groups (T+N+M set, stage_group NULL, DTC) | ℹ️ 9 (pre-existing, not introduced by 392) |
| V7: `__readme` row for Script 392 landed | ✅ PASS |
| V8: Snapshot rowcount = 183 | ✅ PASS |
| V9: CPM rowcount unchanged at 10,871 | ✅ PASS |

---

## Cascade Outcomes: Gross-Flip Rows (research_ids 11599 and 3430)

### research_id=11599 (age 72, PTC, M1)
| Column | Pre-392 | Post-392 |
|--------|---------|---------|
| `ete_grade_final_v2` | `'true'` | `'gross'` |
| `ete_grade` | `'true'` | `'gross'` |
| `ete_ordinal_worst` | NULL | 2 |
| `ajcc8_t_stage` | T3b | T3b (no change — already T3b) |
| `ajcc8_stage_group` | IVB | IVB (no change — M1-driven) |
| `ajcc8_stage_group_corrected` | IVB | IVB |

**T-stage cascade: no-op** (already T3b; T-stage upgrade gate skipped).
**Stage-group: no change** (M1 → IVB regardless of T or ETE grade).

### research_id=3430 (age 10, PTC, M1, N1a)
| Column | Pre-392 | Post-392 |
|--------|---------|---------|
| `ete_grade_final_v2` | `'true'` | `'gross'` |
| `ete_grade` | `'true'` | `'gross'` |
| `ete_ordinal_worst` | NULL | 2 |
| `ajcc8_t_stage` | T2 | **T3b** (upgraded) |
| `ajcc8_stage_group` | II | II (unchanged — M1-driven) |
| `ajcc8_stage_group_corrected` | II | II |

**T-stage cascade: T2 → T3b** (1 row upgraded).
**Stage-group: unchanged at II.** For AJCC8 DTC, age < 55 + M1 → Stage II regardless of T-stage. The pre-392 Stage II was already correct on account of M1; gross ETE recognition doesn't change it. This is not the age-only I path — M1 supersedes T-stage in the stage-group derivation for all age groups.

---

## Queue Entries (Bucket C — 'true' preserved)

| research_id | diagnosis | `ete_grade_final_v2` in CPM | reason in queue |
|-------------|-----------|----------------------------|-----------------|
| 10626 | FTC | `'true'` (preserved) | `boolean_string_no_corroboration` |
| 9708 | FTC | `'true'` (preserved) | `boolean_string_no_corroboration` |

Both also carry pre-existing 390 entries (`boolean_string_upstream_bug` + `adjudicator_unable_to_determine_rule_a_candidate`). The 392 entries are refinement rows providing cross-evidence context for manual adjudication.

---

## Post-Normalization Distribution

| `ete_grade_final_v2` | n (approx) |
|----------------------|-----------|
| NULL | ~6,752 |
| microscopic | ~2,580 |
| gross | **~1,313** (+2 from 392) |
| none | **~195** (+179 from 392) |
| present_ungraded | ~29 |
| absent | ~16 |
| `'true'` (queue-routed) | **2** (preserved) |
| `'false'` | **0** (cleared) |

---

## Out-of-Scope Notes

- `ete_grade_source` for the 181 normalized rows still reads `tumor_episode_master_v2` (deprecated upstream). No rewrite applied per spec — leave for future cleanup if manuscript tables need the distinction.
- The 9 V6 orphan stage_groups are pre-existing; not introduced by 392.

---

See `392_run.log` for full execution trace.
