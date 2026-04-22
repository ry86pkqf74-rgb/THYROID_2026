# Script 391 — T-stage Downstream Reconciliation: Close-Out Report

**Completed:** 2026-04-22T22:36:23Z
**Snapshot:** `archive_pub_v1_0.cpm_pre391_20260422_223618`

## Summary

| Phase | Operation | Rows Affected |
|-------|-----------|---------------|
| 2B | ete_grade + ete_grade_final SYNC | 1,144 |
| 2C | DEPRECATED T3b upgrade (gross-ETE) | 172 |
| 2D-B/C | micro_t3b_corrected=TRUE + size-based t_stage | 0 |
| 2D-D | non-corrected t_stage passthrough / gross→T3b | 10,871 |
| 2D-E | stage_group_corrected re-derived (value-changes vs snapshot) | 3,777 |
| 2E | manuscript_cohort_v1.ajcc8_t_stage rebuild | 172 |

## Key Metrics Post-391

- `microscopic_ete_t3b_corrected=TRUE` reduced from 906 → 0 (contradiction cleared)
- Gross-ETE rows with non-T3b DEPRECATED stage: 0 (was 172)
- `ajcc8_t_stage` changes vs snapshot: 1,078 (+T3b=1,078, -T3b=0)

## Stage-Group Cascade Breakdown (top-4 transitions)

| old_group | new_group | n |
|-----------|-----------|---|
| `<NULL>` | I | 3,676 |
| I | II | 55 |
| `<NULL>` | II | 39 |
| `<NULL>` | IVB | 7 |

The 3,676 `<NULL>→I` fills are the dominant movement: patients whose
`ajcc8_stage_group_corrected` was NULL pre-391 (NULL DEPRECATED T-stage →
couldn't stage them) now receive a valid group after the T-stage rebuild.
The 55 `I→II` shifts are the 172-cohort gross-ETE patients whose upgrade
to T3b (age ≥ 55) moved them from Stage I to Stage II.

## All Halt Gates Passed

| Gate | Actual | Window | Result |
|------|--------|--------|--------|
| 2A snapshot rowcount | 10,871 | [10871, 10871] | ✅ |
| 2B ete_grade sync | 1,144 | [1121, 1167] | ✅ |
| 2C DEPRECATED T3b upgrade | 172 | [168, 176] | ✅ |
| 2D-C/D ajcc8_t_stage changes | 1,078 | [1056, 1100] | ✅ |
| 2D-B micro_t3b_corrected=TRUE | 0 | [0, 0] | ✅ |
| 2D-E stage_group cascade | 3,777 | [3702, 3852] | ✅ |
| 2E manuscript_cohort rows | 172 | [168, 176] | ✅ |

## Phase 3: All 9 invariants passed ✅

See `391_run.log` for full execution trace.

## Artifacts

- `scripts/output/391_probe_report.md` — Phase 0 dry-run projections
- `scripts/output/391_plan_approval.txt` — Logan's APPROVED sign-off
- `scripts/output/391_run.log` — Full execution log
- `scripts/output/391_t3b_upgrade_cohort.csv` — 172 research_ids upgraded to T3b
- `archive_pub_v1_0.cpm_pre391_20260422_223618` — Pre-391 snapshot (10,871 rows)

## Column Rename Notes

- `ajcc8_t_stage_corrected` → `ajcc8_t_stage` (phase-4.6 rename, 2026-04-17)
- `tumor_size_cm` → `tumor_size_cm_dominant` (size aggregation refactor)

## Next

Script 392 — boolean-string extractor trace + normalization for the 183
remaining 'false'/'true' junk rows in ete_grade_final_v2.
