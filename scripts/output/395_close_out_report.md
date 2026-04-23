# Script 395 — Close-out (Phase 3)

**Snapshot:** `archive_pub_v1_0.cpm_t_sync_pre395_20260423_001407`

## Verification summary

| Check | Value |
|---|---|
| V1 n_orphans_remaining (DTC N/M set, stage_group NULL) | 2 (expect 2) |
| V2 n_fully_filled (395_t_synced) | 11 (expect 11) |
| V3 distribution | [('I', 4), ('II', 7)] (expect I=4, II=7) |
| V3 dist_ok | True |
| V4 n_mismatch T vs T_v2 | 0 (expect 0) |
| V5 n_mismatch stage vs corrected | 0 (expect 0) |
| V6 manual_review still NULL T+stage | 2 (expect 2) |
| V7 queue rows source_script=395 | 2 (expect 2) |
| V8 n_cpm | 10871 (expect 10871) |
| V9 T3b DTC orphans | 0 (expect 0) |
| V10 394_fillable stage_group lost | 0 (expect 0) |
| V11 __readme Script 395 rows | 1 (expect 1) |

**Phase 3 pass:** True

## Carry-forward

- **CF-395-1:** research_ids 1404, 12198 in manual-review queue — chart review for AJCC edition.
- **CF-395-2:** Builder should COALESCE/fallback T from ajcc8_t_stage_v2 to avoid future gaps.
