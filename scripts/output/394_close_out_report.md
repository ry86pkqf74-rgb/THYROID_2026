# Script 394 — Close-out (Phase 3)

**Snapshot:** `archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452`

## Verification summary

| Check | Value |
|---|---|
| V1 n_orphans_remaining (NULL-T cohort) | 13 (expect 13) |
| V2 n_filled (394_fillable) | 20 (expect 20) |
| V3 distribution | [('I', 14), ('II', 1), ('IVB', 5)] (expect I=14, II=1, IVB=5) |
| V3 dist_ok | True |
| V4 deferred unchanged (stage_group NULL) | 13 (expect 13) |
| V5 n_cpm | 10871 (expect 10871) |
| V6 snapshot cohort_tag counts | [('394_fillable', 20), ('395_deferred_needs_T', 13)] |
| V6_ok | True |
| V7 __readme rows (Script 394) | 1 (expect 1) |
| V8 n_mismatch fillable cohort | 0 (expect 0) |
| V9 T3b DTC orphans | 0 (expect 0) |

**Phase 3 pass:** True

## CF-394-1 — Deferred to Script 395 (Q0-E verbatim)

13 age≥55 M0 DTC rows: sync `ajcc8_t_stage ← ajcc8_t_stage_v2` where populated; re-derive stage_group; research_ids 1404 and 12198 may need manual review if no T signal.

| research_id | diagnosis_primary | age_at_surgery | N | M | t_v2_signal | path_t_raw |
|---:|---|---:|---|---|---|---|
| 1799 | PTC | 55 | N0 | M0 | T2 | 2 |
| 5781 | PTC | 55 | N1a | M0 | T1b | 1b |
| 497 | PTC | 56 | N1a | M0 | T1 | 1 |
| 165 | PTC | 57 | N0 | M0 | T1a | T1a |
| 1050 | FTC | 58 | N1a | M0 | T1b | 1 |
| 5569 | PTC | 58 | N1a | M0 | T1a | 1a |
| 1138 | FTC | 61 | N0 | M0 | T3a | 3 |
| 12198 | PTC | 61 | N1a | M0 | NULL | · |
| 325 | PTC | 63 | N0 | M0 | T1a | 1 |
| 1404 | PTC | 64 | N1a | M0 | NULL | · |
| 651 | PTC | 72 | N1a | M0 | T1a | 1a |
| 3790 | PTC | 76 | N0 | M0 | T2 | 2 |
| 1074 | PTC | 85 | N1a | M0 | T3a | 3 |
