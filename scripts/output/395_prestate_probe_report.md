# Script 395 — Pre-state probe (Phase 0)

## Q0-A. n_remaining_null_t_orphans

| n_remaining_null_t_orphans |
|---:|
| 13 |

## Q0-B. n_match_394_deferred

| n_match_394_deferred |
|---:|
| 13 |

## Q0-C. Route split (T_syncable vs manual_review)

| route | n |
|---|---:|
| T_syncable | 11 |
| manual_review | 2 |

## Q0-D. T-column corroboration (11 T_syncable rows)

| research_id | t_v2 | t_dom | corroboration |
|---:|---|---|---|
| 1050 | T1b | T1b | corroborated |
| 1074 | T3a | T3a | corroborated |
| 1138 | T3a | T3a | corroborated |
| 165 | T1a | T1a | corroborated |
| 1799 | T2 | T2 | corroborated |
| 325 | T1a | T1a | corroborated |
| 3790 | T2 | · | v2_only_ok |
| 497 | T1 | · | v2_only_ok |
| 5569 | T1a | T1a | corroborated |
| 5781 | T1b | T1b | corroborated |
| 651 | T1a | T1a | corroborated |

## Q0-E. Projected stage_group distribution (age≥55 M0, t_v2 populated)

| projected | n |
|---|---:|
| I | 4 |
| II | 7 |

## Q0-F. Manual-review rows path stage

| research_id | path_stage_raw | gm_path_stage_raw | N | M | age |
|---:|---|---|---|---|---:|
| 12198 | III | III | N1a | M0 | 61 |
| 1404 | III | III | N1a | M0 | 64 |

## Q0-G. Manual-review queue table

| n_tbls (information_schema) | n_rows source_script=395 (if table exists) |
|---:|---:|
| 0 | 0 |
