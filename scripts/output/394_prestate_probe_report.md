# Script 394 — Pre-state probe (Phase 0)

**Generated (UTC):** 2026-04-23T00:05:02.209606+00:00

## Q0-A. NULL-T DTC orphan count (T NULL, N/M set, stage_group NULL)

| n_null_t_dtc_orphans |
|---:|
| 13 |

## Q0-B. Cohort partition (derivation route × builder_corrected)

| derivation_route | builder_corrected | n |
|---|---|---:|
| NOT_DERIVABLE | NULL | 13 |

## Q0-C. Builder vs AJCC8 M-decidable derivation (fillable subset)

| n_match | n_total |
|---:|---:|
| None | 0 |

## Q0-D. Per-row fillable cohort (20 rows expected)

| research_id | diagnosis_primary | age_at_surgery | N | M | builder_corrected | ajcc8_derived |
|---:|---|---:|---|---|---|---|

## Q0-E. Deferred cohort (395 handoff; builder_corrected NULL)

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
