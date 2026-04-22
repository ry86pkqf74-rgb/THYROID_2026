# Script 393 — Pre-state probe (Phase 0)

**Generated (UTC):** 2026-04-22T23:58:19.938404+00:00

## Q0-A. Orphan count (DTC, T3b, T/N/M set, stage_group NULL)

| n_orphans |
|---:|
| 9 |

## Q0-B. Derivation preview

| research_id | diagnosis_primary | T | N | M | age | derived_stage_group |
|---:|---|---|---|---|---:|---|
| 11108 | PTC | T3b | N1a | M1 | 56 | IVB |
| 1412 | PTC | T3b | N0 | M0 | 47 | I |
| 1546 | PTC | T3b | N0 | M0 | 36 | I |
| 1908 | PTC | T3b | N0 | M0 | 57 | II |
| 4430 | PTC | T3b | N1a | M1 | 27 | II |
| 5432 | FTC | T3b | N0 | M0 | 64 | II |
| 550 | PTC | T3b | N1a | M0 | 59 | II |
| 6087 | PTC | T3b | N1a | M0 | 54 | I |
| 7566 | PTC | T3b | N1a | M1 | 44 | II |

**Rows with NULL derived_stage_group:** 0

## Q0-C. Corrected also NULL (among T3b orphan cohort)

| n_corrected_also_null |
|---:|
| 0 |

## Q0-D. DTC stage_group NULL by T-stage (global)

| ajcc8_t_stage | n |
|---|---:|
| None | 33 |
| T3b | 9 |

## Q0-D (single count). DTC `ajcc8_t_stage != 'T3b'` (matches V4; excludes NULL T)

| n |
|---:|
| 0 |
