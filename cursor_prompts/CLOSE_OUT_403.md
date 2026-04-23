# Script 403 — Close-out (rid 6275 PDTC stage_group)

- **Commit SHA (materialize):** `(pending; Phase 4)`
- **Tag:** `v1_0-pdtc-rid6275-stage-group-applied-20260423_045808`
- **UTC:** 2026-04-23T05:00:39.058861+00:00
- **Probe SHA256:** `65880be5f342648336f3bc90949c55c0e663f357f937937819c6a29c9ab5bcad`

## Halt-gate verdict table

| gate | result | detail |
|---|---|---|
| H1 | PASS | lock count=1 |
| H2 | PASS | 6275 in queue 399 count=1 |
| H3 | PASS | queue total=7 |
| H4 | PASS | CPM=10871 |
| H5 | PASS | PDTC staged=46, bucket=6 |
| H6 | PASS | age<55+M0→I static |
| H7 | PASS | archive cpm=0 q=0 |
| H8 | PASS | SET audit |
| H9 | PASS | ok |
| H10 | PASS | ok |
| (pre) malignant NULL | PASS | count=7 |

## Applied

- rid 6275 (age 38, PDTC histology): `ajcc8_stage_group` **I** (AJCC 8th Ch 73 DTC age-stratified rule).
- Queue DELETE: rid 6275.

## Convention corroboration

| check | result |
|---|---|
| Staged PDTC-like rows | 46 (expect 46) |
| age<55, M0 → Stage I bucket | ≥{EXPECT_PDTC_BUCKET_LT55_M0_I_MIN} precedent rows |

## Post-state

- Queue: 7 → 6.
- Malignant NULL `ajcc8_stage_group`: 7 → 6.
- Remaining queue: **1404**, **12198**, **423**, **924**, **9600**, **6768** — reasons unchanged.

## CF

- **CF-401-5:** resolved (rid 6275 staged).
- **CF-403-1 → Script 404:** PDTC `diagnosis_primary` normalization (47-row cohort); per-row adjudication for mixed PTC/FTC with PDTC features.
- **CF-403-2:** rid 6275 may move to `diagnosis_primary='PDTC'` in Script 404; until then `histology_final` is authoritative.
