# Script 271a — Laterality Concordance Audit

**Run:** 2026-04-18T01:54:10Z
**Snapshot:** `"Thyroid 2026 UPdated".archive_pub_v1_0."canonical_patient_master_pre271a_20260418T015410Z"`

## Column transition (pathology_vs_imaging_laterality_concordant)

| Value | Pre-271a | Post-271a |
|---|---:|---:|
| False | 10023 | 2516 |
| None | 0 | 7507 |
| True | 848 | 848 |

FALSE rows pre-271a included 7,507 patients whose FALSE meant "missing data on at least one side" rather than real disagreement. Post-271a, those rows are NULL.

## Real discordance among both-populated subset

- patients with both `cpm.laterality` and `imaging_laterality_rollup` populated: **3364**
  - concordant (TRUE): **848**
  - discordant (FALSE): **2516**  (74.8%)

Coworker (PROMPT 19) estimated ~1,903 discordances. The realised count is 2516 (≈74.8% of the both-populated subset).

## Distinct values present

### `cpm.laterality` (raw, restricted to non-NULL)

| Value | n |
|---|---:|
| `bilateral` | 5571 |
| `left` | 2428 |
| `right` | 2339 |

### `imaging_laterality_rollup` (non-NULL)

| Value | n |
|---|---:|
| `mixed` | 1356 |
| `bilateral` | 1197 |
| `right` | 446 |
| `left` | 384 |
| `isthmus` | 56 |

## Crosstab (raw)

Each row is a (cpm.laterality, imaging_laterality_rollup) pair counted across patients where both inputs are non-NULL.

| cpm.laterality (raw) | imaging_laterality_rollup | n | apparent match? |
|---|---|---:|:---:|
| `bilateral` | `mixed` | 680 |  |
| `bilateral` | `bilateral` | 603 | ✓ |
| `left` | `mixed` | 326 |  |
| `right` | `mixed` | 322 |  |
| `left` | `bilateral` | 294 |  |
| `right` | `bilateral` | 280 |  |
| `bilateral` | `right` | 192 |  |
| `bilateral` | `left` | 169 |  |
| `right` | `right` | 125 | ✓ |
| `left` | `left` | 120 | ✓ |
| `left` | `right` | 114 |  |
| `right` | `left` | 87 |  |
| `bilateral` | `isthmus` | 25 |  |
| `left` | `isthmus` | 17 |  |
| `right` | `isthmus` | 10 |  |

## Crosstab (normalized: lower(trim(path_lat)))

| path_lat_norm | imaging_laterality_rollup | n | match? |
|---|---|---:|:---:|
| `bilateral` | `mixed` | 680 |  |
| `bilateral` | `bilateral` | 603 | ✓ |
| `left` | `mixed` | 326 |  |
| `right` | `mixed` | 322 |  |
| `left` | `bilateral` | 294 |  |
| `right` | `bilateral` | 280 |  |
| `bilateral` | `right` | 192 |  |
| `bilateral` | `left` | 169 |  |
| `right` | `right` | 125 | ✓ |
| `left` | `left` | 120 | ✓ |
| `left` | `right` | 114 |  |
| `right` | `left` | 87 |  |
| `bilateral` | `isthmus` | 25 |  |
| `left` | `isthmus` | 17 |  |
| `right` | `isthmus` | 10 |  |

## Notes for the next operator

- The Step 6 logic compares strict-string-equality after `LOWER(TRIM())` of `cpm.laterality` against the imaging rollup. If the pathology vocabulary uses `'isthmus_left'`, `'left_isthmus'`, `'lt'`, etc., those would be flagged as discordant against `'left'` even when they semantically match. Inspect the **Crosstab (normalized)** rows marked with no ✓ for cases that should arguably be concordant.
- The Step 6 derivation rule only emits `'mixed'` when both left+right AND isthmus appear in inm_v1; pure `'left+isthmus'` → `'left'`. Pathology may report this as a single-side or mixed value, so inspect any `path=mixed` × `img=left/right` cell.
- If the audit reveals systematic vocabulary collapsing, do NOT patch the concordance flag in place; instead introduce a `*_normalized` derivation in cunc_v1/inm_v1 and rebuild the rollup.
