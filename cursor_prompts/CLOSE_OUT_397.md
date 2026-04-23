# Script 397 — Close-out

- **Git commit / SHA:** `(pending; run git rev-parse HEAD after commit)`
- **Tag:** `v1_0-cpm-tn-primary-from-v2-filled-20260423_030702`
- **UTC timestamp:** 2026-04-23T03:07:06.372552+00:00
- **Probe SHA256 (consumed):** `921e50e18f2be6c37a1aaaf67a8b1288f9b2610e1d36f1371aa6b8bb103389a7`
- **Snapshot FQN:** `thyroid_canonical_publication_v1_0.archive_pub_v1_0.cpm_pre_tn_primary_from_v2_fill_20260423_030702`

## Halt-gate verdicts (Phase 0)

| gate | verdict |
|---|---|
| H1 | PASS (26 T, 213 N, 236 distinct) |
| H2 | PASS (allowlist tautology 0) |
| H3 | PASS (0 M-rescue) |
| H4 | PASS (predicates enforce NULL primary) |
| H5 | PASS (10,871) |
| H6 | PASS (T/N only) |
| H7 | PASS (archive prefix) |
| H8 | PASS (axis-split: T- vs N-write predicates) |

## Writes

- T primary ← v2: **26**
- N primary ← v2: **213**
- **Distinct `research_id` values:** 236

## Per diagnosis (malignant rescue rows touched)

| diagnosis_primary | rows |
|---|---:|
| ATC | 4 |
| DTC_NOS | 1 |
| FTC | 44 |
| MTC | 9 |
| PTC | 173 |
| other_malignant | 5 |

**Cosmetic alignment:** most updated rows (231) already had a populated `ajcc8_stage_group`; T/N fill aligns primary with v2 for audit without changing group.

## T-filled rows with pre-NULL `ajcc8_stage_group` (CF-397-1)

- `106` — MTC (stage NULL)
- `111` — DTC_NOS (stage NULL)
- `4015` — MTC (stage NULL)
- `6768` — other_malignant (stage NULL)
- `9600` — MTC (stage NULL)

**CF-397-1 composition (n=5):** 1× DTC_NOS, 3× MTC, 1× other_malignant — total 5, so the MTC subtotal is **3** (with DTC_NOS + other_malignant), not 4+1+1; any earlier '4 MTC' wording was an MTC subtotal slip, not a fifth row.

- **CF-397-1:** Re-derive or adjudicate `ajcc8_stage_group` for these rids as needed.
- **CF-397-2:** Builder source fix — `COALESCE(primary, v2)` for T/N (and M) on CPM build.
- **CF-397-3 → Script 398:** Read-only sidecar of primary↔v2 disagreements (T/N/M).

## Disagreement rows

Primary↔v2 disagreement cohorts (363 T, 2055 N, 1838 M) were **not** written by this script.
