# Script 402 — Phase 0 probe (histologic classification audit sidecar)

| all_pass | True |

## Per-axis counts (H1)

- **PDTC_SCATTER:** 47
- **HURTHLE_SCATTER:** 379
- **DHGTC_CATALOG:** 10
- **PTC_VARIANT_DISCREPANCY:** 788
- **PTC_VARIANT_UNKNOWN:** 271
- **HVA_DATA_QUALITY:** 261
- **AGGRESSIVE_VARIANT_FLAGGED:** 43
- **GRADE_3_OR_4_CROSS_REF:** 118

- **HVA_DATA_QUALITY (DISTINCT research_id) — H1 range gate:** 261
- **Total rows (one per CPM row per axis, sum of 8):** 1917
- **CPM (H3):** 10871
- **manuscript_workspace (H5):** present
- **Target table (H2):** exists=False rows=-1 idem=False
- **H4 (no CPM mutation in generated SQL):** True; no `DELETE FROM main.`: True
- **H6 (CPM columns):** True; target col match: True
- **H7 (no archive name collision):** True
- **H8 (no queue in writes):** True

## Sample research_id (one per axis, first match)

- **PDTC_SCATTER:** `57`
- **HURTHLE_SCATTER:** `1713`
- **DHGTC_CATALOG:** `11909`
- **PTC_VARIANT_DISCREPANCY:** `2041`
- **PTC_VARIANT_UNKNOWN:** `1889`
- **HVA_DATA_QUALITY:** `776`
- **AGGRESSIVE_VARIANT_FLAGGED:** `5740`
- **GRADE_3_OR_4_CROSS_REF:** `11898`

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T04:46:09.885996+00:00
