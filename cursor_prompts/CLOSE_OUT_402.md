# Script 402 — Close-out (histologic classification audit)

- **Commit SHA:** `19b8afd1a994409928a0877a8ec9863e14215456` (tag `v1_0-histologic-classification-audit-20260423_044610`; MotherDuck materialize `5668b7242df949d5511cd14bb18dfd6dc7b87c88`)
- **Tag:** `v1_0-histologic-classification-audit-20260423_044610`
- **UTC (materialize / close-out draft):** 2026-04-23T04:46:13.112990+00:00
- **Phase 4 push:** `git push origin HEAD` and tag push succeeded after `git pull --rebase --autostash origin main` (unstashed local edits).
- **Probe SHA256 (consumed):** `7e2ed768490c1c13a62a5d2e694c7590d70dfed8290a93d64b2690454184c46c`
- **Target FQN:** `thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_histologic_classification_audit_v1`

## Halt-gate table (H1–H8)

| gate | design |
|---|---|
| H1 | Per-axis counts match 47/379/10/788/271/HVA(250-280 rids)/43/118; total=sum |
| H2 | Target absent or full idempotency (rows + per-axis = CPM) |
| H3 | CPM = 10871 |
| H4 | No UPDATE/DELETE to main.canonical_patient_master in static SQL |
| H5 | manuscript_workspace exists |
| H6 | CPM has histology/grade/HVA/variant fields + target columns match spec |
| H7 | No pre-existing cpm_histologic_classification_audit_v1 in archive_pub_v1_0 |
| H8 | No `cpm_stage_group_manual_review_v1` in write SQL |

## Per-axis counts (8 axes) + total

- **PDTC_SCATTER:** 47
- **HURTHLE_SCATTER:** 379
- **DHGTC_CATALOG:** 10
- **PTC_VARIANT_DISCREPANCY:** 788
- **PTC_VARIANT_UNKNOWN:** 271
- **HVA_DATA_QUALITY:** 261
- **AGGRESSIVE_VARIANT_FLAGGED:** 43
- **GRADE_3_OR_4_CROSS_REF:** 118
- **TOTAL:** 1917

## Top intersection patterns (≥2 axes per patient) — up to 5

- `HVA_DATA_QUALITY + PTC_VARIANT_DISCREPANCY`: **75** patients
- `GRADE_3_OR_4_CROSS_REF + PDTC_SCATTER`: **38** patients
- `HURTHLE_SCATTER + HVA_DATA_QUALITY`: **26** patients
- `AGGRESSIVE_VARIANT_FLAGGED + HVA_DATA_QUALITY`: **20** patients
- `GRADE_3_OR_4_CROSS_REF + HURTHLE_SCATTER`: **13** patients

## Zero CPM mutation

- No `UPDATE` / `DELETE` to `main.canonical_patient_master` (Script 402 read-only to CPM).

## Deferred follow-ups (CF-402-1 … CF-402-9)

- **CF-402-1:** PDTC cohort normalization; blocks rid 6275 (CF-401-5).
- **CF-402-2:** Hurthle / oncocytic consolidation (WHO 2022 HCC).
- **CF-402-3:** PTC variant discrepancy — backfill or manual review for multi-variant.
- **CF-402-4:** PTC variant unknown (271) — chart review or accept gap.
- **CF-402-5:** HVA string normalization (case, newlines, pipe order).
- **CF-402-6:** Aggressive variant cohort (43) — prognosis analyses.
- **CF-402-7:** Grade cross-ref — DHGTC/ATC/PDTC patterns.
- **CF-402-8:** NIFTP/FTUMP builder bug (CF-401-1) — separate correction script.
- **CF-402-9:** Script 403 candidate — PDTC consolidation + rid 6275.
