# mig_232 — narrow ACR-missing view close-out

**Date:** 2026-05-01
**Author:** Cline Sonnet 4.6 (v15 batch Prompt 1)
**run_id:** `mig_232_narrow_acr_v15`

## What was built

`manuscript_workspace.vw_us_nodule_tirads_derived_acr_missing_VIEW_v1`

A **narrow** filter over `vw_us_nodule_tirads_any_reported_VIEW_v1` that isolates rows
satisfying **both** conditions simultaneously:
- `acr2017_feature_points_complete = FALSE` (descriptor-incomplete)
- `acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL` (derived-ACR-missing)

Expected row count: **~7,304** (from CF-mig219 reconciliation crosstab).

## Why this view exists alongside `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`

The two sibling views answer **different manuscript denominator questions**:

| View | Filter | Count | Semantic |
|---|---|---|---|
| `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` (mig_219) | `any_reported AND acr2017_feature_points_complete=FALSE` | **24,371** | "Descriptor-incomplete TIRADS" — the legacy CUNC source row was missing ≥1 of the 5 ACR feature fields. Many of these rows still have derived ACR points+category today (via Script 376 backfill). |
| `vw_us_nodule_tirads_derived_acr_missing_VIEW_v1` (mig_232) | `any_reported AND acr2017_feature_points_complete=FALSE AND (acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL)` | **~7,304** | "Derived-ACR-missing" — no ACR score computable at all today, even after normalization/backfill. |

**The distinction matters for manuscript Methods:**
- Use `24,371` (or the complement: `29,504 - 5,120 = 24,384` within any_reported) when describing
  "TIRADS reported but upstream feature descriptors incomplete (as of CUNC v1)".
- Use `~7,304` when describing "TIRADS reported but no derived ACR 2017 score available today".
- Use `5,120` (strict cohort, mig_219) for "full ACR 2017 per-nodule scoring available".

## Carry-forward closed

**CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT** — fully resolved.  The planning expectation of 8,243
was not a bug in mig_219; it was a different filter definition.  mig_221 documented the semantic
difference.  mig_232 now materialises the narrower filter as a named view so it can be
referenced unambiguously.

## Files produced

| File | Purpose |
|---|---|
| `qc_framework_v1/migrations/232_vw_us_nodule_tirads_derived_acr_missing_20260501.sql` | SQL DDL + registry inserts + provenance |
| `scripts/mig_232_apply.py` | Apply + verify script |
| `memory/project_mig_232_narrow_acr_view_closeout_20260501.md` | This file |

## Invariants unchanged

- CPM row count: 10,871
- 5-gate gate1 += 1 (should be 209 post-apply)
- Gates 2-5: unchanged (0)
- No base-table mutations; VIEW only
