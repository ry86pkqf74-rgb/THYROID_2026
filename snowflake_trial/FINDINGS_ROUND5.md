# Snowflake Trial — Round 5 Validation Findings (Prompts 4/6/7/8)

**Date:** 2026-05-01

## Summary

Four more validation prompts run end-to-end on post-mig_255 data. Two big new findings, two clean confirmations.

## Actionable findings

### Prompt 7 (TIRADS / Bethesda) — NEW MAJOR FINDING

Bethesda ROM is dramatically off published Bethesda 2023 expected ranges:

| Bethesda | N | ROM observed | Expected | Verdict |
|---|---|---|---|---|
| 1 | 233 | 30.5% | 5–10% | **WAY ABOVE** — non-diagnostic showing high malignancy is a tell |
| 2 (benign) | 2,033 | 18.9% | 0–3% | **WAY ABOVE** — biggest single concern, 7× expected |
| 3 (AUS/FLUS) | 642 | 50.9% | 6–18% | **WAY ABOVE** |
| 4 (FN) | 624 | 49.0% | 10–40% | **Slightly above** |
| 5 (susp malig) | 273 | 89.0% | 45–60% | **WAY ABOVE** |
| 6 (malig) | 1,221 | 87.6% | 94–96% | **Slightly below** |

The general direction (Bethesda 1-5 over-malignant, 6 slightly under) suggests this is **cohort enrichment** rather than mismapping — your cohort is operative, so it's biased toward malignancy at every Bethesda level. Manuscript reviewers will demand this be addressed in the methods.

But the **Bethesda-2 / 18.9%** number deserves a deeper look. 385 of 2,033 "benign" cytology patients turned out malignant on path. Either:
- (a) the Bethesda 2 reading was wrong (false-negative cytology — well documented in literature, but ~3% not ~19%),
- (b) the Bethesda 2 was on a non-index nodule (the malignancy was in a separate nodule the FNA didn't sample),
- (c) the index-nodule linkage in the canonical maps Bethesda values to the wrong nodule (think: BETHESDA_INDEX_NODULE_LINKAGE_SOURCE column).

Recommend: **Cursor mig to investigate the 385 Bethesda-2 + malignant patients**, with linkage probe + manuscript footnote.

### Prompt 6 (Invasion) — clean

ETE distribution shows expected pattern: 6,752 NULL/no-malignancy, 2,580 microscopic, 1,313 gross, 179 explicit "none", small "absent"/"true" residuals. AI_CLASSIFY 46/50 Consistent on T×ETE pairs. No new bugs. The 2 "true" entries (boolean string?) and 16 "absent" warrant a tiny normalization mig but not blocking.

### Prompt 4 (RAI / Tg) — clean

RAI receivers (n=483) have mean 21.9 Tg results vs 8.5 for non-receivers (n=3,654). 87% of RAI receivers have Tg followup (vs 52% non-receivers) — the post-RAI surveillance pipeline is functioning. AI_CLASSIFY 29/30 Concordant.

The Tg `VALUE_NUMERIC` column is TEXT (handles "<0.9" detection-limit values); needs `TRY_TO_DOUBLE` for numeric ops. Documented in the script.

### Prompt 8 (Complications) — independent confirmation of mig_252

Snowflake independently reproduces the mig_252 audit numbers exactly:

| Complication | Any event | Strict confirmed | Gap |
|---|---:|---:|---:|
| chyle_leak | 1,576 | 3 | 99.8% |
| seroma | 871 | 39 | 95.5% |
| rln_injury | 690 | 21 | 97.0% |
| hypoparathyroidism | 406 | 296 | 27.1% |
| hematoma | 250 | 68 | 72.8% |

If mig_252 hasn't been applied yet, the structured-finding-status bug needs to land before any complication-touching manuscript (M032 / M038).

## Cross-validation against mig_255 (in this round's reload)

After re-export from MD post-mig_255, Snowflake confirmed:
- PM mismatch (any_recurrence_flag=FALSE + time_to_recurrence_days NOT NULL) **= 0** ✓
- any_recurrence_flag=TRUE total **= 560** ✓ (was 514 + 46 path_proven flips)
- benign + recurrence count grew **6 → 8** (expected — A′ rule flipped 2 path_proven patients coded benign; expands mig_256 scope)

## TIRADS analysis caveat

Auto-pick on the TIRADS column hit `NLP_TIRADS_HAS_COMPONENT_DETAIL` (a boolean presence flag), not the actual TR category. The 14+ TIRADS-related columns on CPM include `NLP_TIRADS_LEVEL`, `TIRADS_BEST_CATEGORY_V12` per memory (but the actual schema may differ). A 5-line script edit will pin the right column for a true TIRADS-vs-malignancy table.

## Pending Cursor migs queue

Updated after this round:

| Mig | Status | Notes |
|---|---|---|
| mig_254 | ✅ DONE (commit 531bd74) | 1,058 → 1,018 verified on Snowflake |
| mig_254b | ✅ DONE (commit 28fa4a7) | rid 9600 IVB verified on Snowflake |
| mig_255 | ✅ DONE | 0 mismatch / 560 TRUE verified on Snowflake; A′/B′ hybrid disposition |
| mig_256 | scope grew 6 → 8 | rerun with new patient list |
| mig_257 | unchanged (100 deceased fu>surv) | ready |
| mig_258 | unchanged (1,501 N-stage gap) | Chat→Composer ready |
| mig_259 | architectural | Chat→Composer ready |
| **NEW** mig_260 | candidate | Bethesda-2 false-negatives audit (385 patients) |
| **NEW** mig_261 | candidate | invasion ETE label normalization ("true" / "absent" cleanup) |
| **NEW** mig_262 | candidate | TIRADS column re-pick + true TIRADS×ROM table |

## Snowflake-Cursor ratchet pattern (now reusable)

This round closes the loop: Snowflake validates → Cursor fixes → Snowflake re-validates. End-to-end MD↔Snowflake round-trip is reliable (~70s for full re-export+reload+flat).
