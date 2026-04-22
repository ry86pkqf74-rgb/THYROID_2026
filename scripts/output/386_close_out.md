# Script 386 — post-387 dedup pass close-out
**Date:** 2026-04-22T13:43:27.633091+00:00
**Script:** `scripts/386_v1_0_dedup_pass.py`
**Prompt:** `cursor_prompts/CURSOR_PROMPT_LLM_INTEGRATION_AND_V1_0_DEDUP_20260422_POST387.md`

## Pre-state (387 verification)

- `tier2_objects` = 0
- `verify_objects` = 0
- `dup_view_remaining` = 0
- `ms_legacy_remaining` = 0
- `387_close_out_exists` = True
- `387_dedup_probe_exists` = True
- `387_close_out_has_dedup_section` = True

## Part (a) — within-canonical dedup

### NEW canonicals (auto-applied if collapse>0)

| canonical | rows | distinct | null_key | collapse | post-apply rows | post collapse |
|---|---:|---:|---:|---:|---:|---:|
| `canonical_pathology_clinical_events_v1` | 13,358 | 13,358 | 0 | 0 | n/a | n/a |
| `canonical_cervical_ln_clinical_events_v1` | 4,493 | 4,493 | 0 | 0 | n/a | n/a |
| `canonical_esophageal_invasion_events_v1` | 188 | 188 | 0 | 0 | n/a | n/a |

### Pre-existing canonicals re-verification

No NEW collapses introduced since 387 baseline. Pre-existing carry-forwards (complications, invasion, medications, molecular_genetics_v2, path_malignant, pmh, psh) remain at the 387 baseline counts.


## Part (b) — residual archive_drop scan

- main.* tmp/stg/wip: 0
- registry rows superseded=TRUE: 0
- __readme deprecation log: probe skipped (no structured log column)

## Part (c) — view-naming verification

- main views violating `%_VIEW_v%` convention: 0

## Carry-forward items (filed for follow-up)

### 6.A — Synoptic_diagnosis 88% extraction error rate (Script 368 / vasc v2)
Vascular v2 had 3,187 errors / 3,635 attempts (~88%) on `source_column = synoptic_diagnosis`. RunPod vLLM `InternalServerError`, likely context-length or batch-timing issue specific to this column's text shape. Re-extract on smaller batches with a more conservative inference profile.

### 6.B — `canonical_invasion_events_v1` rebuild (Script 388 candidate)
Cross-domain invasion canonical still references the qwen-era source via `extraction_run_id`. Needs re-derivation from the post-368 source while preserving cross-domain rows from path_malignant + frozen_section + airway + ETE.

### 6.C — Pathology `benign_pathology` entity routing precision
Spot-check found 2/4 borderline cases (atypical-cell + tubular adenoma routing). If `nlp_path_*` rollups are used in any cohort definition, audit the patient list for false positives before publishing.

### 6.D — Capsular_invasion margin-distance false positives (vasc v2)
v2 prompt returns `capsular_invasion` for margin-distance phrases ('inked/capsular margin is very close (0.1mm)', '<1 mm from anterior'). When rebuilding `canonical_invasion_events_v1` (carry-forward 6.B), add evidence-text postfilter rejecting `\d+(\.\d+)?\s*(mm|cm)` patterns that lack other capsular language.

### 6.E — `tier2.*` and `verify.*` schema drop — DONE by Script 387

### 6.F — Tirads_granular absorb chain (Script 388 candidate, NEW)
Script 383 landed `note_entities_llm_tirads_granular` but the 376/377/378 absorb chain has no live inputs (`tirads_v2_nodules_raw`, `note_entities_llm_us_nodule_dynamics`, `note_entities_llm_imaging` were all archived to us_legacy_20260421 on 2026-04-21). A future script needs to reconstitute the parsing pipeline from the new tirads_granular source into `canonical_us_nodule_v2`.

### 6.G — `nlp_vasc_positive_mentioned` patient count slightly above evaluator estimate
Script 368 post-fix produced 776 patients with `nlp_vasc_positive_mentioned=TRUE`, vs the evaluator's spot-check estimate of 719. Within precision-improvement tolerance (well below the >900 fail threshold), but slightly above the [680, 760] sanity range from the verification checklist. Likely driven by qualifier-NULL + present_or_negated='present' rows the evaluator's qualifier-only count excluded.

### 6.H — `chain script flag` discrepancy in cursor prompt
Prompt §3 B3 references `--apply` flag for Scripts 376/377/378/379; the actual scripts accept `--commit`. Documented in Script 383 source comments.

### 6.I — Round-2 ckpt `llm_model` tag is qwen2.5-32b, not gpt-oss-120b
All 4 round-2 ckpt JSONL files are tagged `llm_model: qwen2.5-32b`, but row counts and entity-type distributions match the prompt's gpt-oss-120b stats exactly. Either the ckpt model field is stale (likely) or the prompt's claim is wrong. We preserved the model tag as-is on the source tables; surface to the evaluator for ground-truth on which model actually generated these.

