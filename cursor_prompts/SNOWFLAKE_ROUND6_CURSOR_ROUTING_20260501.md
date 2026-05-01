# Snowflake Round-6 Findings — Cursor Routing Summary

**Generated:** 2026-05-01 by Cowork.

5 Cursor prompts authored from round-6 validation findings (16 CFs surfaced; consolidated into 5 thematic migs). Run order matters — mig_260 + mig_263 unblock manuscripts; mig_261 + mig_262 are mechanical; mig_264 + mig_265 are decision-pass migs.

| # | Mig | Title | Tool | Severity | Run order |
|---|---|---|---|---|---|
| 1 | mig_260 | CPM TIRADS migration drift re-point | **Cursor Composer** | HIGH | First (M037/M025 unblock) |
| 2 | mig_263 | AJCC overlay re-derive (renumbered from mig_259) | **Cursor Chat → Composer** | MED | First (architectural; future-bug-prevention) |
| 3 | mig_261 | path_synoptics CAP label normalization | **Cursor Composer** | MED | Second |
| 4 | mig_262 | Imaging date cleanup + suspicious-LN flag rebuild | **Cursor Composer** | MED | Second |
| 5 | mig_264 | Bethesda-2 false-negative audit (385 patients) | **Cursor Chat → Composer** | HIGH for M025/M027 | Third |
| 6 | mig_265 | PMH `_definitive` rule + NLP coverage manuscript footnote | **Cursor Composer** | LOW (DML); MED (manuscripts) | Third |

## Why Cursor over VSC GPT-5.5

For your workflow, Cursor wins because it has direct access to your `cursor_prompts/` history, MotherDuck MCP, and Desktop Commander. VSC + Copilot Chat is only better when you want raw reasoning without codebase integration — but Cursor Chat with Claude Sonnet 4 / GPT-5 covers that case while keeping context. **Recommendation: do all 6 in Cursor.**

## When to use Chat-first vs Composer-direct

- **Chat-first (mig_263, mig_264):** the rule itself needs human/clinical adjudication. Composer would just guess. Get the rule ratified by Logan in Chat, then paste the §0 message into Composer with the chosen rule embedded.
- **Composer-direct (mig_260, mig_261, mig_262, mig_265):** the rule is mechanical (typo-map, type-flip, code re-point). Composer can run the whole dispatch.

## After all 6 land in MotherDuck

```bash
cd /Users/ros/THyroid\ 2026
source .venv/bin/activate
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
# Then re-run the affected validation prompts:
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/13_prompt7_tirads_bethesda.py  # post mig_260, mig_264
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/16_prompt10_imaging.py         # post mig_262
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/18_prompt12_synoptic.py        # post mig_261
```
Each report should show counts dropping to disposition target.

## File index (all in `cursor_prompts/`)

1. `CURSOR_PROMPT_MIG_260_CPM_TIRADS_DRIFT_REPOINT_20260501.md`
2. `CURSOR_PROMPT_MIG_261_PATH_SYNOPTICS_LABEL_NORM_20260501.md`
3. `CURSOR_PROMPT_MIG_262_IMAGING_DATE_CLEANUP_20260501.md`
4. `CURSOR_PROMPT_MIG_263_AJCC_OVERLAY_RE_DERIVE_20260501.md` *(supersedes the older `MIG_259` prompt of same content)*
5. `CURSOR_PROMPT_MIG_264_BETHESDA2_FALSE_NEG_AUDIT_20260501.md`
6. `CURSOR_PROMPT_MIG_265_PMH_DEFINITIVE_RULE_20260501.md`

## Renumbering note

The earlier `CURSOR_PROMPT_MIG_259_AJCC_OVERLAY_RE_DERIVE_20260501.md` is **superseded** by `MIG_263` (same content, correct mig number). The mig_259 file slot is occupied by `259_ln_status_source_cf_mig258_20260501.sql` (LN status reconciliation, applied).

## Round-6 CF mapping → mig prompts

| CF | → Mig prompt |
|---|---|
| CF-mig260b-NLP-VASCINV-UNDERFIRE | future NLP-refresh scope (not in this batch — needs upstream NLP rerun) |
| CF-mig260c-NLP-LN-DISCORDANCE | M037 manuscript footnote (no Cursor mig — author in M037 methods text) |
| CF-mig260d-NLP-REC-PRESURGERY | future NLP-refresh scope |
| CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY | mig_262 |
| CF-mig260f-CPM-TIRADS-MIGRATION-DRIFT | mig_260 |
| CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE | mig_262 |
| CF-mig261b-PMH-DEFINITIVE-COL-DEAD | mig_265 |
| CF-mig261c-SMOKING-COVERAGE-GAP | mig_265 manuscript footnote |
| CF-mig261d-FAMILY-HX-COVERAGE-GAP | mig_265 manuscript footnote |
| CF-mig261e-HYPERTENSION-UNDERCOUNT | mig_265 manuscript footnote |
| CF-mig262b-CAP-LABEL-DRIFT-FOCALITY | mig_261 |
| CF-mig262c-CAP-LABEL-DRIFT-LVI | mig_261 |
| CF-mig262d-CAP-LABEL-DRIFT-ETE | mig_261 |
| CF-mig262e-PATH-SYNOPTICS-SURG_DATE-TIMESTAMP | mig_261 |
| CF-mig262f-CAP-MULTI-TUMOR-CONFIRM | verification only (no mig — probe and document) |
