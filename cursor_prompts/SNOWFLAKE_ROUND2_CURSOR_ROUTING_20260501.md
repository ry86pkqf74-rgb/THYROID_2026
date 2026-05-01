# Snowflake Round-2 Findings — Cursor Routing Summary

**Generated:** 2026-05-01 by Cowork.

5 Cursor prompts were authored from the round-2 Snowflake validation findings.
Each maps to a specific tool. Run them in this order; later migs depend on earlier ones being applied.

| # | Mig | Title | Tool | Severity |
|---|---|---|---|---|
| 1 | mig_254 | M1 → Stage II reconcile | **Cursor Chat (Claude Sonnet 4 / GPT-5) → Composer** | HIGH |
| 2 | mig_255 | recurrence flag/timing reconcile | **Cursor Composer** | MEDIUM-HIGH |
| 3 | mig_256 | 6 benign + recurrence reconcile | **Cursor Composer** (case-by-case) | LOW |
| 4 | mig_257 | followup > survival repair | **Cursor Composer** (mig_101 pattern) | MEDIUM |
| 5 | mig_258 | N-stage vs LN count reconcile | **Cursor Chat (Claude Sonnet 4 / GPT-5) → Composer** | HIGH |

**When to use Chat-first vs straight-Composer:**
- **Chat-first (mig_254, mig_258):** the rule itself needs human/clinical adjudication. Composer would just guess. Get the rule ratified by Logan in Chat, *then* paste the §0 message into Composer with the chosen rule embedded.
- **Composer (mig_255, mig_256, mig_257):** the rule is mechanical (pattern-match a prior mig); Composer can run the whole dispatch.

**Why Cursor over VSC GPT-5.5:**
For your workflow, Cursor Composer wins on every mig because it has direct access to your file structure, your `cursor_prompts/` history, MotherDuck MCP, and Desktop Commander. VSC + GPT-5.5 (e.g. via Copilot Chat) is only better when you want the strongest reasoning model and don't need codebase integration — but Cursor's Claude Sonnet 4 / GPT-5 in Chat covers that case while keeping the codebase context. **Recommendation: do all 5 in Cursor.**

**After all 5 land in MotherDuck:**
- Re-export from MD via `snowflake_trial/scripts/01_export_md_to_parquet.py`
- Reload Snowflake via `02_load_to_snowflake.py`
- Rerun `06_prompt3_survival.py` and `07_prompt5_staging.py` — counts should drop to the expected disposition target (0 for Options A/B; declared residual for Option C on mig_258).

## File index (all in `cursor_prompts/`)

1. `CURSOR_PROMPT_MIG_254_M1_STAGEII_RECONCILE_20260501.md`
2. `CURSOR_PROMPT_MIG_255_RECUR_FLAG_TIMING_20260501.md`
3. `CURSOR_PROMPT_MIG_256_BENIGN_RECUR_RECONCILE_20260501.md`
4. `CURSOR_PROMPT_MIG_257_FU_POST_DEATH_20260501.md`
5. `CURSOR_PROMPT_MIG_258_NSTAGE_LNCOUNT_RECONCILE_20260501.md`
