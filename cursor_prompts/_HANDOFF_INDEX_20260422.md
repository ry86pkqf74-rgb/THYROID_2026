# Session Handoff — 2026-04-22

New-chat pickup order. Run in this sequence.

## 1. 389 Phase 0B Baseline Patch — RESUME FIRST
**Prompt:** `cursor_prompts/CURSOR_PROMPT_SCRIPT_389_PHASE_0B_BASELINE_PATCH_20260422.md`
**State:** script exists (~2,900 lines); Phase 0B drift gate correctly halted on phantom baselines; 7 patches to apply in place.
**After patches applied:** re-run `python3 scripts/389_us_zombie_view_rewrites_and_complications_audit.py --phase 0`. Expect drift gate PASS at 26,402 / 8,919 / 2,117 / 141.
**Plan-review decision still pending:** write `scripts/output/389_plan_approval.txt` with `Rule A` | `Rule B` | `Rule C` for the complications-audit rule (my recommendation from prior direct probe: **Rule B** — surgical fix, excludes entity_legacy + nlp_proxy from flag_event collapse).
**Ends at:** `v1_0-us-zombies-cleaned-<stamp>` commit + tag.

## 2. 389.1 Registry Schema Migration — PARALLEL-SAFE
**Prompt:** `cursor_prompts/CURSOR_PROMPT_REGISTRY_SCHEMA_MIGRATION_20260422_SCRIPT_389_1.md`
**State:** standalone; no dependencies on 389 or 390; can run any time.
**Scope:** ALTER `detail_table_registry_v1` to add `superseded_by` + `renamed_by_script`; backfill from archive snapshot names.
**Ends at:** `v1_0-registry-migrated-<stamp>`.

## 3. 390 ETE Adjudication Reconciliation — RUN AFTER 389 CLOSES
**Prompt:** `cursor_prompts/CURSOR_PROMPT_ETE_ADJUDICATION_RECONCILIATION_20260422_SCRIPT_390.md`
**State:** prompt complete; Phase 0 probe will halt at plan-review gate.
**Plan-review decision pending:** Rule A (worst-of, ~1,091 flip-up) / Rule B (conservative both-sources, ~749 flip-up) / Rule C (queue-only, no mutation).
**My recommendation:** Rule A + residual re-queue — matches AJCC8 T3b clinical definition.
**Ends at:** `v1_0-ete-reconciled-<stamp>`.

## 4. Carry-forwards expected after 390
- **Script 391:** T-stage downstream reconciliation (`microscopic_ete_t3b_corrected`, `ajcc8_t_stage_*` columns that read `ete_grade_final_v2`).
- **389b (optional):** content-based blob audit on `canonical_us_nodule_v2` if you still want the `length(location_raw) >= 400 OR semicolons >= 2` ~750-row cohort investigated.

---

## Workflow rules the new chat must honor

From `memory/feedback_motherduck_direct_check.md`:
> Before recommending a rule, disposition, next script, or interpreting a probe result, query MotherDuck (`thyroid_canonical_publication_v1_0` and `"Thyroid 2026 UPdated"`) directly for the ground truth — even if the user just pasted a summary, even if memory has a recent snapshot, even if the answer seems obvious from prior conversation.

From `memory/feedback_surgical_git_add.md`:
> Never `git add scripts/output/` or `-A`; stage by explicit path/file-glob.

From `memory/feedback_no_cross_db_canonical_sourcing.md`:
> Canonicals are standalone live objects in `main`; never `FROM archive_pub_v1_0.*`.

From `memory/reference_duckdb_timestamp_tz.md`:
> Always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts cols.

From `memory/feedback_alter_view_dependents.md`:
> `ALTER VIEW RENAME TO` is catalog-only; dependent views keep old SQL body. Must `CREATE OR REPLACE` dependents in same commit.

## 4-place audit pattern (reusable, from 386b)
Any in-place UPDATE on a canonical table:
1. `archive_pub_v1_0.<table>_pre<script>_<stamp>` snapshot
2. `main.__readme` provenance row timestamped at actual UPDATE moment
3. First-run `--apply` stdout (kept in conversation log)
4. `<script>_run.log` commit artifact

## Task state at handoff

| # | status | subject |
|---|---|---|
| 10 | completed | 388 commit + tag |
| 11 | in_progress | 389 Phase 0: discovery + probe (drift-halted; resume via Patch prompt) |
| 12 | pending | 389 Phase 1: plan-review gate — pick Rule A/B/C (complications, not nodule buckets) |
| 13 | pending | 389 Phase 2: apply writes |
| 14 | pending | 389 Phase 3: post-state verification |
| 15 | pending | 389 Phase 4: commit + tag |
| 16 | pending | 389.1 registry schema migration |
| 17 | pending | 390 ETE adjudication reconciliation |

## Memory files loaded at start of new chat
The new chat will auto-load `MEMORY.md` + index entries. Priority reads for this context:
- `project_pub_v1_0_cleanup_script_387_closeout.md`
- `project_round2_llm_integration_script_386_closeout.md`
- `feedback_motherduck_direct_check.md`
- `project_exam_id_portability.md`
- `reference_canonical_naming_convention.md`
