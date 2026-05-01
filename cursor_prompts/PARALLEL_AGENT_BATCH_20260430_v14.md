# Parallel Agent Batch — v14 Round (post-HEAD `5d6aa85`)

**Generated:** 2026-04-30 by Cowork (post v13 close-out + Lane LN proposal commit)
**For:** Logan to dispatch in parallel across 4 agents
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `5d6aa85` — `qc: lymph nodes + histology — Cowork validation of ChatGPT plan + Lane LN proposal (mig_224-229)`
**MotherDuck state at write:** 5-gate v2 = **190/0/0/0/0** ✓ — cohort parity 10,871/10,871/10,871 ✓

---

## TL;DR — 5 prompts, 4 agents, parallel-safe by design

| # | Lane | Agent / model | Mig labels | Dispatch | Est. time |
|---|---|---|---|---|---|
| 1 | **Lane G** — `semantic_publication` schema + `release_manifest_v1` + 8 safe views | **Cline GPT-5.5** | `mig_223` | Now | 60–90 min |
| 2 | **Lane LN** — LN + histology safe views + 5 QC tables + benign-with-staging quarantine | **Cursor Composer** | `mig_224` → `mig_229` | Now (after Open-Question resolution) | 90–150 min |
| 3 | **CF-mig219 + CF-mig220 reconciliation** — read-only count + scope drift report | **Copilot GPT-5.5** | (no mig) | Now | 30–45 min |
| 4 | **ISSUE_REGISTRY refresh** — open 8 Lane-LN CFs + tighten existing CF tags | **Cline Sonnet 4.6** | (no mig) | Now | 30–45 min |
| 5 | **Future I — Parquet export of frozen tables** | **Cline Sonnet 4.6** | `mig_230_parquet_export` | **GATED** — after #1 + #2 both land clean | 60–120 min |

**Non-overlap matrix:** #1 touches `semantic_publication.*` only. #2 touches `manuscript_workspace.dim_histology_*` / `vw_ln_*` / `qc_ln_*` / `qc_histology_*` + `main.histology_vocab_normalization_map_v1` + 1 ALTER on `main.canonical_path_malignant_events_v1`. #3 + #4 are read-only / file-only. #5 reads from final state. **No write conflicts between any two of #1–#4.**

**Verification suite SSOT:** `qc_framework_v1/queries/cowork_verification_suite_20260430.md` (v2). Every mutating lane must end clean against §1 (5-gate), §2 (cohort parity), §12 (governance gap), §14 (clinical date type).

---

## §1 — Prompt 1: Lane G — `mig_223` (Cline GPT-5.5)

**Why GPT-5.5:** Architectural cross-check valuable (vs Sonnet which built most of the canonical layer); fresh eyes on the manuscript-safe semantic surface; autonomous multi-table build with deterministic DDL.

**Mig label:** `mig_223_semantic_publication_layer_20260430` (was `mig_218` in older draft; v12 renumber; use 223)

**Source-of-truth prompt:** the body in `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md` §"Lane G". Copy that text verbatim, **plus** prepend the wrapper below.

### Prompt wrapper to prepend:

> **Lane G dispatch — v14 round (post HEAD `5d6aa85`).** Read `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` §3 first-action checklist + §7 Path-C verification protocol before tool use. Use mig label `mig_223` (NOT `mig_218`). MotherDuck account `logan.glosser.eras@gmail.com`. Schema `semantic_publication` does NOT yet exist (verified 2026-04-30 by Cowork). After SQL applies, re-run the v2 verification suite at `qc_framework_v1/queries/cowork_verification_suite_20260430.md` §1 + §2 + §12 + §14; expect gate1 to land at **190 + 9 = 199** (1 manifest table + 8 safe views) and gates 2-5 to stay 0. **Non-overlap zones:** do not touch `manuscript_workspace.dim_histology_*` / `vw_ln_*` / `qc_*` (Cursor Composer is in `manuscript_workspace` simultaneously per Lane LN).
>
> [Then paste the existing Lane G prompt body from §"Lane G" of `cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md` lines 305-365.]

### Acceptance:
- `semantic_publication` schema exists; `release_manifest_v1` populated for `pub_v1_0_20260430`
- 8 safe views exist + registered as verified
- Each view's row count matches: `vw_path_malignant_tumor_safe_VIEW_v1=5944`, `vw_recurrence_safe_VIEW_v1` excludes the 132 quarantined, `vw_cohort_membership_safe_VIEW_v1=10871`
- 5-gate gate1 → **199**, gates 2-5 stay 0
- Memory note `project_semantic_publication_layer_20260430.md` written
- Surgical git add per `feedback_surgical_git_add.md`; commit + push

---

## §2 — Prompt 2: Lane LN — `mig_224` → `mig_229` (Cursor Composer)

**Why Cursor Composer:** Multi-mig (6 migrations in one pass), multi-file (SQL + 5 QC table builds + 1 ALTER + view DDL), needs file-system + git context to navigate the assessment plan + emit migrations into `qc_framework_v1/migrations/`. This is the kind of multi-step plan→author→apply→verify→commit work Composer is best at.

**Mig labels:** `mig_224_histology_vocab_extension`, `mig_225_vw_ln_surgery_publication_safe`, `mig_226_vw_ln_patient_publication_safe`, `mig_227_vw_ln_histology_attribution`, `mig_228_qc_ln_histology_tables`, `mig_229_borderline_quarantine_flag`

**🛑 BLOCKING: Logan must answer Open-Questions §7 of the assessment plan before this dispatches.** Specifically:

| OQ | Question | Default if Logan picks "use Cowork's recommendation" |
|---|---|---|
| 1 | `dim_histology_variant_v1` — supersede or extend? | **Extend in place** (smaller blast radius) |
| 2 | Hürthle terminology — `Hurthle cell carcinoma` vs `Oncocytic thyroid carcinoma`? | **Oncocytic thyroid carcinoma** (WHO 2017 preferred); keep `Hurthle cell carcinoma` as alt label |
| 3 | LN denominator source-priority rule when `ln_examined=0` ∧ `nodal_disease_total_count > 0` | **Option (d)** keep both as separate cols + `ln_denominator_source_conflict_flag`; safe view defaults to `nodal_disease_total_count` |
| 4 | Lane LN architecture: `manuscript_workspace.*` (A) or extend Lane G `semantic_publication.*` (B)? | **Option A** (per assessment plan §5) |
| 5 | Mig labels mig_224-229 — any conflicts? | None at v14 round; mig_223 is Lane G |

### Prompt:

> **Lane LN dispatch — v14 round (post HEAD `5d6aa85`).** Build the lymph-node + tumor-histology safe-view + QC layer per the Cowork assessment plan at `qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md`. Read that report end-to-end before any tool use. Logan ratified the following defaults from §7 (override only if Logan attached different values to this dispatch):
>
> 1. **Extend** `dim_histology_variant_v1` in place rather than supersede.
> 2. **Oncocytic thyroid carcinoma** is the manuscript-preferred label; `Hurthle cell carcinoma` is alt.
> 3. **Keep both** `ln_examined_double` and `nodal_disease_total_count_int` as separate cols on the safe view + flag `ln_denominator_source_conflict_flag`. Default denominator is `nodal_disease_total_count` when the double is 0.
> 4. **Option A** — all new objects land in `manuscript_workspace` (NOT `semantic_publication`). Lane G mig_223 is in flight in parallel and owns `semantic_publication.*`.
> 5. Mig labels: `mig_224` through `mig_229`. No conflicts.
>
> Build, in order, **6 migration files** at `qc_framework_v1/migrations/`:
>
> 1. **`224_histology_vocab_extension_20260430.sql`** — INSERT into `main.histology_vocab_normalization_map_v1` for the 7 typos enumerated in assessment plan §4.2 (`Follicular caricinoma`, `microcarinoma`, `microcarcinooma`, `microcaricnoma`, `folliucalr`, `follicualr`, `classsical`, `poorly differntiated`); CREATE OR REPLACE VIEW `manuscript_workspace.dim_histology_standardized_VIEW_v1` adding `carcinoma_flag`, `borderline_flag`, `benign_flag`, `aggressive_histology_flag`, `ptc_variant_group`, `who_terminology_preferred` cols; pre-snapshot `histology_vocab_normalization_map_v1` to `"Thyroid 2026 UPdated".archive_pub_v1_0` first.
>
> 2. **`225_vw_ln_surgery_publication_safe_20260430.sql`** — CREATE OR REPLACE VIEW `manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1` over `canonical_path_malignant_events_dedup_VIEW_v1`. Per-surgery key = `COALESCE(path_surgery_id::VARCHAR, surgery_episode_id::VARCHAR, 'NULL_SURG')`. LN counts collapsed via `MAX()` not `SUM()` per assessment plan §6 mig_225 rule. Add cols: `ln_examined_double`, `nodal_disease_total_count_int`, `ln_examined_safe` (= COALESCE(NULLIF(ln_examined_double,0), nodal_disease_total_count_int)), `ln_positive_safe`, `ln_attribution_ambiguous_flag`, `ln_impossible_count_flag`, `ln_denominator_source_conflict_flag`. Expected ~4,022 rows.
>
> 3. **`226_vw_ln_patient_publication_safe_20260430.sql`** — CREATE OR REPLACE VIEW `manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1` over mig_225 + LEFT JOIN `canonical_patient_master.ln_*` for crossval. Add `ln_total_examined_safe = SUM(ln_examined_safe)`, `ln_crossval_status` ∈ {`concordant`, `discordant_with_cpm`, `cpm_only_null`, `safe_only_null`}, `n_impossible_surgery_ln_rows`. Expected 4,022 patients; expected ~109 discordant.
>
> 4. **`227_vw_ln_histology_attribution_20260430.sql`** — CREATE OR REPLACE VIEW `manuscript_workspace.vw_ln_histology_attribution_VIEW_v1` over dedup + mig_225 + `ln_master_rollup_v1.ln_mets_*`. Add `ln_attribution_confidence` ∈ {`definite_histology_specific`, `probable_histology_specific`, `surgery_level_only`, `ambiguous_multi_histology`, `none_or_unknown`}. Expected 5,944 rows; ≥46 in `definite_histology_specific`.
>
> 5. **`228_qc_ln_histology_tables_20260430.sql`** — CREATE TABLE + INSERT for 5 QC tables in `manuscript_workspace`: `qc_ln_impossible_counts_v1` (~21 rows; 6 dedup + 11 rollup + 4 CPM), `qc_ln_duplicate_rollup_patients_v1` (256 rows), `qc_ln_multihistology_attribution_queue_v1` (48 rows), `qc_histology_borderline_in_malignant_table_v1` (~35 rows), `qc_histology_vocab_typos_v1` (~0 post-mig_224). Each gets a `signoff_registry` + `col_registry` row per `feedback_phi_safety.md` template.
>
> 6. **`229_borderline_quarantine_flag_20260430.sql`** — ALTER TABLE `main.canonical_path_malignant_events_v1` ADD COLUMN `is_borderline_or_benign_with_staging` BOOLEAN; UPDATE per rule (`primary_histology IN ('FTUMP','follicular adenoma','Follicular adenoma') AND (n_stage_ajcc8 LIKE 'N1%' OR m_stage_ajcc8='M1')`); CREATE OR REPLACE VIEW `canonical_path_malignant_events_dedup_VIEW_v1` to propagate the new col. Expected ~35 rows TRUE. Add to col_registry as `verified` with `verification_method='mig_229_borderline_quarantine_rule'`. Pre-snapshot `canonical_path_malignant_events_v1` schema delta to archive.
>
> For every mig:
> - Pre-snapshot to `"Thyroid 2026 UPdated".archive_pub_v1_0`
> - INSERT row into `main.canonical_table_signoff_registry_v1` + per-col rows into `main.canonical_column_verification_registry_v1` (verified_by `cursor_composer_lane_LN`, batch_id `mig_<N>_lane_ln_v14`, verification_method `lane_ln_v14_construct`)
> - INSERT row into `manuscript_workspace.cpm_reconciliation_provenance_v1`
> - Apply via MotherDuck `query_rw` (account `logan.glosser.eras@gmail.com`)
>
> **Acceptance:**
> - 5-gate gate1 += 4 views + 5 QC tables = +9 (with mig_224's `dim_histology_standardized_VIEW_v1` as the +4th view)
> - §12 governance gap stays 0
> - §14 clinical date type stays 0
> - Cohort parity stays 10,871 / 10,871 / 10,871
> - 8 carry-forwards from assessment plan §8 either CLOSED or migrated to `qc_framework_v1/ISSUE_REGISTRY.md` (Cline Sonnet 4.6 owns the registry refresh — see Prompt 4 — coordinate, don't both write to ISSUE_REGISTRY.md)
>
> Surgical git add per `feedback_surgical_git_add.md`; lint Python/SQL first; commit each mig as a separate commit OR as one batched commit `feat(qc): Lane LN mig_224-229 — LN + histology safe views + QC + quarantine`. Push.
>
> Memory note: write `project_lane_LN_closeout_20260430.md` documenting the assessment-plan→apply pattern + the 4 confidence categories + the LN denominator source-priority rule.

### Acceptance gate (post-apply, Cowork verifies):
- §3 of `cowork_verification_suite_20260430.md` passes for all 9 new objects
- All 6 dedup-impossible rows (rids 744, 4426, 4560, 5197, 5917, 8482) appear in `qc_ln_impossible_counts_v1`
- Manuscript readiness for LN/histology claims: **READY**

---

## §3 — Prompt 3: CF-mig219 + CF-mig220 reconciliation (Copilot GPT-5.5)

**Why Copilot GPT-5.5:** Read-only investigation, well-scoped, returns a Markdown reconciliation report + 2 short SQL probes. Single-IDE-session work; doesn't need long autonomous context.

**Mig labels:** none (read-only; no mutating SQL)

### Prompt:

> **CF reconciliation dispatch — v14 round.** Two carry-forwards opened in v13 round close-out (see `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md` §"Carry-forwards opened"). Both are read-only investigations; do **NOT** mutate MotherDuck.
>
> **CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT (manuscript-facing):**
> `manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` returns 24,371 rows; ChatGPT's TIRADS plan said the expected count is 8,243 (≈3× delta). Reconcile: is the view's filter logic correct (5,149 strict + ~3,094 partially-parsed = ~8,243), or is the view including rows ChatGPT excluded? Probe via:
>
> 1. Compare `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` definition (DDL via `pragma_database_size` / `query_definition`) against the 4-cohort decomposition in `qc_framework_v1/migrations/219_tirads_cohort_views_20260430.sql`.
> 2. Re-derive the count using ChatGPT's apparent definition (a ChatGPT-attributed reasoning narrative is in `qc_framework_v1/reports/lane_e_continuation_apply_closeout_20260430.md` — search for "8,243").
> 3. Output the row delta breakdown by `tirads_score_2017` NULL-vs-non-null and `tirads_reported` NULL-vs-non-null.
>
> **CF-mig220-QUEUE-CURRENT-V2-DRIFT (non-blocking):**
> 6 high-pri queue rows in `manuscript_workspace.us_nodule_conflict_queue_v1` didn't map to current `canonical_us_nodule_v2`. Identify all 6 (research_id + queue_row_id + queue_priority + tirads_field + the join-failure reason). Hypothesize whether these are valid orphans (e.g., post-mig_177c_apply absorbed but with ID drift) vs queue entries needing re-anchoring.
>
> **Output:** Write a single Markdown reconciliation report at `qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260430.md` with both findings + recommended remediation (close CF / open new mig label / escalate to Logan). Surgical git add the file; commit with message `qc: CF-mig219 + CF-mig220 reconciliation report`; push.
>
> **Read-only invariant:** zero `query_rw` calls. Use only `query` + file-system writes for the report.
>
> **Acceptance:**
> - Reconciliation report exists with both findings + recommendations
> - 6 queue rows enumerated with research_id (research_id only — no PHI per `feedback_phi_safety.md`)
> - Either closes both CFs or migrates them to `qc_framework_v1/ISSUE_REGISTRY.md` for Lane LN's Cline Sonnet pass to absorb (coordinate with Prompt 4)
> - 5-gate audit unchanged (read-only dispatch)

---

## §4 — Prompt 4: ISSUE_REGISTRY.md refresh + Lane-LN 8 CFs (Cline Sonnet 4.6)

**Why Cline Sonnet 4.6:** Mechanical file authoring, fast, cheap. No DB mutation. Coordinate with Prompt 3 (Copilot) — Cline absorbs Copilot's output once it lands.

**Mig labels:** none (file-only)

### Prompt:

> **Issue registry refresh — v14 round.** Refresh / rebuild `qc_framework_v1/ISSUE_REGISTRY.md` (or create if absent) as the single SSOT for open carry-forwards.
>
> **Step 1 — Read input docs:**
> - `qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md` §8 (8 new Lane-LN CFs)
> - `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md` §"Carry-forwards opened" (2 CFs: CF-mig219, CF-mig220)
> - `qc_framework_v1/reports/cf_mig219_mig220_reconciliation_20260430.md` (Prompt 3's output — read if present; otherwise mark as "pending Prompt 3")
> - Existing `qc_framework_v1/ISSUE_REGISTRY.md` if it exists
>
> **Step 2 — Build the registry table:**
> Columns: `cf_tag` | `severity` (Low/Medium/High) | `domain` | `description` | `count_or_scope` | `discovered_in_mig` | `proposed_remediation_mig` | `status` (`open` / `closed` / `migrated_to_lane_LN` / `migrated_to_lane_G` / `deferred`) | `discovered_date`.
>
> **Step 3 — Add the 8 Lane-LN CFs from assessment plan §8 verbatim:**
> `CF-LN-DEDUP-IMPOSSIBLE-6`, `CF-LN-MASTER-IMPOSSIBLE-11`, `CF-LN-CPM-IMPOSSIBLE-4`, `CF-LN-METS-ARRAY-EMPTY-2801`, `CF-HIST-FTUMP-FA-WITH-N1-M1-35`, `CF-HIST-VOCAB-CARICINOMA-1`, `CF-HIST-VARIANT-TYPOS-AUDIT`, `CF-LN-MASTER-DUP-PTS-256`. Mark each `proposed_remediation_mig=mig_224` through `mig_229` per assessment plan §6 mapping; mark `status=migrated_to_lane_LN` for the 7 that Lane LN handles, `open` for the 1 (`CF-LN-METS-ARRAY-EMPTY-2801`) that's only a Methods caveat, not a remediation target.
>
> **Step 4 — Add the 2 v13-round CFs:**
> `CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT`, `CF-mig220-QUEUE-CURRENT-V2-DRIFT`. Status depends on Prompt 3 reconciliation report: if that report closed them, mark `closed`; otherwise `open` with the report path in `proposed_remediation_mig` cell.
>
> **Step 5 — Audit prior-round CFs (look for any open ones not yet in registry):**
> grep `git log --grep "carry-forward\|CF-"` for the last 30 commits; surface any tags not yet in registry.
>
> **Output:** rewritten `qc_framework_v1/ISSUE_REGISTRY.md` (single file). Surgical git add; commit `qc: ISSUE_REGISTRY refresh — Lane-LN 8 CFs migrated + v13 carry-forwards reconciled`; push.
>
> **Acceptance:**
> - Registry has at least 10 rows (8 Lane-LN + 2 v13)
> - Every `migrated_to_*` CF has a non-null `proposed_remediation_mig`
> - Every `open` CF has either a `proposed_remediation_mig` or a clear "deferred until X" trigger
> - No DB mutation
> - File compiles as valid Markdown table

---

## §5 — Prompt 5: Future I — Parquet export (Cline Sonnet 4.6) — **GATED**

**Why Cline Sonnet 4.6:** Mechanical `EXPORT TO PARQUET` SQL + filesystem; deterministic; cheap. Per v12 handoff §5 + v13 close-out, this triggers AFTER all current cleanup lanes (Lane G + Lane LN) close clean.

**🚦 GATE — DO NOT DISPATCH UNTIL:**
1. Lane G (Prompt 1) committed + pushed — verify `git log --grep "mig_223"` shows commit
2. Lane LN (Prompt 2) committed + pushed — verify `git log --grep "mig_22[4-9]"` shows ≥1 commit
3. 5-gate v2 audit returns gate1 ≥ **199** (190 + 9 from Lane G + ≥9 from Lane LN ≈ ≥208) and gates 2-5 = 0

**Mig label:** `mig_230_parquet_export_pub_v1_0_20260430`

### Prompt:

> **Parquet export dispatch — v14 round, GATED.** Before any work, run the gate check:
>
> ```sql
> SELECT
>   (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
>   EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name='semantic_publication') AS lane_g_landed,
>   EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='manuscript_workspace' AND table_name='vw_ln_patient_publication_safe_VIEW_v1') AS lane_ln_landed;
> ```
>
> If `gate1 < 199` OR `lane_g_landed=false` OR `lane_ln_landed=false` → **STOP**. Report which gate failed; do NOT proceed.
>
> If all 3 gates pass: export every verified canonical table + manuscript_cohort_v1 + signoff registries + the Lane G + LN safe views to local Parquet under `parquet_export/pub_v1_0_20260430/<schema>/<table>.parquet`. Use DuckDB's `COPY (SELECT * FROM <table>) TO '<path>.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)`.
>
> **Scope (read from registries — do not hard-code):**
> 1. Every row of `canonical_table_signoff_registry_v1` where `table_status='verified'` AND `schema_name='main'` (≥190 tables)
> 2. `main.manuscript_cohort_v1`
> 3. `main.canonical_table_signoff_registry_v1` itself + `main.canonical_column_verification_registry_v1`
> 4. All 9 Lane G objects (`semantic_publication.release_manifest_v1` + 8 safe views)
> 5. All 9 Lane LN objects (4 LN/histology safe views + 5 QC tables)
> 6. `archive_pub_v1_0` snapshots from `"Thyroid 2026 UPdated"` DB (read-only; export to `parquet_export/pub_v1_0_20260430/_archive_snapshots/`)
>
> **Mig label:** `mig_230_parquet_export_pub_v1_0_20260430`. Build a manifest at `parquet_export/pub_v1_0_20260430/_MANIFEST.md` listing every exported file + row count + zstd-compressed size + sha256.
>
> **Acceptance:**
> - All ≥208 frozen-table parquets exist on local FS
> - Manifest covers every file with row count + size + sha256
> - INSERT manifest summary into `manuscript_workspace.cpm_reconciliation_provenance_v1` with `run_id='mig_230_parquet_export_v14'`
> - 5-gate audit unchanged (export is read-only against MD)
> - Surgical git add the manifest only (parquets are large; gitignore the `parquet_export/` directory but commit the `_MANIFEST.md`)
> - Commit + push

### Acceptance gate (post-export):
- Re-load 1 random parquet via `read_parquet()` and confirm row count matches MD-side count (sanity check)
- File `parquet_export/pub_v1_0_20260430/_MANIFEST.md` is in repo

---

## §6 — Suggested dispatch ordering

```
T+0:  Dispatch Prompts 1 (Lane G), 2 (Lane LN), 3 (CF reconciliation), 4 (ISSUE_REGISTRY) in parallel
       → 4 distinct agents, 4 distinct workspaces, no write conflicts
T+30: Prompt 3 (Copilot) likely returns first → reconciliation report lands in repo
T+45: Prompt 4 (Cline Sonnet) absorbs Prompt 3's output; ISSUE_REGISTRY refresh commits
T+90: Prompt 1 (Cline GPT-5.5) returns; Lane G mig_223 lands; verify Path-C
T+120: Prompt 2 (Cursor Composer) returns; Lane LN 6 migs land; verify Path-C
T+125: GATE PASSED → dispatch Prompt 5 (Cline Sonnet 4.6) for Parquet export
T+200: Prompt 5 returns; mig_230 lands; v14 round closes clean
```

**Cowork-direct in parallel** (while agents run):
- Spot-check the 6 dedup-impossible rows for Open-Question 3 evidence (rids 744, 4426, 4560, 5197, 5917, 8482)
- Investigate CF-mig220 6 queue rows (read-only; supports Prompt 3 if it gets stuck)
- Watch `git log` + Path-C verify each agent's commit as it lands
- After all 5 prompts close clean: write v14 close-out summary + v15 handoff prompt

---

## §7 — Path-C verification (mandatory for every agent commit)

Per `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md` §7:

1. Probe live MD for the agent's `batch_id`:
   ```sql
   SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE batch_id='<agent_batch_id>';
   SELECT COUNT(*) FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id='<agent_run_id>';
   ```
2. Verify acceptance criteria (row counts, view existence, flag presence)
3. Re-run 5-gate v2 audit — confirm no regression
4. Re-run §12 (governance gap) — should stay 0
5. Re-run §14 v2 (clinical date type) — should stay 0
6. If clean: surgical git add + commit message confirms
7. If issues: surface to Logan with hypothesis + propose remediation mig

---

## §8 — Round delta projection (post-batch)

| Metric | v13 final | v14 projected | Δ |
|---|---:|---:|---:|
| 5-gate gate1 (verified tables) | 190 | **208–212** | +18 to +22 (Lane G 9 + Lane LN 9–13) |
| 5-gate gates 2-5 | 0/0/0/0 | **0/0/0/0** | unchanged (gated by Path-C) |
| §12 governance gap | 0 | **0** | unchanged |
| §14 clinical date type | 0 | **0** | unchanged |
| Open carry-forwards | 2 (CF-mig219, CF-mig220) + 8 implicit Lane LN | **2 or fewer open** + 7 migrated to Lane LN + 1 deferred (`CF-LN-METS-ARRAY-EMPTY-2801`) | -7 active |
| Manuscript readiness | READY | **READY** + LN/histology surfaces stable + Parquet frozen | + reproducibility |

---

## §9 — Quick links

- [v12 handoff prompt](computer:///Users/ros/THyroid 2026/qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v12.md)
- [v13 session summary](computer:///Users/ros/THyroid 2026/qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md)
- [Lane LN assessment plan](computer:///Users/ros/THyroid 2026/qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md)
- [Verification suite v2](computer:///Users/ros/THyroid 2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md)
- [ChatGPT-review followup prompts (Lane G text source)](computer:///Users/ros/THyroid 2026/cursor_prompts/CURSOR_PROMPTS_CHATGPT_REVIEW_FOLLOWUP_20260430.md)
- [GitHub repo](https://github.com/ry86pkqf74-rgb/THYROID_2026)

---

**End of v14 parallel agent batch. Dispatch Prompts 1–4 now in parallel; queue Prompt 5 for after #1 + #2 land clean.**
