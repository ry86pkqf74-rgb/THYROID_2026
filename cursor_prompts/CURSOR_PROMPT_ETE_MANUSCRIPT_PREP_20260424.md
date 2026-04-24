# Cursor Prompt — ETE Manuscript Analytic View + Simple-Issue Close-Out

**Date:** 2026-04-24
**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Target schema:** `manuscript_workspace` (view-layer only; DO NOT mutate `main.*`)
**Author:** Logan Glosser
**Context:** Prompts 41–50 closed FNA02–05, OP01–05, GEN13–16 via view-layer overlays. Commit `618b095` on origin/main. The ETE manuscript analysis is blocked on **one remaining deliverable**: a single analyst-ready view that fuses all the relevant clean overlays into one place, so the statistician does not have to re-JOIN every layer by hand.

This prompt has three phases. Run them in order. Do not skip the probes — recent close-outs hit stale registry counts, drifted dates, and silent partition-key bugs. Never trust a summary; always probe.

---

## Phase 0 — Pre-flight checks (do not skip)

Probe MotherDuck directly to verify current state before writing any DDL. For every claim the registry or prior commit message makes, run a `SELECT COUNT(*)` against the live DB.

1. **Confirm the 13 prompt 41–50 deprecation-log rows are present:**
   ```sql
   SELECT closing_prompt, issue_id, deprecation_kind
   FROM manuscript_workspace.canonical_deprecation_log_v1
   WHERE closing_prompt IN ('prompt_41','prompt_42','prompt_43','prompt_44','prompt_45',
                            'prompt_46','prompt_47','prompt_48','prompt_49','prompt_50')
   ORDER BY closing_prompt;
   ```
   Expected: 10 rows. If fewer, stop and report.

2. **Confirm the clean views exist:**
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema='manuscript_workspace'
     AND table_name IN (
       'canonical_path_malignant_events_v1_ete_clean',
       'canonical_path_malignant_events_v1_global_epi',
       'canonical_path_malignant_events_v1_histology_clean',
       'canonical_path_malignant_events_v1_variant_clean',
       'canonical_path_malignant_events_v1_laterality_clean',
       'canonical_path_malignant_events_v1_size_flag',
       'canonical_path_malignant_events_v1_ln_denominator_flag',
       'canonical_path_malignant_events_v1_invasion_clean',
       'canonical_operative_events_v1_rule_clean',
       'canonical_fna_patient_rollup_v1_clean',
       'canonical_molecular_genetics_v2_braf_variant',
       'path_event_discordance_v1',
       'path_episode_multifocality_v1',
       'manuscript_cohort_v1_histology_clean',
       'manuscript_cohort_v1_recurrence_clean',
       'manuscript_cohort_v1_surgery_reconciled'
     )
   ORDER BY table_name;
   ```
   Expected: all 16. If any missing, stop and report — the manuscript view depends on all of them.

3. **Snapshot the queue counts for ETE-adjacent issues:** ETE01/ETE02, PATH04 (variant), PATH05 (margin), PATH08 (size), PATH13 (LN count), PATH18 (invasion), AJCC01/02/03. Treat any drift from the registry as a re-probe signal, not a bug to fix in this prompt.

---

## Phase 1 — Close the registry-artifact mismatches (low risk, high value)

The composite deprecation-log format splits issue IDs with `/` on some rows and `,` on others. That inconsistency caused a 2026-04-24 audit to show 6 issues as "unmatched" when they were actually closed under composites. Normalize now.

1. **Audit which issue IDs appear in the queue but not individually in the deprecation log** (after accounting for both `/` and `,` splits):
   ```sql
   WITH closed AS (
     SELECT DISTINCT TRIM(t.tok) AS issue_id
     FROM manuscript_workspace.canonical_deprecation_log_v1 d,
          UNNEST(string_split(REPLACE(d.issue_id, '/', ','), ',')) AS t(tok)
     WHERE d.issue_id IS NOT NULL
   ),
   queue AS (
     SELECT issue_id, COUNT(*) AS n_rows
     FROM manuscript_workspace.qc_manual_review_queue_v1
     WHERE issue_id IS NOT NULL GROUP BY 1
   )
   SELECT q.issue_id, q.n_rows,
          (SELECT closing_prompt FROM manuscript_workspace.canonical_deprecation_log_v1 d
           WHERE d.issue_id LIKE '%'||q.issue_id||'%' LIMIT 1) AS covering_prompt
   FROM queue q LEFT JOIN closed c USING (issue_id)
   WHERE c.issue_id IS NULL
   ORDER BY n_rows DESC;
   ```
   Expected: 0 rows (after prompt_04 patch landed 2026-04-24). If any rows return, **stop and report** — those are genuine open issues needing a separate migration, not a naming fix.

---

## Phase 2 — Build `manuscript_workspace.ete_manuscript_analytic_v1`

This is the deliverable. A single view, per-event grain (one row per path malignant event), that JOINs all the clean overlays so the manuscript analyst can write a simple `SELECT ... FROM ete_manuscript_analytic_v1 WHERE cohort_ptc=TRUE AND analytic_eligible=TRUE` without fighting linkage bugs.

**File:** `qc_framework_v1/migrations/52_ete_manuscript_analytic_v1.sql`

**Required columns (group by semantic domain):**

### Identity / linkage (primary keys)
- `research_id` (BIGINT)
- `path_surgery_id` (native path event PK)
- `surgery_episode_id_global` (from `canonical_path_malignant_events_v1_global_epi`; **NOT** the stored local ordinal)
- `tumor_ordinal`
- `specimen_id`
- `synoptic_row_ix`

### Cohort flags
- `cohort_ptc` (BOOLEAN — from `qc_manuscript_cohort_v2_ptc`)
- `cohort_descriptive_full` (from `cohort_descriptive_full_cohort_v1` if the row is in scope)
- `analytic_eligible` (composite: non-NULL ETE assessment + non-orphan surgery link + size non-NULL + histology trusted)

### Exposure — ETE (core)
- `ete_raw` (original path column, preserved for audit)
- `ete_norm` (from `canonical_path_malignant_events_v1_ete_clean.ete_norm`) — the trusted value
- `gross_ete_effective` (from ete_clean view)
- `ete_cpm_self_contradiction_flag` (from `cpm_ete_self_contradiction_queue_v1` — TRUE if queued)

### Tumor characteristics
- `primary_histology_trusted` (from `canonical_path_malignant_events_v1_histology_clean`)
- `histology_variant_trusted` (from `_variant_clean`)
- `size_greatest_dimension_cm_trusted` (from `_size_flag` — or pull the chart-review-corrected value if applicable)
- `size_flag_queue_status` (unflagged / under_review / corrected)
- `laterality_trusted` (from `_laterality_clean`)
- `multifocal_flag` (from `path_episode_multifocality_v1` — trust the rebuilt value, not stored)
- `vascular_invasion_trusted`, `lymphatic_invasion_trusted`, `perineural_invasion_trusted` (from `_invasion_clean`)

### LN status
- `ln_examined_total`, `ln_positive_total` (from `manuscript_cohort_v1` LN reconciliation — NOT the raw path columns)
- `ln_denominator_reliable_flag` (from `_ln_denominator_flag`)

### Staging
- `reported_t_stage_ajcc8` (from `path_event_discordance_v1`)
- `derived_t_stage_ajcc8` (ditto — analyst-preferred)
- `t_stage_discordance_flag` (ditto)
- `ajcc_overall_stage_trusted` (from `canonical_path_malignant_events_v1_ajcc_flag` — or CPM AJCC dominant if more reliable; document the choice in a comment)

### Surgery
- `procedure_normalized_trusted` (from `canonical_operative_events_v1_rule_clean` — NULL for OP01/OP02 rule violators)
- `surgery_date_native` (from operative events, not path — they agree via the OP05 rebind)
- `surgery_laterality_trusted` (from operative rule-clean view)

### Pre-op context
- `max_preop_bethesda` (from `canonical_fna_patient_rollup_v1_clean.bethesda_final_recomputed`)
- `braf_variant_derived` (from `canonical_molecular_genetics_v2_braf_variant`)
- `ras_flag`, `tert_flag`, `ret_fusion_flag` (passed through from molecular v2 for descriptive tables)

### Outcome
- `recurrence_ever_trusted` (from `manuscript_cohort_v1_recurrence_clean` — structural only, NOT biochemical; the REC05 downgrade applies)
- `days_to_first_recurrence`
- `last_known_alive_date`
- `vital_status_trusted`

### Provenance / audit
- `ete_source_table` — which view each field came from (helps the analyst trace anything suspicious)
- `build_ts` (CAST(CURRENT_TIMESTAMP AS TIMESTAMP) — not TIMESTAMPTZ; see memory `reference_duckdb_timestamp_tz.md`)

### Acceptance criteria (assert inline in the migration file as `SELECT` probes after the CREATE VIEW):
1. `SELECT COUNT(*)` equals the row count of `main.canonical_path_malignant_events_v1` (no rows dropped).
2. `SELECT COUNT(*) WHERE cohort_ptc` matches `qc_manuscript_cohort_v2_ptc` row count (sanity).
3. `SELECT COUNT(*) WHERE analytic_eligible AND ete_norm IS NOT NULL` ≥ 4,000 (rough floor — check against the registry's current PTC count). If the number looks wrong, investigate before committing.
4. No more than 5% of `analytic_eligible` rows have `t_stage_discordance_flag=TRUE` (spot-check upper bound).
5. Zero rows where `surgery_episode_id_global IS NULL AND analytic_eligible` (OP05 must resolve before eligibility).

---

## Phase 3 — Deprecation log + registry update

Add ONE row to `manuscript_workspace.canonical_deprecation_log_v1`:

- `deprecated_object`: NULL (this is additive, not a deprecation — this is an analytic view built on top)
- `object_kind`: `analytic_view`
- `superseding_object`: `manuscript_workspace.ete_manuscript_analytic_v1`
- `issue_id`: `MANUSCRIPT_ETE`
- `closing_prompt`: `prompt_51`
- `deprecation_kind`: `pointer_only`
- `notes`: Enumerate the 16+ building blocks the view depends on. Flag the known caveats (REC05 biochem-only downgrade, CPM AJCC choice, ETE self-contradiction queue).

Then append to `qc_framework_v1/ISSUE_REGISTRY.md` under Run log:

```
- **2026-04-24 PM** — Prompt 51 / migration 52: `manuscript_workspace.ete_manuscript_analytic_v1` published. Per-event ETE-focused analytic view fusing 16 clean overlays. Acceptance criteria: full row parity with `main.canonical_path_malignant_events_v1`; PTC cohort N matches `qc_manuscript_cohort_v2_ptc`; zero eligible rows with missing `surgery_episode_id_global`. PATH20/PATH21 deprecation-log row patched (was missing from prompt_04 close-out).
```

---

## Rules

- **View-layer only.** Do not `ALTER`, `UPDATE`, or `INSERT` into any `main.*` table. The pattern for this project is overlays under `manuscript_workspace.*` with the source columns preserved on `main` for audit.
- **Never cross-DB source canonicals.** `main.*` only. Do not `FROM archive_pub_v1_0.*` or any other DB. Per memory `feedback_no_cross_db_canonical_sourcing.md`.
- **TIMESTAMP casts.** `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` — never bare `CURRENT_TIMESTAMP` (returns TIMESTAMPTZ, triggers pytz dependency). Per memory `reference_duckdb_timestamp_tz.md`.
- **NULL-safe joins.** If joining on a column that may contain NULLs (e.g. `molecular_episode_id` for records without a known episode), use `a.col IS NOT DISTINCT FROM b.col` instead of `a.col = b.col`. Per prompt_50 lessons.
- **Probe partition keys before ROW_NUMBER.** Run `COUNT(*) vs COUNT(DISTINCT key)` first. Per memory `feedback_mention_grain_partition_probe.md`.
- **Surgical git add.** Stage only these files explicitly — do NOT `git add -A` or `git add scripts/output/`:
  - `qc_framework_v1/migrations/52_ete_manuscript_analytic_v1.sql`
  - `qc_framework_v1/ISSUE_REGISTRY.md`
  - `cursor_prompts/CURSOR_PROMPT_ETE_MANUSCRIPT_PREP_20260424.md` (this file)
- **Commit message prefix:** `qc_framework_v1: prompt 51 — ete_manuscript_analytic_v1`. Include a short summary of the view shape and the acceptance-criteria results (numeric, not descriptive).
- **Push to origin/main** after the commit lands clean.

---

## Anti-goals (do NOT do these)

- Do not close chart-review queue rows. Those belong to clinical analysts, not this migration.
- Do not mutate `main.canonical_path_malignant_events_v1` columns even if a column looks "wrong" — leave it; the overlay view is the fix path.
- Do not rebuild PATH20/PATH21 discordance logic. The view already exists; only the log row needed patching, and that's already landed.
- Do not attempt IFNA01–06 rebuild here. That's a separate prompt and a larger-scope design change.
- Do not modify `TASKS.md` or memory files. Those are managed outside this migration.
- Do not bump `detail_table_registry_v1`. The analytic view is not a canonical scaffold table; it's a manuscript-scoped overlay.

---

## Deliverables

1. `qc_framework_v1/migrations/52_ete_manuscript_analytic_v1.sql` — applied to MotherDuck, committed, pushed.
2. One new row in `canonical_deprecation_log_v1` with `closing_prompt='prompt_51'`, `issue_id='MANUSCRIPT_ETE'`.
3. One appended line in `ISSUE_REGISTRY.md` run-log.
4. Acceptance-criteria probe output pasted into the commit message body (5 numbers: total rows, PTC rows, analytic-eligible + ETE non-null rows, t_stage discordance %, orphan-op-link count).

If any acceptance criterion fails, stop and investigate — do not relax the criterion to make the view land.
