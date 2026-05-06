# Cursor Follow-Up Prompts (post Lane H, 2026-05-06)

After Cursor finishes the in-flight Lane H (Cortex Analyst YAML), pick prompts from this file based on what's most needed next. Each is self-contained — paste in a fresh Cursor session. **Always paste SECTION 0 from `CURSOR_HANDOFF_BQ_MIGRATION_20260506.md` first** to bootstrap BQ access + governance.

---

## Prompt 1 — Resolve THY-13 / THY-14 with Snowflake Cortex (high priority — was blocked, now unblocked)

```
TASK: Add comp_rln_injury_preop and comp_vc_paralysis_preop BOOLEAN columns to
pub_canonical.canonical_patient_master via Snowflake Cortex AI_CLASSIFY.

CONTEXT:
  Linear: https://linear.app/rostemp/issue/THY-13/ and THY-14 (both In Progress).
  Earlier attempt: bq_migrations/mig_079_mig001_002_preop_rln_vc_blocked
    — blocked because Cortex/AI_CLASSIFY auth wasn't available.
  Now unblocked: Cursor has Snowflake access.
  H2 v1 manuscript gap analysis surfaced these as blocking the standing
  complication-temporality rule (memory/feedback_complications_transient_vs_permanent.md).
  Source notes for extraction: pre-operative laryngoscopy notes + outpatient
  ENT visit notes for any thyroid-surgery patient.

GOAL:
  For each research_id in canonical_patient_master, derive:
    comp_rln_injury_preop BOOLEAN  — was RLN injury documented BEFORE the
                                     index thyroid surgery?
    comp_vc_paralysis_preop BOOLEAN — was vocal-cord paralysis documented
                                     BEFORE the index thyroid surgery?
  NULL when no preop note exists; FALSE when notes exist but no finding;
  TRUE when finding is present.

WORKFLOW:
  1. Identify the candidate note set in Snowflake. Probably ops_preop_laryngoscopy
     and any free-text preop ENT/laryngology notes keyed by research_id +
     note_date < first_surgery_date.

  2. Build the Cortex AI_CLASSIFY query. Two-class for each column:
       SELECT research_id,
              SNOWFLAKE.CORTEX.AI_CLASSIFY(
                note_text,
                ['has RLN injury documented preop',
                 'no RLN injury preop',
                 'no relevant content']
              ) AS rln_class,
              ...
       FROM <preop notes>
       WHERE note_date < (SELECT first_surgery_date FROM ... WHERE research_id = ...)
     Aggregate per research_id: TRUE if any note classifies as 'has...';
     FALSE if any note classifies as 'no...' and none as 'has...';
     NULL if no notes exist.

  3. Land the result as a Snowflake table thyroid_research.preop_rln_vc_v1
     keyed on research_id. Validate: check distribution of TRUE/FALSE/NULL
     for sanity (most patients will be NULL since most don't have preop ENT
     workup).

  4. Export to GCS (parquet), then load into BigQuery as
     pub_workspace.canonical_patient_master_preop_rln_vc_v1 (a side table,
     NOT modifying canonical_patient_master directly).

  5. Add a JOINing view or column-update migration that incorporates the
     two new BOOLEANs into canonical_patient_master:
       mig_080_canonical_patient_master_preop_rln_vc.sql
     Use ALTER TABLE ADD COLUMN, then UPDATE from the side table.

  6. Re-run the relevant QC assertions; add new ones if needed.

GOVERNANCE — READ CAREFULLY:
  - PHI: this task touches raw note text. NEVER paste note content into
    Cursor chat, commit messages, or markdown. Counts/distributions only.
  - DFL row in Airtable BEFORE the canonical edit (target_type='BQ
    infrastructure', change_type='migration').
  - bq_migration_log_v1 row on success, with notes referencing the DFL id.
  - The Snowflake side: log to whatever the equivalent audit table is
    (probably manuscript_workspace.cortex_extraction_log or similar — find
    via INFORMATION_SCHEMA).
  - DO NOT modify pub_canonical.canonical_patient_master without (a) a
    pre-snapshot table (rows_before captured), (b) a tested migration that
    only updates the two new columns, leaving everything else.

DELIVERABLES:
  - mig_080_canonical_patient_master_preop_rln_vc.sql (idempotent)
  - DFL row + bq_migration_log_v1 row
  - Brief markdown at _scripts/thy13_thy14_resolution_summary.md (no PHI):
    distribution of TRUE/FALSE/NULL for each column, AI_CLASSIFY threshold
    chosen, any borderline cases that needed manual review
  - Linear comment on THY-13 and THY-14 with the resolution summary
  - If everything looks clean, transition both issues to "In Review" with
    label "auto-close:pending"
```

---

## Prompt 2 — Deploy the Cortex Analyst YAML (after Lane H finishes)

```
TASK: Deploy the Cortex Analyst semantic model YAML produced by Lane H.

INPUTS:
  /Users/loganglosser/THYROID_2026/_semantic_models/m025_m044_v1.yaml
  (or whatever Lane H's final output filename is)

GOAL:
  Push the YAML to Cortex Analyst so natural-language questions about M025
  (TIRADS) and M044 (ETE) can be answered via NL→SQL against pub_views_readable.

PREREQUISITES — verify first:
  1. Cortex Analyst is provisioned for thyroid-canonical-pub-2026 (it's a
     Snowflake feature; confirm the project has the required role grant).
  2. The cohort views referenced in the YAML actually exist in BQ:
       cohort_m025_tirads_performance_v1
       cohort_m044_ajcc_ete_v1
     Verify with `bq show --schema ...`.
  3. The YAML's column-level descriptions don't accidentally surface PHI
     (review for any column that might contain free-text patient data).

DEPLOY (one-time human action via Snowflake/Cortex UI, but Cursor can
automate the YAML upload):
  - Cortex Analyst expects the YAML in a specific stage. Find the documented
    path (likely from CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md).
  - Upload via Snowflake CLI or web UI.
  - Test with 3 natural-language queries:
       "How many M025 patients had max TIRADS category 5?"
       "What is the malignancy rate by TIRADS category in M025?"
       "Show me M044 patients with gross ETE."
  - Each should produce a working SQL query that returns rows.

GOVERNANCE:
  - DFL row (change_type='other', target_type='BQ infrastructure',
    target_record='Cortex Analyst semantic model m025_m044_v1').
  - Don't deploy without verification step 3 — PHI leak via natural-language
    answers is a real risk if column descriptions contain identifiers.

DELIVERABLE:
  - YAML deployed and verified
  - _scripts/cortex_analyst_deployment_summary.md with the 3 test queries
    and their actual SQL output (no PHI in the output — just confirm shape)
  - DFL row
```

---

## Prompt 3 — Tier 2.D: Auto-sync cohort sizes from BQ → Airtable Manuscripts

```
TASK: Add a daily sync that updates Manuscripts.bq_cohort_n from the cohort
view row counts.

CONTEXT:
  - Manuscripts table now has bq_manuscript_id (set 2026-05-06 by Cowork).
  - pub_signoff.manuscript_cohort_health_v1 (Lane A's output) returns
    cohort_view_name + cohort_view_row_count for each manuscript.
  - We want a daily push: for each manuscript with bq_manuscript_id IS NOT
    NULL, write the latest row_count to a new Manuscripts field.

STEP 1 — add the field:
  Use Airtable MCP create_field on Manuscripts (tblLsp8ls3rU1eEc9) in base
  appJYOnUb7KrHKwpV:
    name: bq_cohort_n
    type: number, precision=0
    description: "Daily-synced row count from manuscript_cohort_health_v1.
      Compare to candidate_cohort_n (human-authored) to detect drift."

STEP 2 — add the sync logic:
  Create a Cowork scheduled task `manuscript-cohort-sync-daily`:
    cron: 30 7 * * * (07:30 local; before the QC pipeline at 08:30)
    Each run:
      1. Pull manuscript_cohort_health_v1 from BQ via Desktop Commander bq query
      2. For each row: find Airtable record by bq_manuscript_id; update bq_cohort_n
      3. If bq_cohort_n != candidate_cohort_n by > 10%, post a comment in the
         linked Linear project (Manuscripts.linked_linear_project field)
         saying "Cohort drift detected — bq_cohort_n=X, candidate_cohort_n=Y."

STEP 3 — governance:
  DFL row + bq_migration_log_v1 row. PHI: zero (counts only).

DELIVERABLE:
  - bq_cohort_n field added
  - Cowork scheduled task created (use mcp__scheduled-tasks__create_scheduled_task)
  - First-run output documented in _scripts/cohort_sync_first_run_summary.md
```

---

## Prompt 4 — Tier 3.A: Real survival modeling in Python

> **STATUS (2026-05-06 post-mig_086):** v1 model (`mig_086_cox_recurrence_v1`)
> was trained at 12:31 UTC, before mig_086 (14:42 UTC). Post-mig_086
> verification confirmed cohort is **IDENTICAL** (2,580 pts / 428 events) because
> source `pub_workspace.cohort_m044_ajcc_ete_v1` is a materialized BASE TABLE
> created at 04:19 UTC that is unaffected by mig_086's VIEW facades.
> **v1 is NOT deprecated — it remains canonical.** Verification entry logged as
> `cox_recurrence_v2_post_mig_086` in `bqml_eval_log_v1`; migration log entries
> `mig_087_cox_recurrence_v2`; DFL `DFL-20260506-088`.
> See `_scripts/post_mig_086_model_refit_summary.md`.

```
TASK: Run a Cox proportional hazards model on the recurrence cohort, using
Python `lifelines`. This is the "real survival" follow-up to the BQML
binary baseline (recurrence_5y_baseline_v1, AUC 0.738).

CONTEXT:
  BQML can't do Cox PH or competing risks. The baseline classifier in
  bqml_eval_log_v1 is "alive at 5y vs not", which loses censoring information.
  For the recurrence-prediction manuscript (probably M030 or MULTIMODAL),
  we need a proper survival model.

WORKFLOW:
  1. Pull the cohort + features as a DataFrame from BQ:
     - Cohort: pub_legacy_source_20260416.cohort_m044_ajcc_ete_v1 (or whichever
       is most appropriate — verify with Logan's intent)
     - Features: same as recurrence_5y_baseline_v1 plus competing-risk timeline
     - Outcome: time_to_recurrence (DAYS), recurrence_event (BOOL)
       Censoring: time_to_last_followup if no recurrence

  2. Fit Cox PH with lifelines.CoxPHFitter:
       from lifelines import CoxPHFitter
       cph = CoxPHFitter(penalizer=0.1)
       cph.fit(df, duration_col='time_to_event', event_col='recurrence',
               show_progress=True)
       cph.print_summary()

  3. Compute C-index, Brier score at 1y/3y/5y, time-dependent AUC.

  4. Optionally fit a competing-risks model (lifelines.AalenJohansenFitter
     or pycox) to handle death-without-recurrence properly.

  5. Compare Cox PH performance to the BQML 5y classifier.

DELIVERABLES:
  - A Jupyter notebook at studies/<manuscript_code>/cox_recurrence_v1.ipynb
    (use templates/manuscript_notebook_v1.ipynb from Lane D as a starting point)
  - _scripts/cox_recurrence_v1.md report: C-index, Brier, top covariates by
    HR with 95% CI, comparison vs BQML baseline
  - Insert into pub_workspace.bqml_eval_log_v1 with model_id='cox_recurrence_v1',
    auc=NULL, c_index=<value>, notes='lifelines CoxPHFitter; not a BQML model'
    (or extend the schema if needed)
  - PHI: aggregate stats only; no per-patient predictions in the markdown

GOVERNANCE:
  - DFL row + entry in bqml_eval_log_v1. The notebook itself can live in
    studies/<manuscript_code>/ which is appropriate for research artifacts.
```

---

## Prompt 5 — Tier 3.B: Boosted-tree alternative to the logistic baseline

> **STATUS (2026-05-06 post-mig_086):** v1 model (`mig_087_bqml_boosted_models`)
> was trained at 12:58 UTC, before mig_086 (14:42 UTC). Post-mig_086
> verification confirmed cohort is **IDENTICAL** (1,285 rows / 502 events) because
> source `pub_canonical.canonical_patient_master` is a BASE TABLE unaffected by
> mig_086's VIEW facades. AUC=0.712 unchanged.
> **v1 is NOT deprecated — it remains canonical.** Verification entry logged as
> `recurrence_5y_boosted_v2_post_mig_086` in `bqml_eval_log_v1`; migration log
> `mig_088_bqml_boosted_v2`; DFL `DFL-20260506-089`.
> See `_scripts/post_mig_086_model_refit_summary.md`.

```
TASK: Train a BOOSTED_TREE_CLASSIFIER alongside recurrence_5y_baseline_v1
for direct AUC comparison.

WHY:
  Logistic regression captures linear patterns. If boosted trees significantly
  outperform, that signals non-linear interactions in the data that the
  manuscript should discuss. If they don't, that's also worth knowing
  (suggests the cohort is small enough that simpler is fine).

WORKFLOW:
  1. CREATE OR REPLACE MODEL `pub_workspace.recurrence_5y_boosted_v1`
     OPTIONS(
       model_type='BOOSTED_TREE_CLASSIFIER',
       input_label_cols=['recurrence_5y'],
       num_parallel_tree=8,
       max_iterations=50,
       early_stop=TRUE,
       enable_global_explain=TRUE,
       data_split_method='RANDOM',
       data_split_eval_fraction=0.2
     ) AS
     SELECT <same features as recurrence_5y_baseline_v1>, recurrence_5y FROM cohort;

  2. ML.EVALUATE, ML.ROC_CURVE, ML.GLOBAL_EXPLAIN

  3. Insert into bqml_eval_log_v1 with model_id='recurrence_5y_boosted_v1'

  4. Add a QC assertion: boosted-tree AUC should be ≥ logistic baseline AUC.
     If it's significantly worse, we have data drift or feature leakage to
     investigate.

  5. Same exercise for survival_5y_boosted_v1 if useful.

DELIVERABLE:
  - mig_081_bqml_boosted_models.sql
  - _scripts/bqml_recurrence_v2_boosted.md comparing AUCs + top features
    from ML.GLOBAL_EXPLAIN
  - DFL row, bq_migration_log_v1 row
```

---

## Prompt 6 — Merge duplicate Linear project for Mo36/M036

```
TASK: After 2026-05-06 supersession of M036 → Mo36, the Linear team THY has
two projects pointing at the same manuscript. Merge them.

PROJECTS:
  Keep:   https://linear.app/rostemp/project/mo36-ddad9b8fe80d  (Mo36)
  Merge:  https://linear.app/rostemp/project/m036-ata-rss-comparison-v3-e35d5cfdf636  (M036, now superseded)

WORKFLOW:
  1. List all issues in the M036 project. For each: change project to Mo36
     project (use save_issue with project=<Mo36 project id>).
  2. After all issues moved, archive the M036 project (Linear save_project
     with state=archived if API allows, else mark in description that it's
     merged into Mo36 and set its name to '[MERGED→Mo36] M036…').
  3. Update Airtable Manuscripts.linked_linear_project on the Mo36 record
     (recp7f3k3sQmy0H39) to confirm it points at the Mo36 project URL (it
     should already; verify).

GOVERNANCE:
  DFL row (target_record='Linear: M036 project merge'). PHI: zero.

DELIVERABLE: 1-line summary with count of issues moved, before/after project IDs.
```

---

## Order to run

1. **Prompt 1 (THY-13/14)** is the highest-leverage now-unblocked task. Run first.
2. **Prompt 2 (Cortex Analyst deploy)** depends on Lane H finishing first; pick up immediately after.
3. **Prompt 3 (cohort sync)** is small and pays off quickly; can interleave with 1 or 2.
4. **Prompt 4 / 5 (Tier 3 modeling)** are exploratory; do when you want richer ML.
5. **Prompt 6 (Linear project merge)** is small cleanup; run anytime.

If running in parallel: 1 + 3 + 6 are independent and can run simultaneously. 2 runs solo (Cortex deploy is delicate). 4 and 5 share the modeling cohort and should run sequentially in one session.
