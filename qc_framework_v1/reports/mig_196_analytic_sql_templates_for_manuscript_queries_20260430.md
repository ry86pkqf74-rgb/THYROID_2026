# mig_196 — analytic SQL templates for manuscript queries

**Date:** 2026-04-30  
**Lane:** `mig_196` / `analytic_sql_templates_for_manuscript_queries`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** Repo-side authoring only (no MotherDuck execute in this session).  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig196_analytic_sql_templates_for_manuscript_queries_20260430.md`

`git fetch origin` / `git pull origin main`: workspace already **up to date** with `origin/main`.

---

## §1 Template inventory

| # | Path | Summary |
|---|------|-----------|
| 1 | `qc_framework_v1/manuscript/analytic_templates/01_overall_survival.sql` | OS person-rows; death + vital string guard; strata × stage / histology / age band |
| 2 | `qc_framework_v1/manuscript/analytic_templates/02_recurrence_free_survival.sql` | RFS; `canonical_recurrence_v1` + optional `any_recurrence_flag`; strata include T/N/R-class |
| 3 | `qc_framework_v1/manuscript/analytic_templates/03_stage_group_by_histology.sql` | Histology bucket × `ajcc8_stage_group_resolved` with `%` within histology |
| 4 | `qc_framework_v1/manuscript/analytic_templates/04_complication_rate_by_surgery_type.sql` | Surgery × complication grid; acute 30 d vs any-time rules; Wald CI |
| 5 | `qc_framework_v1/manuscript/analytic_templates/05_cohort_flow_and_exclusions.sql` | QUERY A = mig_195 counts; QUERY B = long `research_id` pools |

Companion: `qc_framework_v1/manuscript/analytic_templates/README.md`.

Preview placeholders: `qc_framework_v1/manuscript/analytic_templates/previews/*.csv` (5 files).

---

## §2 Cohort consistency check

Templates **01–04** share one embedded definition:

`step4_pool` (last_contact present) ∩ `canonical_path_malignant_events_v1` ∩ malignant CPM spine, with mig_186b indeterminate-only exclusion and histology/T-stage NULL exclusion—aligned with **cohort_flow_diagram.sql** steps **3–5** plus malignant-event intersection.

**Deviation vs Table 1:** `table_1_cohort_characteristics.sql` does **not** require `last_contact_date`. Expect denominator mismatch unless Logan harmonizes.

Template **05 QUERY A** matches `cohort_flow_diagram.sql` **verbatim**.

---

## §3 Limitations per template

| Template | Limitations |
|----------|-------------|
| 01 OS | Competing risk from recurrence ignored; vital string heuristic needs vocabulary verification; triple-row strata export |
| 02 RFS | Many confirmed recurrences lack calendar `recurrence_date` (event_indicator 0 with fallback flag); death as competing risk not modeled |
| 03 Stage × histology | Histology buckets are pragmatic keyword buckets; `T0_or_unstaged` mixes true T0 vs missing data |
| 04 Complications | Acute numerators exclude NULL `timing_days`; Wald CI inaccurate at extreme sparse counts; RLNP merges `rln_injury` + `vocal_cord_paralysis` |
| 05 Flow | QUERY B ID lists can be large—export to Parquet/CSV; clients may need to run QUERY A and QUERY B as separate batches |

---

## §4 Suggested next steps

- Multivariable Cox with shared frailty or cluster by surgeon/hospital if identifiers ever approved.  
- Propensity scores for non-randomized arms (e.g., central neck dissection).  
- Fine–Gray subdistribution hazards if recurrence and death are jointly modeled.  
- Replace Wald intervals with exact (Clopper–Pearson) for sparse complication cells.  
- After MotherDuck runs: refresh `previews/*.csv` via `COPY` and attach row counts to this report.

---

_End mig_196 report._
