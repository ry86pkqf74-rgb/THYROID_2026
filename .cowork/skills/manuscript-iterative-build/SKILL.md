---
name: manuscript-iterative-build
metadata:
  version: 1.0.0
description: |
  MANDATORY iterative-build protocol for every manuscript built from the THYROID_2026 BigQuery
  database (`thyroid-canonical-pub-2026`). Load and run this WHENEVER a manuscript cohort or its
  numbers are (re)built, refreshed, iterated, locked, frozen, or exported — i.e. any v2/v3/vN
  rebuild of an M-code cohort, any "regenerate Table N", "refresh the cohort", "rebuild
  manuscript_cohort", "lock M0xx", "freeze the numbers", or any change to a manuscript's
  underlying BQ tables/views. Triggers: manuscript build, cohort rebuild, iteration, v2, v3,
  refresh cohort, regenerate table, regenerate figure, lock manuscript, freeze, manuscript
  snapshot, manuscript_cohort_v1, cohort N, locked numbers, M025/M032/M036/M037/M038/M044/M048/
  M083/M085/M098/Mo36/H1/H2 rebuild, pub_canonical cohort, BQ cohort. This is the per-manuscript
  counterpart to the project-wide `cowork_qc_nonblocking_pipeline_v1`. It runs ALONGSIDE the
  `thyroid-integration` skill's Session Opening Protocol, not instead of it.
---

# Manuscript Iterative-Build Protocol (BigQuery)

Every manuscript built from `thyroid-canonical-pub-2026` is iterated — v1 → v2 → vN — as
cohorts get re-scoped, feeders rebuild, and QC fixes land. Iterative builds fail not because a
number is wrong once, but because a number **drifts silently between iterations** and nobody
notices until a co-author or reviewer does. This skill is the fixed protocol that runs on
**every iteration of every BQ-sourced manuscript** so drift is caught at build time.

Memory: `memory/feedback_manuscript_iterative_build_process.md`.

## When this runs

Run the full protocol whenever you (re)build, refresh, iterate, lock, or export a
manuscript's cohort or its numbers from BigQuery. A pure read ("what's the N for M044?")
does not need the full protocol, but still cite the provenance (step 4).

## The protocol — run in order, every iteration

### 1. Snapshot before overwrite
Before any `CREATE OR REPLACE` of a manuscript cohort table, snapshot the current version to
`pub_archive` (project convention — nothing is ever destroyed):
```sql
CREATE TABLE `thyroid-canonical-pub-2026.pub_archive.<cohort>_pre_<iteration>_<YYYYMMDD>` AS
SELECT * FROM `thyroid-canonical-pub-2026.pub_canonical.<cohort>`;
```

### 2. Iteration diff — the highest-value check
Diff the new cohort against the previous iteration and report:
- patients **added** and **dropped** (by `research_id`),
- which **locked/headline numbers moved**, and the **reason** (feeder rebuilt? filter
  changed? QC fix landed? builder didn't retrigger?).

Use `notebooks/manuscript_iteration_diff_qc.ipynb` — set `COHORT_TABLE` and
`PRIOR_COHORT_TABLE` and run. A numbers-diff that no one can explain is a stop-the-line event:
do not lock the iteration until every moved number has a reason.

### 3. Cohort-scoped QC — before numbers are locked
The project-wide `cowork_qc_nonblocking_pipeline_v1` runs daily; this is its per-manuscript
counterpart. Run the QC assertions (catalog: `pub_workspace.qc_rules_v1`) **filtered to this
manuscript's cohort**. The notebook does this; the concrete critical checks are SURG01,
LN01/LN02/LN03, REC02/REC03, HIST01, FNA01. Every iteration must start from a known-clean
slice — file/update a Linear issue for any violation (step 5); never lock over a known
critical violation.

### 4. Column source-of-truth assessment (AI/Gemini step — automatic)
Run `.cowork/skills/manuscript-iterative-build/sql/manuscript_column_source_assessment.sql`
against the manuscript's column list. It checks every column the manuscript uses against the
**competing-source register** and, where a Vertex AI connection is configured, adds a
Gemini-written plain-language assessment via `AI.GENERATE`. It flags any column that is
contested or pending a canonical decision. Known contested columns as of 2026-05-14:

| Concept | Columns in play | Status |
|---|---|---|
| Surgery date | `first_surgery_date` / `surg_first_date` / `surgery_date` | **THY-87 open** — `surg_first_date` and `surgery_date` are identical duplicates; `first_surgery_date` diverges in 171 patients. No canonical pick yet. |
| LN-positive count | `path_ln_positive_raw` / `ln_positive_final` | **THY-89 open** — 51 disagreements + 38 impossible rows. No canonical pick yet. |
| Histology | `histology_final` | HIST01–03 — whitespace / unnormalized variants / metastatic-prefix. |
| Recurrence | `any_recurrence_flag` / `recurrence_date` | REC01–03 — flag/date mismatches. |

If the manuscript leans on a contested column, **say so in the build output and the
manuscript's methods notes** — do not silently bake in a column that is about to be
deprecated.

### 5. Provenance manifest + route findings to Linear
- **Provenance:** record, per iteration, the exact canonical tables/views and their
  `build_ts` (or table `last_modified_time`) that fed the numbers. Save it next to the
  cohort (`<cohort>_provenance_<iteration>.md` or a row in the manuscript's evidence pack).
  Three months later, "where did this N come from" must be answerable.
- **Linear:** any QC violation, numbers-diff anomaly, or contested-column flag found during
  the build is filed/updated as a Linear issue under `Database Reconciliation & QA` — never
  left in a comment. Follow the `thyroid-integration` skill's logging rules.

## Division of labor

The agent does **detection and diffing every iteration**. The human makes the **few real
decisions once**: an agent can flag that a manuscript uses a contested column, but it cannot
pick the authoritative source — that is Logan's call (THY-87, THY-89). Do not "resolve" a
source-of-truth conflict autonomously inside a manuscript build.

## Reusable artifacts in this skill

- `sql/manuscript_column_source_assessment.sql` — the step-4 AI/Gemini column assessment.
- `notebooks/manuscript_iteration_diff_qc.ipynb` (repo `notebooks/`) — steps 2 + 3 in one
  parameterized run.

## Versioning

v1.0.0 — initial protocol (2026-05-14). Bump patch for clarifications, minor for new checks
or artifacts, major for changes to the ordered protocol. When the competing-source register
changes (a THY-xx decision lands), update the step-4 table and bump.
