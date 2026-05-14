# Feedback — manuscript iterative-build process (BQ database)

**Established:** 2026-05-14 (Cowork session, BigQuery Studio Integration Plan)
**Scope:** every manuscript built from the BigQuery database (`thyroid-canonical-pub-2026`).
**Codified as skill:** `.cowork/skills/manuscript-iterative-build/SKILL.md` — load it for any manuscript build/iteration.

## The pattern

Iterative manuscript builds fail not because a number is wrong once, but because a number
**drifts silently between iterations** (a feeder rebuilt, a QC fix landed, a cohort filter
changed, a builder didn't retrigger). The fix is a fixed protocol run on **every** iteration
of every manuscript cohort:

1. **Iteration diff** — diff the cohort and the locked numbers against the previous iteration.
   Report patients added/dropped and which key numbers moved, with the reason. This is the
   single highest-value check for iterative builds.
2. **Cohort-scoped QC** — run the QC assertions (`pub_workspace.qc_rules_v1`) filtered to the
   manuscript's own cohort, before any numbers are locked. Every iteration starts from a
   known-clean slice.
3. **Column source-of-truth assessment** — an AI/Gemini step assesses every column the
   manuscript uses against the competing-source register (surgery date, LN-positive,
   histology, recurrence). Flags any column that is contested or pending a canonical decision
   (THY-87 surgery date, THY-89 LN-positive).
4. **Provenance manifest** — record the exact canonical tables/views and `build_ts` values
   that fed the numbers, so a reviewer's "where did this N come from" is answerable later.
5. **Snapshot before overwrite + route findings to Linear** — snapshot the cohort table to
   `pub_archive` before each rebuild; file/update a Linear issue for any violation found,
   never leave it in a comment.

## Division of labor

The agent does **detection and diffing every iteration**. The human makes the **few real
decisions once** — an agent can flag that a manuscript uses a contested column, but it
cannot pick the authoritative source. That stays with Logan (THY-87, THY-89).

## Reusable artifacts

- `notebooks/manuscript_iteration_diff_qc.ipynb` — parameterized notebook: point it at a
  cohort table + its prior iteration, get the diff + cohort-scoped QC in one run.
- `.cowork/skills/manuscript-iterative-build/sql/manuscript_column_source_assessment.sql` —
  the AI/Gemini column source-of-truth assessment step.

## Related

- THY-87 (canonical surgery-date decision), THY-89 (canonical LN-positive decision) — the
  open source-of-truth decisions the column assessment surfaces.
- `docs/bigquery_studio_integration/Source_of_Truth_Decisions.md` — quantified write-up.
- `cowork_qc_nonblocking_pipeline_v1` (BigQuery pipeline) — the project-wide daily QC run;
  the cohort-scoped check is the per-manuscript counterpart.
