# BigQuery Studio Integration — Capabilities Built

**Project:** `thyroid-canonical-pub-2026`
**Date:** 2026-05-14
**Built by:** Cowork (BigQuery Studio Feature Integration Plan)
**Migration ID:** `mig_cw_workup_census_canonical_20260514`

This folder documents the capabilities stood up to attack the recurring data-quality, lineage, and legacy-migration pain points in the publication database. Every object below is **live in the project** unless noted. The `sql/` subfolder holds reproducible DDL.

---

## 1. The global evaluation layer (`pub_eval`)

A new dataset, **views only**, isolated from production datasets. Answers "what work-up actually happened, and where is the record thin" in one query.

| Object | Type | Rows | Purpose |
|---|---|---|---|
| `pub_eval.vw_nuclear_med_dated_v1` | view | 2,220 | Nuclear-medicine scans with scan dates parsed from the raw `scandate` string (the upstream `scandate_parsed` was unpopulated — see Linear THY-86). `scandate_quality` flags blank / unparseable / implausible-year rows. |
| `pub_eval.vw_patient_workup_census_v1` | view | 10,871 | One row per patient. Preop/postop performed flags + day intervals for ultrasound, CT, MRI, FNA, nuclear medicine; reoperation signals; prior-thyroid-procedure pathology-gap review flags; a preop-workup completeness tier. |
| `pub_eval.vw_workup_census_summary_v1` | view | 15 | Long-format aggregate roll-up (one row per metric) for dashboards. |

DDL: `sql/pub_eval_vw_nuclear_med_dated_v1.sql`, `sql/pub_eval_vw_patient_workup_census_v1.sql`, `sql/pub_eval_vw_workup_census_summary_v1.sql`

**Caveats (carried in column/table descriptions):** US/CT/MRI pre/post are derived from the canonical `*_first_date` / `*_last_date` columns (patient-level — postop reference is the last exam, `n_exams` is a patient total). FNA and nuclear medicine are event-level and exact. Surgery anchor is `first_surgery_date` (see THY-87). `prior_procedure_path_gap_flag` is a chart-review trigger, not a determination.

---

## 2. Promoted into the canonical fabric (`pub_canonical`)

The verified census was promoted into `pub_canonical` so it lives alongside the other `canonical_*_patient_rollup` tables, **without altering `canonical_patient_master`**.

| Object | Type | Rows | Cols | Notes |
|---|---|---|---|---|
| `pub_archive.canonical_patient_master_pre_workup_census_merge_20260514` | table | 10,871 | 2,314 | Point-in-time snapshot taken before the integration (safety net; the base table was not mutated). |
| `pub_canonical.canonical_patient_workup_census_v1` | table | 10,871 | 65 | Materialized census, clustered on `research_id`, with `build_ts` / `build_script`. |
| `pub_canonical.canonical_patient_master_v1_9` | view | 10,871 | 2,375 | `canonical_patient_master` left-joined with the census on `research_id`. Use this view when the workup-census columns are wanted alongside the master. Four columns already on the master (`ct_n_exams`, `mri_n_exams`, `n_surgeries`, `research_id`) are kept from the master; the census duplicates are excluded. |

DDL: `sql/pub_canonical_canonical_patient_workup_census_v1.sql`, `sql/pub_canonical_canonical_patient_master_v1_9.sql`

**Governance:** logged to `pub_signoff.bq_migration_log_v1` as `mig_cw_workup_census_canonical_20260514` (with rollback SQL), and both objects registered in `pub_signoff.canonical_table_signoff_registry_v1`. The 65 census columns are integrity-validated (no null IDs, no null anchors, no negative intervals) but not yet individually verified in the column verification registry (`n_not_started = 65`).

**Rollback:** `DROP VIEW pub_canonical.canonical_patient_master_v1_9; DROP TABLE pub_canonical.canonical_patient_workup_census_v1;` (the snapshot and base table are untouched).

---

## 3. BigQuery Studio console artifacts

Built by driving the console; not reproducible from SQL alone.

| Artifact | Name | What it does |
|---|---|---|
| **Data Canvas** | `pub_eval — Global Evaluation Layer (workup census)` | Two live SQL nodes against the census and summary views — visual prototyping surface. |
| **Looker Studio dashboard** | `pub_eval — Workup Census Dashboard` | Live BigQuery connection to `vw_workup_census_summary_v1`; 16-metric table (modality coverage + cohort flags). |
| **Pipeline** | `cowork_qc_nonblocking_pipeline_v1` | Non-blocking QC pipeline — runs the assertion below, does **not** gate publishing. Scheduled (`cowork_qc_daily_check`) **daily at 06:00 CDT**. |
| **Custom Agent** | `Workup Census Agent` (`agent_a81ec730-641c-4e32-9d73-3a76d2b265dc`) | Project-aware agent seeded with conventions; knowledge sources = the three `pub_eval` views. Answers work-up questions in plain language. Saved as Draft (not published). |
| **Dataplex glossary** | `Thyroid Pub DB Glossary` | Business glossary documenting the 9-dataset taxonomy, plus terms `research_id (join key)` and `surgery anchor date`. |
| **Notebook** | `Untitled notebook 2026-05-14` | Documented intro cell; code cells to be pasted manually (Colab editor is a cross-origin iframe). Queries in `sql/notebook_queries.sql`. |

### QC pipeline assertion

The pipeline's assertion (`sql/cowork_pipeline_qc_assertions_v1.sqlx`) returns one row per integrity violation; 0 rows = pass. It covers:

- `pub_eval` integrity (negative preop intervals, null surgery anchor)
- **SURG01** — the three surgery-date columns disagree
- **LN01** — `ln_positive_final` > `path_ln_examined_raw` (impossible)
- **LN02** — positive count with no examined count
- **LN03** — `path_ln_positive_raw` ≠ `ln_positive_final`

Because it is scheduled, these issues now surface every morning instead of being discovered inside a manuscript.

---

## 4. Data issues found, fixed, and tracked

| Linear | Issue | Status |
|---|---|---|
| **THY-86** | Nuclear-medicine scan dates unparsed — `scandate_parsed` NULL for all 2,220 rows. Raw `scandate` is a clean `MM/DD/YYYY` string; 2,218/2,220 parse. Interim fix shipped in `vw_nuclear_med_dated_v1`; recommends an upstream backfill. | High |
| **THY-87** | Three competing surgery-date columns. Quantified: `surg_first_date` and `surgery_date` are identical duplicates; `first_surgery_date` (most complete) diverges from them in 171 patients; max gap ~59 years. | Medium |
| **THY-88** | `pub_eval` evaluation layer created — wire into a QC gate / Pipeline refresh. | Medium |
| **THY-89** | Pick the canonical LN-positive count. Quantified: 51 raw-vs-final disagreements, plus 38 impossible rows (10 LN01 + 28 LN02). | High |

See `Source_of_Truth_Decisions.md` for the full quantified write-up of THY-87 / THY-89 — these need a clinical/domain decision on which column is authoritative; tooling can't make that call.

---

## 5. Documents in this folder

| File | What it is |
|---|---|
| `BigQuery_Studio_Integration_Plan.md` / `.docx` | The full feature-integration plan: pain-point audit, feature map, the broader GCP ecosystem evaluation, phased rollout, governance. |
| `Source_of_Truth_Decisions.md` | Quantified comparison of the competing surgery-date and LN-positive columns; the decisions that need a human. |
| `Workspace_Lifecycle_Policy.md` | A labels + TTL + recurring-sweep policy for `pub_workspace` (480 objects; a legibility problem, not a storage one). |
| `build_doc.py` / `build_doc.js` | Scripts that generate the plan `.docx` (python-docx and docx-js versions). |
| `sql/` | Reproducible DDL for every BigQuery object listed above. |

---

## 6. What still needs a human

This integration made the pain points **visible and self-policing going forward** — the QC pipeline catches surgery-date and LN divergence every morning, the workspace policy flags unlabeled tables, the census is governed canonical data. It did **not** clear the existing backlog, because the core decisions are domain calls:

1. **THY-87** — which surgery-date column is authoritative; chart-review the 171 divergences + the ~59-year-gap outliers.
2. **THY-89** — which LN-positive count is canonical; correct or exclude the 38 impossible rows.
3. **Column verification** — sign off the 65 `canonical_patient_workup_census_v1` columns in the column verification registry.
4. **Workspace sweep** — execute the targeted first sweep in `Workspace_Lifecycle_Policy.md`.
5. **Confirm** the QC pipeline shows "Deployed" so the schedule runs the expanded (6-check) assertion.
