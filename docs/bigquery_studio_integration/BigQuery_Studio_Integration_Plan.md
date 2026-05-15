# BigQuery Studio Feature Integration Plan

### Reducing data-quality, lineage, and legacy-migration friction in the Thyroid Canonical Publication database

**Project:** thyroid-canonical-pub-2026  **Prepared for:** the research data team  **Date:** May 14, 2026
**Status:** Draft for team review — first deliverables already executed (see Section 2)
**Scope:** BigQuery Studio (Data Preparation, Data Canvas, Notebooks, Visualizations, Apache Spark, Pipelines, custom Agents, Gemini) plus the broader Google Cloud analytics ecosystem

---

## 1. Executive summary

Our BigQuery project is large, mature, and carefully governed — roughly 10,871 patients spread across nine datasets and several hundred tables, with a real sign-off registry, a query-cost log, and a snapshot-everything discipline. The problem is not that the data is bad. The problem is that almost all of our quality control happens *after the fact*: we discover a broken link, a disagreeing column, or a builder that did not re-run only once it has already propagated into a cohort or a manuscript number, and then we spend days backtracking.

BigQuery Studio — together with the wider Google Cloud analytics stack — now ships a set of features that let us move that quality control to the front of the process and make the whole dataset legible to the team. This plan maps each feature to a specific pain point we are actually hitting, records the first deliverables that have already been built, proposes a new "global evaluation layer" of patient-level census views, evaluates the broader GCP toolset, and lays out a phased rollout.

**The single highest-leverage change is to stop treating our QC rules as a script we run when we remember to, and instead make them a Pipeline gate that canonical tables must pass before they are published.** Everything else in this plan supports or extends that idea.

---

## 2. What has already been executed

This is a working plan, not just a proposal. The directly-executable piece — the global evaluation layer — has already been built in the live project, and the data issues it surfaced have been logged.

> **Live in the project today**
> - **New dataset `pub_eval`** — isolated, views only, no underlying data mutated.
> - **`vw_patient_workup_census_v1`** — one row per patient (10,871). Preop and postop performed-flags and intervals for ultrasound, CT, MRI, FNA, and nuclear medicine; reoperation signals; and a prior-thyroid-procedure pathology-gap review flag.
> - **`vw_workup_census_summary_v1`** — long-format aggregate roll-up, built to drive a dashboard.
> - **`vw_nuclear_med_dated_v1`** — nuclear-medicine scans with scan dates recovered (see below).

### 2.1 Data issue found and fixed: nuclear medicine had no usable dates

The first build of the census view showed nuclear medicine as entirely absent — zero patients, preop or postop. The cause was not missing data: `pub_workspace.nuclear_med_clean.scandate_parsed` was NULL for all 2,220 rows because the parser step never ran, even though the raw `scandate` column is a clean MM/DD/YYYY string that parses for 2,218 of the 2,220 rows. A new view, `vw_nuclear_med_dated_v1`, parses those dates, and the census now correctly resolves nuclear medicine to 1,148 patients with scans on file — 511 with a preoperative scan (median 174 days before surgery) and 686 with a postoperative scan (median 99 days after). Two rows (a typo'd year and one blank) were routed to chart review.

### 2.2 Issues logged to Linear

Every issue that surfaced during this work was filed in Linear under the Thyroid Database team:

| Issue | Title | Disposition |
|---|---|---|
| THY-86 | Nuclear medicine scan dates unparsed — scandate_parsed NULL for all 2,220 rows | High priority. Interim fix shipped in pub_eval; recommends an upstream backfill plus a QC assertion. |
| THY-87 | Three competing surgery-date columns on canonical_patient_master (SURG01) | Medium. The census view had to pick `first_surgery_date` as its anchor; one canonical column should be chosen. |
| THY-88 | pub_eval global evaluation layer created — wire into a QC gate / Pipeline refresh | Medium. Tracks promoting the views, adding QC assertions, and resolving the US/CT/MRI lineage caveats. |

### 2.3 What the census shows

| Modality | Preop coverage | Median days pre-op | Postop coverage |
|---|---|---|---|
| FNA | 46.6% (5,063) | 91 | 3.1% (336) |
| Ultrasound | 32.6% (3,539) | 212 | 15.6% (1,699) |
| CT | 23.8% (2,592) | 181 | 8.6% (930) |
| Nuclear medicine | 4.7% (511) | 174 | 6.3% (686) |
| MRI | 2.9% (319) | 339 | 1.0% (113) |

Patient-level flags: reoperation 10.1%, completion thyroidectomy 6.1%, prior thyroid procedure documented 8.7%, and 42.8% of patients in the "sparse" preop-workup tier (no preoperative ultrasound and no preoperative FNA on record) — itself a finding worth a closer look.

---

## 3. Where the project stands today

### 3.1 What is already working well

- A layered dataset structure exists: `pub_raw`, `pub_staging`, `pub_legacy_source_20260416`, `pub_canonical`, `pub_semantic`, `pub_views_readable`, `pub_workspace`, `pub_signoff`, and `pub_archive`.
- A genuine governance layer exists in `pub_signoff`: a table sign-off registry (239 tables), a column verification registry (6,819 columns), a deprecation registry, a 111-row migration log, a master drift gate, and daily spend / expensive-query logs.
- A QC rule framework exists: 20 documented rules in `qc_rules_v1`, 368 logged violations, 18 run-based assertions, and daily/weekly QC summaries.
- Discipline is strong: every mutation is snapshotted into `pub_archive`, conventions are written down in a `__conventions` table, and a `__readme` change-log records what each build script did.

### 3.2 The recurring pain points

| Pattern | What it looks like in the project | Cost to us |
|---|---|---|
| **Validation is reactive** | QC rules live in `pub_workspace` and are run as scripts; violations are logged but do not block a table from being published or read. | Bad values reach cohorts and manuscript numbers before anyone notices. |
| **Competing sources of truth** | Rules flag three disagreeing surgery-date columns, two LN-positive columns that disagree, and a cross-source T/N/M disagreement table with 4,256 rows. | Analysts must know which column to trust; different analyses silently pick differently. |
| **Legacy-migration scarring** | ~135 frozen tables in `pub_legacy_source`, ~175 pre-migration snapshots in `pub_archive`, dozens of `MIG_*` tables, and a `__readme` full of post-migration patches. | Work done in the legacy system breaks when carried over; fixes are one-off. |
| **Broken / opaque lineage** | Orphan-reference and unresolved-pointer audit tables exist; `research_id` is VARCHAR in some tables and INTEGER in others, which silently produces 0% join overlap. | Backtracking to find why something is not linked is a frequent, manual task. |
| **Builder gaps found late** | The `__readme` repeatedly notes "builder did not retrigger" and "pipeline gap." The nuclear-medicine date gap in Section 2 is exactly this pattern. | Derived tables go stale silently; nobody is told a dependency moved. |
| **Workspace sprawl** | `pub_workspace` holds ~330 tables: many near-duplicate cohort versions, dry-run tables, and snapshots with no lifecycle policy. | Hard to tell live from dead; `pub_staging` sits empty while `pub_raw` holds only two tables. |

Read together, the theme is clear: **we have the discipline and the audit trail, but not the automation and the visibility.**

---

## 4. BigQuery Studio feature map

### 4.1 Data Preparation
*Addresses: data validation & quality; legacy migration.*

Profiles a table, surfaces anomalies and schema drift, and lets you build a Gemini-assisted, reusable cleaning recipe instead of a one-off SQL script.

- Build prep recipes for histology vocabulary, free-text ETE strings, the VARCHAR-vs-INTEGER `research_id` mismatch, and exactly the kind of unparsed date column the nuclear-medicine fix had to handle by hand.
- Use it as the standard intake path for `pub_legacy_source_20260416` — profile each legacy table, capture the transform as a recipe, land the result in `pub_staging` (currently empty).
- Recipes are versioned and re-runnable, so a legacy fix is documented once and applied consistently.

### 4.2 Data Canvas
*Addresses: broken / opaque lineage; visibility & collaboration.*

A visual, node-based workspace for joining, exploring, and tracing data.

- Prototype new evaluation views on a canvas before committing SQL.
- Trace disagreeing columns back to their feeder tables visually — the "which source do we trust" conversation over a picture.
- Use saved canvases as onboarding artifacts.

### 4.3 Notebooks (and the Notebook gallery)
*Addresses: validation & quality; visibility & collaboration.*

- Convert the ad hoc decision-log JSON files into a data-quality report notebook that runs the QC rules and charts the violation trend.
- Create a gallery template for "new manuscript cohort" so every cohort starts with the same checks — attacks the `cohort_m*` sprawl.
- Make notebooks the home of cohort feasibility analysis.

### 4.4 Visualizations
*Addresses: visibility & collaboration.*

- Stand up a QC dashboard from `qc_violations_v1` and the daily/weekly summaries.
- Publish `vw_workup_census_summary_v1` as a standing completeness dashboard — it was built in long format specifically to feed this.
- Build a cohort-attrition waterfall.

### 4.5 Apache Spark (serverless)
*Addresses: broken lineage; legacy migration at scale.*

- Run cross-source disagreement detection at scale (the work behind the 4,256-row T/N/M table) as a repeatable job.
- Fuzzy matching for broken-linkage repair where exact joins fail.
- Batch reconciliation of LLM-extracted entity tables against the structured canonical tables.

### 4.6 Pipelines
*Addresses: all four priorities — especially "builder did not retrigger."*

- Model the canonical build as a DAG: ingest → stage → validate (gate) → canonical → rollup → QC → publish. The nuclear-medicine date gap is precisely the failure this prevents.
- Insert the QC rules as a blocking gate.
- Schedule the QC dashboard and the `pub_eval` refresh so they are never stale (THY-88).

### 4.7 Custom Agents and Gemini in queries
*Addresses: visibility & collaboration; validation.*

- Build a project-aware agent seeded with the `__conventions` rules.
- Let clinicians ask "how many patients had a preoperative FNA but no pathology on file?" without hand-writing joins — the `pub_eval` census answers exactly this.
- Use Gemini in queries for day-to-day SQL, with the agent as the convention guardrail.

### 4.8 Connections and Files explorer
*Addresses: legacy migration; project hygiene.*

- Bring legacy Excel/Parquet sources in through managed external tables instead of one-off load scripts.
- Keep notebook and pipeline assets organized as the team grows.

---

## 5. The global evaluation layer (built)

The "what work-up actually happened, and where is the record thin" question is now consolidated in the `pub_eval` dataset.

### 5.1 `vw_patient_workup_census_v1`

One row per patient (10,871, validated: no null IDs, no null anchors, no negative intervals). Anchored on `first_surgery_date` (exposed as `surgery_anchor_date`; see THY-87).

| Field group | Type | Definition / source |
|---|---|---|
| `us_*` preop/postop performed, dates, intervals | BOOL / DATE / INT | Ultrasound, from `canonical_patient_master` `us_first_exam_date` / `us_last_exam_date`. |
| `ct_*`, `mri_*` (same pattern) + `ct_n_exams` / `mri_n_exams` | BOOL / DATE / INT | CT and MRI, from canonical `*_first_date` / `*_last_date` columns. |
| `fna_*` preop/postop performed, dates, intervals, counts | BOOL / DATE / INT | FNA, event-level from `canonical_fna_events_v1` — exact preop/postop. |
| `nucmed_any_on_file` / `nucmed_n_total` / `nucmed_date_resolved` / `nucmed_preop_*` / `nucmed_postop_*` | BOOL / INT / DATE | Nuclear medicine, event-level from `pub_eval.vw_nuclear_med_dated_v1`. |
| `n_preop_modalities` / `preop_core_workup_score` / `preop_workup_tier` | INT / FLOAT / STRING | Completeness summary (tier: core_complete / partial / sparse). |
| `n_surgeries` / `n_completion_thyroidectomies` / `any_reoperative_field` / `reoperation_flag` | INT / FLOAT / BOOL | From `canonical_operative_patient_rollup_v1_1`. |
| `prior_thyroidectomy_documented` / `prior_neck_surgery_documented` / `prior_thyroid_procedure_documented` | BOOL | From `canonical_psh_patient_rollup_v1`. |
| `has_surgical_pathology_on_file` / `prior_procedure_path_gap_flag` | BOOL | Gap flag = prior procedure documented AND no surgical pathology on file — a chart-review trigger. |
| `lobectomy_first_flag` / `completion_path_definite_flag` / `lobectomy_first_no_completion_path_flag` | BOOL | From `patient_completion_oed_path_linkage_v1` — completion-thyroidectomy pathology gaps. |

### 5.2 `vw_workup_census_summary_v1`

Long-format aggregate roll-up — one row per metric — to drive the dashboard. `imaging_modality` rows give preop/postop coverage and interval medians; `cohort_flag` rows give patient-level prevalences.

### 5.3 Known caveats (tracked in THY-88)

- US/CT/MRI pre/post are derived from patient-level first/last date columns, so the postop reference is the *last* exam, not the first postop exam. FNA and nuclear medicine are event-level and exact. Event-level US/CT/MRI feeds would make all five symmetric.
- These are views, not materialized tables — promotion + scheduled Pipeline refresh + census-specific QC assertions are the next step.
- `prior_procedure_path_gap_flag` should be validated against a chart-reviewed sample before manuscript use.

---

## 6. The broader Google Cloud analytics ecosystem

BigQuery Studio is the core, but several adjacent services map onto our pain points. Each is rated for fit *for this project specifically*. The guiding principle: keep the stack consolidated around BigQuery, add a service only where it closes a real gap.

### 6.1 Recommended

| Service | How we would use it |
|---|---|
| **Looker / Looker Studio** | The natural front end for what's already built. Point it at `vw_workup_census_summary_v1` and the `pub_signoff` QC tables for standing completeness and QC-trend dashboards. Looker Studio is the no-cost starting point. |
| **Data Catalog / Dataplex** | Directly targets the broken-lineage pain. A searchable catalog with table/column tags and lineage means people stop guessing which table is canonical. The metadata backbone the project is missing. |
| **Managed Airflow (Cloud Composer)** | Orchestration for the ingest → validate → publish DAG. BigQuery Pipelines may suffice; Composer is the answer if we outgrow it or need cross-service orchestration. |
| **Managed Apache Spark / Dataproc** | Serverless PySpark for heavy reconciliation — disagreement detection, fuzzy linkage repair, LLM-output reconciliation. |
| **Document AI** | High value for a chart-heavy dataset. Parses scanned/free-text pathology reports, op notes, and nuclear-med narratives into structured fields — directly relevant to THY-86 and the LLM-extraction effort. |
| **Agent Platform / Gemini Enterprise** | Where the project-aware custom agent (4.7) would be built and deployed against the `pub_eval` views. |

### 6.2 Situational — adopt if a specific need appears

| Service | When it would make sense |
|---|---|
| **Healthcare API / Healthcare data storage** | If we ever ingest EHR data directly or need a FHIR/HL7 representation. Not needed for a database built from periodic Excel/Parquet extracts. |
| **NotebookLM for Enterprise** | A useful adjunct for the team to query its own protocols, drafts, and docs — not part of the pipeline. |
| **Data Fusion / Alteryx Designer Cloud** | Visual ETL — overlaps with BigQuery Data Preparation (the lighter native option to try first). |
| **Lakehouse (Iceberg on Cloud Storage)** | Could be the landing format for legacy Parquet sources feeding `pub_raw` / `pub_staging`. Nice-to-have. |
| **Datastream** | Only if live EHR replication ever becomes a requirement. |

### 6.3 Not applicable to this project

Pub/Sub, Dataflow, Managed Service for Apache Kafka, and Confluent Cloud are real-time streaming/messaging tools — this is a static, batch research dataset with no streaming workload. Databricks and Elastic Cloud would fragment a stack that BigQuery + serverless Spark already covers. Earth Engine, Search for commerce, Talent Solution, AI Edge Portal, and the CCAI Platform target domains unrelated to clinical thyroid research.

---

## 7. Phased rollout

### Phase 0 — Foundations and quick wins (Weeks 1–2)

1. **DONE** — `pub_eval` global evaluation layer built; nuclear-medicine dates recovered; THY-86/87/88 filed.
2. Build the first data-quality notebook from a gallery template: run the 20 QC rules, chart the violation trend, share it.
3. Recreate the canonical patient spine and its main feeders as a saved Data Canvas.
4. Agree a naming and lifecycle convention for `pub_workspace`.

### Phase 1 — Validation as a gate (Weeks 3–6)

1. Consolidate the QC rules into one location and wrap them in a Pipeline step; add the census-specific assertions from THY-88.
2. Make that step a blocking gate: critical-severity rules must pass before publish.
3. Build Data Preparation recipes for histology vocabulary, ETE strings, the `research_id` type mismatch, and unparsed date columns.
4. Publish the QC dashboard and the workup-census dashboard in Looker Studio.

### Phase 2 — Lineage and metadata (Weeks 6–10)

1. Stand up Data Catalog / Dataplex: tag canonical vs workspace vs archive, turn on lineage tracking.
2. Promote the `pub_eval` views to materialized tables with a scheduled refresh; resolve the US/CT/MRI event-level caveat.
3. Adopt Data Canvas lineage tracing as the standard first step on column disagreements.
4. Stand up the first serverless Spark reconciliation job; route legacy intake through Connections + Data Preparation into `pub_staging`.

### Phase 3 — Agents and self-serve (Weeks 10+)

1. Build the project-aware custom Agent on Agent Platform, seeded with `__conventions`.
2. Pilot Document AI on a batch of pathology / nuclear-medicine narratives.
3. Publish the "new manuscript cohort" Notebook gallery template; automate the `pub_workspace` lifecycle policy.
4. Review: confirm each Section 3.2 pain pattern has a gate, a view, or a catalog entry behind it.

---

## 8. Governance and ownership

- **One QC home.** Rules, violations, and the gate live together and are versioned.
- **The gate is real.** A table that fails a critical rule is not published — the Pipeline enforces it.
- **`pub_staging` is used.** Legacy/raw intake lands there first, promoted only after a prep recipe and the QC gate.
- **Workspace has a lifecycle.** Every `pub_workspace` table has an owner and a sunset trigger, or a TTL.
- **Lineage is a picture and a catalog.** Disagreements are investigated on a Data Canvas and recorded in Data Catalog.
- **Findings are tracked.** Issues surfaced by the QC gate or analysis are filed in Linear — as THY-86/87/88 already are.

---

## Appendix A — Feature-to-pain matrix

| Feature | Validation & quality | Legacy migration | Lineage / linking | Visibility |
|---|---|---|---|---|
| Data Preparation | Primary | Primary | Supporting | — |
| Data Canvas | — | Supporting | Primary | Primary |
| Notebooks | Primary | Supporting | — | Primary |
| Visualizations / Looker | Supporting | — | — | Primary |
| Apache Spark / Dataproc | Supporting | Primary | Primary | — |
| Pipelines / Composer | Primary | Primary | Primary | Supporting |
| Custom Agents / Gemini | Supporting | — | Supporting | Primary |
| Data Catalog / Dataplex | Supporting | Supporting | Primary | Primary |
| Document AI | Supporting | Primary | — | — |

## Appendix B — pub_eval objects created

| Object | Description |
|---|---|
| `pub_eval` | New dataset (us-central1). Views only — non-destructive. |
| `vw_patient_workup_census_v1` | One row per patient (10,871). Preop/postop modality flags + intervals, reoperation signals, prior-procedure pathology-gap flags. |
| `vw_workup_census_summary_v1` | Long-format aggregate roll-up of the census, for dashboards. |
| `vw_nuclear_med_dated_v1` | Nuclear-medicine scans with scan dates parsed from the raw `scandate` string; carries a `scandate_quality` flag. |

## Appendix C — First-two-weeks checklist

- **DONE** — `pub_eval` evaluation layer built and validated.
- **DONE** — nuclear-medicine dates recovered; THY-86/87/88 filed.
- Data-quality notebook live and shared.
- Canonical-spine Data Canvas saved.
- `pub_workspace` naming + lifecycle convention agreed in writing.
- Owners named for Phase 1 (QC gate) and Phase 2 (metadata & lineage).

---

*Prepared as a draft for team review. Figures (dataset, table, rule, and census counts) reflect a live inspection of `thyroid-canonical-pub-2026` on May 14, 2026 and should be re-checked before this plan is circulated widely.*
