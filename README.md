# THYROID_2026 — Data Build Repo

Logan's multi-year thyroid surgical, pathological, and molecular research database —
the build and governance repo for the canonical publication dataset.

## Platform

The canonical database runs entirely on **Google Cloud BigQuery**, project
**`thyroid-canonical-pub-2026`**.

> **Migration note:** the project was previously built on MotherDuck + local DuckDB.
> It has since been fully migrated to BigQuery; **MotherDuck and DuckDB are no longer
> used.** Any `motherduck_*` / DuckDB-era documents still under `docs/` are retained
> for historical reference only and do not describe the current platform.

## What this project is

A thyroid surgical / pathological / molecular research database of **~10,871 patients**
(`pub_canonical.canonical_patient_master`), supporting a portfolio of 80+ planned and
active manuscripts. All patient-grain data is keyed on a de-identified `research_id`.

## Canonical data on BigQuery

The project is organized into layered datasets in `thyroid-canonical-pub-2026`:

| Dataset | Purpose |
|---|---|
| `pub_raw` | Raw landed source data |
| `pub_staging` | Staging area for in-progress transforms |
| `pub_legacy_source_20260416` | Frozen legacy-system source tables (pre-migration) |
| `pub_canonical` | **The canonical layer** — `canonical_patient_master` (one row per patient) plus the `canonical_*` event and rollup tables |
| `pub_semantic` / `pub_views_readable` | Semantic and human-readable view layers |
| `pub_workspace` | Cohort views, analytic tables, work-in-progress (incl. `manuscript_feasibility_v1` and per-manuscript cohort views) |
| `pub_signoff` | Governance — sign-off registry, column-verification registry, migration log, QC framework, drift gate, cost logs |
| `pub_eval` | Global evaluation layer — patient work-up census views |
| `pub_archive` | Point-in-time snapshots taken before every mutation |

**Key canonical tables:** `canonical_patient_master` (10,871 patients),
`canonical_molecular_genetics_v2` (molecular tests), and the
`canonical_*_patient_rollup_*` tables (operative, complications, lymph node,
recurrence, invasion, ultrasound, etc.).

**Analyst entry point:** `pub_canonical.manuscript_cohort_v1` and the per-manuscript
cohort views in `pub_workspace`. See [`MANUSCRIPT_DATA_START_HERE.md`](MANUSCRIPT_DATA_START_HERE.md)
for the exact tables, views, and rules for citing data in manuscripts.

## Governance & QC (`pub_signoff`)

Every canonical change is snapshotted to `pub_archive` and logged in
`pub_signoff.bq_migration_log_v1`. Governance objects:

| Object | Purpose |
|---|---|
| `canonical_table_signoff_registry_v1` | Per-table sign-off status |
| `canonical_column_verification_registry_v1` | Per-column verification status |
| `bq_migration_log_v1` | Append-only log of every migration, with rollback SQL |
| `master_drift_gate_v1` | Row- and feature-count "fail-if-below" gates that catch silent table shrinkage |
| `qc_assertions_v1` | Catalogue of QC rules — each row holds a `check_sql` |
| `run_qc_assertions()` | Stored procedure that executes every active rule and writes `qc_violations_v1` |
| `qc_violations_v1` / `qc_daily_summary_v1` / `qc_weekly_trend_v1` | QC run log and summaries |

**Daily QC:** the scheduled query **`qc_daily_runner`** (us-central1) runs
`CALL pub_signoff.run_qc_assertions()` every 24 h — a non-blocking gate that surfaces
integrity violations across the whole dataset: cohort sizes, joins/orphans, enum and
date ranges, molecular-layer provenance, pathology consistency, and drift. Coverage is
documented in [`docs/bigquery_studio_integration/`](docs/bigquery_studio_integration/).

**Evaluation layer:** `pub_eval` holds patient work-up census views, with a Looker
Studio dashboard and a Dataplex glossary on top. See
[`docs/bigquery_studio_integration/README.md`](docs/bigquery_studio_integration/README.md).

## Project tracking & manuscript pipeline (Airtable + Linear + Claude)

> Anyone editing this repo or drafting a manuscript must read [`CLAUDE.md`](CLAUDE.md)
> and [`INTEGRATION_PROPOSAL.md`](INTEGRATION_PROPOSAL.md) first. The integration is the
> audit trail of record for cohort decisions, override evidence, manuscript drafts, and
> verification checks.

**Airtable** holds the structured inventory — every Source File, every Column with
verification status, every Verification Check, every Override Decision, every Cohort
Patient (`research_id` only), and every Manuscript. **Linear** (team `Thyroid Database`
/ `THY`) holds work-in-flight: per-manuscript projects, QA findings, override-review
tasks, drafting issues. **Claude** runs a daily sync that creates Linear issues from
new Airtable findings, mirrors closed issues back to advance Airtable lifecycle,
snapshots evidence on submission, and appends to the immutable Issue Ledger.

### IDs at a glance

| Resource | Identifier |
|---|---|
| Airtable workspace | `wspDGHtW2HNuT20GQ` |
| Airtable base — Data Registry (9 tables) | `appTGeB1jIizZbjnw` |
| Airtable base — Manuscript (7 tables) | `appJYOnUb7KrHKwpV` |
| Linear team | Thyroid Database / `THY` / `c4afb51b-8bca-413a-a53e-15eb825cffbd` |
| Daily-sync anchor issue | [THY-6](https://linear.app/rostemp/issue/THY-6/) |
| Scheduled daily sync | `thyroid-daily-sync` (07:04 local) |
| Cowork skill | `.cowork/skills/thyroid-integration/` |

### Hard rules (full list in [`CLAUDE.md`](CLAUDE.md))

1. **No PHI in Airtable or Linear, ever.** `research_id` only. Pathology text, op notes,
   MRNs, and dates of service narrower than year stay in the access-controlled BigQuery
   canonical project and local files. Override Decision evidence is summarized 1–2
   sentences, never raw text.
2. **Nothing is ever deleted.** Linear issues close, never delete. Airtable records
   archive (`lifecycle = Archived`). `Manuscript-Locked` records cannot be edited without
   explicit unlock. Canonical tables are snapshotted to `pub_archive` before any mutation.
3. **Every change at user request gets logged BEFORE the change.** Manuscript edits →
   `Manuscript Feedback Log`; data / registry / canonical edits → `Data Feedback Log` and
   `pub_signoff.bq_migration_log_v1`. The log row is created first; if logging fails, the
   edit doesn't happen.
4. **Pending Auto-Close, not auto-close.** When a Verification Check or Section reaches
   Verified/Finalized, the linked Linear issue moves to state `In Review` + label
   `auto-close:pending`. After 48 h with no `/keep-open` it transitions to Done.

### Where to find the full architecture

| Doc | Purpose |
|---|---|
| [`CLAUDE.md`](CLAUDE.md) | Project context auto-loaded by Claude — hard rules, Session Opening Protocol, IDs |
| [`INTEGRATION_PROPOSAL.md`](INTEGRATION_PROPOSAL.md) | Integration architecture — schema, lifecycle, daily-sync prompt |
| [`docs/bigquery_studio_integration/`](docs/bigquery_studio_integration/) | BigQuery Studio integration — `pub_eval` layer, governance, QC pipeline, Looker/Dataplex artifacts |
| [`.cowork/skills/thyroid-integration/SKILL.md`](.cowork/skills/thyroid-integration/SKILL.md) | Operational playbook (auto-loads on thyroid keywords) |
| [`.cowork/skills/thyroid-integration/references/`](.cowork/skills/thyroid-integration/references/) | Full Airtable + Linear schemas and live IDs |

## Querying the data

The canonical data lives in BigQuery project `thyroid-canonical-pub-2026`. Query it via
the BigQuery console, the `bq` CLI, the BigQuery MCP connector, or the Python client
(`google.cloud.bigquery`). Per-study extraction and derivation pipelines live under
`studies/`; canonical build and migration SQL lives under `scripts/` and `bq_migrations/`.

## Repository layout

```
.
├── CLAUDE.md / INTEGRATION_PROPOSAL.md   # Integration architecture + project context
├── docs/                                 # Architecture, governance, and audit docs
│   └── bigquery_studio_integration/      # BigQuery Studio QC + governance layer
├── scripts/                              # Build, migration, and view-creation scripts
├── bq_migrations/                        # BigQuery migration SQL
├── studies/                              # Per-manuscript extraction / analysis folders
├── llm_extraction/                       # LLM + regex clinical-note entity extraction
├── manuscripts/                          # Manuscript working trees
├── CHANGELOG.md / RELEASE.md             # Build history + version registry
└── data_dictionary.md                    # Schema documentation
```

## License

Private research data — do not redistribute without permission.
