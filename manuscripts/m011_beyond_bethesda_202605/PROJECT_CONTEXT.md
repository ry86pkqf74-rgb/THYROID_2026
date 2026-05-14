# THYROID_2026 — Project Context & Memory

> Durable reference for the thyroid surgery registry / TI-RADS research program.
> Created 2026-05-14. Keep this current; future sessions should read it first.
>
> **For manuscript work, read `MANUSCRIPT_WRITING_PLAYBOOK.md` next** — it has the
> full BigQuery table map, join keys/bridges, BQML patterns, data caveats, the
> standard pipeline, and GitHub conventions. Promote it to a Cowork skill
> (`thyroid-manuscripts`) and commit it to the THYROID_2026 repo.

## BigQuery

- **Project ID:** `thyroid-canonical-pub-2026`
- **Project number:** `915373663815`
- **Connector:** BigQuery MCP (`execute_sql_readonly` preferred; `execute_sql` for DDL/CREATE MODEL).
  Every call needs `projectId: thyroid-canonical-pub-2026`.

### Datasets
- `pub_canonical` — governed canonical layer (source of truth for analysis).
- `pub_eval` — evaluation views.
- `pub_workspace` — per-manuscript workspace tables (e.g. `m006_*`, `m036_*`, and now `m011_*`).
- `pub_archive` — snapshots / pre-migration backups.

### Key objects
- `pub_canonical.canonical_patient_master` — 10,871 patients × ~2,314 cols. Canonical master; never altered in place.
- `pub_canonical.canonical_patient_master_v1_9` — master + workup census left-joined, 10,871 × ~2,375 cols.
- `pub_canonical.canonical_patient_workup_census_v1` — 10,871 × 65 cols, clustered on `research_id`.
- Patient key: `research_id`.
- Governance: `bq_migration_log_v1`, `canonical_table_signoff_registry_v1`, `qc_rules_v1`.

### Known data caveats (from QC work 2026-05-14)
- `surg_first_date` and `surgery_date` are identical duplicates.
- `first_surgery_date` is most complete but diverges from the above in 171 patients (some corrupt dates, max gap 21,550 days).
- Lymph-node fields: 51 raw-vs-final disagreements + 38 impossible rows. Authoritative-source decision pending (Linear THY-87, THY-89).
- Daily QC pipeline `cowork_qc_daily_check` runs 6:00 AM CDT (checks SURG01, LN01-03, pub_eval integrity).

## Repos & infra
- **Analysis repo:** `THYROID_2026` — github.com/ry86pkqf74-rgb/THYROID_2026 (owner: Logan Glosser / ry86pkqf74-rgb).
  BigQuery DDL lives in `docs/bigquery_studio_integration/sql/`.
- ResearchFlow ML fleet (`ROS_FLOW_2_1`) is a **separate** system — not used for manuscript analysis.
- Airtable: manuscript tracker base; M011 record `recY4el1867Zbopiu`; Manuscript Feedback Log + Data Feedback Log.
- Linear: workspace `rostemp` (issues prefixed `THY-`).

## Manuscripts (this program)
- **M1 / TIRADS Systems Comparison** — patient-level preoperative comparison of US risk-stratification systems. Done (v4). Cohort 3,737 patients. Output: `~/Desktop/TIRADS_Systems_comparison/`.
- **M006** — preoperative molecular testing & surgical extent. Primary cohort n=1,048.
- **M036** — ATA 2025 RSS recurrence. Table `pub_workspace.m036_ata_2025_rss_v3`, 1,946 rows.
- **M011 — "Beyond Bethesda?"** Incremental value of TI-RADS, US morphology, molecular testing AFTER Bethesda cytology, esp. Bethesda III/IV. Workspace folder: `~/Desktop/m011/`. **Build complete 2026-05-14:** frames + audit + outcomes + 17 BQML models + tables + figures in `pub_workspace.m011_*`. Primary cohort n=2,479 (Bethesda III/IV 723; molecular-tested 458). Headline: TI-RADS adds little after Bethesda (ΔAUROC +0.015, NS); molecular testing adds more (ΔAUROC +0.088, sig). Recommended framing B. Remaining: run `scripts/m011_advanced_stats.{R,py}` for bootstrap CIs/DeLong/calibration once sandbox is back; NIFTP & clin-sig sensitivity reruns.

## Environment notes
- Linux sandbox (`mcp__workspace__bash`) was DOWN 2026-05-14 with "no space left on device" — Python/R unavailable. Workaround: do stats in BigQuery ML; write R/Python scripts for the rest. Retry sandbox periodically.
- Session transcripts via `session_info` expose only assistant narration, NOT tool-call arguments — don't rely on them to recover IDs/paths.
