# Cursor Prompt — mig_204 populate manuscript CSVs + run analytic templates against live MD

**Date:** 2026-04-30
**Lane:** mig_204 / populate_manuscript_csvs_run_analytic_templates
**Batch (proposed):** `mig_204_populate_manuscript_csvs_run_analytic_templates_20260430`
**Predecessor:** mig_195 + mig_196 (Cursor authored SQL templates with placeholder CSV outputs).
**Posture:** **READ-ONLY query execution + CSV export.** Cursor runs SELECTs and exports CSVs. No data mutation.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces populated manuscript CSVs.
**Tool recommendation:** **Cline + Sonnet 4.6** — long-running SQL execution + CSV authoring. Sonnet 4.6 is good at clinical interpretation of results (sanity-check denominators, spot-check distributions) and at iterative SQL fixing if templates fail on edge cases.

---

## Background

mig_195 + mig_196 authored SQL templates and placeholder CSVs:
- `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql` + `.csv`
- `qc_framework_v1/manuscript/cohort_flow_diagram.sql` + `.csv`
- `qc_framework_v1/manuscript/analytic_templates/01_overall_survival.sql` + preview placeholder
- `qc_framework_v1/manuscript/analytic_templates/02_recurrence_free_survival.sql` + preview
- `qc_framework_v1/manuscript/analytic_templates/03_stage_group_by_histology.sql` + preview
- `qc_framework_v1/manuscript/analytic_templates/04_complication_rate_by_surgery_type.sql` + preview
- `qc_framework_v1/manuscript/analytic_templates/05_cohort_flow_and_exclusions.sql` + preview

mig_204 runs each SQL against live post-chain MD, exports populated CSVs, and surfaces any template bugs found at runtime.

---

## Mission

Run all 7 SQL files end-to-end. Capture any errors / unexpected denominators / clinical anomalies. Output populated CSVs.

---

## Required scope

### §1 Pre-flight

Confirm post-chain state:
- HEAD `51e201a` or later
- `canonical_path_malignant_events_v1` row count = 6,469 ✓
- `canonical_path_malignant_patient_rollup_v1` row count = 4,022 ✓
- `canonical_us_exam_master_VIEW_v2` row count = 11,880 ✓
- `canonical_us_lymph_node_events_v2` exam_id_source distribution: 100% `exam_master_reused` ✓
- AJCC `*_resolved` cols populated (`ajcc8_stage_group_resolved` non-null on most malignant patients)

### §2 Run Table 1 SQL

Execute `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql` → export to `qc_framework_v1/manuscript/table_1_cohort_characteristics.csv`.

Spot-check:
- Total cohort N matches expectation (~4,022 malignant patients per post-186b state)
- Histology distribution: PTC dominant, MTC ~167, ATC ~26, FTC, etc.
- T-stage distribution: T1a most common, includes 60 T0 events
- Stage group: I dominant for age <55; II/III/IVA for older + MTC

### §3 Run cohort flow SQL

Execute `cohort_flow_diagram.sql` → export to CSV.

Each step produces n_excluded + n_remaining. Verify:
- Step 1 total cohort: 10,871
- Step 2 NIFTP/UMP exclusion: ~115 edge patients (those with NO other malignant event)
- Final analytic cohort: ~4,022 (matches Table 1)

### §4 Run 5 analytic templates

For each:
- 01_overall_survival.sql
- 02_recurrence_free_survival.sql
- 03_stage_group_by_histology.sql
- 04_complication_rate_by_surgery_type.sql
- 05_cohort_flow_and_exclusions.sql

Execute → export full result to `previews/0X_<name>_preview.csv` (overwriting placeholders).

For each, surface in report:
- Row count of output
- Any NULL/NaN/Inf values that suggest template bug
- Spot-check 3 representative output rows for clinical plausibility

### §5 Bug surfacing

If any SQL fails at runtime (binder error, null arithmetic, missing col), document the error + propose minimal patch. Patches go to `qc_framework_v1/manuscript/{table_1,cohort_flow,analytic_templates/<file>}.sql`.

### §6 Audit/report

Author `qc_framework_v1/reports/mig_204_populate_manuscript_csvs_run_analytic_templates_20260430.md`:
- §1 7 SQLs run + result row counts
- §2 spot-check observations per output (clinical plausibility)
- §3 any patches applied
- §4 manuscript-readiness assessment of each template (ready for analyst use? blocker?)
- §5 unblocking checklist for Logan to use these CSVs in manuscript drafting

### §7 Mark READY

Header: `<!-- READY FOR LOGAN MANUSCRIPT DRAFTING -->`

---

## Governance reminders

- Read-only investigation only. No `query_rw` (only SELECTs).
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Do NOT edit cohort definition without Logan ratification — surface any cohort question to Logan rather than self-correcting.

---

## Deliverables

1. Populated `qc_framework_v1/manuscript/table_1_cohort_characteristics.csv`
2. Populated `qc_framework_v1/manuscript/cohort_flow_diagram.csv`
3. Populated `qc_framework_v1/manuscript/analytic_templates/previews/0{1,2,3,4,5}_*_preview.csv`
4. `qc_framework_v1/reports/mig_204_populate_manuscript_csvs_run_analytic_templates_20260430.md`
5. (conditional) any patched SQL files with `<!-- mig_204 patch: <description> -->` comment
6. `exports/mig204_manuscript_csv_population_20260430/manifest.json`

Commit message: `qc: mig_204 populate manuscript CSVs + run 5 analytic templates against live MD (post-chain state); spot-check clinical plausibility; manuscript drafting unblocked`

---

End of prompt.
