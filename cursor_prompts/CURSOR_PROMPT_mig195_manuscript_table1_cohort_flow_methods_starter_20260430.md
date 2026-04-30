# Cursor Prompt — mig_195 manuscript Table 1 + cohort flow + methods section starter

**Date:** 2026-04-30
**Lane:** mig_195 / manuscript_table1_cohort_flow_methods_starter
**Batch (proposed):** `mig_195_manuscript_table1_cohort_flow_methods_starter_20260430`
**Posture:** **READ-ONLY scoping + authoring.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces SQL + Markdown drafts

---

## Background

The publication lakehouse is at gate1=172, PM verified 1,596/24/0, AJCC `*_resolved` cols populated post-mig_188b. Manuscript work needs to start. mig_195 produces three foundational artifacts:

1. **Table 1 SQL + draft** — demographic and clinical characteristics of the analytic cohort
2. **Cohort flow diagram data** — 10,871 → exclusions (NIFTP, etc.) → analytic cohort, with each exclusion step quantified
3. **Methods section starter** — text drafts for "Data Sources", "Cohort Definition", "Variable Definitions", "Statistical Approach"

These are STARTER artifacts — Logan refines content/style. Cursor's job is to produce SQL that pulls the right metrics and Markdown templates with placeholders that Logan fills in.

---

## Conditional pre-flight gate

If mig_188b/186b/185b/187 NOT yet applied (registry rows for these batch_ids = 0), **defer this lane** and write a placeholder report. Table 1 and cohort flow depend on the post-apply state for resolved staging cols and post-NIFTP-exclusion denominators.

---

## Required scope

### §1 Table 1 — analytic cohort characteristics

Author `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql`:

Cohort definition (post-mig_186b NIFTP exclusion; analytic cohort): `canonical_patient_master.is_malignant=TRUE AND research_id IN (SELECT research_id FROM canonical_path_malignant_events_v1)` — patients with at least one remaining malignant path event after NIFTP exclusion.

Stratification variables to include (with COALESCE to the new `*_resolved` cols where applicable):
- Age at first surgery (mean ± SD; median IQR)
- Sex (n, %)
- Race/ethnicity (n, %)
- Histology (PTC / FTC / MTC / ATC / DTC / other) — from `histology_final` or `histologic_types_all`
- Tumor size at primary (cm; median IQR; bucket counts <1 / 1-2 / 2-4 / >4)
- AJCC 8 stage group resolved (I / II / III / IVA / IVB / IVC / unstaged) — `ajcc8_stage_group_resolved`
- T stage resolved (T0 / T1a / T1b / T2 / T3a / T3b / T4a / T4b / unknown)
- N stage resolved (N0 / N1 / N1a / N1b / NX)
- M stage resolved (M0 / M1 / unknown)
- Multifocality (any > 1 tumor)
- Extrathyroidal extension any (gross / minimal / absent)
- Lymphovascular invasion any
- Margin status final (R0 / R1 / R2 / unknown)
- Lymph node involvement (any positive node)
- Distant metastasis at presentation (M1)
- Surgery type (total thyroidectomy vs hemithyroidectomy vs neck dissection only)
- Radioactive iodine ever (rai_first_date IS NOT NULL)
- Recurrence ever (any_recurrence_flag)
- Time to last contact (years; median IQR)
- Vital status at last contact (alive / deceased) — if `vital_status` exists

Output: SQL + columnar CSV at `manuscript/table_1_cohort_characteristics.csv` ready for Logan to format into the manuscript Table 1.

### §2 Cohort flow diagram data

Author `qc_framework_v1/manuscript/cohort_flow_diagram.sql`:

Each step quantified:
1. **Total cohort:** 10,871 distinct research_id in `canonical_patient_master`
2. **Excluded: not malignant per CPM:** `is_malignant=FALSE` count
3. **Excluded: NIFTP/UMP-only via mig_186b:** count where `research_id IN canonical_path_indeterminate_events_v1` AND `research_id NOT IN canonical_path_malignant_events_v1`
4. **Excluded: no histology resolved:** count where `histology_final IS NULL AND ajcc8_t_stage_resolved IS NULL`
5. **Excluded: no follow-up data:** count where `last_contact_date IS NULL`
6. **Analytic cohort:** the remainder

Output: 6-row table at `manuscript/cohort_flow_diagram.csv` with `step | description | n_excluded | n_remaining`.

### §3 Methods section starter

Author `qc_framework_v1/manuscript/methods_section_starter.md`:

Template sections:

#### Data Sources
- Single-institution thyroid cancer registry at Emory University, [date range]
- Sources: pathology reports (synoptic + free text), operative reports, ultrasound reports, NSQIP linkage for surgical complications, medications NLP, etc.
- Pipeline: Tier 1 raw extraction → Tier 2 canonical events / patient_rollup → Tier 3 patient master (canonical_patient_master)
- Data lake: MotherDuck publication database (`thyroid_canonical_publication_v1_0`) with 172 verified canonical tables, 1,596 verified cols on patient master, cohort N=10,871 distinct patients

#### Cohort Definition
- Inclusion: any patient with [criterion]
- Exclusion: NIFTP and uncertain malignant potential (UMP) tumors per WHO 2017 reclassification (excluded via mig_186b lane; preserved in `canonical_path_indeterminate_events_v1` for sensitivity analysis); patients without follow-up data

#### Variable Definitions
- AJCC 8 staging derived per Logan-ratified 8-rule R1 derivation (mig_188b); `*_resolved` columns prefer over legacy stored cols
- T0 (no primary identified at this surgery) used per AJCC convention for LN-only metastatic cases without primary tumor evidence on this surgery
- Histology coded per [reference] with multi-component tumors classified by most-aggressive component (MTC > PTC > FTC; ATC highest)
- Recurrence: structural disease detection per `canonical_recurrence_v1` event grain
- Survival: time from first surgery to last contact / death

#### Statistical Approach
- Descriptive: median (IQR) for continuous, n (%) for categorical
- Survival: Kaplan-Meier with log-rank; Cox proportional hazards for multivariable
- [other methods placeholder]

#### Data Quality Notes
- 533 source-distinct duplicate event rows preserved in `canonical_path_malignant_events_v1` flagged via `is_source_distinct_duplicate_grain=TRUE`; analytic queries should use `COUNT(DISTINCT (research_id, surgery_episode_id, tumor_ordinal))` for tumor counts (mig_185b)
- 220 NIFTP/UMP events excluded from malignant cohort but preserved in indeterminate landing table (mig_186b)
- 121 LN-NLP-only ultrasound exam dates seeded into exam master with deterministic md5 IDs (mig_187 R-A)
- Carry-forward CFs documented in supplementary appendix (manuscript_appendix_candidates per mig_190)

### §4 Audit/report

Author `qc_framework_v1/reports/mig_195_manuscript_table1_cohort_flow_methods_starter_20260430.md`:
- §1 deliverables list
- §2 Logan-curatable placeholders (anywhere Cursor inserted `[…]` or `[reference]` etc.)
- §3 unblocking checklist for Logan to refine into final manuscript text

### §5 Mark deliverables READY

Header per artifact: `<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Methods section is a STARTER — Logan owns voice, scope, and final language.

---

## Deliverables

1. `qc_framework_v1/manuscript/table_1_cohort_characteristics.sql`
2. `qc_framework_v1/manuscript/table_1_cohort_characteristics.csv` (placeholder; populated post-apply via Cowork)
3. `qc_framework_v1/manuscript/cohort_flow_diagram.sql`
4. `qc_framework_v1/manuscript/cohort_flow_diagram.csv` (placeholder)
5. `qc_framework_v1/manuscript/methods_section_starter.md`
6. `qc_framework_v1/reports/mig_195_manuscript_table1_cohort_flow_methods_starter_20260430.md`

Commit message: `qc: mig_195 manuscript Table 1 + cohort flow + methods section starter (3 deliverables: T1 SQL/CSV; cohort flow SQL/CSV; methods MD; Logan-curatable placeholders)`

---

End of prompt.
