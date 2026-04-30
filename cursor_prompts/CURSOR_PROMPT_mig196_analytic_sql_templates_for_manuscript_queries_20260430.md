# Cursor Prompt — mig_196 analytic SQL templates for manuscript queries

**Date:** 2026-04-30
**Lane:** mig_196 / analytic_sql_templates_for_manuscript_queries
**Batch (proposed):** `mig_196_analytic_sql_templates_for_manuscript_queries_20260430`
**Posture:** **READ-ONLY scoping + SQL authoring.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces SQL templates + sample outputs

---

## Background

Post-mig_188b/186b/185b/187 apply, the publication has populated `*_resolved` AJCC cols, post-NIFTP-excluded malignant events, and corrected COUNT(DISTINCT) rollups. mig_196 produces 5 ready-to-run analytic SQL templates that Logan can use directly for manuscript queries. Each template includes documentation, expected schema, and a sample-row preview.

These are TEMPLATES — Logan adapts inclusion/exclusion criteria per analysis. Cursor's job is to author SQL that uses the right cols (preferring `*_resolved` over legacy) and is documented well enough for reuse.

---

## Conditional pre-flight gate

If mig_188b/186b/185b/187 NOT yet applied, **defer this lane** and write a placeholder. The templates depend on the post-apply state.

---

## Required scope

### §1 Template inventory (5 SQL files)

Author each at `qc_framework_v1/manuscript/analytic_templates/`:

#### Template 1: Overall Survival
File: `01_overall_survival.sql`
- Time-to-event: from `first_surgery_date` to `last_contact_date` (or death if `vital_status` exists)
- Censoring: alive at last contact
- Stratification examples: by AJCC8 stage_group_resolved, by histology_final, by age tertile (<55 / 55-70 / ≥70)
- Output cols: research_id, time_to_event_years, event_indicator (0=censored, 1=death), strata_var
- Cohort: post-NIFTP-exclusion analytic cohort
- Documentation block at top with assumptions + caveats

#### Template 2: Recurrence-Free Survival
File: `02_recurrence_free_survival.sql`
- Time-to-event: from `first_surgery_date` to `recurrence_date` (or last_contact_date if no recurrence)
- Use `canonical_recurrence_v1` for definitive recurrence events; fall back to `any_recurrence_flag` if needed
- Stratification: same as overall survival + by initial T-stage / N-stage / margin status
- Output cols: research_id, time_to_event_years, event_indicator (0=no recurrence, 1=recurrence), strata_var

#### Template 3: Stage Group Distribution by Histology
File: `03_stage_group_by_histology.sql`
- Cross-tab: `histology_final` × `ajcc8_stage_group_resolved`
- N (%) per cell with row totals
- Include `T0` / unstaged buckets explicitly so the manuscript can transparently cite "no primary identified"
- Output: long-format CSV-ready (histology, stage_group, n, pct_of_histology)

#### Template 4: Surgical Complication Rate by Surgery Type
File: `04_complication_rate_by_surgery_type.sql`
- Surgery types: total thyroidectomy, hemithyroidectomy, neck dissection (central / lateral / both)
- Complication categories: hypocalcemia / hypoparathyroidism / vocal cord palsy / chyle leak / hematoma / mortality (from `canonical_complications_events_v1`)
- Use 30-day post-op window for acute complications; full follow-up for chronic
- Output: surgery_type × complication_category cross-tab with N (%) and 95% CI

#### Template 5: Cohort Flow + Inclusion/Exclusion
File: `05_cohort_flow_and_exclusions.sql`
- Mirrors mig_195's cohort_flow_diagram but exposed as a reusable template with parameterizable cutoffs
- Each inclusion/exclusion criterion as a separate CTE with COUNT
- Outputs each step's affected research_ids (for sensitivity analyses where Logan wants to test the impact of each exclusion)

### §2 Sample preview per template

For each template, also produce a sample preview output (~20 rows or summary stats) at `qc_framework_v1/manuscript/analytic_templates/previews/0X_<name>_preview.csv` to confirm the SQL works end-to-end. (Logan / Cowork runs the SQL post-apply to populate; Cursor authors only the SQL + documentation.)

### §3 README

Author `qc_framework_v1/manuscript/analytic_templates/README.md`:
- Purpose of each template
- Cohort definition consistency (link to mig_195's cohort flow)
- Notes on `*_resolved` vs legacy cols (always prefer resolved)
- Recommended adaptations per analysis
- Caveats: 533 source-distinct dups (use COUNT DISTINCT), 220 NIFTP excluded (post-186b), T0 cohort interpretation

### §4 Audit/report

Author `qc_framework_v1/reports/mig_196_analytic_sql_templates_for_manuscript_queries_20260430.md`:
- §1 5 templates summary
- §2 Cohort consistency check (ensure all templates use the same analytic cohort definition)
- §3 known limitations / data quality caveats for each template
- §4 next-step suggestions (multivariable Cox, propensity scoring, etc.)

### §5 Mark templates READY

Header per SQL: `-- LOGAN-ADAPTABLE TEMPLATE; READY FOR MANUSCRIPT USE POST mig_188b/186b/185b/187 APPLY`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- These are STARTER templates — Logan adapts cohort criteria + statistical approach per his manuscript design.

---

## Deliverables

1. `qc_framework_v1/manuscript/analytic_templates/01_overall_survival.sql`
2. `qc_framework_v1/manuscript/analytic_templates/02_recurrence_free_survival.sql`
3. `qc_framework_v1/manuscript/analytic_templates/03_stage_group_by_histology.sql`
4. `qc_framework_v1/manuscript/analytic_templates/04_complication_rate_by_surgery_type.sql`
5. `qc_framework_v1/manuscript/analytic_templates/05_cohort_flow_and_exclusions.sql`
6. `qc_framework_v1/manuscript/analytic_templates/previews/*.csv` (5 placeholder previews)
7. `qc_framework_v1/manuscript/analytic_templates/README.md`
8. `qc_framework_v1/reports/mig_196_analytic_sql_templates_for_manuscript_queries_20260430.md`

Commit message: `qc: mig_196 analytic SQL templates for manuscript queries (5 ready-to-run: OS, RFS, stage-group×histology, complications×surgery, cohort flow; preview placeholders; README)`

---

End of prompt.
