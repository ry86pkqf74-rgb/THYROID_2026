# Analytic SQL templates (mig_196)

**Batch:** `mig_196_analytic_sql_templates_for_manuscript_queries_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Author (templates):** Logan Glosser &lt;logan.glosser@gmail.com&gt;

## Purpose

Five starter queries for manuscript analytics **after** mig_188b / mig_186b / mig_185b / mig_187 apply. Each file is read-only against MotherDuck—adapt cohort rules and stratifiers per hypothesis.

## Files

| File | Topic |
|------|--------|
| `01_overall_survival.sql` | Person-level OS: surgery index → death or administrative censor; multiple stratifiers via `strata_role` |
| `02_recurrence_free_survival.sql` | RFS using `canonical_recurrence_v1` + optional `any_recurrence_flag` fallback (`params` CTE) |
| `03_stage_group_by_histology.sql` | Long-format AJCC8 stage group × simplified histology bucket with `%` within histology |
| `04_complication_rate_by_surgery_type.sql` | Surgery bucket × mapped complication categories; Wald 95% CI |
| `05_cohort_flow_and_exclusions.sql` | **QUERY A:** mig_195 cohort-flow counts; **QUERY B:** exclusion/inclusion `research_id` lists |

## Cohort consistency

Default analytic spine for Templates **01–04**:

- Malignant `canonical_patient_master` rows  
- Post–mig_186b pool (exclude **indeterminate-only** patients per `cohort_flow_diagram.sql`)  
- Require resolved staging signal: **not** both `histology_final` and `ajcc8_t_stage_resolved` NULL  
- Require `last_contact_date` **present** (matches cohort-flow step 5)  
- Intersect `canonical_path_malignant_events_v1` distinct patients  

Template **05 QUERY A** matches `qc_framework_v1/manuscript/cohort_flow_diagram.sql` exactly. **QUERY B** lists IDs behind each exclusion arm plus the final analytic intersection.

Table 1 SQL (`table_1_cohort_characteristics.sql`) uses the **malignant path event** join without the last-contact exclusion—if Table 1 denominator must match survival templates, either add the same gate to Table 1 or remove it here (document in Methods).

## Resolved vs legacy columns

Prefer manuscript-facing resolved columns where present:

- `ajcc8_stage_group_resolved`, `ajcc8_t_stage_resolved`, `ajcc8_n_stage_resolved`, `ajcc8_m_stage_resolved`  
- Margin display: `r_class_true` (adjudicated R-class)  
- Vital / dates: `canonical_survival_followup_v1` (`vital_status_current`, `death_date`, `last_known_alive_date`, `first_surgery_date`) with CPM fallbacks  

Avoid legacy-only staging columns for publication tables unless documenting drift.

## Adaptations

- **OS:** Tune deceased string matching on `vital_status_current` / CPM `vital_status` to match institutional vocabulary.  
- **RFS:** Set `recurrence_fallback_any_flag` FALSE to insist on `canonical_recurrence_v1` confirmation only. Restrict to patients with non-null `recurrence_date` for strict calendar RFS.  
- **Complications:** Acute window fixed at 0–30 d with `timing_days`; hypoparathyroidism and mortality use any-time windows. Map additional enum values if consolidation adds types.  
- **Strata duplication:** Template 01 emits three copies per patient (`strata_role`); filter one `strata_role` before exporting to survival software.

## Caveats (governance)

- **COUNT DISTINCT:** analytic cohorts use distinct patients; never rely on raw row counts where feeder tables fan out.  
- **~220 NIFTP / indeterminate-only exclusions** post–mig_186b—flows documented in mig_195 report.  
- **T0 / unstaged:** Template 03 buckets missing resolved stage group as `T0_or_unstaged` for transparency.  
- **533 source-distinct duplicate hygiene** (historical audit)—always qualify aggregates with `COUNT(DISTINCT research_id)` when summing patients.  
- **DATE vs TIMESTAMP:** cast recurrence / survival bridge columns to **DATE** before `DATE_DIFF` (CF-100 / mig_121 family).

## Previews

`previews/*.csv` are **placeholders** until Logan runs each SQL on MotherDuck and exports (`COPY … TO`).
