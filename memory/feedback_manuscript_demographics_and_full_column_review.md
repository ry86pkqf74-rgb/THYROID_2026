---
type: feedback
description: Every manuscript analysis must (1) include demographic/baseline characterization and (2) systematically review the entire dataset for relevant columns before finalizing cohort views or analysis plans.
---

# Manuscript standing rule: demographics + full-dataset column review

**Rule (set by Logan, 2026-05-01):** Before drafting or finalizing the analysis plan for *any* manuscript (M001–M083 or any future addition), the working agent MUST:

1. **Build a demographic/baseline-characteristics Table 1.** Required at minimum: age, sex, race, ethnicity. Strongly preferred where coverage is adequate: BMI, insurance/payer, smoking status, comorbidity burden (diabetes, CAD/CHF, CKD, prior thyroid disease), surgical era/year, surgeon volume bucket, hospital site. The Table 1 stratification should mirror the primary exposure (e.g., for a definition paper, stratify by the focal "exposed" cell vs. non-exposed; for a comparative-effectiveness paper, stratify by treatment arm).

2. **Run a systematic review of the entire dataset for relevant columns** before declaring the cohort view complete. This means querying `information_schema.columns` (and/or `list_columns` on `canonical_patient_master` and adjacent canonical rollups) using keyword filters tied to the manuscript's domain, and explicitly inventorying which columns to pull in and which to skip — with a one-line rationale for each.

## Why

Cowork has historically built cohort views with the bare-minimum columns asked for by the RQ, leaving demographic context and adjacent-domain variables off the table until drafting time. That has produced two failure modes:

- **Reviewer-blocking demographic gaps.** Surgical and clinical journals expect a Table 1 with a standard demographic set. Submitting without one invites desk rejection or major revisions.
- **Missed analyzable variables.** Examples discovered late in M038 planning (2026-05-01): `proc_nlp_tracheostomy`, `nsqip_length_of_stay_days`, `nsqip_transfusion`, `ops_difficult_airway`, `op_nlp_tracheal_involvement` all existed on `canonical_patient_master` but were not in the cohort view. Pulling them in *after* the analysis plan is written wastes review cycles and risks the analysis plan being shaped by what's-handy rather than what's-best.

## Operationalization

For every cohort view (`manuscript_workspace.cohort_M0XX_*`), the build process is:

**Step 1 — Cohort scoping.** Establish primary exposure(s) and outcome(s) from the master list / RQ.

**Step 2 — Column-review pass (NEW; required).**
Run, at minimum:
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_patient_master'
ORDER BY column_name;
```
Plus topic-keyword searches across the schema. Keyword bundles to use by default:

- **Demographics:** age, sex, race, ethnicity, bmi, weight, height, insurance, payer, marital, language, smok, alcohol, tobacco
- **Comorbidities:** diabetes, htn, hypertension, cad, chf, copd, ckd, dialysis, cancer_history, immun, anticoag, antiplatelet
- **Surgical context:** surg_year, surgeon, attending, fellow, resident, hospital, facility, los, length_of_stay, blood_loss, transfus, or_time, operative_time, approach, sternot, awake, fiberoptic
- **Anesthesia & airway:** intub, airway, difficult, awake, tracheost, asa, anesth
- **Pathology & labs:** weight, volume, ki67, brafv600e, tert, ras, ttf, calcitonin, tg, tsh, pth, calcium, vitamin_d, alk_phos
- **Follow-up & outcomes:** followup, recur, death, mortality, ed_visit, readmiss, complic
- **Manuscript-specific keywords** drawn from the master-list title/RQ.

**Step 3 — Column inventory artifact.** Write a `M0XX_column_inventory.md` (or extend the planning doc) listing each candidate column, its coverage in the cohort, and a one-line keep/skip rationale. Coverage = `COUNT(col) / COUNT(*)` and ideally also coverage in the primary-exposure subset.

**Step 4 — Cohort view migration.** The cohort view selects in (a) all kept columns from the inventory plus (b) any computed derivations (composite flags, cutoffs).

**Step 5 — Demographic Table 1 stub query.** Confirm the cohort view supports the planned Table 1 stratification before declaring the view ready.

## What "demographics" means concretely

Standard Table 1 row set for a thyroid-surgery cohort:

| Row | Source field on `canonical_patient_master` |
|---|---|
| Age at surgery, median (IQR) | `age_at_surgery` |
| Female sex, n (%) | `sex` |
| Race, n (%) by category | `race` |
| Ethnicity, n (%) | `ethnicity` (or equivalent) |
| BMI, median (IQR) — if covered | `bmi` (review coverage; report as available) |
| Smoking status — if covered | `smoking_status` (review coverage) |
| Charlson / comorbidity index — if available | search canonical_patient_master |
| Diabetes mellitus | `comorbidity_dm` or equivalent |
| Hypertension | `comorbidity_htn` or equivalent |
| Hospital site / surgeon volume bucket | `facility_id`, `surgeon_id` |
| Surgical era (e.g., 1999–2009 / 2010–2019 / 2020–2024) | derived from `surg_first_date` |
| Procedure type (total vs. hemi) | `surg_procedure_type` |
| Malignant histology, n (%) | `is_malignant` |
| Median follow-up | `followup_years` |

Variables with <30% coverage in the focal subset should still be reported as available, with a footnote on n with data.

## What "relevant column review" means concretely

The agent should never build a cohort view from the bare RQ alone. The agent should always pull `information_schema.columns` for the primary canonical tables and search by domain keywords, then make a deliberate keep/skip call on each candidate. The output of that pass becomes the inventory artifact stored alongside the planning doc.

## Scope of the rule

Applies to: every manuscript-tier cohort view (`manuscript_workspace.cohort_M0XX_*`) and every standalone analysis dataset built for a manuscript draft.

Does not apply to: registry-tier or governance-tier objects (those have their own QA gate framework).

## Trigger to revisit

If demographic columns expand on `canonical_patient_master` (e.g., insurance adds, comorbidity-index materialization), re-run the column-review pass against any in-flight manuscripts and consider re-building affected cohort views.
