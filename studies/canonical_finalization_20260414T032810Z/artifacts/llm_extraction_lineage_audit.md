# LLM extraction lineage audit — 2026-04-14T03:30Z

**Source:** Live MotherDuck `main.canonical_extracted_fact_long_v2`  
**Total facts:** 55,500  
**Total domains:** 23  
**Extraction runs referenced:** 23 (one per domain; all from single run)

## Domain inventory

| Domain | Facts | Patients | Runs | Linked |
|--------|------:|--------:|-----:|--------|
| pathology | 10,867 | 2,219 | 1 | 100% |
| survival_followup | 9,806 | 2,982 | 1 | 100% |
| imaging | 8,403 | 1,755 | 1 | 100% |
| vascular_invasion | 4,241 | 998 | 1 | 100% |
| past_surgical_hx | 3,918 | 1,877 | 1 | 100% |
| rai_detailed | 3,744 | 650 | 1 | 100% |
| functional_outcomes | 3,289 | 1,826 | 1 | 100% |
| airway_invasion | 3,076 | 1,448 | 1 | 100% |
| labs | 2,447 | 841 | 1 | 100% |
| physical_exam | 1,919 | 659 | 1 | 100% |
| past_medical_hx | 865 | 295 | 1 | 100% |
| patient_decision_adherence | 640 | 397 | 1 | 100% |
| rad_treatment | 518 | 187 | 1 | 100% |
| frozen_section_detail | 375 | 303 | 1 | 100% |
| recurrence | 281 | 137 | 1 | 100% |
| presenting_symptoms | 279 | 120 | 1 | 100% |
| parathyroid_detail | 248 | 117 | 1 | 100% |
| tirads_granular | 176 | 44 | 1 | 100% |
| tg_kinetics | 167 | 58 | 1 | 100% |
| cervical_ln_detail | 104 | 48 | 1 | 100% |
| dynamic_risk_response | 52 | 24 | 1 | 100% |
| us_nodule_dynamics | 47 | 18 | 1 | 100% |
| synoptic_pathology_enrichment | 38 | 8 | 1 | 100% |

## Unresolved gaps

- **0 facts with NULL extraction_run_id** — all facts are linked to an extraction run
- **3 orphan runs in `note_extraction_runs`** — runs exist in the orchestration log with no matching facts (likely failed/superseded)
- All 23 canonical domains have 100% `source_file_id` and `fact_domain` coverage
