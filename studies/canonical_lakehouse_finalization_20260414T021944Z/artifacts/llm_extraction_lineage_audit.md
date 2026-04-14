# LLM Extraction Lineage Audit

**Generated:** 2026-04-14 (canonical lakehouse finalization pass)

## Summary

- **Total canonical facts:** 55,500
- **Lineage completeness:**
  - canonical_domain: 55,500 / 55,500 (100.0%)
  - source_file_id: 0 / 55,500 (0.0%)
  - extraction_run_id: 55,500 / 55,500 (100.0%)
  - extraction_method: 55,500 / 55,500 (100.0%)
  - extracted_at timestamp: 55,500 / 55,500 (100.0%)
  - fact_id: 55,500 / 55,500 (100.0%)
- **Extraction run batches:** 23
- **Source domains:** 23
- **note_extraction_runs table entries:** 3

## Verdict

INCOMPLETE lineage.

## Domain coverage

| Domain | Facts | Patients | Runs |
|--------|------:|--------:|-----:|
| pathology | 10,867 | 2,219 | 1 |
| survival_followup | 9,806 | 2,982 | 1 |
| imaging | 8,403 | 1,755 | 1 |
| vascular_invasion | 4,241 | 998 | 1 |
| past_surgical_hx | 3,918 | 1,877 | 1 |
| rai_detailed | 3,744 | 650 | 1 |
| functional_outcomes | 3,289 | 1,826 | 1 |
| airway_invasion | 3,076 | 1,448 | 1 |
| labs | 2,447 | 841 | 1 |
| physical_exam | 1,919 | 659 | 1 |
| past_medical_hx | 865 | 295 | 1 |
| patient_decision_adherence | 640 | 397 | 1 |
| rad_treatment | 518 | 187 | 1 |
| frozen_section_detail | 375 | 303 | 1 |
| recurrence | 281 | 137 | 1 |
| presenting_symptoms | 279 | 120 | 1 |
| parathyroid_detail | 248 | 117 | 1 |
| tirads_granular | 176 | 44 | 1 |
| tg_kinetics | 167 | 58 | 1 |
| cervical_ln_detail | 104 | 48 | 1 |
| dynamic_risk_response | 52 | 24 | 1 |
| us_nodule_dynamics | 47 | 18 | 1 |
| synoptic_pathology_enrichment | 38 | 8 | 1 |

## Files

- `llm_extraction_lineage_audit.csv` — per-extraction-run-id fact counts
- `llm_extraction_domain_coverage.csv` — per-domain coverage
- `note_extraction_runs_full.csv` — full note_extraction_runs table dump
