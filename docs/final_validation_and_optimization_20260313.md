# Final Validation & Optimization Report

**Date:** 2026-03-13

---

## MotherDuck ANALYZE Results

17 key tables analyzed for query optimization:

| Table | Rows | Status |
|-------|------|--------|
| manuscript_cohort_v1 | 10,871 | ANALYZED |
| patient_analysis_resolved_v1 | 10,871 | ANALYZED |
| episode_analysis_resolved_v1_dedup | 9,368 | ANALYZED |
| lesion_analysis_resolved_v1 | 11,851 | ANALYZED |
| operative_episode_detail_v2 | 9,371 | ANALYZED |
| imaging_nodule_master_v1 | 19,891 | ANALYZED |
| rai_treatment_episode_v2 | 1,857 | ANALYZED |
| molecular_test_episode_v2 | 10,126 | ANALYZED |
| tumor_episode_master_v2 | 11,691 | ANALYZED |
| survival_cohort_enriched | 61,134 | ANALYZED |
| patient_refined_master_clinical_v12 | 12,886 | ANALYZED |
| thyroid_scoring_py_v1 | 10,871 | ANALYZED |
| complication_phenotype_v1 | 5,928 | ANALYZED |
| longitudinal_lab_canonical_v1 | 39,961 | ANALYZED |
| extracted_recurrence_refined_v1 | 10,871 | ANALYZED |
| provenance_enriched_events_v1 | 50,297 | ANALYZED |
| demographics_harmonized_v2 | 11,673 | ANALYZED |

---

## Validation Check Results

### Patient-Level Deduplication

| Table | Duplicates | Status |
|-------|-----------|--------|
| manuscript_cohort_v1 | 0 | PASS |
| patient_analysis_resolved_v1 | 0 | PASS |
| patient_refined_master_clinical_v12 | 2,015 | KNOWN (multi-pathology) |
| episode_analysis_resolved_v1_dedup | 0 | PASS |

### Scoring System Calculability

| System | Calculable % | Among All 10,871 |
|--------|-------------|------------------|
| AJCC 8th Edition | 37.6% | 4,087 patients |
| ATA 2015 Risk | 28.9% | 3,142 patients |
| MACIS | 37.5% | 4,076 patients |
| AGES | 100.0% | 10,871 patients |
| AMES | 100.0% | 10,871 patients |

### Linkage Completeness

| Linkage | Coverage |
|---------|----------|
| FNA → Molecular | 100% (708/708) |
| Preop → Surgery | 100% (3,591/3,591) |
| Surgery → Pathology | 100% (9,409/9,409) |
| Pathology → RAI | 100% (23/23) |
| Imaging → FNA | 0% (rebuilt as 9,024 rows separately) |

### Manuscript Cohort Key Field Coverage

| Field | Non-NULL | Coverage |
|-------|----------|---------|
| age_at_surgery | 10,871 | 100% |
| sex | 10,871 | 100% |
| race | 10,862 | 99.9% |
| surg_first_date | 8,731 | 80.3% |
| ete_grade_final | 4,075 | 37.5% |
| braf_positive_final | 10,871 | 100% (boolean) |
| tert_positive_final | 10,871 | 100% (boolean) |
| analysis_eligible_flag | 4,136 | 38.0% eligible |

---

## New Tables Created This Session

| Table | Rows | Purpose |
|-------|------|---------|
| streamlit_patient_conflicts_v | 1,015 | Dashboard patient conflicts |
| streamlit_patient_manual_review_v | 7,552 | Dashboard manual review queue |
| adjudication_progress_summary_v | 0 | Dashboard adjudication progress |

---

## Validation Artifacts

- `exports/final_validation_and_optimization_20260313/validation_results.json`
- This document
