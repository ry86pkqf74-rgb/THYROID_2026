# Cohort build

**Primary cohort:** Preoperative imaging-defined index nodule 2.0–4.0 cm (`imaging_nodule_long_v2.size_cm_max`, exam on/before index surgery). Rationale: reflects size available at surgical decision time.

**Sensitivity cohort:** Pathology-defined size 2.0–4.0 cm from `surgery_pathology_linkage_v3.path_size_cm` with fallbacks documented in `cohort_logic.py`.

**Primary analytic N (strict nodal):** 558
**Broad nodal exclusion preop cohort N:** 635 (see `patient_level_dataset_broad_nodal_exclusion.csv`).
**Pathology-defined sensitivity N (strict LN exclusion):** 0
