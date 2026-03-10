# QA Reconciliation Report

Generated against `thyroid_master.duckdb`

| Category | Manual | DB | Diff | % Diff | Status |
|---|---:|---:|---:|---:|---|
| PTC (all) | 3,000 | 3,278 | +278 | 9.3% | 🟡 close |
| FTC | 500 | 499 | -1 | 0.2% | 🟢 match |
| MTC | 155 | 161 | +6 | 3.9% | 🟢 match |
| ATC | 15 | 24 | +9 | 60.0% | 🔴 investigate |
| Follicular adenoma | 925 | 952 | +27 | 2.9% | 🟢 match |
| Hurthle adenoma | 266 | 270 | +4 | 1.5% | 🟢 match |
| MNG | 6,000 | 6,381 | +381 | 6.3% | 🟡 close |
| Graves | 589 | 625 | +36 | 6.1% | 🟡 close |
| Hashimoto | 2,168 | 2,168 | +0 | 0.0% | 🟢 match |
| Parathyroid tissue mentioned | 3,332 | 3,873 | +541 | 16.2% | 🔴 investigate |
| Parathyroid adenoma (strict) | 94 | 98 | +4 | 4.3% | 🟢 match |
| TGDC | 219 | 205 | -14 | 6.4% | 🟡 close |
| Hyalinizing trabecular | 9 | 9 | +0 | 0.0% | 🟢 match |

## Status Legend

- 🟢 **match** — within 5% of manual count
- 🟡 **close** — within 15% (review rounding or inclusion criteria)
- 🔴 **investigate** — >15% discrepancy, needs manual review

## Notes

- **PTC** DB count (3,278) is ~9% above manual (~3,000). The DB includes all PTC variants; manual may reference classic-only.
- **Parathyroid adenoma** is defined strictly as `parathyroid_abnormality = 'adenoma' AND removal_intent = 'intentional'` (288 patients). The user's manual count of 94 may use a narrower clinical definition. Consider adding note_intent_inferred = 'intentional' for further filtering.
- **TGDC** sourced from `thyroid_sizes.final_path_diagnosis_original` ILIKE '%thyroglossal%' (210 patients). The benign_pathology table does not contain a dedicated TGDC flag.
- **Graves** DB count 625 vs manual 589 — likely includes borderline cases.

## Standardized Columns Added

| Table | Column | Type | Logic |
|---|---|---|---|
| tumor_pathology | `variant_standardized` | VARCHAR | Maps histology_variant to clean categories |
| tumor_pathology | `surgery_type_normalized` | VARCHAR | Normalizes procedure label casing/aliases |
| benign_pathology | `is_mng` | BOOLEAN | multinodular_goiter = True |
| benign_pathology | `is_graves` | BOOLEAN | graves_disease = True |
| benign_pathology | `is_follicular_adenoma` | BOOLEAN | follicular_adenoma = True |
| benign_pathology | `is_hurthle_adenoma` | BOOLEAN | hurthle_adenoma = True |
| benign_pathology | `is_hashimoto` | BOOLEAN | hashimoto_thyroiditis = True |
| benign_pathology | `is_hyalinizing_trabecular` | BOOLEAN | hyalinizing_trabecular = True |
| benign_pathology | `is_tgdc` | BOOLEAN | research_id in thyroid_sizes with thyroglossal dx |
| benign_pathology | `surgery_type_normalized` | VARCHAR | Normalizes surgery_type casing/aliases |
| parathyroid | `is_parathyroid_adenoma` | BOOLEAN | abnormality='adenoma' AND intent='intentional' |

## Clinical Notes Long (Phase 7)

Source: `raw/Notes 12_1_25.xlsx` (Sheet1 + Sheet2)

| Metric | Value |
|--------|------:|
| Total note rows | 11,037 |
| Unique patients | 5,641 |
| Note types | 8 |

### Row counts by note_type

| Note type | Rows | Patients | Avg chars | Max chars |
|-----------|-----:|--------:|---------:|---------:|
| h_p | 4,221 | 4,009 | 6,592 | 32,767 |
| op_note | 4,680 | 4,450 | 4,803 | 16,396 |
| other_history | 525 | 525 | 2,228 | 15,963 |
| endocrine_note | 519 | 519 | 6,527 | 32,772 |
| ed_note | 498 | 497 | 814 | 32,767 |
| history_summary | 249 | 249 | 981 | 6,360 |
| dc_sum | 185 | 184 | 5,283 | 29,794 |
| other_notes | 160 | 160 | 1,944 | 19,503 |

## Entity Extraction Results (Phase 7)

| Entity table | Rows | Patients | Present | Negated |
|-------------|-----:|--------:|-------:|-------:|
| note_entities_procedures | 21,942 | 4,723 | 21,691 | 251 |
| note_entities_problem_list | 11,579 | 4,037 | 10,733 | 846 |
| note_entities_complications | 9,359 | 2,840 | 9,064 | 295 |
| note_entities_medications | 7,501 | 2,070 | 7,059 | 442 |
| note_entities_staging | 3,807 | 1,639 | 3,758 | 49 |
| note_entities_genetics | 1,738 | 605 | 1,600 | 138 |
| **Total** | **55,926** | **5,186** | | |

### Top 20 extracted problems (counts)

| Problem | Count |
|---------|------:|
| hypothyroidism | 2,284 |
| hypertension | 2,032 |
| hyperthyroidism | 1,715 |
| diabetes | 1,595 |
| obesity | 587 |
| depression | 508 |
| GERD | 506 |
| diabetes_type2 | 504 |
| asthma | 489 |
| CAD | 485 |
| COPD | 374 |
| CKD | 290 |
| atrial_fibrillation | 186 |
| breast_cancer | 113 |
| lung_cancer | 65 |

### Top 20 extracted procedures (counts)

| Procedure | Count |
|-----------|------:|
| hemithyroidectomy | 9,389 |
| total_thyroidectomy | 8,660 |
| central_neck_dissection | 1,012 |
| completion_thyroidectomy | 988 |
| tracheostomy | 626 |
| lateral_neck_dissection | 529 |
| laryngoscopy | 448 |
| parathyroid_autotransplant | 188 |
| modified_radical_neck_dissection | 102 |

### Top 20 extracted complications (counts)

| Complication | Count |
|-------------|------:|
| chyle_leak | 3,023 |
| hypocalcemia | 2,806 |
| seroma | 1,353 |
| rln_injury | 975 |
| hypoparathyroidism | 550 |
| vocal_cord_paralysis | 323 |
| hematoma | 224 |
| wound_infection | 65 |
| vocal_cord_paresis | 40 |

## Final Publication & Trial Exit Report (Script 14 - 20260310_0414)

**Executed:** 2026-03-10 04:14

- New MVs validated
- Publication bundle created: `exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/`
- Local DuckDB backup created (trial-downgrade safety)
- Streamlit dashboard tabs benchmarked (sub-second)
- Ready for MotherDuck free-tier downgrade

**Publication folder:** `exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/`
