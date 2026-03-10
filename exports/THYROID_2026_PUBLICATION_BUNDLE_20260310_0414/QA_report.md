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
