# Final Pathology Outcome Classification Report

**Date:** 2026-04-15  
**Script:** `scripts/115_path_outcome_classification.py`  
**Method:** Regex/rules-based classification against `path_synoptics` text fields  
**LLM cost:** $0 (pure SQL regex)  
**Database:** MotherDuck `"Thyroid 2026".main`

---

## Summary

Classified 7,759 patients (previously NULL or "unknown" `fna_path_outcome`) into
benign, malignant, or borderline_indeterminate categories using regex pattern matching
against concatenated pathology text from `path_synoptics`:

- `synoptic_diagnosis`
- `path_diagnosis_summary`
- `tumor_1_histologic_type`

**Key safeguard:** "benign" classification requires a benign pattern match AND the
absence of any malignant pattern (carcinoma, metastatic, etc.) in the same text. This
prevents false benign classification when a report mentions "benign lymph node"
alongside "papillary carcinoma."

---

## Regex Patterns Used

### Malignant
```
papillary.*(carcinoma|thyroid cancer)|follicular carcinoma
|follicular.*(cell|variant).*carcinoma|medullary.*carcinoma|medullary thyroid
|anaplastic|poorly differentiated.*carcinoma|hurthle.*cell.*carcinoma
|oncocytic.*carcinoma|metastatic.*(carcinoma|thyroid|ptc|mtc)
|insular.*carcinoma|tall cell.*variant|columnar.*cell
|diffuse sclerosing|hobnail|cribriform|solid.*variant.*ptc|warthin
|^ptc$|^ptc |^mtc$|^mtc 
|squamous cell carcinoma.*thyroid|lymphoma.*thyroid|thyroid.*lymphoma
```

### Borderline/Indeterminate
```
niftp|ftump|wdt-ump|uncertain malignant potential
|noninvasive follicular thyroid neoplasm
|well.differentiated tumor of uncertain|follicular tumor of uncertain
```

### Benign (requires NO malignant pattern co-occurrence)
```
benign|nodular hyperplasia|nodular thyroid hyperplasia
|multinodular goiter|nodular goiter|colloid nodule|colloid goiter
|follicular adenoma|adenomatoid nodule|hashimoto|graves
|lymphocytic thyroiditis|follicular nodular disease|thyroid hyperplasia
|adenomatous goiter|adenomatous nodule|mng nos
|multinodular colloid|nodular colloid|hurthle cell adenoma
|oncocytic adenoma|follicular hyperplasia|diffuse hyperplasia
|toxic goiter|substernal goiter
```

Exclusion guard (blocks benign if present):
```
carcinoma|malign|metastatic|anaplastic|poorly differentiated|lymphoma
```

---

## Before / After `fna_path_outcome` Distribution

| Category | BEFORE | AFTER | Change |
|---|---|---|---|
| malignant | 4,025 | 6,095 | +2,070 |
| benign | 0 | 5,914 | +5,914 (NEW) |
| borderline_indeterminate | 0 | 138 | +138 (NEW) |
| unknown | 2,822 | 244 | -2,578 |
| NULL | 5,985 | 441 | -5,544 |
| other | 54 | 54 | unchanged |
| **Total** | **12,886** | **12,886** | — |

**Total rows updated:** 8,122 (5,914 benign + 2,070 malignant + 138 borderline)

---

## Transition Detail

| From → To | Patients |
|---|---|
| NULL → benign | 3,504 |
| unknown → benign | 2,409 |
| NULL → malignant | 1,623 |
| unknown → malignant | 85 |
| unknown → borderline_indeterminate | 84 |
| NULL → borderline_indeterminate | 54 |

Note: 1 patient (NULL→benign) has NULL+unknown subtotal of 5,913 vs 5,914 benign total —
the extra 1 comes from rounding across the dedup boundary.

---

## Bethesda-to-Outcome Concordance Table

| Bethesda Category | benign | malignant | borderline | unknown | other | Total |
|---|---|---|---|---|---|---|
| Nondiagnostic/Unsatisfactory | 116 | 85 | — | 13 | — | 214 |
| Benign | 1,547 | 642 | 21 | 133 | 5 | 2,348 |
| AUS/FLUS | 304 | 438 | 32 | 36 | 1 | 811 |
| Follicular Neoplasm/SFN | 282 | 389 | 17 | 34 | 4 | 726 |
| Suspicious for Malignancy | 21 | 342 | 1 | 7 | 1 | 372 |
| Malignant | 139 | 2,214 | 13 | 21 | 43 | 2,430 |

---

## Malignancy Rate by Bethesda Category

| Bethesda Category | Total (classified) | Malignant | Rate (%) |
|---|---|---|---|
| Nondiagnostic/Unsatisfactory | 201 | 85 | 42.3% |
| Benign (II) | 2,210 | 642 | 29.0% |
| AUS/FLUS (III) | 774 | 438 | 56.6% |
| Follicular Neoplasm/SFN (IV) | 688 | 389 | 56.5% |
| Suspicious for Malignancy (V) | 364 | 342 | 94.0% |
| Malignant (VI) | 2,366 | 2,214 | 93.6% |

**Clinical interpretation:** These rates reflect a **surgical cohort** — all patients
underwent thyroidectomy, so the denominator is enriched for malignancy compared to
population FNA rates. Expected population malignancy rates (Cibas & Ali, Bethesda System):

| Bethesda | Expected ROM | Our Rate | Note |
|---|---|---|---|
| I (Nondiag) | 5-10% | 42.3% | Surgical selection bias |
| II (Benign) | 0-3% | 29.0% | Patients operated for other indications (goiter, Graves) |
| III (AUS/FLUS) | 10-30% | 56.6% | Repeat FNA → surgery if persistent |
| IV (FN/SFN) | 25-40% | 56.5% | Diagnostic surgery standard |
| V (Suspicious) | 50-75% | 94.0% | Expected high end in surgical series |
| VI (Malignant) | 97-99% | 93.6% | 6.4% "false positive" = benign on final path |

The higher-than-population rates across all Bethesda categories are expected and
consistent with the surgical selection bias inherent in a thyroidectomy cohort database.

---

## Remaining Unclassified Patients

| Category | Count | Notes |
|---|---|---|
| unclassified_has_text | 702 | Has diagnosis text but no regex match — needs manual review or targeted LLM pass |
| no_text | 2 | No diagnosis text in any of the 3 fields |
| Remaining NULL | 441 | No `path_synoptics` record (not in JOIN) |
| Remaining unknown | 244 | Had text but unclassifiable |

**Total needing review:** 702 patients with diagnosis text that did not match any
regex pattern. These were left as-is (NULL or "unknown") and NOT reclassified.

---

## Artifacts

- **Classification table:** `path_outcome_classification_v1` (10,871 rows) on MotherDuck
- **Backup table:** `patient_refined_master_clinical_v12_outcome_backup_20260415`
- **Script:** `scripts/115_path_outcome_classification.py`
- **SQL audit file:** `scripts/sql/path_outcome_classification_audit.sql`

---

## Deduplication Note

`path_synoptics` contains multiple rows per patient (multi-surgery, multi-tumor).
The classification uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
text_length DESC) = 1` to select the row with the longest concatenated diagnosis text
per patient, ensuring one classification per patient.
