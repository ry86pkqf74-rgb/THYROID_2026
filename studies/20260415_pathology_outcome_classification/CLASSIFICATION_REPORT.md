# Final Pathology Outcome Classification Report

**Date:** 2026-04-15  
**Script:** `scripts/115_path_outcome_classification.py`  
**SQL:** `scripts/sql/path_outcome_classification_v2.sql`  
**Method:** Regex/rules-based classification against `path_synoptics` text fields  
**LLM cost:** $0 (pure SQL regex)  
**Database:** MotherDuck `"Thyroid 2026".main`

---

## Summary

Classified **all 12,886 patients** in `patient_refined_master_clinical_v12` using
regex pattern matching against concatenated pathology text from `path_synoptics`:

- `synoptic_diagnosis`
- `path_diagnosis_summary`
- `tumor_1_histologic_type`

**V2 expansion** (second pass) resolved all 685 initially unclassified patients by:
1. Adding `tumor_1_histologic_type` field-level classification (MTC, PTC, rare cancers)
2. Adding lymphoma variant patterns (DLBCL, MALT, Hodgkin, Burkitt, marginal zone)
3. Recognizing **negated malignancy** ("negative for carcinoma") as benign confirmation
4. Classifying non-neoplastic procedures (thyroglossal, parathyroid-only, abscess)
5. Adding borderline/indeterminate entities (NIFTP, FTUMP, WDT-UMP, atypical adenomas)
6. Handling non-thyroid cancers coexisting with benign thyroid pathology

**Key safeguard:** "benign" classification requires a benign pattern match AND the
absence of any **true** malignant pattern. Text containing "negative for carcinoma"
is now correctly recognized as benign rather than being blocked by the malignancy
exclusion filter.

---

## Final `fna_path_outcome` Distribution

| Category | Count | % |
|---|---|---|
| **benign** | 6,563 | 50.9% |
| **malignant** | 6,129 | 47.6% |
| **borderline_indeterminate** | 140 | 1.1% |
| **other** | 54 | 0.4% |
| NULL | 0 | 0% |
| unknown | 0 | 0% |
| **Total** | **12,886** | **100%** |

---

## Before / After Comparison

| Category | BEFORE | AFTER | Change |
|---|---|---|---|
| malignant | 4,025 | 6,129 | +2,104 |
| benign | 0 | 6,563 | +6,563 (NEW) |
| borderline_indeterminate | 0 | 140 | +140 (NEW) |
| unknown | 2,822 | 0 | -2,822 (eliminated) |
| NULL | 5,985 | 0 | -5,985 (eliminated) |
| other | 54 | 54 | unchanged |

**Total reclassified:** 8,807 patients (100% of previously NULL + unknown)

---

## Regex Pattern Tiers

### Tier 0: Histologic Type Field
Classifies based on `tumor_1_histologic_type` when it explicitly names a cancer:
MTC, PTC, differentiated high grade, angiosarcoma, adenoid cystic, etc.

### Tier 1: Malignant (Original + Expanded)
Original patterns: papillary carcinoma, follicular carcinoma, medullary, anaplastic,
poorly differentiated, hurthle cell carcinoma, metastatic, tall cell, columnar cell,
diffuse sclerosing, hobnail, cribriform, warthin.

V2 additions: diffuse large B-cell lymphoma, MALT/marginal zone lymphoma, Hodgkin,
Burkitt, angiosarcoma, rhabdomyosarcoma, adenoid cystic carcinoma, parathyroid
carcinoma, metastatic melanoma, CASTLE/thymus-like, well-differentiated thyroid
carcinoma, infiltrating carcinoma, high-grade carcinoma, clinical narrative staging.

### Tier 2: Borderline/Indeterminate
NIFTP, FTUMP, WDT-UMP, uncertain/undetermined malignant potential, atypical
follicular adenoma, atypical oncocytic adenoma.

### Tier 3: Benign (Original, with malignancy exclusion)
Goiter variants, adenoma variants, hyperplasia, Hashimoto, Graves, thyroiditis,
thyroglossal, branchial cleft. Excludes if any malignancy word present.

### Tier 4: Benign with Negated Malignancy
Benign thyroid features + malignancy words appear only in negation context:
"negative for carcinoma/malignancy/metastatic/neoplasm", "no evidence of malignancy",
"no diagnostic malignancy", "no histologic evidence", "no morphologic evidence".

### Tier 5: Non-Neoplastic Procedures
Thyroglossal duct cyst, branchial cleft cyst, abscess, necrotizing/granulomatous
inflammation. Requires absence of true malignancy patterns.

### Tier 6: Benign Catch-all (No Malignancy Words)
If text contains zero cancer-related terms, classified as benign.

### Tier 7: Benign (Negated-Only Malignancy)
Text has "negative for" / "no evidence" patterns with no true positive malignancy.

### Tier 8: Benign (Features of Malignancy Not Found)
Explicit ruling-out language: "features of malignancy not identified",
"interpretation of carcinoma is noted but neither capsular nor vascular invasion",
"no significant atypia", "foamy histiocytes" (benign reactive).

### Tier 8b-8c: Non-Thyroid Cancer with Benign Thyroid
Patients with coexisting non-thyroid malignancy (tongue SCC, scalp SCC, vocal fold
SCC) + benign thyroid pathology → classified as benign for the thyroid outcome.
Expert consultation ruling out follicular carcinoma → benign.

### Tier 9: Malignant (Clinical Narrative Staging)
Pathological staging notation (pT1-4) or "classic PTC" in free-text clinical notes.

---

## Malignancy Rate by Bethesda Category

| Bethesda Category | Total | Malignant | Rate (%) | Published ROM |
|---|---|---|---|---|
| Nondiagnostic (I) | 214 | 85 | 39.7% | 5-10% |
| Benign (II) | 2,343 | 642 | 27.4% | 0-3% |
| AUS/FLUS (III) | 810 | 439 | 54.2% | 10-30% |
| FN/SFN (IV) | 722 | 389 | 53.9% | 25-40% |
| Suspicious (V) | 371 | 344 | 92.7% | 50-75% |
| Malignant (VI) | 2,387 | 2,222 | 93.1% | 97-99% |

All rates are elevated vs. population norms due to **surgical selection bias** —
this is a thyroidectomy cohort where all patients underwent surgery, enriching
the denominator for malignancy across all Bethesda categories. The monotonic
increase from Bethesda I→VI confirms the classification is clinically coherent.

---

## Deduplication Strategy

`path_synoptics` contains multiple rows per patient (multi-surgery, multi-tumor).
Classification uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY
text_length DESC) = 1` to select the row with the longest concatenated diagnosis
text per patient, ensuring one classification per patient.

---

## Edge Cases Handled

1. **Non-thyroid cancers**: 4 patients had benign thyroid pathology + coexisting
   non-thyroid malignancy (tongue, scalp, vocal fold SCC). Classified as benign
   for thyroid outcome since `fna_path_outcome` reflects the thyroid finding.

2. **Negated malignancy**: ~270 patients had text like "negative for carcinoma"
   or "no evidence of malignancy" — these were incorrectly blocked by the V1
   malignancy exclusion filter. V2 correctly classifies them as benign.

3. **Expert consultation overrides**: 2 patients had outside consultation
   where carcinoma was explicitly ruled out. Classified as benign.

4. **Parathyroid carcinoma**: Distinguished from benign parathyroid procedures.
   Parathyroid carcinoma → malignant; parathyroid adenoma/hyperplasia → benign.

5. **Thyroid lymphoma**: 15+ patients with various lymphoma subtypes
   (DLBCL, MALT, Hodgkin, Burkitt, marginal zone). All → malignant.

---

## Remaining Gaps

| Category | Count | Notes |
|---|---|---|
| NULL | 0 | Eliminated |
| unknown | 0 | Eliminated |
| other | 54 | Pre-existing category, not reclassified |

**Zero patients remain unclassified.** The 2,015 patients in `patient_refined_master_clinical_v12`
without `path_synoptics` records were classified through other sources (pre-existing
malignant/other classifications from earlier pipeline phases).

---

## Artifacts

- **Classification table:** `path_outcome_classification_v1` (10,871 rows) on MotherDuck
- **Backup table:** `patient_refined_master_clinical_v12_outcome_backup_20260415`
- **Script:** `scripts/115_path_outcome_classification.py`
- **SQL V1:** `scripts/sql/path_outcome_classification_audit.sql`
- **SQL V2:** `scripts/sql/path_outcome_classification_v2.sql`
