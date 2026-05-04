# M004 — Submission Package v1.0
## Autoimmune Thyroid Disease and Cancer Risk: A 25-Year Institutional Cohort (NLP-Augmented)

**Target journal:** Thyroid (Mary Ann Liebert) — AMA references  •  TBD per Logan  
**Cohort:** THYROID_2026 canonical publication v1.0 — n = 10,871  
**Release:** `pub_v1_1_20260504` (post-NLP-augmentation milestone; successor to `pub_v1_0_20260430` after mig_281–mig_300)  
**NLP-augmented (Option 2):** Hashimoto n = 348 (syn+NLP) · Graves n = 1,604 (syn+NLP)  
**Primary outcome:** is_malignant (binary) · n_malig = 4,019  

---

## Files

| # | File | Contents |
|---|------|----------|
| 01 | `01_title_page.docx` | Authors, affiliations, corresponding author, word/figure/table counts, financial disclosure, IRB, COI [PLACEHOLDERS — Logan to populate] |
| 02 | `02_manuscript.docx` | Full manuscript body: Abstract → Introduction → Methods → Results → Discussion → Conclusions → References |
| 03 | `03_supplement.docx` | Supplementary Methods (NLP pipeline) + Tables S1–S4 |
| 04 | `04_tables.xlsx` | Tables 1–3 + Supp S1–S4 + data dictionary + QA |
| 05b | `05b_per_patient_with_sources.xlsx` | Per-patient analytic file (n=10,871 × ~60 cols) + Source Map + data dictionary |
| 06 | `06_figures/` | Figure PNGs (300 DPI) + underlying CSVs for Figures 1–4 |
| 07 | `07_response_to_reviewers_template.docx` | Empty template for post-review response |
| 08 | `08_analysis_code/` | `M004_autoimmune_analysis.sql` + Python build scripts |
| 08b | `08_analysis_outputs/` | Raw logreg output, concordance tables, validation snapshots |
| 09 | `09_validation_report.md` | Independent verification of all numbers vs MotherDuck |

---

## Key results (NLP-augmented, n=10,871)

### Autoimmune × malignancy breakdown

| Category | n | n_malignant | % malignant |
|---|---:|---:|---:|
| Both (Hashimoto + Graves) | 52 | 21 | 40.4% |
| Hashimoto only | 348 | 153 | 44.0% |
| Graves only | 1,604 | 554 | 34.5% |
| Neither | 8,867 | 3,291 | 37.1% |

### Locked logistic regression (multivariable)

| Predictor | aOR | 95% CI | p |
|---|---:|---|---:|
| **Hashimoto** (NLP+syn combined) | **1.37** | 1.12–1.68 | **0.002** |
| **Graves** (NLP+syn combined) | **0.87** | 0.78–0.98 | **0.017** |
| Male sex | 1.58 | 1.44–1.74 | <0.0001 |
| Age (per year) | 0.99 | 0.99–1.00 | <0.0001 |

Pseudo-R² (McFadden) = 0.0093 · LR vs null χ² = 132.6 (df=4)

### NLP concordance with synoptic

| Disease | Synoptic-only | NLP-only | Both | Total |
|---|---:|---:|---:|---:|
| Hashimoto | 217 | 152 | 31 | 400 |
| Graves | 270 | 1,082 | 304 | 1,656 |

---

## Source paths

- **Cohort view:** `manuscript_workspace.cohort_m004_autoimmune_cancer_v1` (mig_298)
- **NLP rollup:** `manuscript_workspace.m004_nlp_autoimmune_rollup_v1` (mig_298)
- **SF NLP sources:** `NLP_HASHIMOTO_FULL_RESULTS_v1` · `NLP_GRAVES_FULL_RESULTS_v1` (Snowflake THYROID_VALIDATION)
- **Ready-for-writing brief:** `manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md`
- **Locked logreg:** `snowflake_trial/reports/M004_logreg_nlp_augmented_20260504.md`
- **Migration log:** `qc_framework_v1/migrations/301_m004_submission_package_20260504.sql`

---

## Outstanding [VERIFY] items for Logan

1. Authorship list and corresponding author (title page + manuscript byline)
2. IRB protocol number (Methods §2.1 + title page)
3. Final journal selection (currently formatted for Thyroid / AMA)
4. ~25 AMA references (stub list in `08_analysis_code/M004_autoimmune_analysis.sql` header comment)
5. Graves "paradox" sub-analysis: stratify by surgical indication (thyrotoxicosis vs nodule) for Discussion
6. Funding & conflict-of-interest disclosures
7. Sensitivity analyses: (a) NLP-only vs syn-only vs combined; (b) restrict Hashimoto to PTC histology

---

## Reproducibility

All numbers reproduce from `thyroid_canonical_publication_v1_0` via:

```sql
-- Run via: .venv/bin/python scripts/mig_301_m004_submission_package.py --md
SELECT * FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1;
```

Full SQL package: `08_analysis_code/M004_autoimmune_analysis.sql`  
Python pipeline: `08_analysis_code/build_m004_tables.py` · `build_m004_figures.py`
