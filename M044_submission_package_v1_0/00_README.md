# M044 — Submission Package v1.0
## Microscopic Versus Gross Extrathyroidal Extension in Differentiated Thyroid Cancer

**Target journal:** Thyroid (Mary Ann Liebert)  •  **Style:** AMA references
**Release:** `pub_v1_1_20260504` (post-NLP-augmentation milestone; successor to `pub_v1_0_20260430` after mig_281–mig_300)
**Cohort:** THYROID_2026 canonical publication v1.0 (n = 4,128)
**Strict-DTC primary subset:** n = 3,789  •  Primary 3-level analytic n = 3,756 (events 139)

## Files

1. `01_title_page.docx` — Authors, affiliations, corresponding author, word/figure/table counts, financial disclosure, IRB statement, conflict-of-interest statement [PLACEHOLDERS — Logan to populate].
2. `02_manuscript.docx` — Full manuscript body (Abstract → References → Figures embedded).
3. `03_supplement.docx` — Supplement with Tables S1–S7 and supplementary methods.
4. `04_tables.xlsx` — Tables 1–5 + Supplement S1, S6, S7 + Demographics & molecular + Data dictionary + Model outputs + QA.
5. `05_master_data.xlsx` — Per-patient analytic file (n=4,128 × 109 cols) + 6 raw source tabs + crosswalk + dictionary + QA flags.
6. `06_figures/` — High-resolution PNGs (300 DPI) and underlying CSVs for Figures 1–7.
7. `07_response_to_reviewers_template.docx` — Empty template, to be populated after first review.
8. `08_analysis_code/` — `M044_ETE_analysis.sql` + `m044_ete_fit_models.py` + `m044_make_figures.py` + `build_m044_master_excel.py`.
9. `09_validation_report.md` — Independent verification of all numbers vs MotherDuck.

## Key results (strict-DTC primary, no-RAI)

- Gross-vs-microscopic ETE: aOR 1.80 (95% CI 1.22–2.67), p = 0.003
- No/negative-vs-microscopic ETE: aOR 0.52 (95% CI 0.22–1.23), p = 0.14
- Cox HR (gross-vs-microscopic, surg-date known + FU>0, n=2,025): HR 2.34 (1.35–4.06), p = 0.003
- ETE × N stage interaction: LR p = 0.391 (NS)
- Stratified Gross-vs-Microscopic OR within N1a: 1.95 (1.21–3.15), p = 0.006

## Outstanding [VERIFY] items for Logan

- Authorship list and corresponding author
- IRB protocol number
- 35+ AMA reference DOIs (run Zotero against the Elicit literature report)
- Final journal selection (currently formatted for Thyroid / AMA)

## Reproducibility

All numbers reproduce from `thyroid_canonical_publication_v1_0` via the SQL package
`M044_ETE_analysis.sql` and the Python pipelines in `08_analysis_code/`.
