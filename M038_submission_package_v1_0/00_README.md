# M038 README — Submission Package v1.0

## Massive Goiter at a Tertiary Referral Center — A Composite-Definition Descriptive Cohort of 2,501 Patients

**Target journal:** Surgery / Annals of Surgical Oncology / Thyroid (TBD)
**Style:** AMA references (TBD by journal)
**Release:** `pub_v1_1_20260504` (post-NLP-augmentation milestone; successor to `pub_v1_0_20260430` after mig_281–mig_300)
**Cohort:** THYROID_2026 canonical publication v1.0 (n = 10,871 total; 2,501 composite massive)

## Files

1. `01_title_page.docx` — Authors, affiliations, corresponding author, word/figure/table counts, financial disclosure, IRB statement, conflict-of-interest statement [PLACEHOLDERS — Logan to populate].
2. `02_manuscript.docx` — Full manuscript body (Abstract → References → Figures embedded), US Letter, Arial 11pt.
3. `03_supplement.docx` — Supplement with supplementary Methods, Results, and Tables S1–S6.
4. `04_tables.xlsx` — Tables 1–5 + Supp S1–S6 + Data dictionary + QA reconciliation.
5. `05b_per_patient_with_sources.xlsx` — Per-patient analytic file (n=10,871 × ~80 cols) + Source map sheet documenting every column's source DB.schema.table.column. **[PARKED — awaiting MotherDuck eras-account auth on local duckdb CLI]**
6. `06_figures/` — 4 figures (Venn, era prevalence, complications bar, component coverage); 300 DPI PNGs + underlying CSVs.
7. `07_response_to_reviewers_template.docx` — Empty template, to be populated after first review.
8. `08_analysis_code/` — `M038_descriptive_analysis.sql` + Python build scripts for tables, figures, per-patient master, manuscript docx.
9. `09_validation_report.md` — Independent verification of all 156 numeric cells vs MotherDuck (153 PASS, 3 DIFF post-Cursor patch, 0 FAIL).

## Key results

- **Composite massive flag prevalence:** 2,501 / 10,871 = 23.0% over 25 years.
- **Demographic distinction:** massive arm older (median 56 vs 50), less female (70.8% vs 79.9%), more Black or AA (62.2% vs 31.2%), more comorbid (HTN, DM ~2× prevalence), higher ASA III–IV (65.0% vs 42.7% in NSQIP-linked subset).
- **Procedure preference:** total thyroidectomy 66.9% massive vs 51.7% non-massive (post-mig_253 procedure-type completeness 100% / 99.98%).
- **Strict-definition any-complication rate:** 5.28% massive vs 3.20% non-massive (RR ≈ 1.65).
- **Hypoparathyroidism (per standing rule):** transient (<6mo) 3.32% vs 2.35% (RR ≈ 1.41); permanent (>6mo) 0.16% vs 0.14% (RR ≈ 1.12).
- **Era-stratified rise:** 12% (pre-2015) → 24.9% (2015–2019) → 28.5% (2020–2025); largely documentation-expansion driven (see Figure 4).

## Outstanding [VERIFY] items for Logan

- Authorship list and corresponding author (title page, manuscript byline, supplement attribution)
- IRB protocol number (Methods §2.1 + title page)
- Final journal selection (currently formatted for AMA references)
- ~30 AMA references (BibTeX stubs at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`)
- Confidence intervals on RR estimates (author-input gap #11; suggested method: Wald or exact for n<10)
- Funding & conflict-of-interest disclosures
- 05b per-patient master (parked on MotherDuck eras auth — 30s task once auth in)

## Reproducibility

All numbers reproduce from `thyroid_canonical_publication_v1_0` via:
- `08_analysis_code/M038_descriptive_analysis.sql` — Single SQL package reproducing every numeric cell.
- `08_analysis_code/build_m038_tables.py` — Tables Excel build script.
- `08_analysis_code/build_m038_figures.py` — Figures build script.
- `08_analysis_code/build_m038_per_patient.py` — Per-patient Excel build script (parked).

Database release: `pub_v1_1_20260504`. Most-recent applied migration: `mig_255_cohort_m038_complication_temporality_columns_20260502`. Standing-rule reference: `memory/feedback_complications_transient_vs_permanent.md`.

---

