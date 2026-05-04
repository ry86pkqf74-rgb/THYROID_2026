# M032 Submission Package — Closeout Notes
**mig_290 | 2026-05-04 | Cursor Composer**

---

## Status: READY FOR WRITING

The M032 submission package v1.0 is complete and ready for PI review and manuscript writing.

---

## What was built

| Component | Status |
|---|---|
| Directory structure (mirrors M044/M038) | ✓ |
| SQL reproducibility package | ✓ |
| build_m032_tables.py → 04_tables.xlsx + 05_master_data.xlsx | ✓ |
| build_m032_figures.py → 4 × 300 DPI PNG + CSV data | ✓ |
| build_m032_manuscript_md.py → live number sheet | ✓ |
| 01_title_page.docx (placeholders) | ✓ |
| 02_manuscript.docx (from draft v1.1 + mig_290 number refresh) | ✓ |
| 03_supplement.docx (Tables S1-S3 + Supp Methods) | ✓ |
| 07_response_to_reviewers_template.docx | ✓ |
| 09_validation_report.md (9/9 QA PASS) | ✓ |
| qc_framework_v1/migrations/290_m032_submission_package_20260504.sql | ✓ |
| signoff_migration entry for mig_290 | ✓ |

---

## Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-M032-READY-FOR-WRITING | **CLOSED** | Package complete; writing chat can begin |
| CF-M032-LOGREG-MULTIVARIABLE | **OPEN** | If manuscript expands to multivariable (era × outcomes), open new Cowork session |
| CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE | **OPEN** | Smoking/family-hx 72.2% unknown; dedicated NLP extraction pass needed for full coverage |

---

## Author action items before submission

1. **Authorship** — confirm author list, order, affiliations, corresponding author
2. **IRB** — insert Emory IRB protocol number in 01_title_page.docx
3. **Journal selection** — Annals of Surgical Oncology, Surgery, or Thyroid
4. **n_malig finalization** — live value 4,019 (vs Cowork lock 4,018); use 4,019 in final manuscript
5. **Temporal-trend analyses** — if required by reviewers: stratified by 5-year era from Table 2/3/4 (already in 04_tables.xlsx)
6. **Median follow-up** — current draft says "~7 years"; compute exact value from cohort_m032 view
7. **References** — expand BibTeX stubs at `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib`
8. **Figure 1** — update n_malig from 4,018 to 4,019 in CONSORT box (minor)
9. **Word count** — compute after editorial pass for target journal compliance

---

## Rebuild if numbers drift

```bash
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_tables.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_figures.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_manuscript_md.py
```

---

## Cross-references

- Prior draft: `manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`
- Table 1 with NLP: `snowflake_trial/reports/M032_table1_with_nlp_20260504.md`
- Cowork brief: `manuscript_outputs/v1_0_20260501/M032_READY_FOR_WRITING_BRIEF.md`
- Migration SQL: `qc_framework_v1/migrations/290_m032_submission_package_20260504.sql`
- M044 pattern: `M044_submission_package_v1_0/`
- M038 pattern: `M038_submission_package_v1_0/`

---

*mig_290 CLOSED | 2026-05-04*
