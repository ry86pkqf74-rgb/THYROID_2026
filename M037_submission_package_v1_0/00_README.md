# M037 — Submission Package v1.0
## Predictors of Lymph Node Metastasis in Differentiated Thyroid Cancer

**Target journal:** TBD (Thyroid / JCEM / Surgery) • **Style:** AMA references  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)  
**Cohort view:** `manuscript_workspace.cohort_m037_ln_metastasis_v1` (malignant; LN examined > 0 or LN-positive flag)  
**Augmentation:** `mig_286` — NLP family history + `family_syndrome_flag` on CPM joined into the view.

## Files

| # | Path | Purpose |
|---|------|---------|
| 1 | `01_title_page.docx` | Placeholder from M044 template — replace with M037 authorship / IRB / disclosures. |
| 2 | `02_manuscript.docx` | Placeholder — paste final M037 draft or regenerate via Manuscript MD + pandoc. |
| 3 | `03_supplement.docx` | Placeholder — supplementary tables / methods. |
| 4 | `04_tables.xlsx` | Tables 1–5 + Supp sheets (from `build_m037_tables.py`). |
| 5 | `05_master_data.xlsx` | Per-patient analytic extract + dictionary. |
| 6 | `06_figures/` | PNGs (300 DPI) + CSVs — from `build_m037_figures.py`. |
| 7 | `07_response_to_reviewers_template.docx` | Post–peer review. |
| 8 | `08_analysis_code/` | `M037_ln_predictors_analysis.sql` + Python builders. |
| 9 | `09_validation_report.md` | Reconciliation vs live MotherDuck. |
| — | `CLOSEOUT_NOTES.md` | Git + migration signoff checklist. |

## Primary adjusted model (Cowork lock, post–`mig_286`)

Multivariable logistic regression; outcome **LN-positive** (AJCC-8 N1a/N1b vs N0/Nx). Covariates: sex, age, tumor size (cm), NLP thyroid family history (`pmhx_nlp_family_hx_thyroid`, missing → false).

**Headline:** Thyroid cancer family history **not** independently associated (aOR 1.05, 95% CI 0.74–1.51, p = 0.77).  
**Significant:** male sex OR 1.81; age OR 0.98/year; tumor size OR 1.18/cm (see `08_analysis_outputs/` after rebuild).

## Reproducibility

```bash
cd /path/to/THYROID_2026
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_tables.py
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_figures.py
.venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_manuscript_md.py
```

Requires MotherDuck RW token (`motherduck.local.toml` or env). SQL excerpts for manual re-run: `M037_ln_predictors_analysis.sql`.

## Migration

Apply `qc_framework_v1/migrations/291_m037_submission_package_20260504.sql` after package QA passes (inserts `signoff_migration.mig_291`).
