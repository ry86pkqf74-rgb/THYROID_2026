# M037 — validation report (submission package v1.0)

**Package:** `M037_submission_package_v1_0/`  
**DB:** `thyroid_canonical_publication_v1_0`  
**Cohort view:** `manuscript_workspace.cohort_m037_ln_metastasis_v1`  
**Build UTC:** 2026-05-04 (see `08_analysis_outputs/m037_run_snapshot.json`)

## Live build metrics

| Metric | Value |
|--------|------:|
| Cohort n (view × CPM join) | 2,234 |
| LN+ (AJCC N1+) n / % | 1,124 / 50.31% |
| Primary 4-var model complete-case n | 2,148 |
| McFadden pseudo-R² | 0.040 |

**Cowork lock check:** complete-case n target **2,147** → live **2,148** (Δ 1 ✅).

## Primary model vs Cowork reference (`M037_logreg_family_hx_20260504.md`)

| Term | Cowork OR (95% CI) | Package OR (95% CI) |
|------|---------------------|---------------------|
| Male sex | 1.812 (1.485–2.210) | 1.815 (1.488–2.214) |
| Family hx (fhx) | 1.055 (0.737–1.511) | 1.040 (0.727–1.487) |
| Age / year | 0.982 (0.977–0.988) | 0.982 (0.977–0.988) |
| Tumor size / cm | 1.177 (1.119–1.237) | 1.176 (1.118–1.237) |

All within rounding / one-digit cohort drift.

## Automated QA tab

See `04_tables.xlsx` → sheet **QA**. `primary_complete_n_approx_cowork` should register **PASS** (1,800 ≤ n ≤ 2,400).

## Figures (300 DPI)

- `06_figures/m037_fig1_cohort_flow.png` + `_data.csv`
- `06_figures/m037_fig2_ln_rate_by_size.png` + `_data.csv`
- `06_figures/m037_fig3_forest_primary.png` + `_data.csv`
- `06_figures/m037_fig4_ln_rate_by_ln_examined_quartiles.png` + `_data.csv`

## Migration signoff

- File: `qc_framework_v1/migrations/291_m037_submission_package_20260504.sql` (idempotent `INSERT … WHERE NOT EXISTS`).
- Apply log: `scripts/output/mig_291_apply_log.txt`
- **Applied:** 2026-05-04 (see log / `signoff_migration` row `mig_291`).

## Docx placeholders

`01_title_page.docx`, `02_manuscript.docx`, `03_supplement.docx`, `07_response_to_reviewers_template.docx` were copied from **M044** templates — replace body copy with M037-specific text before submission.
