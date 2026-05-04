# M025 — Submission Package v1.0
## ACR TI-RADS diagnostic performance vs pathologic malignancy (operative cohort)

**Target journal:** TBD (radiology / endocrine imaging) • **Style:** AMA references  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)  
**Cohort view:** `manuscript_workspace.cohort_m025_tirads_performance_v1`  
**Augmentation:** `mig_288` — join `canonical_patient_master.tirads_resolved` (TR1–TR5 enum) alongside cohort TIRADS fields.

## Files

| # | Path | Purpose |
|---|------|---------|
| 1 | `01_title_page.docx` | Placeholder — replace with authorship / IRB / disclosures. |
| 2 | `02_manuscript.docx` | Placeholder — paste final M025 draft or export from Markdown. |
| 3 | `03_supplement.docx` | Placeholder — supplementary analyses / ROC / literature benchmarks. |
| 4 | `04_tables.xlsx` | Tables 1–4 + Supp S1–S2 (from `build_m025_tables.py`). |
| 5 | `05_master_data.xlsx` | Per-patient analytic extract + run snapshot + dictionary stub. |
| 6 | `06_figures/` | PNGs (300 DPI) + CSV sidecars — from `build_m025_figures.py`. |
| 7 | `07_response_to_reviewers_template.docx` | Post–peer review. |
| 8 | `08_analysis_code/` | `M025_tirads_analysis.sql` + Python builders. |
| 9 | `09_validation_report.md` | Reconciliation checklist vs live cohort + tirads counts. |
| — | `CLOSEOUT_NOTES.md` | Git + migration signoff checklist. |

## Headline content (Cowork baseline)

Operational diagnostic performance varies by TI-RADS threshold; **operative-cohort malignancy enrichment** exceeds ACR-published expected ROM at every TI-RADS category — manuscript Methods must carry the operative-bias caveat (see `snowflake_trial/reports/m025_tirads_performance.md`).

## Reproducibility

From repo root (requires MotherDuck RW token via `motherduck.local.toml` or env):

```bash
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_tables.py
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_figures.py
.venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_manuscript_md.py
```

SQL excerpts for manual QA: `M025_tirads_analysis.sql`.

## Migration

Apply `qc_framework_v1/migrations/292_m025_submission_package_20260504.sql` after package QA passes (inserts `signoff_migration.mig_292`; idempotent `WHERE NOT EXISTS`).
