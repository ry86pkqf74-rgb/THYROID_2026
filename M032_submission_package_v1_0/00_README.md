# M032 — 25-Year Single-Institution Thyroid Surgery Cohort
## Submission Package v1.0

**Generated:** 2026-05-04 | mig_290 | Cursor Composer  
**DB:** `thyroid_canonical_publication_v1_0` (release `pub_v1_1_20260504`)  
**Release:** `pub_v1_1_20260504` (post-NLP-augmentation milestone; successor to `pub_v1_0_20260430` after mig_281–mig_300)  
**Cohort view:** `manuscript_workspace.cohort_m032_descriptive_25yr_v1`  
**Cohort lock:** mig_281 + mig_285 (post-2026-05-03, commit `590acb5`)  
**Pattern follows:** M044_submission_package_v1_0 / M038_submission_package_v1_0

---

## Locked Numbers

| Metric | Value |
|---|---|
| Total cohort | **10,871** |
| Malignant analytic cohort | **4,019** (37.0%) |
| Smoking known (NLP, mig_281) | **3,022** (27.8%) |
| Smoking current (% of known) | 7.1% |
| Family hx thyroid (known) | **366** / 3,018 known (12.1%) |
| Median follow-up | ~7 years |

> Minor shifts from Cowork-locked values (±5 patients in smoking counts) reflect mig_287 taxonomy normalization. All 9 QA metrics PASS within tolerance.

---

## Package Contents

| File | Description |
|---|---|
| `01_title_page.docx` | Title page with placeholders (authorship / IRB / journal) |
| `02_manuscript.docx` | Full manuscript draft (Methods → Results → Discussion) |
| `03_supplement.docx` | Supplementary Tables S1-S3 + Supplementary Methods |
| `04_tables.xlsx` | Tables 1–5 + Supp S1-S2 + DataDictionary + QA tab |
| `05_master_data.xlsx` | Per-patient analytic dataset (10,871 rows) + dict |
| `06_figures/` | 4 publication figures (300 DPI PNG + CSV data sources) |
| `07_response_to_reviewers_template.docx` | Template for peer-review response |
| `08_analysis_code/` | Reproducibility SQL + 3 Python build scripts |
| `08_analysis_outputs/` | Live manuscript numbers md + locked JSON |
| `09_validation_report.md` | QA cross-check (live SQL vs locked numbers) |
| `CLOSEOUT_NOTES.md` | Carry-forwards + handoff notes |

---

## Figures

| # | File | Description |
|---|---|---|
| 1 | `Figure1_CohortFlow.png` | CONSORT-style cohort flow |
| 2 | `Figure2_MalignancyRateByEra.png` | Era × malignancy rate + Wilson 95% CI |
| 3 | `Figure3_StageDistributionByEra.png` | AJCC 8th ed. stage by era (stacked bar) |
| 4 | `Figure4_SmokingTrendByEra.png` | Smoking prevalence + NLP coverage by era |

---

## Tables

| # | Sheet | Description |
|---|---|---|
| 1 | `Table1_Demographics` | Full cohort + malignant sub-columns |
| 2 | `Table2_HistologyEra` | Histology distribution + malignancy rate by era |
| 3 | `Table3_StageMigration` | TNM stage migration 1999–2025 |
| 4 | `Table4_Treatment` | Surgery extent + RAI + outcomes by era |
| 5 | `Table5_SmokingFHx` | Smoking + family hx by era (post-mig_281 NLP) |
| S1 | `SuppS1_SubHistology` | Detailed sub-histology by era (malignant) |
| S2 | `SuppS2_RaceEra` | Race/ethnicity trends by era |

---

## Rebuild Instructions

```bash
# From repo root:
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_tables.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_figures.py
.venv/bin/python M032_submission_package_v1_0/08_analysis_code/build_m032_manuscript_md.py
```

Requires: MotherDuck RW token in `motherduck.local.toml`.

---

## Known Caveats

1. **Smoking coverage (72.2% unknown):** NLP extraction covers 27.8% cohort-wide; residual gap = `CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE`
2. **Malignancy N = 4,019 (vs 4,018 in Cowork lock):** 1-patient edge case from mig_285 view update; immaterial for manuscript
3. **Era F_unknown:** Small number of patients with undatable surgery; excluded from era-stratified tables
4. **smoking_status_combined vs pmhx_nlp_smoking_status:** Combined field (4,232 known) includes structured EHR + NLP; locked report (3,022) used NLP-only. Table 5 uses combined field; abstract numbers use NLP-only
5. **AJCC8 IVB collapse (mig_263):** CPM uses IVB for all M1 disease; IVA/IVC granularity available in `ajcc8_stage_group_resolved`

---

*M032 submission package | mig_290 | 2026-05-04*
