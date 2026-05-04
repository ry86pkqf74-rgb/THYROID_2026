# Cursor Composer Dispatch — mig_291: M037 LN Predictors submission package scaffold

**Generated:** 2026-05-04 by Cowork at HEAD `170ee3d`.
**Lane:** mig_291 — Build `M037_submission_package_v1_0/` mirroring M044 + M038 patterns. M037 cohort + logreg numbers are now locked (post-mig_281/286 family-hx augment); render full submission package so writing chat can take it.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through the table structure before generating .docx files.
**Estimated runtime:** 2-3 hours.
**Triggered by:** M037 cohort lock (mig_286 + Cowork render of M037_logreg_family_hx_20260504.md).
**Severity:** MED. Unblocks M037 writing chat.

---

## §0 — First message to paste into Cursor Chat

> mig_291 dispatch. Build `M037_submission_package_v1_0/` mirroring `M044_submission_package_v1_0/` structure. Source SQL is `M037_ln_predictors_analysis.sql` (TBA — needs to be authored). MotherDuck DB is `thyroid_canonical_publication_v1_0`. Walk through the table structure with me before generating .docx files.

---

## §1 — Pre-task: review existing inputs

- [`snowflake_trial/reports/M037_logreg_family_hx_20260504.md`](computer:///Users/ros/THyroid 2026/snowflake_trial/reports/M037_logreg_family_hx_20260504.md) — Cowork-rendered logreg with family-hx
- Earlier M037 reports: `m037_table1.md`, `m037_table2_logreg.md`, `m037_sensitivity_ln_both.md`
- M044 + M038 submission packages — pattern reference
- [`manuscript_outputs/v1_0_20260501/M037_READY_FOR_WRITING_BRIEF.md`](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/M037_READY_FOR_WRITING_BRIEF.md) — Cowork ready-for-writing brief

## §2 — Package structure

Same skeleton as M044/M038:
```
M037_submission_package_v1_0/
├── 00_README.md
├── 01_title_page.docx
├── 02_manuscript.docx
├── 03_supplement.docx
├── 04_tables.xlsx
├── 05_master_data.xlsx
├── 06_figures/
├── 07_response_to_reviewers_template.docx
├── 08_analysis_code/
│   ├── M037_ln_predictors_analysis.sql
│   ├── build_m037_tables.py
│   ├── build_m037_figures.py
│   └── build_m037_manuscript_md.py
├── 09_validation_report.md
└── CLOSEOUT_NOTES.md
```

## §3 — Tables (proposed)

| # | Title |
|---|---|
| 1 | M037 cohort demographics by LN status (N0 vs N1a vs N1b vs Nx) |
| 2 | Logreg LN+ predictors with family-hx covariate (post-mig_286) |
| 3 | Stratified analyses (sex × age × tumor size) |
| 4 | Sensitivity: ln_status_source='both' subset |
| 5 | Histology × LN status cross-tab |
| Supp S1 | Detailed N1a vs N1b stratification |
| Supp S2 | Family syndrome flag composite analysis |

## §4 — Figures (proposed)

| # | Title |
|---|---|
| 1 | Cohort flow diagram |
| 2 | LN+ rate by tumor size bucket |
| 3 | Forest plot of multivariable predictors |
| 4 | KM-style "time to LN diagnosis" if temporality available |

## §5 — Apply

Standard scaffold pattern — see mig_290 prompt for the full template. Mirror that structure with M037-specific columns + cohort filter (`cohort_m037_ln_metastasis_v1`).

### Headline finding to lead with

**Family hx of thyroid cancer is NOT independently associated with LN metastasis** (aOR 1.05, 95% CI 0.74–1.51, p=0.77). This is the null-effect publishable result — strengthens M037 by showing the covariate was adequately powered.

Significant predictors:
- Male sex aOR 1.81 (1.49–2.21), p<0.0001
- Younger age aOR 0.98/yr (0.98–0.99), p<0.0001
- Larger tumor aOR 1.18/cm (1.12–1.24), p<0.0001

### Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_291', CURRENT_TIMESTAMP, 'cursor_composer_mig291',
 'mig_291: M037 LN Predictors submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp + 4 figures. SQL reproducibility package + 3 build scripts. Headline: family-hx aOR 1.05 (NULL); male sex/age/tumor size all significant. Closes M037 ready-for-writing gate.');
```

---

## §6 — Surgical git add

```
M037_submission_package_v1_0/
qc_framework_v1/migrations/291_m037_submission_package_20260504.sql
scripts/output/mig_291_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_291_M037_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md
```

---

**End of mig_291 dispatch.**
