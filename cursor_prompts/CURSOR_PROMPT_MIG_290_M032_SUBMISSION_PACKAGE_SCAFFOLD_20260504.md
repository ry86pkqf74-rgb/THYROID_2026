# Cursor Composer Dispatch — mig_290: M032 25-yr Descriptive submission package scaffold

**Generated:** 2026-05-04 by Cowork.
**Lane:** mig_290 — Build `M032_submission_package_v1_0/` directory mirroring M044 + M038 patterns. M032 cohort + numbers are now locked (post-mig_281/285 NLP augment); render the full submission package so writing chat can take it.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through the analysis + table structure before generating files.
**Estimated runtime:** 2-3 hours.
**Triggered by:** M032 cohort lock (mig_285 + Cowork render of M032_table1_with_nlp_20260504.md).
**Severity:** MED. Unblocks M032 writing chat.

---

## §0 — First message to paste into Cursor Chat

> mig_290 dispatch. Build `M032_submission_package_v1_0/` mirroring `M044_submission_package_v1_0/` structure. Source SQL is `M032_descriptive_analysis.sql` (TBA — needs to be authored). MotherDuck DB is `thyroid_canonical_publication_v1_0`. Walk through the table structure with me before generating .docx files.

---

## §1 — Pre-task: review existing inputs

- [`snowflake_trial/reports/M032_table1_with_nlp_20260504.md`](computer:///Users/ros/THyroid 2026/snowflake_trial/reports/M032_table1_with_nlp_20260504.md) — Cowork-rendered Table 1 with smoking + family-hx + era stratification
- [`manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md) — prior draft
- M044 + M038 submission packages — pattern reference

## §2 — Package structure to build

```
M032_submission_package_v1_0/
├── 00_README.md
├── 01_title_page.docx          (placeholders)
├── 02_manuscript.docx          (Methods → Results → Discussion → Refs)
├── 03_supplement.docx          (Tables S1-Sx)
├── 04_tables.xlsx              (Tables 1-5 + Supp + dict + QA)
├── 05_master_data.xlsx         (per-patient analytic + dict)
├── 06_figures/                 (era trends, stage migration, etc.)
├── 07_response_to_reviewers_template.docx
├── 08_analysis_code/
│   ├── M032_descriptive_analysis.sql       (full reproducibility SQL)
│   ├── build_m032_tables.py
│   ├── build_m032_figures.py
│   └── build_m032_manuscript_md.py
├── 09_validation_report.md     (live SQL re-derivation audit)
└── CLOSEOUT_NOTES.md
```

---

## §3 — Tables (proposed, pending Logan ratification)

| # | Title |
|---|---|
| 1 | Cohort demographics by surgery era (1999-2005 / 2005-2010 / 2010-2015 / 2015-2020 / 2020-2025) |
| 2 | Histology distribution + malignancy rate trends by era |
| 3 | TNM stage migration over 25 yrs (AJCC 7→8 era effect) |
| 4 | Treatment patterns (surgery extent + RAI) by era |
| 5 | Smoking + family hx prevalence by era (post-mig_281 NLP augment) |
| Supp S1 | Detailed sub-histology by era |
| Supp S2 | Race/ethnicity trends by era |
| Supp S3 | Geographic catchment over time |

---

## §4 — Figures (proposed)

| # | Title |
|---|---|
| 1 | Cohort flow diagram (CONSORT-style) |
| 2 | Era × malignancy rate (line chart with CI bands) |
| 3 | TNM stage distribution by era (stacked bar) |
| 4 | Smoking prevalence trend (current/former/never by era) |

---

## §5 — Apply

### §5a — Write reproducibility SQL package
Mirror `M044_submission_package_v1_0/08_analysis_code/M044_ETE_analysis.sql` style — a single SQL file with annotated query blocks, expected counts inline as comments.

### §5b — Build .py scripts
- `build_m032_tables.py` — same openpyxl pattern as M044/M038
- `build_m032_figures.py` — matplotlib (Agg backend), 300 DPI PNGs + CSVs
- `build_m032_manuscript_md.py` — markdown skeleton (Methods + Results + numbered placeholders)

### §5c — Validation report
Run M032_descriptive_analysis.sql blocks against MD; tabulate PASS/DIFF/FAIL per cell (mirror M038's 156-cell audit pattern).

### §5d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_290', CURRENT_TIMESTAMP, 'cursor_composer_mig290',
 'mig_290: M032 25-yr Descriptive submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-5 + Supp S1-S3 + 4 figures. SQL reproducibility package + 3 build scripts. Validation report N/M cells PASS. Closes M032 ready-for-writing gate.');
```

---

## §6 — Surgical git add

```
M032_submission_package_v1_0/  (whole directory)
qc_framework_v1/migrations/290_m032_submission_package_20260504.sql
scripts/output/mig_290_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_290_M032_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md
```

---

## §7 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-M032-READY-FOR-WRITING | **CLOSED on apply** | Submission package complete |
| CF-M032-LOGREG-MULTIVARIABLE | **OPEN** | If M032 expands to multivariable analysis (era × outcomes), Cowork follow-up |

---

**End of mig_290 dispatch.**
