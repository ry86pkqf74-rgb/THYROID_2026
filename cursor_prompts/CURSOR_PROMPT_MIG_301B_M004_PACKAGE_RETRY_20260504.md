# Cursor Composer Dispatch — mig_301b: M004 submission package retry (complete docx/xlsx)

**Generated:** 2026-05-04 by Cowork at HEAD `d4bdebd`.
**Lane:** mig_301b — Original mig_301 partially landed: `M004_submission_package_v1_0/` exists with 00_README, 06_figures, 08_analysis_code, 08_analysis_outputs, 09_validation_report, CLOSEOUT_NOTES, **but is MISSING** 01_title_page.docx, 02_manuscript.docx, 03_supplement.docx, 04_tables.xlsx, 05_master_data.xlsx, 07_response_to_reviewers_template.docx. PACKAGE_MANIFEST shows only 10 files vs M044's 43 / M032's 25.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** 90 min.
**Severity:** MED. Closes the 7th submission package.
**Closes:** CF-mig301-PARTIAL.

---

## §0 — First message

> mig_301b dispatch (mig_301 retry). Read `cursor_prompts/CURSOR_PROMPT_MIG_301_M004_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md` for full scope. The directory exists but is missing 6 files (.docx + .xlsx). Use the existing M044/M032/M037/M025 packages as templates; populate the .docx/xlsx with M004-specific content from `manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md`. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

## §1 — What's missing

Confirmed via `M004_submission_package_v1_0/PACKAGE_MANIFEST.json`:
- `01_title_page.docx`
- `02_manuscript.docx`
- `03_supplement.docx`
- `04_tables.xlsx`
- `05_master_data.xlsx`
- `07_response_to_reviewers_template.docx`

## §2 — Source of truth

- [`manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md`](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md) — locked numbers
- [`snowflake_trial/reports/M004_logreg_nlp_augmented_20260504.md`](computer:///Users/ros/THyroid 2026/snowflake_trial/reports/M004_logreg_nlp_augmented_20260504.md) — full logreg + concordance
- `manuscript_workspace.cohort_m004_autoimmune_cancer_v1` — cohort source

## §3 — Locked numbers to embed

- Cohort: 10,871 / 4,019 malig
- Hashimoto combined: 400 (174 malig)
- Graves combined: 1,656 (575 malig)
- Either positive: 2,004 (728 malig)
- aOR Hashimoto: **1.37 (1.12-1.68), p=0.002**
- aOR Graves: **0.87 (0.78-0.98), p=0.017**
- Pseudo-R² 0.0093, LR χ² 132.6

## §4 — Apply

Copy templates from M032 / M037 / M025 packages; populate M004-specific content; save to `M004_submission_package_v1_0/`. Re-run `python snowflake_trial/scripts/build_submission_manifest.py` to rebuild PACKAGE_MANIFEST.json.

## §5 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_301b', CURRENT_TIMESTAMP, 'cursor_composer_mig301b_retry_of_301',
 'mig_301b: Completed M004_submission_package_v1_0 (mig_301 retry). 6 missing .docx/.xlsx files generated. PACKAGE_MANIFEST.json refreshed (~25 files). Closes CF-mig301-PARTIAL. M004 ready-for-writing pathway closed.');
```

## §6 — Surgical git add

```
M004_submission_package_v1_0/01_title_page.docx
M004_submission_package_v1_0/02_manuscript.docx
M004_submission_package_v1_0/03_supplement.docx
M004_submission_package_v1_0/04_tables.xlsx
M004_submission_package_v1_0/05_master_data.xlsx
M004_submission_package_v1_0/07_response_to_reviewers_template.docx
M004_submission_package_v1_0/PACKAGE_MANIFEST.json
qc_framework_v1/migrations/301b_m004_package_retry_20260504.sql
scripts/output/mig_301b_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_301B_M004_PACKAGE_RETRY_20260504.md
```

---

**End of mig_301b dispatch.**
