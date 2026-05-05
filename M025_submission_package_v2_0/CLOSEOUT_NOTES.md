# M025 v2.0 submission package — mig_307 closeout

## Git

- [ ] `git add M025_submission_package_v2_0/ qc_framework_v1/migrations/307_m025_v2_submission_package_20260504.sql`
- [ ] Commit, e.g. `feat(m025): nodule-level submission package v2.0 scaffold (mig_307)`

## MotherDuck

- [ ] Run builders (RW token via `motherduck.local.toml` or env):
  - `.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_tables.py`
  - `.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_figures.py`
  - `.venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_manuscript_md.py`
- [ ] Apply `qc_framework_v1/migrations/307_m025_v2_submission_package_20260504.sql`
- [ ] Verify: `SELECT * FROM main.signoff_migration WHERE mig_id = 'mig_307';`

## QC

- [ ] Strict nodule count ≈ 3,687; total spine rows ≈ 37,438 (mig_306 gates).
- [ ] Threshold metrics + ROM compare CSVs align with `§5` headline in `cursor_prompts/CURSOR_PROMPT_MIG_306_NODULE_LEVEL_SPINE_20260504.md` within rounding.
- [ ] `M025_submission_package_v1_0/` untouched except intentional shared references.

## Manuscript

- [ ] Replace placeholder `.docx` with final title page / manuscript / supplement using working title in `00_README.md`.
- [ ] Cite v1.0 as patient-level sister analysis in Discussion.
