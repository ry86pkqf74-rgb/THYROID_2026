# M025 Submission Package — closeout checklist

## Git

- [ ] `git add M025_submission_package_v1_0/ qc_framework_v1/migrations/292_m025_submission_package_20260504.sql scripts/output/mig_292_apply_log.txt`
- [ ] Conventional commit, e.g. `feat(m025): scaffold submission package v1.0 (mig_292)`

## MotherDuck

- [ ] Confirm `build_m025_tables.py` run completed without Binder errors.
- [ ] Apply `qc_framework_v1/migrations/292_m025_submission_package_20260504.sql` via RW MotherDuck client.
- [ ] Verify: `SELECT * FROM main.signoff_migration WHERE mig_id = 'mig_292';`

## Manuscript authoring

- [ ] Replace placeholder `.docx` with final title page, manuscript, supplement.
- [ ] Cross-check Tables 2 cutoffs vs journal-preferred Primary threshold (typically TR≥TR4).
- [ ] Methods: cite operative cohort + NIFTP / gold-standard conventions aligned with mig_266 footprint.
