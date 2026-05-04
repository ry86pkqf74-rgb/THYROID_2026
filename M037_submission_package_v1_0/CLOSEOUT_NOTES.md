# M037 submission package v1.0 — closeout

**Dispatch:** `cursor_prompts/CURSOR_PROMPT_MIG_291_M037_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md`  
**Date:** 2026-05-04  

## Delivered

- `M037_submission_package_v1_0/` skeleton mirroring M044/M038.
- `qc_framework_v1/migrations/291_m037_submission_package_20260504.sql` — documentation + `signoff_migration` insert.
- `scripts/output/mig_291_apply_log.txt` — captured stdout from migration apply (when run).

## Preconditions

- MotherDuck DB `thyroid_canonical_publication_v1_0` available.
- `manuscript_workspace.cohort_m037_ln_metastasis_v1` resolves (mig_280 + mig_286).
- `main.canonical_patient_master.ln_status_source` present (mig_259).

## Manual steps (Logan)

1. Replace placeholder `.docx` bodies (copied from M044) with M037-specific title page, manuscript, supplement.
2. Run AMA / journal reference formatter on final draft.
3. Confirm institutional IRB / conflict / funding boilerplate on title page.

## Git

Suggested commit (after QA):

```text
manuscript(M037): mig_291 submission package v1.0 scaffold

- M037_submission_package_v1_0: tables/figures builders + SQL excerpts
- qc_framework_v1/migrations/291_m037_submission_package_20260504.sql
```
