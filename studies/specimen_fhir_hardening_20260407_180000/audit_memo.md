# Specimen + FHIR hardening — machine audit memo
Generated: 2026-04-07T07:02:40.866365+00:00Z
Git SHA: a9e1e0692a02b7721e7c8f29bd41f8aebcea11c2
custom_user_agent: specimen_fhir_hardening_v1

## MotherDuck snapshot
- Attempt: `specimen_fhir_pre_20260407_070214`
- Result detail: InvalidInputException('Invalid Input Error: Database is not a native duckdb database so it does not have snapshots') — CREATE SNAPSHOT "specimen_fhir_pre_20260407_070214" OF "Thyroid 2026";

## README vs sign-off vs live
- README (2026-04-07): states MotherDuck formalized; release-mode may still fail on manual_review_queue.
- studies/20260407_signoff_memo/signoff_memo.md: NOT READY (v2_stage/provenance blockers per that memo).
- Checked-in validation artifacts may be stale vs live catalog; this run reflects DB state at execution time.

## Stale vs current artifacts
- Any checked-in `studies/*/validation_report.md` older than this run's DB time is potentially stale.
- `docs/motherduck_database_contract_v1.md` documents specimen/FHIR surfaces (commit with this change).

## Validation rows (qa.val_specimen_contract_v1)
- specimen_master_fingerprint_unique: **PASS** (True,)
- specimen_master_id_unique: **PASS** (True,)
- specimen_focus_fingerprint_unique: **PASS** (True,)
- genomic_assay_id_unique: **PASS** (True,)
- fhir_specimen_subject_ref: **PASS** (True,)
