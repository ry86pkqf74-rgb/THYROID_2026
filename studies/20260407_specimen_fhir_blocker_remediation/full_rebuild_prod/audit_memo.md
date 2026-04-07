# Specimen + FHIR hardening — machine audit memo
Generated: 2026-04-07T15:14:18.579091+00:00Z
Git SHA: a60e9d5815e68824045f778a6919ff94106097f1
Identity build_run_id: specimen_identity_build_v1_22c5e511bb05
custom_user_agent (pipeline): specimen_fhir_export_v1
QA diagnostics deploy UA: `specimen_fhir_release_ops_v1` (see `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`)

## MotherDuck snapshot
- Attempt: `specimen_fhir_pre_20260407_151331`
- Result detail: InvalidInputException('Invalid Input Error: Database is not a native duckdb database so it does not have snapshots') — CREATE SNAPSHOT "specimen_fhir_pre_20260407_151331" OF "Thyroid 2026";

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
- specimen_focus_orphan_guard: **PASS** (True,)
- multi_synoptic_fp_isolation: **PASS** (True,)
- genomic_assay_id_unique: **PASS** (True,)
- fhir_specimen_subject_ref: **PASS** (True,)
- fhir_specimen_collection_procedure_ref: **PASS** (True,)
- fhir_procedure_encounter_ref: **PASS** (True,)
- fhir_encounter_episode_ref: **PASS** (True,)
- fhir_episode_id_unique: **PASS** (True,)
- fhir_procedure_orphan_guard: **PASS** (True,)
