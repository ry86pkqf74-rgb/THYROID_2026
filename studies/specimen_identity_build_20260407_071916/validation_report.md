# Specimen identity — validation report
Generated (UTC): 2026-04-07T07:19:50.131478+00:00
Git SHA: `27ec05fe96a675028dbc40e7c75deb6c5af0e27a`
custom_user_agent: `specimen_identity_build_v1`
identity_build_run_id: `specimen_identity_build_v1_8719306184b5`

## Snapshot
- Name: `specimen_identity_pre_20260407_071916`
- Detail: InvalidInputException('Invalid Input Error: Database is not a native duckdb database so it does not have snapshots') — CREATE SNAPSHOT "specimen_identity_pre_20260407_071916" OF "Thyroid 2026";

## qa.val_specimen_contract_v1
- **PASS** `specimen_master_fingerprint_unique` — (True,)
- **PASS** `specimen_master_id_unique` — (True,)
- **PASS** `specimen_focus_fingerprint_unique` — (True,)
- **PASS** `specimen_focus_orphan_guard` — (True,)
- **PASS** `multi_synoptic_fp_isolation` — (True,)
- **PASS** `specimen_master_provenance_build_id` — (True,)
- **PASS** `specimen_focus_provenance_build_id` — (True,)

## Row counts (informational)
- specimen_master_v1: 10139
- specimen_tumor_focus_v1: 11103
- specimen_source_xref_v1: 11277
- specimen_merge_review_queue_v1: 1