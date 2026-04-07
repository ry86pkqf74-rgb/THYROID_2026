# Implementation report — specimen + FHIR hardening
Timestamp (UTC): 2026-04-07T07:02:40.942101+00:00
**Commit SHA:** `a9e1e0692a02b7721e7c8f29bd41f8aebcea11c2`

## Source inventory
- `scripts/sql/138_specimen_fhir_layer_ddl.sql` — DDL
- `scripts/138_md_specimen_fhir_layer.py` — orchestrator
- `utils/specimen_fingerprint.py` — fingerprint test helpers

## Table contract
- `main.specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `specimen_source_xref_v1`
- `qa.specimen_merge_review_queue_v1`, `qa.val_specimen_contract_v1`
- `main.fhir_*_v1` + `main.fhir_bundle_specimen_export_v1`

## Matching policy
- Auto-merge: exact `specimen_fingerprint_sha256` only (full rebuild replaces derived tables).
- Near-duplicate pairs → `qa.specimen_merge_review_queue_v1` (same patient/day/surgery_episode, distinct FP).
- Genomics: molecular episodes via v3 linkage chain; genetic_testing append requires exact platform string match.

## Unresolved review burden
- See row count `SELECT COUNT(*) FROM qa.specimen_merge_review_queue_v1` on target DB.

## Test / lint
- Run `pytest tests/test_specimen_fhir_layer.py` and `ruff` / `mypy` per CI.

## MotherDuck snapshot / share
- Snapshot attempt recorded in audit_memo.md for this run (`specimen_fhir_pre_20260407_070214`).
- Optional read-only share: attach promoted DB in MotherDuck UI; document token path per org policy.
