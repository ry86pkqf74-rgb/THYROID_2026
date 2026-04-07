# Implementation report — specimen + FHIR hardening
Timestamp (UTC): 2026-04-07T15:14:18.655096+00:00
**Commit SHA:** `a60e9d5815e68824045f778a6919ff94106097f1`

## Source inventory
- `scripts/sql/139_specimen_identity_layer_ddl.sql` — identity DDL
- `scripts/sql/138_specimen_fhir_tail_ddl.sql` — genomic + FHIR DDL
- `scripts/139_md_specimen_identity_layer.py` — standalone identity runner
- `scripts/138_md_specimen_fhir_layer.py` — orchestrator
- `utils/specimen_fingerprint.py` — fingerprint test helpers

## Table contract
- `main.specimen_master_v1`, `specimen_tumor_focus_v1`, `specimen_genomic_assay_v1`, `specimen_source_xref_v1`
- `qa.specimen_merge_review_queue_v1`, `qa.val_specimen_contract_v1`
- `main.fhir_*_v1` + `main.fhir_bundle_specimen_export_v1`

## Matching policy
- Auto-merge: exact `specimen_fingerprint_sha256` only (full rebuild replaces derived tables).
- Near-duplicate pairs → `qa.specimen_merge_review_queue_v1` (same patient/day/surgery_episode, distinct FP).
- Genomics: scripts/140 — v3 linkage chain, optional genetic_testing + ThyroSeq JSON explosion.

## Unresolved review burden
- See row count `SELECT COUNT(*) FROM qa.specimen_merge_review_queue_v1` on target DB.

## Test / lint
- Run `pytest tests/test_specimen_fhir_layer.py` and `ruff` / `mypy` per CI.

## MotherDuck snapshot / share
- Snapshot attempt recorded in audit_memo.md for this run (`specimen_fhir_pre_20260407_151331`).
- Optional read-only share: attach promoted DB in MotherDuck UI; document token path per org policy.
