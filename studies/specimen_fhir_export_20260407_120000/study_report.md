# Specimen analytic FHIR export layer — study report

**UTC:** 2026-04-07 (study artifact timestamp `20260407_120000`)  
**Scope:** Analytic / de-identified FHIR-shaped tables in `main` + optional NDJSON export (not production interoperability).

## Objectives delivered

1. **`main.fhir_patient_deid_map_v1`** — bare `patient_fhir_id` (fix: no `Patient/Patient/` double prefix in references).
2. **`main.fhir_specimen_v1`** — identifiers, `status`, `type`, `subject`, `receivedTime` / `collection.collectedDateTime`, optional `collection.bodySite`, `collection.procedure` → Procedure; surrogate id columns for QA joins.
3. **`main.fhir_procedure_collection_v1`** — collection procedure with `identifier`, `status`, `code`, `performedDateTime`, `encounter`, analytic `extension` (specimen `valueReference` + occurrence `valueDateTime` when date known).
4. **`main.fhir_encounter_v1`** — `identifier`, `status`, `class` (IMP vs AMB), `type`, `period`, `episodeOfCare`, `subject`; `episode_fhir_id` column for bundle integrity.
5. **`main.fhir_episode_of_care_v1`** — **deduplicated** per `(research_id, surgery_episode_id)`, tied to `tumor_episode_master_v2` for period bounds; `episode_fhir_id` key.
6. **`main.fhir_bundle_specimen_export_v1`** — `type=collection` bundle with Specimen, Procedure, Encounter, Episode; join Episode via `fe.episode_fhir_id = fo.episode_fhir_id`.
7. **Export:** `scripts/141_fhir_specimen_json_export.py --md` → `exports/fhir_specimen_<ts>/specimen_bundles.ndjson` + `manifest.json`.

## MotherDuck governance

- Orchestrator `scripts/138_md_specimen_fhir_layer.py` uses `connect_md_or_file(..., fail_closed=True, custom_user_agent='specimen_fhir_export_v1')`.
- Prerequisite added: `main.tumor_episode_master_v2` (EpisodeOfCare period + DDL join).
- Export reader uses `connect_md_fail_closed(..., custom_user_agent='specimen_fhir_export_v1')` (no local DuckDB attach for `--md`).

## Validation

- `scripts/138_md_specimen_fhir_layer.py` persists extended checks to `qa.val_specimen_contract_v1` (specimen ↔ procedure id, procedure ↔ encounter, encounter ↔ episode, episode id uniqueness, procedure orphan guard).
- Offline: `pytest tests/test_specimen_fhir_layer.py` (bundle JSON + cross-resource references).

## Live catalog run

Operator should run:

```bash
.venv/bin/python scripts/138_md_specimen_fhir_layer.py --md
.venv/bin/python scripts/141_fhir_specimen_json_export.py --md
```

If `md_information_schema.query_history` is available, telemetry memo from 138 filters `user_agent = 'specimen_fhir_export_v1'`.

## Files touched (this change set)

- `scripts/sql/138_specimen_fhir_tail_ddl.sql`
- `scripts/138_md_specimen_fhir_layer.py`
- `scripts/141_fhir_specimen_json_export.py` (new)
- `tests/test_specimen_fhir_layer.py`
- `scripts/119_md_formalization_validate.py` (`fhir_patient_deid_map_v1` in Check 13 list)
- `docs/motherduck_database_contract_v1.md` (FHIR export subsection)
