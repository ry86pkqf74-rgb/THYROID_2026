# Specimen + FHIR hardening — blocked (prerequisites)
Generated: 2026-04-07T06:50:04.998235+00:00Z
Git SHA: 57d449bf5e247c30747aa8100c0c0edf0a259503
custom_user_agent: specimen_fhir_hardening_v1

## MotherDuck snapshot
- Attempt: `specimen_fhir_pre_20260407_065004`
- Result detail: InvalidInputException('Invalid Input Error: Database is not a native duckdb database so it does not have snapshots') — CREATE SNAPSHOT "specimen_fhir_pre_20260407_065004" OF "Thyroid 2026";

## Prerequisite tables missing on catalog
The following `main.*` objects must exist before DDL:
- `main.synoptic_tumor_long_v1`
- `main.path_synoptics_encounter_qc_v1`
- `main.surgery_pathology_linkage_v3`
- `main.fna_molecular_linkage_v3`
- `main.preop_surgery_linkage_v3`

## Remediation (typical)
- `synoptic_tumor_long_v1`: run `scripts/108_synoptic_tumor_long_v1.py --md` (needs `processed/path_synoptics.parquet`).
- `path_synoptics_encounter_qc_v1`: run `scripts/109_synoptic_encounter_qc.py --md` (needs `path_synoptics`).
- `surgery_pathology_linkage_v3`, `fna_molecular_linkage_v3`, `preop_surgery_linkage_v3`, `molecular_test_episode_v2`:
  load analysis/episode contract assets (e.g. `scripts/117_md_contract_views.py --md` + manuscript freeze parquets, or your org’s linkage materialization).

No DDL was applied; fix prerequisites and re-run.
