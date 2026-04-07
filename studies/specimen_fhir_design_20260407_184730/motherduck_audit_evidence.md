# MotherDuck audit evidence — specimen FHIR design audit

**When:** 2026-04-07 (agent session, America/New_York workspace)  
**Connection:** `utils.md_connect.connect_md_or_file(..., md=True, fail_closed=True, custom_user_agent='specimen_fhir_design_audit_v1')`  
**Database:** `Thyroid 2026` (from live attach)

## Row counts (main / qa)

| Object | Rows |
|--------|-----:|
| main.synoptic_tumor_long_v1 | 11,103 |
| main.path_synoptics_encounter_qc_v1 | 11,688 |
| main.surgery_pathology_linkage_v3 | 9,409 |
| main.fna_molecular_linkage_v3 | 838 |
| main.preop_surgery_linkage_v3 | 3,517 |
| main.molecular_test_episode_v2 | 10,126 |
| main.specimen_master_v1 | 10,139 |
| main.specimen_tumor_focus_v1 | 11,103 |
| main.specimen_genomic_assay_v1 | 10,126 |
| main.specimen_source_xref_v1 | 11,103 |
| main.fhir_specimen_v1 | 10,139 |
| main.fhir_procedure_collection_v1 | 10,139 |
| main.fhir_encounter_v1 | 10,139 |
| main.fhir_episode_of_care_v1 | 10,139 |
| qa.specimen_merge_review_queue_v1 | 1 |
| qa.val_specimen_contract_v1 | 5 |

## Named snapshot attempt

```text
CREATE SNAPSHOT "specimen_fhir_design_audit_snapshot_20260407" OF "Thyroid 2026";
```

**Result:** Not supported on this catalog — `InvalidInputException: Database is not a native duckdb database so it does not have snapshots` (DuckLake / MotherDuck managed storage). **No write fallback** was attempted (per audit constraints: snapshot or scratch clone only; none required for SELECT evidence).

## Notes

- `path_synoptics_encounter_qc_v1` count exceeds `synoptic_tumor_long_v1` because the QC view/table is defined over **all** `path_synoptics` rows (one row per synoptic line), while tumor-long keeps **one row per populated tumor slot** (filters empty slots).
- Specimen layer materialization appears **present and non-empty** on the attached MotherDuck; this contradicts a purely “not yet deployed” reading of older sign-off language (see `artifact_reconciliation.md`).
