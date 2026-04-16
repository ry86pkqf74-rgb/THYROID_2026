# FHIR specimen bundle export (analytic, de-identified)

- **Build (UTC):** 2026-04-13T20:27:54.919059+00:00
- **Git SHA:** 4795d42df5f7b63b1c94a69395a4db8300657a9d
- **Export route:** `bundle_table`
- **From pre-built bundle view:** True
- **Source catalog (resolved):** `Thyroid 2026`
- **Source views:** main.fhir_bundle_specimen_export_v1
- **Query user-agent:** `specimen_fhir_export_restore_v1`
- **MotherDuck session hint:** `specimen_fhir_export_restore_v1`
- **Bundle rows (NDJSON lines):** 10139

## Source tables (`main`)

- `fhir_patient_deid_map_v1`: 8422
- `fhir_specimen_v1`: 10139
- `fhir_procedure_collection_v1`: 10139
- `fhir_encounter_v1`: 10139
- `fhir_episode_of_care_v1`: 9486
- `fhir_bundle_specimen_export_v1`: 10139

Machine-readable metadata: `manifest.json`. One FHIR Bundle JSON object per line:
`specimen_bundles.ndjson`.

## Reviewer read-scaling

After a writer snapshot, run `REFRESH DATABASE` on the read-scaling connection
before export; use `MD_READ_SCALING_TOKEN` + this script's `--read-scaling`.
Do not commit tokens; use `motherduck.local.toml` or your secret manager.
