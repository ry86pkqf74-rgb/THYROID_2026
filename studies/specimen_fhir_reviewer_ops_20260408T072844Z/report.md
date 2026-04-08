# Specimen / FHIR reviewer operations — evidence (2026-04-08)

## Git SHA (export run)

- **Manifest / export git SHA:** `bbf91484f1cd087cf9cef8c22940ea9ab2e3accc` (repo `HEAD` at time of MotherDuck export).
- **Docs / script commits (this deliverable):** `b5065ee23777838f45740c46924718317288c309` (operationalize 141 + runbooks); follow-up commits on `main` through **`bdb0f61`** (see `git log` from repo root).

## NDJSON export (reviewer-ready bundle)

- **Script:** `scripts/141_fhir_specimen_json_export.py --md`
- **Output (gitignored):** `exports/fhir_specimen_20260408_072840/`
  - `specimen_bundles.ndjson` — 10,139 lines (FHIR Bundle JSON per line)
  - `manifest.json` — build metadata
  - `README.md` — human-readable counts
- **Catalog:** `Thyroid 2026` (prod), fail-closed RW attach; query user-agent `specimen_fhir_export_v1`.

### Source table row counts (`main`)

| Table | Rows |
|-------|------|
| `fhir_patient_deid_map_v1` | 8,422 |
| `fhir_specimen_v1` | 10,139 |
| `fhir_procedure_collection_v1` | 10,139 |
| `fhir_encounter_v1` | 10,139 |
| `fhir_episode_of_care_v1` | 9,486 |
| `fhir_bundle_specimen_export_v1` | 10,139 |

Bundle view verified current by successful full export (no missing table errors).

## Reviewer access model

- **Build operator:** `MD_SA_TOKEN` or `MOTHERDUCK_TOKEN` (RW), optionally loaded from **`.streamlit/secrets.toml`** (gitignored) per `motherduck_client.get_token()`.
- **Reviewer (least privilege):** `MD_READ_SCALING_TOKEN` + `MD_READ_SCALING_SESSION_HINT`; use `scripts/141_fhir_specimen_json_export.py --read-scaling` **after** `REFRESH DATABASE` / `scripts/136_md_read_scaling_snapshot_refresh.py reader` on the reader connection.
- **Dedicated service account / restricted share:** No Admin REST calls from this repo. Use MotherDuck UI: **Organization → Service accounts** (create RW vs read-only per policy) and **Shares** (Restricted, Manual update if pinning snapshots); grant **Read** only to the reviewer identity. Do not commit tokens or share URLs containing secrets.

## Snapshot / refresh boundary

1. Writer: `CREATE SNAPSHOT OF "Thyroid 2026"` or `136 writer` (RW).
2. Reader: set `MD_READ_SCALING_SESSION_HINT`, connect with read-scaling token, run `REFRESH DATABASE` or `136 reader`.
3. Export: `141 --read-scaling` or operator `141 --md`.

## Telemetry (`md_information_schema.recent_queries`)

- **Availability:** Readable on `Thyroid 2026` with RW token used for this audit.
- **Custom user-agent in table:** `user_agent` column matched DuckDB client strings (e.g. `duckdb/v1.4.4(osx_amd64) python/3.14`), not the connection `custom_user_agent` parameter for specimen/FHIR.
- **Specimen/FHIR-shaped queries (excerpt):** Filtering `query_text` for `%fhir_bundle_specimen_export%` returned 2 recent rows (2026-04-08) including:
  - `SELECT COUNT(*) FROM main.fhir_bundle_specimen_export_v1`
  - `SELECT cast(bundle_json AS VARCHAR) FROM main.fhir_bundle_specimen_export_v1 ORDER BY specimen_id`
- **Implication:** Use `query_text` patterns or org-level logging if you require `specimen_fhir_export_v1` / `specimen_fhir_release_truth_v1` attribution in analytics.

## Compute scaling (Duckling)

- **Not changed** for this run (no temporary Jumbo/Mega escalation). Record here if future large exports require org-admin adjustment and reversion.

## Code / docs delivered in-repo

- `scripts/141_fhir_specimen_json_export.py`: per-export `README.md`, `--read-scaling` mode.
- `docs/specimen_fhir_contract_review.md`, `docs/release_runbook.md`: reviewer runbook, tokens, refresh, telemetry caveat.
- `tests/test_specimen_fhir_scripts_offline.py`: asserts `README.md` presence.
