# Specimen FHIR JSON exporter — restore / hardening (2026-04-08)

## Summary

The canonical entrypoint remains **`scripts/141_fhir_specimen_json_export.py`**. This pass **extends manifest provenance** and **offline test coverage** so exports are fully reviewable without guessing which SQL path produced the bundles.

## Behavior (unchanged routing)

1. **Preferred:** read `main.fhir_bundle_specimen_export_v1` (`bundle_json`, ordered by `specimen_id`).
2. **Fallback:** same `collection` Bundle shape as `scripts/sql/138_specimen_fhir_tail_ddl.sql`, built from  
   `main.fhir_specimen_v1` ⟵ `main.fhir_procedure_collection_v1` ⟵ `main.fhir_encounter_v1` ⟵ `main.fhir_episode_of_care_v1` with `fe.patient_fhir_id = fo.patient_fhir_id` on the episode join.
3. **Flags:** `--force-reconstruct` skips the bundle table even when populated (reconstruction-only).

## MotherDuck

- **`custom_user_agent`:** default `specimen_fhir_export_restore_v1` (`MOTHERDUCK_CUSTOM_USER_AGENT` overrides).
- **`motherduck_session_hint`:** default `specimen_fhir_export_restore_v1` (`MOTHERDUCK_SESSION_HINT` overrides).
- **Tokens:** use repo-root **`motherduck.local.toml`** (gitignored), following **`motherduck.local.toml.example`** — never commit secrets. **`--md`** uses the RW attach path via `utils.md_connect.connect_md_fail_closed`; **`--read-scaling`** uses read-scaling token only (after `REFRESH DATABASE` on that connection).

## Outputs

Per run: **`exports/fhir_specimen_<YYYYMMDD_HHMMSS>/`**

| File | Content |
|------|---------|
| `specimen_bundles.ndjson` | One JSON Bundle per line (`type=collection`) |
| `manifest.json` | Provenance (see below) |
| `README.md` | Human-readable summary |

## Manifest additions (this delivery)

| Key | Purpose |
|-----|---------|
| `source_catalog` | Resolved `current_database()` or `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` |
| `source_views` | Explicit list: bundle table **or** four reconstructed resource tables |
| `from_prebuilt_bundle_view` | Boolean — `true` iff rows came from `fhir_bundle_specimen_export_v1` |
| `timestamp` | Compact UTC string (`YYYY-MM-DDTHH:MM:SSZ`) alongside ISO `build_timestamp_utc` |
| `export_source_row_count` | Rows returned by SQL before omitting blank JSON lines |
| `bundle_row_count` | NDJSON lines written |

Existing fields retained: `git_sha`, `custom_user_agent`, `export_route`, `source_tables_main`, `source_database_probe`, etc.

## Offline tests

```bash
python3 -m pytest tests/test_specimen_fhir_scripts_offline.py -k 141 -v
```

Coverage: minimal bundle-table export, reconstruction without bundle table, blank `bundle_json` rows, **`--force-reconstruct`** ignoring a decoy bundle row, unknown git SHA.

## Verification (this workspace)

```text
python3 -m py_compile scripts/141_fhir_specimen_json_export.py
python3 -m ruff check scripts/141_fhir_specimen_json_export.py tests/test_specimen_fhir_scripts_offline.py
python3 -m mypy scripts/141_fhir_specimen_json_export.py
python3 -m pytest tests/test_specimen_fhir_scripts_offline.py -k 141 -v
```

## Doc touchpoints

- `README.md` — exporter row in Key references.
- `docs/specimen_fhir_contract_review.md` — manifest field table + pytest hint.
- `docs/motherduck_database_contract_v1.md` — export paragraph + token / manifest detail.
