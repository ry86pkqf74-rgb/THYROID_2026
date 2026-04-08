# Specimen FHIR NDJSON export — restore / hardening (2026-04-08)

## Summary

The canonical exporter **`scripts/141_fhir_specimen_json_export.py`** was already present on `main`. This delivery **hardens** it for reviewer/ops use: fail-closed MotherDuck attach with **`specimen_fhir_export_restore_v1`** as the default `custom_user_agent` (overridable via `MOTHERDUCK_CUSTOM_USER_AGENT`), stable default **`motherduck_session_hint`** (`specimen_fhir_export_restore_v1`, overridable via `MOTHERDUCK_SESSION_HINT`), **`export_route`** in `manifest.json` (`bundle_table` vs `reconstructed_from_resources`), **catalog probe** metadata (including snapshot-semantics caveat — no DDL probe), and a **reconstruction** path when `main.fhir_bundle_specimen_export_v1` is absent or unreadable (same bundle JSON shape as `scripts/sql/138_specimen_fhir_tail_ddl.sql`).

**PHI:** Exports are de-identified FHIR-shaped bundles only (no raw notes, no evidence text). Output trees under `exports/fhir_specimen_<ts>/` remain **gitignored**; this study folder is the git-visible provenance.

## Exact entrypoint

```bash
# RW operator (token from motherduck.local.toml / MD_SA_TOKEN / MOTHERDUCK_TOKEN — never commit)
.venv/bin/python scripts/141_fhir_specimen_json_export.py --md

# Reviewer read-scaling (after writer snapshot: REFRESH DATABASE on this connection)
.venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling

# Offline / CI
.venv/bin/python scripts/141_fhir_specimen_json_export.py --local-duckdb /path/to.db
```

Optional: `--force-reconstruct` skips the bundle table even if present. `--limit N` caps rows.

## Source tables

| Route | Source |
|-------|--------|
| `bundle_table` | `main.fhir_bundle_specimen_export_v1` (`bundle_json`, `specimen_id`) |
| `reconstructed_from_resources` | Join: `fhir_specimen_v1` ⟵ `fhir_procedure_collection_v1` ⟵ `fhir_encounter_v1` ⟵ `fhir_episode_of_care_v1` on `specimen_id` / `episode_fhir_id` |

Manifest field `source_tables_main` lists row counts (or `missing` / `error`) for all six analytic tables including `fhir_patient_deid_map_v1` and the bundle table.

## Export artifacts

Per run: `exports/fhir_specimen_<UTC_timestamp>/`

- `specimen_bundles.ndjson` — one FHIR `Bundle` (`type=collection`) per line
- `manifest.json` — `git_sha`, `build_timestamp_utc`, `source_catalog_env`, `source_database_probe`, `export_route`, `custom_user_agent`, `motherduck_session_hint`, `bundle_row_count`, `source_tables_main`, `reconstructed_from_tables` when applicable
- `README.md` — human-readable mirror + reviewer read-scaling reminder

## Offline tests

`tests/test_specimen_fhir_scripts_offline.py` covers:

- Minimal DB with bundle table (`bundle_table` route)
- Reconstruction without bundle table
- Skipping empty / null `bundle_json` rows
- **143** stub DB fixed with empty `specimen_tumor_focus_v1` so `142` DDL applies on CI

## Reviewer delivery (least privilege)

**Preferred:** Issue a **read-scaling** token (`MD_READ_SCALING_TOKEN`) bound to the reviewer; set `MOTHERDUCK_SESSION_HINT` (e.g. `thy_review_<ticket>`). After each writer snapshot, the reviewer runs **`REFRESH DATABASE`** on the read-scaling connection, then `141 --read-scaling`.

**Alternative:** Restricted **hidden share** with **READ** grant to reviewer identity only; attach via MotherDuck UI/docs — **never** embed tokens or share URLs with embedded secrets in git.

Tokens live in **`motherduck.local.toml`** (gitignored) or your secret manager; not in repo.

## Telemetry

Filter `md_information_schema.query_history` / `recent_queries` by `user_agent IN ('specimen_fhir_export_restore_v1','specimen_fhir_export_v1')` or by `query_text` containing `fhir_bundle_specimen_export_v1`. `scripts/144_md_repo_current_state_summary.py` includes both UAs in its telemetry filter list.

MotherDuck **org Admin** flows (service accounts, hidden shares, Duckling sizing) are **not automated** in this repo; operators use the MotherDuck UI or Admin API per org policy. No tokens are committed.

## Restored vs new

**Restored/enhanced** — `141` existed; this change adds reconstruction, richer manifest, UA/session defaults, and tests. Not a new script number.

## Commit SHA

The commit that introduced this study + exporter hardening is whichever commit last modified this file:

```bash
git log -1 --format=%H -- studies/specimen_fhir_export_restore_20260408_120000/report.md
```
