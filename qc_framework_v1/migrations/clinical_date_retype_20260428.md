# clinical_date_retype_20260428

**Script:** `scripts/413_clinical_date_retype.py`  
**MotherDuck DB:** `thyroid_canonical_publication_v1_0`  
**Date:** 2026-04-28

## Scope

Clinical event date columns retyped **VARCHAR/TIMESTAMP → DATE** under the audit query in `CURSOR_PROMPT_clinical_date_retype_20260428`.

| Table | Column | Before | After |
| --- | --- | --- | --- |
| `canonical_esophageal_invasion_events_v1` | `note_date` | VARCHAR (`''` for all rows) | DATE |
| `canonical_frozen_section_events_v1` | `frozen_section_date` | VARCHAR | DATE |
| `canonical_operative_events_v1` | `surgery_date_native`, `resolved_surgery_date`, `note_date_resolved` | TIMESTAMP / VARCHAR / TIMESTAMP | DATE |
| `canonical_path_malignant_events_v1` | `surgery_date` | TIMESTAMP | DATE |

Row counts unchanged: 188 / 7081 / 11773 / 6689 respectively for the four bases.

Pre-snapshot tables (`"Thyroid 2026 UPdated".archive_pub_v1_0`):  
`<table>_pre_date_retype_20260428` for each rebuilt base table.

Registry: `canonical_column_verification_registry_v1` — `notes` appended per column; `verification_status` unchanged (`verified`). `verified_ts` touched.

Post-migration audit (same predicate as prompt): **0** rows — no TIMESTAMP/VARCHAR “clinical date” outliers left on verified `canonical_*` tables.

## Dependent views

26 distinct `(schema_name, view_name)` referencing the four bases were **CREATE OR REPLACE**’d from `duckdb_views().sql` so column types match rebuilt tables.

## Caveats / carry-forwards

- **`manuscript_workspace.ete_manuscript_analytic_v1`** — DDL contains `JOIN manuscript_workspace.path_malignant_event_fingerprint_v1`, which is **missing** from the catalog. Recreate/smoke emits WARN only; assumed **pre-existing** broken dependency, not introduced by date retyping.
- **CF-100-DATE-RETYPE** (`frozen_section_date` VARCHAR→DATE): **CLOSED** by this migration (per Cowork backlog).
