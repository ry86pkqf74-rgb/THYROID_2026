# MotherDuck archive cleanup — 2026-05-04

## What this is

Audit trail of an archive/legacy purge executed against the MotherDuck account on 2026-05-04. The goal was to keep only canonical / verified objects in the live publication DB and prevent agents (Cursor, ChatGPT review, Cline) from accidentally querying stale archive copies.

## What was dropped

All 11 archive/legacy schemas in the `Thyroid 2026 UPdated` MotherDuck DB, plus 3 orphan `main.*_archived_20260422` tables.

| schema | n_tables | total_rows | total_cols |
|---|---:|---:|---:|
| archive_pub_v1_0 | 584 | 4,482,198 | 78,517 |
| archive_legacy | 121 | 868,243 | 4,196 |
| us_legacy_20260421 | 18 | 222,830 | 368 |
| note_entities_llm_legacy_20260422 | 9 | 121,164 | 203 |
| tier2_legacy_20260422 | 12 | 73,303 | 567 |
| molecular_legacy_20260421 | 13 | 70,584 | 378 |
| llm_invasion_legacy_20260425 | 2 | 68,705 | 46 |
| manuscript_workspace_legacy_20260422 | 12 | 56,461 | 108 |
| cpm_tirads_legacy_20260421 | 15 | 21,755 | 3,292 |
| main (3 orphans) | 3 | 17,906 | 1,654 |
| verify_legacy_20260422 | 2 | 13,117 | 25 |
| **TOTAL** | **791** | **~6.0M** | **89,354** |

Per-table inventory: see `manifest.csv` in this directory.

## Pre-flight safety check (zero PUB-view dependencies)

Before any drop, ran this check against `thyroid_canonical_publication_v1_0`:

```sql
SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE view_definition ILIKE '%Thyroid 2026 UPdated%'
   OR view_definition ILIKE '%archive_pub_v1_0%'
   OR view_definition ILIKE '%archive_legacy%'
   OR view_definition ILIKE '%_legacy_2026%';
-- 0 rows
```

No live PUB view referenced any of the dropped schemas, so the "ALTER VIEW dependent bodies" trap (memory: feedback_alter_view_dependents) does not apply.

## What was KEPT

- `thyroid_canonical_publication_v1_0` — entire publication DB untouched (canonicals, manuscript_workspace, views_readable, semantic_publication, raw)
- `thyroid_canonical_publication_v1_0.archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_restore_20260504` — fresh pre-snapshot dated today, looks like an in-flight recurrence migration; deliberately preserved
- `Thyroid 2026 UPdated` DB itself stays attached but now contains only `main` (empty post-cleanup)

## Why this is safe

1. **Verification phase complete**: per memory `project_2026-04-30_v12_round_complete` and follow-ups, all 174→190 canonical objects are 5-gate verified. The Protocol-v2 CTC-equivalence pattern that uses `archive_pub_v1_0` pre-snapshots as the value-source-of-truth for inherited columns has served its purpose.
2. **No live dependencies**: zero PUB views, zero canonical builders reference any of these schemas (verified above).
3. **Manuscript readiness**: per memory `project_2026-04-30_v11_round_complete` the manuscript has no Logan-blocking items.

## How to re-create if ever needed

The publication DB (`thyroid_canonical_publication_v1_0`) is self-contained and reproducible from upstream `note_entities_llm_*` raw tables + the migration scripts in `cursor_prompts/`. To re-create any specific pre-snapshot, look up the source table in `manifest.csv` and run the corresponding `mig_NNN` script from the repo's history at the noted commit.

## Replay SQL

The drops executed (preserved for the audit trail):

```sql
-- 11 schema drops + 3 main orphan drops, in this order
DROP TABLE IF EXISTS "Thyroid 2026 UPdated"."main"."v_mete_ptc_analytic_archived_20260422";
DROP TABLE IF EXISTS "Thyroid 2026 UPdated"."main"."note_entities_staging_archived_20260422";
DROP TABLE IF EXISTS "Thyroid 2026 UPdated"."main"."canonical_patient_master_pre_cleanup_20260422";
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."verify_legacy_20260422" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."llm_invasion_legacy_20260425" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."note_entities_llm_legacy_20260422" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."tier2_legacy_20260422" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."manuscript_workspace_legacy_20260422" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."molecular_legacy_20260421" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."cpm_tirads_legacy_20260421" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."us_legacy_20260421" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."archive_legacy" CASCADE;
DROP SCHEMA IF EXISTS "Thyroid 2026 UPdated"."archive_pub_v1_0" CASCADE;
```

## Data NOT in this audit

This is a manifest-only audit (schema, table, row count, column count). The actual table contents (~6M rows) are not exported because:
- They'd be many GB of Parquet, too large for git without LFS.
- Source-of-truth for re-creation is the migration scripts in the repo, not a frozen snapshot.

If you need the row data exported (e.g., for an external audit), tell me and I'll re-export to Parquet via Git LFS or an external blob store before continuing.
