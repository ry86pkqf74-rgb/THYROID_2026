# mig_294b — Consumer audit: `canonical_patient_master.nlp_tirads_max_category`

**Date:** 2026-05-04 (UTC)  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck, `connect_locked`)

## MD view / catalog scan

### `information_schema.views`

```sql
SELECT table_catalog, table_schema, table_name
FROM information_schema.views
WHERE view_definition ILIKE '%nlp_tirads_max_category%'
ORDER BY 1,2,3;
```

**Result:** 0 rows.

### `duckdb_views()` (all attached databases)

```sql
SELECT database_name, schema_name, view_name
FROM duckdb_views()
WHERE sql ILIKE '%nlp_tirads_max_category%'
ORDER BY 1,2,3;
```

**Result:** 0 rows.

## Repo consumers (post-mig)

| Area | Action |
|------|--------|
| `scripts/runpod_402_tirads_granular_qwen25_rerun.py` | Rollup now merges clean TR1–TR5 into `tirads_resolved` via `COALESCE(nlp_clean_tr, c.tirads_resolved)`; no longer writes dropped column. |
| `scripts/212_nlp_entity_rollup.py` | Targets legacy DB `thyroid_ete_fix_20260413` / `canonical_patient_master_v1` — unchanged. |
| `qc_framework_v1/migrations/*` (feasibility strings) | Textual references in UPDATE strings only; no CPM DDL dependency. |

## SSOT

Use **`tirads_resolved`** on `main.canonical_patient_master` for manuscript-grade TIRADS category (mig_288).

**End of audit.**
