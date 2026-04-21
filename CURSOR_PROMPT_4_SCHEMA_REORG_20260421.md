# Cursor Prompt 4 — Schema Reorganization: move Tier 2 + verification tables out of `main`

**Date:** 2026-04-21
**Author:** handoff from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Runs after:** Prompts 1, 2, and 3 (Scripts 288–336) have all completed.
**Purpose:** Move the 48 Tier 2 + verification tables produced by Prompt 2 out of `main` into two dedicated schemas so `main` contains only canonical production truth + source tables. Pure reorganization — no data transforms, no archiving, no new tables.

## Goal

`main` currently holds 168 objects. 48 of them are Prompt-2 deliverables (Tier 2 parses + side-by-side verify tables) that clutter the canonical namespace. Move them into two dedicated schemas so Logan can query `main` and see only production truth:

- **Schema `tier2`** — the 12 `*_event_v1` tables + 12 `*_patient_wide_v1` tables (24 total). These are typed parses of `note_entities_llm_*.result_json`.
- **Schema `verify`** — the 24 `verify_*` + `verify_*_summary_v1` tables. These are the Excel-vs-LLM-vs-source-text comparison tables.

After this prompt runs, `main` should drop from 168 → ~120 objects (all core canonical + source + LLM entity tables).

## What stays in `main`

- `canonical_patient_master`
- `canonical_*_v1` (8 tables)
- `clinical_notes_long` (source of truth — NEVER move)
- `note_entities_llm_*` (23 tables — raw LLM extraction JSON, source of truth)
- `note_entities_*` (7 older parsed tables)
- Excel source tables: `path_synoptics`, `ultrasound_reports`, `ct_imaging`, `mri_imaging`, `nuclear_med`, `fna_cytology`, `molecular_results`, `molecular_testing`, `molecular_variant_long`
- Domain masters: `tirads_v2_*`, `imaging_*`, `molecular_*`, `rai_treatment_episode_v2`, `operative_episode_detail_v2`, `longitudinal_lab_canonical_v1`, `thyroglobulin_lab_canonical_v1`, `tg_*`, `ln_master_rollup_v1`, `complication_*`, `synoptic_tumor_long_v1`, `path_outcome_classification_v1`, `canonical_recurrence_v1`, `fna_episode_master_v2`, `tumor_episode_master_v2`
- `__readme`
- Any remaining `extracted_*_v1` that Prompt 3 Script 335 kept

## What moves

- `main.<anything>_event_v1` → `tier2.<same_name>`
- `main.<anything>_patient_wide_v1` → `tier2.<same_name>`
- `main.verify_<anything>` (including `_summary_v1`) → `verify.<same_name>`

## Operating constraints

1. **PHI safety**: research_id only; no clinical text in stdout.
2. **No data transforms**: this is move only. `SELECT COUNT(*)` and column count must be identical pre-move and post-move for every table.
3. **Archive the move itself**: log every move to `manuscript_workspace.schema_reorg_move_log_v1` with `(source_schema, source_name, dest_schema, dest_name, rowcount_src, rowcount_dest, moved_at)`.
4. **MotherDuck schema move pattern**: DuckDB does not support `ALTER TABLE ... SET SCHEMA` reliably across MotherDuck. Use CTAS + verify + drop:
   ```sql
   CREATE SCHEMA IF NOT EXISTS "thyroid_canonical_publication_v1_0".<dest_schema>;
   CREATE TABLE "thyroid_canonical_publication_v1_0".<dest_schema>.<name> AS
     SELECT * FROM "thyroid_canonical_publication_v1_0".main.<name>;
   -- verify rowcount + column count parity
   DROP TABLE "thyroid_canonical_publication_v1_0".main.<name>;
   ```
5. **Reference-safety first**: before dropping from main, scan all views/tables in all schemas for references to the source name. If a reference exists outside this move batch, log and skip. This is identical to the Script 325 pattern.
6. **One schema per script**: Script 337 handles `tier2`, Script 338 handles `verify`. Keeps commits clean.
7. **Env**: `scripts/_md_connect.py::connect_locked()`.

---

## Script 337 — Move Tier 2 tables to `tier2` schema

**Sources (24 tables, expected from Prompt 2 Phase A):**

Event tables (12):
- `airway_invasion_event_v1`
- `dynamic_risk_response_event_v1`
- `frozen_section_event_v1`
- `functional_outcomes_event_v1`
- `parathyroid_detail_event_v1`
- `past_medical_hx_event_v1`
- `past_surgical_hx_event_v1`
- `patient_decision_adherence_event_v1`
- `physical_exam_event_v1`
- `presenting_symptoms_event_v1`
- `rad_treatment_event_v1`
- `vascular_invasion_event_v1`

Patient-wide tables (12) — enumerate dynamically:
```sql
SELECT table_name FROM duckdb_tables()
 WHERE database_name = 'thyroid_canonical_publication_v1_0'
   AND schema_name = 'main'
   AND table_name LIKE '%_patient_wide_v1';
```

**Procedure per table:**
1. Reference-safety scan: query `duckdb_views()` across all schemas for any view definition containing the source table name. If any non-self reference found, log blocker and skip.
2. `CREATE SCHEMA IF NOT EXISTS "thyroid_canonical_publication_v1_0".tier2;`
3. CTAS into `tier2.<name>`.
4. Verify `COUNT(*) src = COUNT(*) dest` AND `column count src = column count dest`.
5. Log move to `manuscript_workspace.schema_reorg_move_log_v1` (create table if not exists with the columns listed in constraint 3).
6. `DROP TABLE main.<name>`.
7. Re-verify: `SELECT COUNT(*) FROM tier2.<name>` returns the same number.

**Invariants:**
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='tier2'` = 24.
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE '%_event_v1'` = 0.
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE '%_patient_wide_v1'` = 0.
- No CPM row count change.
- `manuscript_workspace.schema_reorg_move_log_v1` has exactly 24 rows for this script.

**Script:** `scripts/337_move_tier2_to_schema.py`.

---

## Script 338 — Move verify tables to `verify` schema

**Sources (expected 24 tables from Prompt 2 Phase B):**

Enumerate dynamically:
```sql
SELECT table_name FROM duckdb_tables()
 WHERE database_name = 'thyroid_canonical_publication_v1_0'
   AND schema_name = 'main'
   AND table_name LIKE 'verify_%'
 ORDER BY table_name;
```

Expected list (24):
- `verify_airway_invasion_v1`, `verify_airway_invasion_summary_v1`
- `verify_frozen_section_v1`, `verify_frozen_section_summary_v1`
- `verify_genetics_per_test_v1`, `verify_genetics_per_test_summary_v1`
- `verify_labs_v1`, `verify_labs_summary_v1`
- `verify_ln_v1`, `verify_ln_summary_v1`
- `verify_operative_v1`, `verify_operative_summary_v1`
- `verify_parathyroid_v1`, `verify_parathyroid_summary_v1`
- `verify_pathology_synoptics_v1`, `verify_pathology_synoptics_summary_v1`
- `verify_rai_v1`, `verify_rai_summary_v1`
- `verify_recurrence_v1`, `verify_recurrence_summary_v1`
- `verify_us_nodule_v1`, `verify_us_nodule_summary_v1`
- `verify_vascular_invasion_v1`, `verify_vascular_invasion_summary_v1`

**Procedure per table:** identical to Script 337.

**One wrinkle:** `verify_*_summary_v1` tables may reference `verify_*_v1` tables within their definition if they were built as views, not CTAS. Check first:
```sql
SELECT table_name, table_type FROM duckdb_tables()
 WHERE database_name='thyroid_canonical_publication_v1_0'
   AND schema_name='main' AND table_name LIKE 'verify_%';
```
If any are `VIEW`, move them LAST after all the `_v1` tables are relocated. DuckDB view definitions will break when the source moves — you'll need to recreate the view in the new schema with rewritten FROM clauses.

**Invariants:**
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='verify'` = 24 (or however many actually exist).
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE 'verify_%'` = 0.
- `manuscript_workspace.schema_reorg_move_log_v1` has 24 more rows for this script (cumulative 48).
- Every `verify_*_summary_v1` remains queryable from the new schema and returns the same concordance percentages it did in `main`.

**Script:** `scripts/338_move_verify_to_schema.py`.

---

## Script 339 — Reference sweep + `__readme` refresh + final audit

1. **Reference sweep**: scan every view and table DDL in the database for any unresolved reference to `main.<name>` where `<name>` was moved. Fix by rewriting the reference to the new schema, or if the referencing object is itself obsolete, flag it to `manuscript_workspace.schema_reorg_orphan_references_v1` for Logan's review.
2. **Refresh `main.__readme`**: regenerate the categorical map with the new 3-schema layout:
   ```
   main (120 objects) = canonical production truth + LLM/parsed source tables
   tier2 (24 objects) = typed per-event and per-patient pivots from LLM JSON
   verify (24 objects) = side-by-side Excel / LLM / source-text verification tables
   archive_pub_v1_0 (Thyroid 2026 UPdated, ~260 objects) = historical archive
   manuscript_workspace (separate) = work queues, audits, reorg logs
   ```
3. **Final invariants**:
   - `canonical_patient_master`: rows=10,871, distinct_rid=10,871.
   - `main` object count ≤ 125.
   - `tier2` object count = 24.
   - `verify` object count = 24 (or whatever Prompt 2 Phase B actually produced — if 22, that's fine).
   - `manuscript_workspace.schema_reorg_move_log_v1` total rows = 48 (or matches actual).
   - No orphan references to moved tables outside of `schema_reorg_orphan_references_v1`.
4. Write `scripts/output/339_schema_reorg_audit.md` with pre/post object counts per schema, list of moves, list of any orphan references repaired.

**Script:** `scripts/339_schema_reorg_audit.py`.

---

## Git discipline

Per script:
```bash
cd "/Users/ros/THyroid 2026"
git add scripts/<N>_*.py
python -m pyflakes scripts/<N>_*.py
git commit -m "Script <N>: <summary>"
git push origin main
```

## Definition of done

1. `main` has ≤ 125 objects (down from ~155 at end of Prompt 3).
2. `tier2` schema has 24 Tier 2 parse tables.
3. `verify` schema has 24 verification tables (or whatever count Prompt 2 actually produced).
4. Every move logged in `manuscript_workspace.schema_reorg_move_log_v1`.
5. `main.__readme` refreshed with 3-schema layout.
6. No orphan references (or all documented).
7. CPM invariants unchanged.
8. `scripts/output/339_schema_reorg_audit.md` committed.

## Why this is safe

- No data is transformed. CTAS + drop + re-verify is atomic per table.
- `clinical_notes_long` and all LLM source tables stay in `main` — anything that joined to them still works.
- CPM was backfilled from Tier 2 data via one-time UPDATE statements during Prompts 1–3. CPM does not live-query Tier 2 tables. Moving them doesn't break CPM.
- Reference sweep catches the rare view that did reference moved tables.
- Every move is reversible from `archive_pub_v1_0` snapshots (those haven't moved).

## Post-Prompt-4 usage for Logan

After this runs, when you open MotherDuck and expand `thyroid_canonical_publication_v1_0`:
- `main` — clean canonical namespace (CPM, canonical_*, note_entities_*, Excel sources, domain masters)
- `tier2` — the typed Tier 2 parses; query these when you want per-event detail (e.g. `SELECT * FROM tier2.frozen_section_event_v1 WHERE research_id=...`)
- `verify` — query these when you want side-by-side Excel/LLM/source-text comparison (e.g. `SELECT * FROM verify.verify_pathology_synoptics_v1 WHERE research_id=...`)
- `archive_pub_v1_0` (in the `Thyroid 2026 UPdated` database) — everything we've ever archived, with `_pre<NNN>_<UTCZ>` naming

No more "shit ton of tables in main" — you'll see the core production tables and can reach into `tier2`/`verify` when you need them.
