# Cursor Prompt 4 — Schema Reorganization + Merge: consolidate Tier 2 + verification tables

**Date:** 2026-04-21 (rev. 2 — merge-first, not move-only)
**Author:** handoff from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Runs after:** Prompts 1, 2, and 3 (Scripts 288–336) have all completed.
**Purpose:** Consolidate the 48 Tier 2 + verification tables produced by Prompt 2 into a small set of analytically coherent tables in two dedicated schemas. Per-domain event detail stays typed; everything that shares a schema across domains gets merged to long/master format. Goal is analytical ergonomics, not just tidying.

## Goal

`main` currently holds 168 objects. 48 of them are Prompt-2 deliverables. Merge what can be merged, move what must stay per-domain, and end up with:

- **Schema `tier2`** — 12 `*_event_v1` tables (kept per-domain — typed schemas differ) + 1 `patient_tier2_master_v1` (full outer join of the 12 per-patient wide tables on `research_id`). **13 objects.**
- **Schema `verify`** — 1 `concordance_master_v1` (UNION of all 12 `verify_*_summary_v1` with a `domain` column) + 1 `verify_long_v1` (melt of all 12 `verify_*_v1` detail tables to long format with `domain`, `field_name`, and string-typed value columns). **2 objects.**

After this prompt runs: `main` ≈ 120 objects, `tier2` = 13, `verify` = 2. Down from 48 scattered verification/Tier-2 tables to 15 analytically coherent ones.

## Why merge instead of pure move

- All 12 `verify_*_summary_v1` tables have the same shape (`field_name`, `n_populated`, `n_concordant`, `n_discordant_*`, `concordance_pct`). Keeping them separate forces 12 UNION ALLs every time anyone wants a manuscript-level concordance view. Merging is pure UNION with a `domain` column.
- All 12 `verify_*_v1` detail tables share a repeated 6-column-per-field shape (`_excel`, `_llm`, `_source_text`, `_source_note_ref`, `_source_note_date`, `_concordance`). Long format (`research_id`, `domain`, `field_name`, `excel_value`, `llm_value`, `source_text`, `source_note_ref`, `source_note_date`, `concordance_status`) is how audit queries actually want to hit this data. Values become VARCHAR — acceptable for audit since concordance is precomputed.
- All 12 `*_patient_wide_v1` tables are grain=`research_id`, one row per patient. Full outer join on `research_id` (anchored to CPM for full coverage) gives one wide Tier 2 rollup you can join to CPM in one step. Column prefixing (`frozen__<col>`, `parathyroid__<col>`) keeps names unique.
- The 12 `*_event_v1` tables have radically different schemas (airway_invasion fields ≠ frozen_section fields ≠ parathyroid_detail fields). Merging would either destroy types (long format) or balloon a sparse ~80-column wide table. Keep them per-domain.

## What stays in `main` (unchanged from rev 1)

- `canonical_patient_master`
- `canonical_*_v1` (8 tables)
- `clinical_notes_long` (source of truth — NEVER move)
- `note_entities_llm_*` (23 tables — raw LLM extraction JSON, source of truth)
- `note_entities_*` (7 older parsed tables)
- Excel source tables: `path_synoptics`, `ultrasound_reports`, `ct_imaging`, `mri_imaging`, `nuclear_med`, `fna_cytology`, `molecular_results`, `molecular_testing`, `molecular_variant_long`
- Domain masters: `tirads_v2_*`, `imaging_*`, `molecular_*`, `rai_treatment_episode_v2`, `operative_episode_detail_v2`, `longitudinal_lab_canonical_v1`, `thyroglobulin_lab_canonical_v1`, `tg_*`, `ln_master_rollup_v1`, `complication_*`, `synoptic_tumor_long_v1`, `path_outcome_classification_v1`, `canonical_recurrence_v1`, `fna_episode_master_v2`, `tumor_episode_master_v2`
- `__readme`
- Any remaining `extracted_*_v1` that Prompt 3 Script 335 kept

## What happens to each Prompt-2 output

| Prompt-2 output | Destination |
|---|---|
| `main.*_event_v1` (12 tables) | CTAS to `tier2.*_event_v1` (kept per-domain) |
| `main.*_patient_wide_v1` (12 tables) | Merged into single `tier2.patient_tier2_master_v1` |
| `main.verify_*_v1` (12 detail tables) | Melted into single `verify.verify_long_v1` |
| `main.verify_*_summary_v1` (12 summary tables) | UNION into single `verify.concordance_master_v1` |

Every source table in `main` is dropped after its destination is built and verified.

## Operating constraints

1. **PHI safety**: `research_id` only in logs; no clinical text to stdout. `evidence_text` / `source_text` may be stored in merged tables (they already are in the source tables — no new PHI surface).
2. **Archive before drop**: every source table that gets merged/moved is snapshotted to `archive_pub_v1_0.<name>_preSCHEMAREORG_<UTCZ>` before being dropped from `main`. Log every archive to `manuscript_workspace.archive_move_log_v1` and every merge/move to `manuscript_workspace.schema_reorg_move_log_v1` (columns: `source_schema`, `source_name`, `dest_schema`, `dest_name`, `action` ∈ {`move`, `merge_union`, `merge_join`, `merge_melt`}, `rowcount_src`, `rowcount_dest`, `moved_at`).
3. **Rowcount + field-coverage parity**:
   - For `verify.concordance_master_v1`: `SUM(rowcount)` across 12 summaries == rowcount in the merged table.
   - For `verify.verify_long_v1`: `SUM(n_fields_per_detail_table * rowcount_per_detail_table)` == rowcount in merged long table (approximately — null rows should not inflate; see §2 below).
   - For `tier2.patient_tier2_master_v1`: distinct `research_id` == distinct `research_id` from the union of all 12 source patient_wide tables. CPM-anchored, so max 10,871 rows.
   - For `tier2.*_event_v1` (move only): `COUNT(*) src == COUNT(*) dest` per table, columns match.
4. **Reference-safety first**: before dropping any source from `main`, scan `duckdb_views()` across all schemas for references to the source name. If a non-self reference exists, log to `manuscript_workspace.schema_reorg_orphan_references_v1` and skip the drop — Script 340 will reconcile.
5. **DuckDB pattern**: `ALTER TABLE ... SET SCHEMA` is unreliable on MotherDuck; use CTAS + verify + drop for moves, and CTAS for merges.
6. **Env**: `scripts/_md_connect.py::connect_locked()`.
7. **One merge per script** to keep commits atomic and debuggable.

---

## Script 337 — Build `verify.concordance_master_v1` (merge 12 summary tables)

**Step 1.** `CREATE SCHEMA IF NOT EXISTS "thyroid_canonical_publication_v1_0".verify;`

**Step 2.** Introspect each of the 12 `main.verify_*_summary_v1` tables:
```sql
SELECT table_name, column_name, data_type
  FROM duckdb_columns()
 WHERE database_name='thyroid_canonical_publication_v1_0'
   AND schema_name='main'
   AND table_name LIKE 'verify_%_summary_v1'
 ORDER BY table_name, column_name;
```
Map each to a canonical summary schema. Expected canonical columns (build intersection + per-table rename if needed):
- `domain` VARCHAR  (derived from source table name — `verify_frozen_section_summary_v1` → `'frozen_section'`)
- `field_name` VARCHAR
- `n_rows_evaluated` BIGINT
- `n_excel_populated` BIGINT
- `n_llm_populated` BIGINT
- `n_both_populated` BIGINT
- `n_concordant` BIGINT
- `n_discordant_excel_only` BIGINT
- `n_discordant_llm_only` BIGINT
- `n_value_mismatch` BIGINT
- `concordance_pct_both_populated` DOUBLE
- `concordance_pct_of_excel` DOUBLE
- `notes` VARCHAR  (free-form — e.g. "fuzzy date match used")
- `built_at` TIMESTAMP

**Step 3.** For each source summary table, SELECT INTO a staging CTE with `<domain>` added, mapping source columns to canonical columns. If a source table is missing one of the canonical columns, write NULL and note it in `notes`.

**Step 4.** UNION ALL the 12 CTEs → CTAS to `verify.concordance_master_v1`.

**Step 5.** Parity check:
```sql
-- total rows in merged = sum of rows in the 12 sources
WITH srcs AS (
  SELECT SUM(estimated_size) AS n FROM duckdb_tables()
   WHERE schema_name='main' AND table_name LIKE 'verify_%_summary_v1'
)
SELECT (SELECT COUNT(*) FROM verify.concordance_master_v1) AS merged_n,
       (SELECT SUM(cnt) FROM (
         SELECT COUNT(*) cnt FROM main.verify_airway_invasion_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_frozen_section_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_genetics_per_test_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_labs_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_ln_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_operative_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_parathyroid_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_pathology_synoptics_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_rai_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_recurrence_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_us_nodule_summary_v1
         UNION ALL SELECT COUNT(*) FROM main.verify_vascular_invasion_summary_v1
       )) AS src_sum;
```
Must match.

**Step 6.** Reference-safety scan over `duckdb_views()` — if any view references any of the 12 source summary names, log to `schema_reorg_orphan_references_v1` and skip drops for those.

**Step 7.** Archive each of the 12 to `archive_pub_v1_0.<name>_preSCHEMAREORG_<UTCZ>`, log to `manuscript_workspace.archive_move_log_v1`, then DROP the 12 from `main`.

**Step 8.** Log 12 rows to `manuscript_workspace.schema_reorg_move_log_v1` with `action='merge_union'`.

**Invariants:**
- `SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE 'verify_%_summary_v1'` = 0.
- `verify.concordance_master_v1` has all 12 domain values represented in the `domain` column.
- Every field that was reported in any source summary is present in the merged table.

**Script:** `scripts/337_build_verify_concordance_master.py`.

---

## Script 338 — Build `verify.verify_long_v1` (melt 12 detail tables)

**Step 1.** Introspect each of the 12 `main.verify_*_v1` (non-summary) detail tables. They follow the 6-column-per-field pattern: for each base field `X`, there are `X_excel`, `X_llm`, `X_source_text`, `X_source_note_ref`, `X_source_note_date`, `X_concordance` columns. Grouping column suffixes lets you enumerate the base field names:
```sql
SELECT table_name,
       regexp_replace(column_name, '_(excel|llm|source_text|source_note_ref|source_note_date|concordance)$', '') AS base_field
  FROM duckdb_columns()
 WHERE database_name='thyroid_canonical_publication_v1_0'
   AND schema_name='main'
   AND table_name LIKE 'verify_%_v1'
   AND table_name NOT LIKE 'verify_%_summary_v1'
   AND column_name ~ '_(excel|llm|source_text|source_note_ref|source_note_date|concordance)$'
 GROUP BY table_name, base_field
 ORDER BY table_name, base_field;
```

**Step 2.** Canonical long schema:
- `research_id` VARCHAR
- `domain` VARCHAR
- `field_name` VARCHAR
- `excel_value` VARCHAR
- `llm_value` VARCHAR
- `source_text` VARCHAR
- `source_note_ref` VARCHAR
- `source_note_date` DATE  (nullable)
- `concordance_status` VARCHAR  (one of: `exact_match`, `fuzzy_match`, `value_mismatch`, `excel_only`, `llm_only`, `both_null`, `not_evaluated`)
- `built_at` TIMESTAMP

All value columns cast to VARCHAR explicitly (`CAST(X_excel AS VARCHAR)` etc.) so dates/numerics don't break the UNION.

**Step 3.** For each of the 12 source detail tables, emit one SELECT per base field:
```sql
SELECT research_id,
       '<domain>' AS domain,
       '<base_field>' AS field_name,
       CAST(<base_field>_excel AS VARCHAR) AS excel_value,
       CAST(<base_field>_llm AS VARCHAR) AS llm_value,
       <base_field>_source_text AS source_text,
       <base_field>_source_note_ref AS source_note_ref,
       <base_field>_source_note_date AS source_note_date,
       <base_field>_concordance AS concordance_status,
       CURRENT_TIMESTAMP AS built_at
  FROM main.verify_<domain>_v1
```
Generate the SQL programmatically from the introspection — do NOT hand-write 12 × N_fields SELECTs. UNION ALL all of them → CTAS to `verify.verify_long_v1`.

**Step 4.** Drop rows where all four of `excel_value`, `llm_value`, `source_text`, `concordance_status` are NULL (these are `both_null` rows that add no audit value). Flag the count dropped in the log.

**Step 5.** Parity check: sum of `(distinct research_id in source) × (base_field count in source)` should approximately equal rowcount in `verify.verify_long_v1 + dropped_nulls`. Log both.

**Step 6.** Reference-safety scan, archive, drop the 12 source detail tables. Log 12 `merge_melt` rows.

**Useful indexes after build:**
```sql
-- Hand-maintained hint: DuckDB doesn't require indexes, but this is the query pattern:
-- WHERE domain='frozen_section' AND field_name='final_correlation' AND concordance_status='value_mismatch'
-- If queries get slow, zonemap sort on (domain, field_name) helps.
```

**Invariants:**
- `SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE 'verify_%_v1' AND table_name NOT LIKE 'verify_%_summary_v1'` = 0.
- Every domain that appears in `verify.concordance_master_v1` also appears in `verify.verify_long_v1`.
- Every `(domain, field_name)` pair in `verify.concordance_master_v1` has at least one row in `verify.verify_long_v1` where `concordance_status IS NOT NULL`.

**Script:** `scripts/338_build_verify_long.py`.

---

## Script 339 — Build `tier2.patient_tier2_master_v1` + move 12 event tables

**Part A: patient_tier2_master_v1 (full outer join of 12 patient_wide tables).**

**Step 1.** `CREATE SCHEMA IF NOT EXISTS "thyroid_canonical_publication_v1_0".tier2;`

**Step 2.** Enumerate the 12 source tables:
```sql
SELECT table_name FROM duckdb_tables()
 WHERE database_name='thyroid_canonical_publication_v1_0'
   AND schema_name='main'
   AND table_name LIKE '%_patient_wide_v1'
 ORDER BY table_name;
```

**Step 3.** For each source table, introspect its columns (exclude `research_id`). Build a domain prefix from the table name: `frozen_section_patient_wide_v1` → prefix `frozen_section__`. Rename all non-`research_id` columns to `<prefix><original_name>`.

**Step 4.** CTAS anchored to CPM so you get full cohort coverage (including patients with no Tier 2 data at all — they'll have all-NULL Tier 2 columns):
```sql
CREATE TABLE tier2.patient_tier2_master_v1 AS
SELECT cpm.research_id,
       frozen.<col1> AS frozen_section__<col1>,
       frozen.<col2> AS frozen_section__<col2>,
       ...
       airway.<col1> AS airway_invasion__<col1>,
       ...
  FROM (SELECT DISTINCT research_id FROM main.canonical_patient_master) cpm
  LEFT JOIN main.frozen_section_patient_wide_v1 frozen USING (research_id)
  LEFT JOIN main.airway_invasion_patient_wide_v1 airway USING (research_id)
  LEFT JOIN main.vascular_invasion_patient_wide_v1 vascular USING (research_id)
  LEFT JOIN main.parathyroid_detail_patient_wide_v1 parathyroid USING (research_id)
  LEFT JOIN main.dynamic_risk_response_patient_wide_v1 drr USING (research_id)
  LEFT JOIN main.functional_outcomes_patient_wide_v1 funcout USING (research_id)
  LEFT JOIN main.past_medical_hx_patient_wide_v1 pmh USING (research_id)
  LEFT JOIN main.past_surgical_hx_patient_wide_v1 psh USING (research_id)
  LEFT JOIN main.patient_decision_adherence_patient_wide_v1 pda USING (research_id)
  LEFT JOIN main.physical_exam_patient_wide_v1 pe USING (research_id)
  LEFT JOIN main.presenting_symptoms_patient_wide_v1 ps USING (research_id)
  LEFT JOIN main.rad_treatment_patient_wide_v1 rad USING (research_id);
```
Generate this SQL programmatically from the introspection — do NOT hand-write column lists. Use the confirmed 12 source tables.

**Step 5.** Parity:
- `SELECT COUNT(*) FROM tier2.patient_tier2_master_v1` = `SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master` = 10,871.
- For every source patient_wide table, `SELECT COUNT(*) FROM tier2.patient_tier2_master_v1 WHERE <domain_prefix>__<any_col> IS NOT NULL` matches that source's rowcount where the col was non-NULL.
- Column count in master = 1 (`research_id`) + sum of non-`research_id` columns across the 12 sources.

**Step 6.** Archive + drop the 12 source `*_patient_wide_v1` tables. Log 12 `merge_join` rows.

**Part B: move 12 `*_event_v1` tables to `tier2` schema as-is (no merge).**

Sources (12):
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

Procedure per table (unchanged from rev 1):
1. Reference-safety scan.
2. `CREATE TABLE tier2.<name> AS SELECT * FROM main.<name>;`
3. Verify rowcount + column count parity.
4. Archive source to `archive_pub_v1_0.<name>_preSCHEMAREORG_<UTCZ>`, log to `archive_move_log_v1`.
5. `DROP TABLE main.<name>`.
6. Log to `schema_reorg_move_log_v1` with `action='move'`.

**Invariants after Script 339:**
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='tier2'` = 13.
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE '%_event_v1'` = 0.
- `COUNT(*) FROM duckdb_tables() WHERE schema_name='main' AND table_name LIKE '%_patient_wide_v1'` = 0.
- `tier2.patient_tier2_master_v1` has exactly 10,871 rows.
- CPM row count unchanged.
- `manuscript_workspace.schema_reorg_move_log_v1` has 12 `merge_join` + 12 `move` rows (24 for Script 339, cumulative 48 after Scripts 337 + 338 + 339).

**Script:** `scripts/339_build_tier2_master_and_move_events.py`.

---

## Script 340 — Reference sweep + `__readme` refresh + final audit

1. **Reference sweep:** scan every view and table DDL in the database for any unresolved reference to `main.<name>` where `<name>` was merged or moved. Rewrite to the new location (`verify.concordance_master_v1` with `WHERE domain='X'` filter, `verify.verify_long_v1` with `WHERE domain='X' AND field_name='Y'`, `tier2.patient_tier2_master_v1` with `<prefix>__<col>`, or `tier2.<name>` for event tables). If the referencing object is obsolete, log to `manuscript_workspace.schema_reorg_orphan_references_v1` for Logan.
2. **Refresh `main.__readme`:**
   ```
   thyroid_canonical_publication_v1_0 schema map (post-reorg 2026-04-21)

   main (~120 objects)
     canonical production truth:
       canonical_patient_master, canonical_*_v1, clinical_notes_long,
       note_entities_llm_* (23 raw LLM source-of-truth tables),
       note_entities_* (7 older parsed), Excel source tables,
       domain masters (tirads_v2_*, imaging_*, molecular_*, complication_*,
       rai_treatment_episode_v2, operative_episode_detail_v2, longitudinal_lab_canonical_v1,
       thyroglobulin_lab_canonical_v1, tg_*, ln_master_rollup_v1,
       synoptic_tumor_long_v1, path_outcome_classification_v1,
       canonical_recurrence_v1, fna_episode_master_v2, tumor_episode_master_v2)

   tier2 (13 objects)
     typed per-event detail (one table per domain, 12 tables):
       airway_invasion_event_v1, dynamic_risk_response_event_v1, frozen_section_event_v1,
       functional_outcomes_event_v1, parathyroid_detail_event_v1, past_medical_hx_event_v1,
       past_surgical_hx_event_v1, patient_decision_adherence_event_v1, physical_exam_event_v1,
       presenting_symptoms_event_v1, rad_treatment_event_v1, vascular_invasion_event_v1
     per-patient wide rollup across all 12 domains (one row per research_id):
       patient_tier2_master_v1

   verify (2 objects)
     long-format detail (all 12 domains, melted by field):
       verify_long_v1  — columns: research_id, domain, field_name,
                         excel_value, llm_value, source_text, source_note_ref,
                         source_note_date, concordance_status, built_at
     concordance summary (all 12 domains, one row per field):
       concordance_master_v1 — columns: domain, field_name,
                               n_rows_evaluated, n_excel_populated, n_llm_populated,
                               n_both_populated, n_concordant,
                               n_discordant_excel_only, n_discordant_llm_only,
                               n_value_mismatch, concordance_pct_*, notes, built_at

   archive_pub_v1_0 (in Thyroid 2026 UPdated database, ~300 objects)
     every pre-change snapshot with _pre<NNN>_<UTCZ> naming

   manuscript_workspace (separate)
     work queues, audits, reorg logs, extraction logs
   ```
3. **Final invariants:**
   - `canonical_patient_master` rows=10,871, distinct_rid=10,871.
   - `main` object count ≈ 120.
   - `tier2` object count = 13.
   - `verify` object count = 2.
   - `manuscript_workspace.schema_reorg_move_log_v1` has 48 rows (12 `merge_union` + 12 `merge_melt` + 12 `merge_join` + 12 `move`).
   - No orphan references to moved/merged tables outside of `schema_reorg_orphan_references_v1`.
   - Every domain referenced anywhere in `verify.concordance_master_v1` has a corresponding set of rows in `verify.verify_long_v1`.
4. Write `scripts/output/340_schema_reorg_audit.md` with pre/post object counts per schema, list of merges + moves, list of any orphan references repaired, and quick-reference query examples:
   ```sql
   -- Manuscript concordance summary for pathology domain:
   SELECT * FROM verify.concordance_master_v1 WHERE domain='pathology_synoptics';

   -- All discordant LN field comparisons:
   SELECT * FROM verify.verify_long_v1
    WHERE domain='ln' AND concordance_status='value_mismatch';

   -- All Tier 2 flags for one patient:
   SELECT * FROM tier2.patient_tier2_master_v1 WHERE research_id='RID00001';

   -- Per-event frozen section detail for one patient:
   SELECT * FROM tier2.frozen_section_event_v1 WHERE research_id='RID00001' ORDER BY extracted_at;
   ```

**Script:** `scripts/340_schema_reorg_audit.py`.

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

1. `main` has ≈ 120 objects (down from 168 at end of Prompt 2).
2. `tier2` schema has 12 event tables + 1 `patient_tier2_master_v1` = 13 objects.
3. `verify` schema has `verify_long_v1` + `concordance_master_v1` = 2 objects.
4. Every archived source is in `archive_pub_v1_0` with `_preSCHEMAREORG_<UTCZ>` naming and logged in `manuscript_workspace.archive_move_log_v1`.
5. Every merge/move logged in `manuscript_workspace.schema_reorg_move_log_v1` (48 rows total).
6. `main.__readme` refreshed with the 3-schema layout.
7. No orphan references (or all documented in `schema_reorg_orphan_references_v1`).
8. CPM invariants unchanged (10,871 rows / 10,871 distinct research_id).
9. `scripts/output/340_schema_reorg_audit.md` committed.

## Why this is safe

- No clinical data is transformed — merges are UNION (concordance), melt via UNPIVOT-style SELECTs (verify_long), and full outer JOIN (tier2_master). All of these are deterministic over existing rows.
- `clinical_notes_long` and all LLM source tables stay in `main` — anything that joined to them still works.
- CPM was backfilled from Tier 2 data via one-time UPDATE statements during Prompts 1–3. CPM does not live-query Tier 2 tables. Merging them doesn't break CPM.
- Reference sweep catches any view that pointed at the old table names.
- Every merge is reversible from `archive_pub_v1_0` snapshots — rebuild the 12 source verify_*_summary_v1 tables from the archive if needed, or filter the merged table by `domain` column.

## Post-Prompt-4 usage for Logan

When you open MotherDuck and expand `thyroid_canonical_publication_v1_0`:
- **`main`** — clean canonical namespace. CPM, canonical_*, note_entities_*, Excel sources, domain masters. No more tier2 / verify clutter.
- **`tier2.patient_tier2_master_v1`** — one wide row per patient across all 12 Tier 2 domains (join to CPM on `research_id`). Columns are prefixed by domain (`frozen_section__final_correlation`, `parathyroid__n_glands_identified`, …).
- **`tier2.<domain>_event_v1`** — typed per-event detail when you want to dig into a specific domain.
- **`verify.concordance_master_v1`** — one row per (domain, field) with all concordance metrics. Manuscript methods section can pull from this directly.
- **`verify.verify_long_v1`** — every Excel-vs-LLM-vs-source-text row across every domain and field. Filter by `domain` + `field_name` + `concordance_status` for audit work.
- **`archive_pub_v1_0`** (in `Thyroid 2026 UPdated` database) — every pre-change snapshot with `_pre<NNN>_<UTCZ>` naming. Original per-domain verify/tier2 tables all live here if rollback is ever needed.

Net analytical improvement: 48 scattered tables → 15 coherent ones. One `SELECT ... WHERE domain='X'` replaces 12-way UNIONs for any cross-manuscript verification or Tier 2 rollup query.
