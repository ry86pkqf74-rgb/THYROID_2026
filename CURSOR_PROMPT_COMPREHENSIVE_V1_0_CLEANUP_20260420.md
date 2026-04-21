# Cursor Prompt — Comprehensive `thyroid_canonical_publication_v1_0` Cleanup

**Date:** 2026-04-20
**Author:** handed off from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Supersedes:** `CURSOR_PROMPT_OPERATIVE_AND_GAPS_20260420.md` (absorbed in full)

## Goal (one sentence)

Leave `thyroid_canonical_publication_v1_0` with only **current, verified, fully-typed, fully-integrated** publication tables/views. Move every stale, snapshot, versioned-predecessor, or legitimately-archived object to `"Thyroid 2026 UPdated".archive_pub_v1_0` as backup. Close every known missing-data gap by **integrating data that is already parsed** (don't re-extract) and by **rebuilding tables whose source was itself archived**.

---

## Operating constraints (do NOT violate)

1. **PHI safety**: Never print clinical notes, names, MRNs, DOBs. `research_id` is the only patient identifier permitted in logs, samples, or print output.
2. **Never overwrite non-NULL v1 values**: for every `v1 ↔ v2` column-pair backfill, only fill where the v1 column IS NULL. Conflict rows (v1 non-NULL but wrong) require a separate, explicitly-approved override step — do NOT auto-resolve them.
3. **Archive before drop / replace**: Before `DROP TABLE` or `CREATE OR REPLACE TABLE`, write the pre-change copy to `"Thyroid 2026 UPdated".archive_pub_v1_0."<table>_pre<NNN>_<UTC_ISO_Z>"` (matching the existing archive naming convention — see examples below). Log the move to `manuscript_workspace.archive_move_log_v1`.
4. **Invariants checked pre + post every script that touches CPM**:
   - `SELECT COUNT(*) FROM main.canonical_patient_master = 10871`
   - `SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master = 10871`
   - `SELECT COUNT(*) WHERE fna_path_outcome IS NULL = 0`
5. **Backfill log**: every column-level change (backfill or type-change) logs a row to `manuscript_workspace.cpm_backfill_log_v1` with: `backfilled_at, cpm_column, source_description, threshold, n_rows_updated, n_distinct_rid, sample_values, script`.
6. **Commit workflow**: stage → lint Python (`python -m pyflakes`) → commit with descriptive message → push to origin. Every script commits individually so `git blame` remains readable.
7. **Confidence rule**: If you encounter a case that requires **judgment about clinical semantics** (e.g., whether an entity's `evidence_text` constitutes "present" vs "suspected", whether two complication tiers should collapse), STOP and write a queue table in `manuscript_workspace.*` for Logan's manual adjudication. Do NOT guess.
8. **Env / connection**: Use `scripts/_md_connect.py::connect_locked()` (MotherDuck auth via `motherduck.local.toml`). Cross-DB queries use fully-qualified names: `"Thyroid 2026 UPdated".archive_pub_v1_0.foo`.

---

## Database state today (2026-04-20)

| DB | Schema | Tables | Views |
|---|---|---|---|
| `thyroid_canonical_publication_v1_0` | `main` | 121 | 2 |
| `thyroid_canonical_publication_v1_0` | `manuscript_workspace` | 50 | 68 |
| `thyroid_canonical_publication_v1_0` | `views_readable` | 0 | 46 |
| `"Thyroid 2026 UPdated"` | `archive_pub_v1_0` | 226 | — |
| `"Thyroid 2026 UPdated"` | `archive_legacy` | 121 | — |

CPM invariants (confirmed): rows=10871, distinct_rid=10871, null_fna=0.
CPM column count: 1,536.
Backfill log already present: `manuscript_workspace.cpm_backfill_log_v1` (contains Scripts 286/287 entries).

---

## Work items (execute in order, commit each as its own script)

### Script 288 — Fix CPM column types (DDL bug)

**Problem.** Five CPM columns were declared `INTEGER` but are meant to hold dates/strings. They have been 100% NULL since creation and any backfill errors with `Conversion Error: Unimplemented type for cast (TIMESTAMP_NS -> INTEGER)` (this is exactly the error that blocked Script 286 for `biochemical_concern_first_date`).

| CPM column | Current type | Correct type | Correct-type sibling that is already populated |
|---|---|---|---|
| `biochemical_concern_first_date` | `INTEGER` | `DATE` | (none — derived in Script 224 from `canonical_recurrence_v1.recurrence_date`) |
| `path_stage_raw` | `INTEGER` | `VARCHAR` | `gm_path_stage_raw VARCHAR` |
| `recurrence_histology` | `INTEGER` | `VARCHAR` | `recurrence_histology_v2 VARCHAR` (118 pop) |
| `recurrence_site_primary` | `INTEGER` | `VARCHAR` | `recurrence_site_v2 VARCHAR` (100 pop) |
| `rai_scan_findings_v9` | `INTEGER` | `VARCHAR` | (none — see Script 292) |

**Approach (conservative — columns are 100% NULL anyway):**

```python
ALTER_PLAN = [
    ("biochemical_concern_first_date", "DATE"),
    ("path_stage_raw",                 "VARCHAR"),
    ("recurrence_histology",           "VARCHAR"),
    ("recurrence_site_primary",        "VARCHAR"),
    ("rai_scan_findings_v9",           "VARCHAR"),
]
for col, new_type in ALTER_PLAN:
    # Sanity: must be 100% NULL before we retype
    n_nonnull = con.execute(
        f'SELECT COUNT(*) FROM main.canonical_patient_master WHERE "{col}" IS NOT NULL'
    ).fetchone()[0]
    assert n_nonnull == 0, f"{col} has {n_nonnull} non-NULL — refuse to retype"
    con.execute(
        f'ALTER TABLE main.canonical_patient_master ALTER COLUMN "{col}" SET DATA TYPE {new_type}'
    )
```

**Also fix `canonical_recurrence_v1`** which has the same DDL bug (INTEGER where it means VARCHAR / DATE for `recurrence_date`, `recurrence_site`, `recurrence_histology`). Confirm 100% NULL, then retype. If NOT 100% NULL, STOP and report.

**Post-verify**: re-run `duckdb_columns()` and confirm the types. Commit as `288_fix_cpm_ddl_types.py`.

---

### Script 289 — Backfill CPM recurrence text fields from `_v2` siblings

**Finding (confirmed 2026-04-20).** `recurrence_histology_v2` (118 pop), `recurrence_site_v2` (100 pop), and `recurrence_date_v2` (189 pop) are already populated by Script 224 but never written back to their v1 siblings. This is the same pattern as Script 287 surgery consolidation.

**Fills** (after Script 288 retypes the v1 columns):

| v1 column (target, retyped) | v2 source | Expected fill |
|---|---|---|
| `recurrence_histology` | `recurrence_histology_v2` | ~118 |
| `recurrence_site_primary` | `recurrence_site_v2` | ~100 |
| (no v1 date field) | `recurrence_date_v2` → already `recurrence_date TIMESTAMP` populated separately | n/a |

**Policy**: mirror Script 287 — UPDATE WHERE `v1 IS NULL AND v2 IS NOT NULL`. Log each column to `cpm_backfill_log_v1`. Invariants pre + post. Commit as `289_cpm_recurrence_v1_to_v2_consolidation.py`.

---

### Script 290 — Backfill `biochemical_concern_first_date` from `canonical_recurrence_v1`

**Source logic** (from Script 224, lines 795–801):

```sql
SELECT research_id, recurrence_date AS biochemical_concern_first_date
  FROM main.canonical_recurrence_v1
 WHERE recurrence_type IN ('biochemical_tg_rise', 'persistent_biochemical_disease')
```

Plus the Script 286 fallback (Tg > 2 ng/mL surveillance windows):

```sql
SELECT research_id, MIN(window_first_date) AS biochemical_concern_first_date
  FROM main.tg_postop_surveillance_windows_v1
 WHERE analyte = 'Tg' AND value_max > 2.0 AND window_first_date IS NOT NULL
 GROUP BY research_id
```

**Policy**:
- Prefer `canonical_recurrence_v1` (clinically adjudicated).
- For research_ids NOT in canonical_recurrence_v1 but with Tg>2 windows, use the Tg-window date.
- UPDATE WHERE `biochemical_concern_first_date IS NULL` only.
- Invariants pre + post.

Commit as `290_cpm_biochemical_concern_first_date.py`.

---

### Script 291 — Integrate TSH from `note_entities_llm_labs` into `longitudinal_lab_canonical_v1`

**Finding (confirmed 2026-04-20).** `longitudinal_lab_canonical_v1` has TSH for only 413 patients. `note_entities_llm_labs` has 886 rows (861 distinct patients) with TSH values in `result_json`. This is **parsed-but-not-integrated** data — the LLM already extracted it; nobody loaded it into the canonical table.

**Sample `result_json`** (already verified):
```json
{"entity_type":"tsh","entity_value":"0.96 mIU/L","entity_date":null,"confidence":0.95,...}
```

**Integration approach** (preserve provenance):

```sql
-- 1. Extract TSH entities from JSON (one row per entity)
CREATE TEMP TABLE _tsh_llm AS
WITH src AS (
  SELECT research_id, note_id, note_date,
         CAST(json_extract(result_json, '$.entities') AS VARCHAR[]) AS arr
    FROM main.note_entities_llm_labs
   WHERE result_json IS NOT NULL
)
SELECT research_id, note_id, note_date,
       json_extract_string(e, '$.entity_type')  AS entity_type,
       json_extract_string(e, '$.entity_value') AS entity_value,
       json_extract_string(e, '$.entity_date')  AS entity_date,
       TRY_CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence
  FROM src, UNNEST(arr) AS t(e)
 WHERE LOWER(json_extract_string(e, '$.entity_type')) = 'tsh';

-- 2. Parse numeric value + unit from entity_value string ("0.96 mIU/L" -> 0.96)
--    Use regexp_extract for the leading number. Document any rows where parsing fails
--    as a queue table `manuscript_workspace.llm_tsh_parse_queue_v1` for Logan review.

-- 3. Upsert into longitudinal_lab_canonical_v1 with source='llm_notes'
--    (add that provenance value; keep existing rows untouched).
--    Dedup: if a research_id/date already has a TSH from a higher-priority source,
--    do NOT overwrite.
```

**Safety**:
- Do not delete or overwrite any existing `longitudinal_lab_canonical_v1` rows.
- Tag LLM-sourced rows with `source='llm_notes'` (or whichever provenance column exists — confirm via `duckdb_columns()` first).
- Log summary: `n_new_rows, n_distinct_rid_added, n_parse_failures`.
- If parse failures > 5% of rows, STOP and write the queue table instead.

Commit as `291_tsh_llm_integration.py`.

---

### Script 292 — Rebuild `operative_episode_detail_v2` from `note_entities_operative_detail`

**Finding (confirmed 2026-04-20).** The original source table (`operative_details`) no longer exists in V1_0 — it was archived in the Phase 3/4 cleanup. The current `operative_episode_detail_v2` rows hardcode detail flags to `FALSE` (see `scripts/22_canonical_episodes_v2.py` lines 588–668):

```sql
-- From the stale build SQL:
FALSE AS rln_monitoring_flag,
NULL::VARCHAR AS rln_finding_raw,
FALSE AS parathyroid_autograft_flag,
FALSE AS gross_ete_flag,
FALSE AS local_invasion_flag,
FALSE AS tracheal_involvement_flag,
FALSE AS esophageal_involvement_flag,
```

**New source**: `main.note_entities_operative_detail`. Confirmed entity types present include `esophageal_involvement` (2 rows / 2 patients). Before building, enumerate **all** distinct entity types with counts:

```sql
SELECT entity_type, COUNT(*) n, COUNT(DISTINCT research_id) n_rid
  FROM main.note_entities_operative_detail
 GROUP BY 1 ORDER BY n DESC;
```

Map each to the corresponding flag/text column on `operative_episode_detail_v2`. For every mapping decision, emit a one-line comment in the script explaining the semantic (e.g., "tracheal_involvement → tracheal_involvement_flag=TRUE when entity_value_norm='present'").

**Rebuild approach**:
1. ARCHIVE the current `operative_episode_detail_v2` to `"Thyroid 2026 UPdated".archive_pub_v1_0.operative_episode_detail_v2_pre288_<UTCZ>` (see archive section below for exact SQL).
2. Build a staging table `_op_detail_rebuild_v1` that joins `operative_episode_detail_v2` (for episode skeleton: research_id, surgery_date, procedure) with rolled-up entities per (research_id, surgery_date).
3. Roll up entities by taking the most-confident `present`/`TRUE` per (research_id, surgery_date, entity_type).
4. Write `CREATE OR REPLACE TABLE main.operative_episode_detail_v2 AS SELECT ... FROM _op_detail_rebuild_v1`.
5. Verify episode skeleton is identical to pre-rebuild (same row count, same research_id × surgery_date set) — if not, STOP.
6. Update any dependent CPM columns (`op_esophageal_inv_any`, `op_tracheal_inv_any`, `op_rln_monitoring_any`, etc.) with the same v1-NULL-only policy.

Commit as `292_rebuild_operative_episode_detail_v2.py`.

---

### Script 293 — Integrate RAI scan findings into `rai_scan_findings_v9`

**Finding (confirmed 2026-04-20).** `note_entities_llm_rai_detailed` has 11,037 rows / 5,641 patients. Entity type breakdown:

| entity_type | count |
|---|---|
| treatment_episode_number | 701 |
| rai_dose_mci | 634 |
| **post_treatment_wbs_findings** | **559** |
| preparation_method | 434 |
| pre_rai_tsh | 338 |
| side_effects | 287 |
| rai_indication | 285 |
| isolation_days | 273 |
| pre_rai_tg | 122 |
| stunning_concern | 31 |
| rai_date_administered | 30 |
| diagnostic_i123_scan | 25 |
| cumulative_rai_dose | 22 |
| rai_administration | 5 |
| no_subsequent_rai_treatment | 1 |

`post_treatment_wbs_findings` (559 rows) is the target for `rai_scan_findings_v9` (currently 100% NULL, type was INTEGER — retyped VARCHAR in Script 288).

**Approach**:

```sql
CREATE TEMP TABLE _rai_findings AS
WITH src AS (
  SELECT research_id, CAST(json_extract(result_json,'$.entities') AS VARCHAR[]) AS arr
    FROM main.note_entities_llm_rai_detailed
   WHERE result_json IS NOT NULL
),
ents AS (
  SELECT research_id,
         json_extract_string(e,'$.entity_type')  AS entity_type,
         json_extract_string(e,'$.entity_value') AS entity_value,
         json_extract_string(e,'$.entity_date')  AS entity_date,
         json_extract_string(e,'$.evidence_text') AS evidence_text,
         TRY_CAST(json_extract_string(e,'$.confidence') AS DOUBLE) AS confidence
    FROM src, UNNEST(arr) AS t(e)
)
SELECT research_id,
       STRING_AGG(DISTINCT entity_value, ' | ' ORDER BY entity_value) AS findings
  FROM ents
 WHERE entity_type = 'post_treatment_wbs_findings'
   AND entity_value IS NOT NULL
 GROUP BY research_id;

UPDATE main.canonical_patient_master c
   SET rai_scan_findings_v9 = f.findings
  FROM _rai_findings f
 WHERE c.research_id = f.research_id
   AND c.rai_scan_findings_v9 IS NULL;
```

Also consider rolling up the other useful entities into dedicated CPM columns (pre_rai_tsh, pre_rai_tg, rai_dose_mci_per_episode) — enumerate and propose before writing, don't auto-add columns.

Commit as `293_rai_findings_integration.py`.

---

### Script 294 — Derive `path_stage_raw` / `gm_path_stage_raw` from `path_synoptics`

**Finding (confirmed 2026-04-20).** `path_synoptics` has tumor_1..5 × {t,n,m,stage_group} × {ajcc7, ajcc8} fully parsed (tumor_1_t_stage_ajcc8 populated for 4,041 patients). The `path_stage_raw` field is meant to be the concatenated per-tumor stage string — trivially derivable.

**Approach**:

```sql
CREATE TEMP TABLE _path_stage AS
SELECT research_id,
       -- dominant tumor AJCC8 stage_group as primary
       COALESCE(tumor_1_stage_group_ajcc8, tumor_1_stage_group_ajcc7) AS stage_primary,
       -- concatenation across tumors for multifocal
       CONCAT_WS(' | ',
         COALESCE(tumor_1_t_stage_ajcc8, tumor_1_t_stage_ajcc7),
         COALESCE(tumor_2_t_stage_ajcc8, tumor_2_t_stage_ajcc7),
         COALESCE(tumor_3_t_stage_ajcc8, tumor_3_t_stage_ajcc7),
         COALESCE(tumor_4_t_stage_ajcc8, tumor_4_t_stage_ajcc7),
         COALESCE(tumor_5_t_stage_ajcc8, tumor_5_t_stage_ajcc7)
       ) AS t_stages_concat
  FROM main.path_synoptics;

UPDATE main.canonical_patient_master c
   SET path_stage_raw = COALESCE(p.stage_primary, p.t_stages_concat)
  FROM _path_stage p
 WHERE c.research_id = p.research_id
   AND c.path_stage_raw IS NULL
   AND COALESCE(p.stage_primary, p.t_stages_concat) IS NOT NULL;
```

Mirror for `gm_path_stage_raw` (already VARCHAR) where NULL.

Commit as `294_path_stage_raw_from_synoptics.py`.

---

### Script 295 — Re-run VC complication tiering to classify the 159 untiered rows

**Finding (confirmed 2026-04-20).** `complication_phenotype_v1` has 88 VC paralysis + 71 VC paresis rows, all stuck at `final_complication_status='absent_or_unconfirmed'` even though 32 of those have positive flags. The phenotyping script (`235_parathyroid_calcium_fix.py`) never classified vocal_cord entities — only parathyroid.

**Approach**:
1. READ `235_parathyroid_calcium_fix.py` to learn the tiering pattern (evidence text + temporal + flag-combination rules).
2. Extend the pattern to `vocal_cord_paralysis` and `vocal_cord_paresis` phenotypes.
3. Materialize a queue table `manuscript_workspace.vc_complication_tiering_v1` with the **proposed** tier per row BEFORE updating `complication_phenotype_v1`. Include all evidence text (NO PHI — evidence_text in this table is already redacted-per-pipeline; confirm by sampling 5 rows and checking for PHI before the bulk update).
4. Pause the script with a `--commit` gate. In dry-run mode, print the queue summary by proposed-tier.
5. On `--commit`, UPDATE `complication_phenotype_v1` AND roll up to CPM's `comp_vc_paralysis_evidence_tier`, `comp_vc_paresis_evidence_tier` columns.

Commit as `295_vc_complication_tiering.py`.

---

### Script 296 — Resolve the 598 `n_surgeries` v1↔v2 conflicts

**Finding (from Script 287 run).** 598 patients have `n_surgeries_v1 = 1` but `n_surgeries_v2 ≥ 2`. Script 287 correctly refused to auto-resolve these (v1 was wrong, not merely missing). User has confirmed patients with up to 6 surgeries exist — `n_surgeries_v2` is authoritative.

**Approach**:
1. Materialize the conflict set into `manuscript_workspace.n_surgeries_v1_v2_conflict_v1` (one row per research_id with v1, v2, surgery dates, confidence).
2. For rows where `n_surgeries_v2` is supported by ≥2 distinct `patient_surgery_dates_rebuilt_v1` dates (note: this source was archived — substitute `operative_episode_detail_v2.first_surgery_date`/`second_surgery_date_v2` non-null-date count).
3. Under `--commit`, UPDATE `n_surgeries` := `n_surgeries_v2` only for those well-supported rows. Log every change.

Commit as `296_n_surgeries_v1_v2_conflict_resolve.py`.

---

### Script 297 — Archive stale objects from V1_0 to `archive_pub_v1_0`

**Candidates (confirmed 2026-04-20)**:

| Source object (v1_0 DB) | Reason | Archive name (suggested) |
|---|---|---|
| `main.note_entities_llm_synoptic_pathology_enrichment__march2026_broken` | Explicit `_broken` suffix | `note_entities_llm_synoptic_pathology_enrichment__march2026_broken_pre297_<UTCZ>` |
| `main._molecular_patient_rollup_v227` | Leading `_`, versioned | `_molecular_patient_rollup_v227_pre297_<UTCZ>` |
| `main.data_dictionary_v279` | Versioned (check for newer v280+ first; if none, keep) | (conditional) |
| `main.path_size_adjudication_v241` | Versioned adjudication — check if still referenced by CPM | (conditional) |
| `main.ret_note_entity_adjudication_v226` | Versioned; check references | (conditional) |
| `main.ret_patient_adjudicated_v226` | Versioned; check references | (conditional) |
| `manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417` | Snapshot | `archive` |
| `manuscript_workspace.manuscript_dive_map_v1_pre272_snapshot` | Snapshot | `archive` |
| `manuscript_workspace.view_definitions_snapshot_bigcleanup` | Snapshot | `archive` |
| `manuscript_workspace.collision_resolution_v265` | Versioned review | `archive` |
| `manuscript_workspace.cpm_cols_unmapped_review_v265` | Versioned review | `archive` |
| `manuscript_workspace.cpm_unmapped_triage_v266a` | Versioned review | `archive` |
| `manuscript_workspace.fusion_flag_unparsed_review_v265` | Versioned review | `archive` |
| `manuscript_workspace.fusion_parse_error_review_v265` | Versioned review | `archive` |
| `manuscript_workspace.ln_extract_noncohort_orphan_v279` | Versioned review | `archive` |
| `manuscript_workspace.registry_end_to_end_validation_v273` | Versioned validation | `archive` |
| `manuscript_workspace.registry_v2_resolution_audit_v273` | Versioned audit | `archive` |
| `manuscript_workspace.registry_v2_unresolved_pointers_v273` | Versioned review | `archive` |
| `manuscript_workspace.thin_wrapper_pi_review_v273` | Versioned review | `archive` |
| `manuscript_workspace.vc_paralysis_recalibration_v236` | Versioned review (will be superseded by Script 295) | `archive` |

Plus the `v1_1`-suffixed objects in `manuscript_workspace`:
`legacy_column_sweep_v1_1`, `nan_string_audit_v1_1`, `registry_normalization_review_v1_1`, `v1_1_finalization_audit_v1`, `v1_1_tech_debt_v1`.

**Reference-safety check (MANDATORY before archive)**:
```sql
-- For each candidate, enumerate views and tables that reference it:
SELECT DISTINCT d.database_name, d.schema_name, d.view_name
  FROM duckdb_views() d
 WHERE d.sql ILIKE '%<candidate>%';
-- If any non-archive reference found -> do NOT archive; report to Logan.
```

**Archive procedure (per object)**:
```sql
-- 1. Copy to archive DB (CTAS preserves types)
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.<new_name> AS
  SELECT * FROM thyroid_canonical_publication_v1_0.<schema>.<table>;

-- 2. Log to manuscript_workspace.archive_move_log_v1
INSERT INTO manuscript_workspace.archive_move_log_v1
  (moved_at, source_schema, source_table, dest_schema, dest_table, reason, script)
VALUES (CURRENT_TIMESTAMP, '<schema>', '<table>',
        'archive_pub_v1_0', '<new_name>', '<reason>', '297_archive_stale_objects');

-- 3. Drop from V1_0
DROP TABLE thyroid_canonical_publication_v1_0.<schema>.<table>;
```

Do each candidate in a separate transaction-equivalent (DuckDB doesn't have multi-statement transactions, so verify row count `src == dest` before DROP).

Commit as `297_archive_stale_objects.py`.

---

### Script 299 — Build `main.canonical_us_nodule_master_v1` (integrate scattered per-nodule US/TIRADS data)

**Problem (confirmed 2026-04-20).** US/TIRADS data is correct and present but **scattered across 12+ tables** with no integrated per-nodule master. The tables Logan already has:

| Table | rows | distinct_rid | Notable fields |
|---|---|---|---|
| `canonical_us_nodule_characteristics_v1` | 37,016 | 6,126 | Base per-nodule long format: laterality, nodule_index_within_exam, size_cm, composition, echogenicity, shape, margin, echogenic_foci, **tirads_score_2017**, **tirads_category_v2**, **tirads_level_2017**, tirads_reported |
| `imaging_nodule_master_v1` | 37,016 | 6,126 | Same grain — has `tirads_acr_recalculated`; **likely redundant duplicate** of canonical (verify and archive if so) |
| `tirads_v2_nodules_raw` | 11,914 | 3,021 | Rich v2 LLM: chammas_type, elastography_category, interval_growth_flag, extrathyroidal_extension_on_us, fna_recommended_this_nodule, prior_size_mm_max, **tirads_total_points**, composition_points, margin_points, shape_points, foci_points, echogenicity_points |
| `tirads_llm_extracted_v2` | 5,636 | 1,429 | Component points breakdown (composition_pts, margin_pts, shape_pts, foci_pts, echogenicity_pts, total_pts_2017, tirads_level_2017) |
| `note_entities_llm_tirads_granular` | 11,037 | 5,641 | `result_json` — **NOT YET PARSED into structured per-nodule columns** |
| `note_entities_llm_us_nodule_dynamics` | 11,037 | 5,641 | `result_json` — growth/dynamics per nodule, **NOT YET PARSED** |
| `imaging_fna_linkage_v3` | 9,911 | 1,938 | FNA-to-nodule linkage (nodule_id ↔ fna_episode_id) |
| `serial_imaging_us` | 4,162 | 1,443 | Longitudinal us_date, us_findings_impression |
| `ultrasound_reports` | 6,793 | 4,074 | Wide format nodule_1..nodule_14 — source of truth raw but already pivoted into `canonical_us_nodule_characteristics_v1` |
| `us_nodules_tirads` | 10,859 | — | **Legacy wide ingest** — archive candidate (superseded by long format) |

**Approach**:
1. Use `canonical_us_nodule_characteristics_v1` as the base grain. Declare the match key: `(research_id, exam_date, laterality, nodule_index_within_exam)`; if exam_date isn't present, use `imaging_exam_id` from `imaging_exam_master_v1` joined on its own grain.
2. **Parse the unparsed LLM JSON** into a per-nodule staging table first (do NOT re-extract; the entities already exist):
   ```sql
   CREATE OR REPLACE TABLE manuscript_workspace.tirads_granular_parsed_v1 AS
   WITH ent AS (
     SELECT research_id, note_id, extraction_timestamp,
            UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS entity_json
       FROM main.note_entities_llm_tirads_granular
      WHERE result_json IS NOT NULL
   )
   SELECT research_id, note_id, extraction_timestamp,
          json_extract_string(entity_json, '$.laterality')            AS laterality,
          CAST(json_extract_string(entity_json, '$.nodule_index') AS INTEGER) AS nodule_index_within_exam,
          json_extract_string(entity_json, '$.composition')           AS composition_llm,
          json_extract_string(entity_json, '$.echogenicity')          AS echogenicity_llm,
          json_extract_string(entity_json, '$.shape')                 AS shape_llm,
          json_extract_string(entity_json, '$.margin')                AS margin_llm,
          json_extract_string(entity_json, '$.echogenic_foci')        AS foci_llm,
          CAST(json_extract_string(entity_json, '$.size_cm') AS DOUBLE)         AS size_cm_llm,
          CAST(json_extract_string(entity_json, '$.tirads_points') AS INTEGER)  AS tirads_points_llm,
          json_extract_string(entity_json, '$.tirads_category')       AS tirads_category_llm,
          json_extract_string(entity_json, '$.evidence_text')         AS evidence_text_llm
     FROM ent;
   ```
   Mirror the same pattern for `note_entities_llm_us_nodule_dynamics` → `manuscript_workspace.us_nodule_dynamics_parsed_v1` (prior_size_mm, interval_growth_mm, dynamics_category, evidence_text).
3. Build `main.canonical_us_nodule_master_v1` by LEFT-joining everything onto the base. **Never overwrite a non-NULL base value** with an LLM/v2 value; use v2/LLM only where base is NULL. Columns (minimum):
   - Keys: research_id, imaging_exam_id, exam_date, laterality, nodule_index_within_exam, nodule_master_id (surrogate)
   - Dimensions: size_cm_max, size_cm_ap, size_cm_transverse, size_cm_longitudinal, volume_ml
   - Features: composition, echogenicity, shape, margin, echogenic_foci_type
   - TIRADS: tirads_points_total, tirads_category (ACR TI-RADS 2017), tirads_points_composition, tirads_points_echogenicity, tirads_points_shape, tirads_points_margin, tirads_points_foci, tirads_reported_in_note (raw), tirads_recalculated (from our points)
   - Dynamics: prior_size_mm_max, interval_growth_mm, interval_growth_flag, dynamics_category, has_prior_comparison
   - Advanced: chammas_type, elastography_category, extrathyroidal_extension_on_us, fna_recommended_this_nodule_on_report
   - FNA linkage: linked_fna_episode_id, linked_fna_date, linked_fna_cytology_bethesda
   - Provenance: source_base, source_tirads_v2, source_tirads_llm, source_dynamics_llm, source_fna_linkage (booleans indicating which sources contributed)
4. **Discordance queue** — write `manuscript_workspace.tirads_v1_v2_discordance_v1` for every nodule where `tirads_category_v1 != tirads_category_v2` (both non-NULL). Include all component points + evidence for adjudication. Do NOT auto-resolve.
5. **Invariants**: `COUNT(*) = COUNT(DISTINCT (research_id, imaging_exam_id, laterality, nodule_index_within_exam))`; `COUNT(DISTINCT research_id) ≈ 6,126 ± 5`; `SUM(tirads_category IS NOT NULL) / COUNT(*)` ≥ the current canonical rate (never regress fill).
6. Verify `imaging_nodule_master_v1` vs `canonical_us_nodule_characteristics_v1` — if semantically duplicate (same grain, overlapping field set, no unique info in nodule_master), queue `imaging_nodule_master_v1` for archival in Script 297's list (append via `INSERT INTO manuscript_workspace.archive_move_queue_v1`).

Commit as `299_canonical_us_nodule_master_v1.py`.

---

### Script 300 — Build `main.canonical_us_exam_master_v1` (per-exam rollup)

**Problem.** `imaging_exam_master_v1` exists (13,347 rows / 6,126 patients) but predates the integrated per-nodule master from Script 299.

**Approach**:
1. Roll `canonical_us_nodule_master_v1` up to the exam grain `(research_id, imaging_exam_id, exam_date)`.
2. Columns: n_nodules_on_exam, largest_nodule_cm, second_largest_nodule_cm, bilateral_flag (nodules in both lobes), isthmus_nodule_flag, **worst_tirads_category_this_exam**, **worst_tirads_points_this_exam**, max_growth_mm_this_exam, any_nodule_with_extrathyroidal_extension, any_nodule_fna_recommended_on_report, count_tr5_nodules, count_tr4_nodules, count_tr3_nodules, count_tr2_nodules, count_tr1_nodules.
3. Add longitudinal rank: `exam_rank_for_patient` (1 = earliest available US) and `is_preop_exam` (flag exams before `first_surgery_date` from CPM).
4. Cross-check: for each (research_id, exam_date) confirm nodule rows in Script 299's master ≥ `n_nodules_on_exam` reported here.
5. Diff output table `manuscript_workspace.exam_master_v1_vs_v0_diff` showing what changed vs legacy `imaging_exam_master_v1`; archive the legacy one on confirmation.

Commit as `300_canonical_us_exam_master_v1.py`.

---

### Script 301 — Build `main.canonical_us_patient_master_v1` (per-patient rollup)

**Problem.** CPM already has `imaging_tirads_best/worst` (32%) and `tirads_v2_worst_category` (23%) but these are fragments, not a full patient-grain master with provenance.

**Approach**:
1. Roll the exam master from Script 300 up to `(research_id)`, one row per patient.
2. Columns: has_any_us, n_us_exams, first_us_date, last_us_date, preop_us_available_flag, **max_tirads_category_ever**, **max_tirads_points_ever**, tirads_category_at_first_exam, tirads_category_at_last_preop_exam, n_nodules_total_across_exams, n_distinct_nodules_tracked (using the nodule_master_id when stable, else max-per-exam), bilateral_disease_flag_ever, multifocal_flag_ever (>1 nodule on same lobe), any_suspicious_nodule_ever (TR4 or TR5), any_nodule_with_extrathyroidal_extension_ever, longitudinal_growth_detected_flag, first_high_risk_tirads_date.
3. Backfill CPM columns from this master **conservatively** (only where CPM column is NULL):
   - `imaging_tirads_best` ← `max_tirads_category_ever` if CPM NULL
   - `imaging_tirads_worst` ← same
   - `tirads_v2_worst_category` ← same (if v2 NULL)
   - `max_tirads_ever` ← `max_tirads_category_ever`
   - `preop_tirads_best/_worst` ← `tirads_category_at_last_preop_exam`
4. Every backfill logs a row to `manuscript_workspace.cpm_backfill_log_v1` with `source_description='canonical_us_patient_master_v1'`.
5. **Invariants**: `COUNT(*) ≤ 10,871` (one row per patient, only tested patients get one); `SUM(has_any_us) = COUNT(DISTINCT research_id FROM canonical_us_nodule_master_v1)`.

Commit as `301_canonical_us_patient_master_v1.py`.

---

### Script 302 — Build `main.genetics_per_test_master_v1` (per-test drill-down master)

**Problem (confirmed 2026-04-20).** ~1,286 patients had molecular testing (Thyroseq/Afirma/other). The drill-down data — per-variant mutations, allele fractions, zygosity, CNAs, fusions, gene-expression signatures — exists scattered across 11 tables but **there is no integrated per-test master**. Logan has asked for this explicitly.

**Source tables**:

| Table | rows | distinct_rid | Critical fields |
|---|---|---|---|
| `molecular_test_episode_v2` | 10,650 | 10,026 | episode_id, test_date, platform, bethesda_category, **13 per-gene flags** (braf/ras/tert/tp53/pax8_pparg/alk/ntrk/ret/eif1ax/loh/cna/fusion/pik3ca/tshr), linked_fna_episode_id, linked_nodule_id, linked_surgery_episode_id |
| `molecular_results` | 10,861 | — | assay_name, panel_version, vendor, **raw_payload_json**, qc_flags |
| `molecular_variant_long` | 1,640 | 703 | **Per-variant:** gene_symbol, canonical_hgvs, cdna_hgvs, protein_hgvs, genomic_hgvs, allele_fraction, zygosity, fusion_partner, partner_gene_symbol, variant_class, interpretation_text, risk_call |
| `thyroseq_molecular_enrichment` | 10,861 | — | Per-gene flags (BRAF/RAS/TERT/TP53/PIK3CA/RET/NTRK/ALK/PPARG/TSHR) + **cna_raw/norm** + **fusion_genes_json** + **gep_raw/norm** + **allele_fractions_json** + pathology_raw |
| `canonical_molecular_tested_v1` | 1,286 | — | has_thyroseq, has_afirma, braf_positive_canonical, ras_positive_canonical, tert_positive_canonical, braf_variant_raw, ras_subtype_raw, molecular_risk_tier, platform_canonical |
| `note_entities_genetics` | 1,738 | 605 | entity_type, entity_value_norm, entity_value_raw, evidence_span, present_or_negated, confidence |
| `extracted_braf_recovery_v1` | 730 | 376 | braf_status, braf_variant, detection_method |
| `extracted_ras_patient_summary_v1` | 321 | — | ras_positive, ras_primary_subtype, allele_frequency_pct |

**Approach**:
1. Declare test grain: `(research_id, molecular_episode_id)`. Base is `molecular_test_episode_v2` filtered to `platform IS NOT NULL OR bethesda_category IS NOT NULL OR any per-gene flag IS NOT NULL` (gating out the 10,650 − 1,286 ≈ 9,364 placeholder rows if present; verify the real tested-episode count matches the ~1,286 patient count before proceeding).
2. LEFT-join `molecular_results` on the episode key to pull `assay_name, panel_version, vendor, raw_payload_json`.
3. Roll `molecular_variant_long` up to the episode grain as a JSON array of variants:
   ```sql
   WITH var AS (
     SELECT research_id, molecular_episode_id,
            LIST({
              'gene': gene_symbol,
              'hgvs_canonical': canonical_hgvs,
              'hgvs_protein': protein_hgvs,
              'hgvs_cdna': cdna_hgvs,
              'hgvs_genomic': genomic_hgvs,
              'allele_fraction': allele_fraction,
              'zygosity': zygosity,
              'variant_class': variant_class,
              'fusion_partner': fusion_partner,
              'partner_gene': partner_gene_symbol,
              'risk_call': risk_call,
              'interpretation': interpretation_text
            }) AS variants_json
       FROM main.molecular_variant_long
      GROUP BY research_id, molecular_episode_id
   )
   ```
4. LEFT-join `thyroseq_molecular_enrichment` (on research_id + best-effort episode match via test_date ± 14 days if episode_id is absent in enrichment — Logan to confirm the join key; if ambiguous, queue to `manuscript_workspace.genetics_enrichment_join_ambiguity_v1`) for `cna_raw/norm`, `fusion_genes_json`, `gep_raw/norm`, `allele_fractions_json`, `pathology_raw`.
5. Roll `note_entities_genetics` up to the episode grain (nearest test_date within ±30 days) as a JSON array of entity rollups.
6. Final columns of `main.genetics_per_test_master_v1`:
   - **Keys**: research_id, molecular_episode_id, test_date, test_rank_for_patient
   - **Platform**: platform (thyroseq/afirma/other), assay_name, panel_version, vendor, bethesda_category
   - **Clinical linkage**: linked_fna_episode_id, linked_fna_date, linked_nodule_id, linked_surgery_episode_id, specimen_site
   - **Per-gene flags** (from episode_v2): braf_positive_this_test, ras_positive_this_test, tert_positive_this_test, tp53_positive_this_test, pik3ca_positive_this_test, ret_point_positive_this_test, ret_fusion_positive_this_test, alk_fusion_positive_this_test, ntrk_fusion_positive_this_test, pax8_pparg_positive_this_test, eif1ax_positive_this_test, tshr_positive_this_test, any_loh_flag, any_cna_flag, any_fusion_flag
   - **Variant drill-down**: variants_json (LIST of structs as above), n_variants, n_variants_pathogenic, max_allele_fraction
   - **Enrichment**: cna_raw, cna_norm, fusion_genes_json, gep_raw, gep_norm, allele_fractions_json, pathology_raw
   - **LLM entities rollup**: genetics_entities_json
   - **Interpretation**: overall_result_class, molecular_risk_tier (low/intermediate/high), risk_call, detailed_findings_raw
   - **Raw**: raw_payload_json (full vendor report JSON when available)
   - **Provenance booleans**: source_episode_v2, source_results, source_variant_long, source_thyroseq_enrichment, source_note_entities, source_canonical_tested, source_braf_recovery, source_ras_summary
7. **Invariants**: `COUNT(DISTINCT research_id) ≈ 1,286 ± 10` (warn Logan if outside); `COUNT(*) ≥ COUNT(DISTINCT research_id)` (some patients have >1 test); `SUM(any per-gene flag is TRUE) ≤ COUNT(*)`; every row where `variants_json IS NOT NULL` must also have `n_variants > 0`.
8. Never coerce an untested/unknown to FALSE. If a per-gene flag cannot be determined, leave NULL; only populate TRUE/FALSE when the source explicitly says so.
9. **Discordance queue** `manuscript_workspace.genetics_per_test_discordance_v1` for cases where different sources disagree on the same test (e.g., episode_v2 says BRAF positive but variant_long has no BRAF variant with AF > 0).

Commit as `302_genetics_per_test_master_v1.py`.

---

### Script 303 — Build `main.genetics_per_patient_master_v1` (per-patient rollup) + archive duplicates

**Problem.** CPM has `braf_positive`, `ras_positive`, `tert_positive` forced to 100% fill by coercing untested → FALSE. This is wrong: untested patients should be NULL (or carry an explicit `was_tested=FALSE` flag), never FALSE on gene status. Logan needs a clean per-patient genetics master with explicit tested/untested distinction.

**Approach**:
1. Grain: one row per patient (full `canonical_patient_master` cohort — 10,871 rows). **Every patient gets a row**, with `was_tested=FALSE` for the ~9,585 untested patients and all gene-status fields NULL for them.
2. For tested patients, roll `genetics_per_test_master_v1` (Script 302) up to patient grain using worst/first/last logic per field:
   - `n_tests`, `first_test_date`, `last_test_date`, `test_platforms_list` (array), `has_thyroseq`, `has_afirma`, `has_other_platform`
   - For each gene (braf/ras/tert/tp53/pik3ca/ret_fusion/alk_fusion/ntrk_fusion/pax8_pparg/eif1ax/tshr): `<gene>_status` ∈ {positive, negative, indeterminate, NULL (untested)} — priority: any positive → positive; else any negative → negative; else indeterminate.
   - `braf_variants_list`, `ras_variants_list`, `tert_variants_list` — arrays of distinct variants from `variants_json`.
   - `any_high_risk_marker_flag` (TRUE if braf_positive OR tert_positive OR tp53_positive OR any_high_risk_fusion).
   - `molecular_risk_tier_final` (max tier across tests: high > intermediate > low > unknown).
   - `cna_summary_json`, `fusion_summary_json`, `gep_summary_json` — rolled up across tests.
   - `aggregated_findings_text` — a readable concatenation for manuscript writing (one line per test).
3. **Backfill CPM conservatively** only where CPM column is NULL **and** the source is unambiguous (do NOT overwrite the existing coerced-to-FALSE values since those are a separate category-coding choice already in use downstream):
   - Instead, write a new CPM column `genetics_master_v1_link_flag` and `genetics_master_v1_episode_count` referencing this master, rather than mutating existing genetics columns.
4. **Invariants**: `COUNT(*) = 10,871`; `COUNT(DISTINCT research_id) = 10,871`; `SUM(was_tested) ≈ 1,286`; for every patient `SUM(gene statuses IS NOT NULL) > 0 WHERE was_tested=TRUE`.
5. **Archive superseded tables** once this master is built and validated:
   - `main.canonical_molecular_tested_v1` → `archive_pub_v1_0.canonical_molecular_tested_v1_pre303_<UTCZ>` (superseded by per-patient master).
   - `main.us_nodules_tirads` (legacy wide) → `archive_pub_v1_0.us_nodules_tirads_pre303_<UTCZ>` (superseded by Script 299's long-format master).
   - If Script 299 confirmed `imaging_nodule_master_v1` is redundant, archive that too.
   - Follow the reference-safety check from Script 297 before each DROP. Log to `manuscript_workspace.archive_move_log_v1`.

Commit as `303_genetics_per_patient_master_v1.py`.

---

### Script 298 — Final V1_0 lint / verification pass

After Scripts 288–303 complete:

1. Re-run all four CPM invariants.
2. Re-run the missing-data audit from `scripts/285_cpm_missing_data_provenance.py` (or its equivalent) and diff against the pre-run snapshot — target is ≥60% of previously-empty columns now populated.
3. Re-check column types against a hard-coded whitelist (no INTEGER where DATE/VARCHAR is correct).
4. Re-query the view layer (`views_readable.*`) and confirm every view still resolves (`SELECT COUNT(*) FROM views_readable.<view>` doesn't error).
5. Verify the three new US masters exist, resolve, and pass grain invariants:
   - `main.canonical_us_nodule_master_v1`: `COUNT(*) = COUNT(DISTINCT (research_id, imaging_exam_id, laterality, nodule_index_within_exam))`
   - `main.canonical_us_exam_master_v1`: `COUNT(*) = COUNT(DISTINCT (research_id, imaging_exam_id))`
   - `main.canonical_us_patient_master_v1`: `COUNT(*) = COUNT(DISTINCT research_id) ≤ 10,871`
6. Verify the two new genetics masters exist, resolve, and pass grain invariants:
   - `main.genetics_per_test_master_v1`: `COUNT(DISTINCT research_id) ≈ 1,286 ± 10`; every row with `was_tested=TRUE` has at least one gene status populated.
   - `main.genetics_per_patient_master_v1`: `COUNT(*) = 10,871`; `SUM(was_tested) ≈ 1,286`; no untested patient has a TRUE/FALSE gene status (all NULL for untested).
7. Confirm discordance queues are non-empty where expected (`tirads_v1_v2_discordance_v1`, `genetics_per_test_discordance_v1`) and that none have been auto-resolved.
8. Write `scripts/output/298_postcleanup_audit.md` summarizing all changes with absolute row counts and column-fill deltas, plus a section enumerating every new master table with its row count, distinct_rid, and non-null coverage for flagship columns (TIRADS category, BRAF status, RAS status, TERT status).
9. Print `git log --oneline scripts/28[6-9]*.py scripts/29[0-9]*.py scripts/30[0-3]*.py` to confirm all commits landed.

Commit as `298_postcleanup_verification.py`.

---

## Git discipline

```bash
cd "/Users/ros/THyroid 2026"
# Per-script:
git add scripts/<N>_*.py
python -m pyflakes scripts/<N>_*.py   # must pass
git commit -m "Script <N>: <summary>"
git push origin main
```

If `git push` is rejected (non-fast-forward), use `git pull --rebase origin main` first; if the rebase reports uncommitted files, `git reset --soft origin/main && git reset HEAD && git add <targeted files> && git commit` rather than stashing (stash has historically failed with "patch too large" on this repo).

---

## What's NOT in scope for this prompt

- Touching `"Thyroid 2026 UPdated".archive_legacy.*` — that's the deeper legacy tier; leave alone.
- Editing `views_readable.*` definitions beyond making them resolve. Any new friendly-name views should be in a follow-up prompt.
- Re-extraction with an LLM — every fix here uses data **already present** in V1_0 (either in canonical tables or in `note_entities_*` JSON).
- Anything that requires a new OpenAI / Anthropic API call.

---

## Definition of done

1. All CPM columns with correct types (no misdeclared INTEGER).
2. Every v1↔v2 sibling column pair either reconciled (v1 ← v2 where v1 NULL) or explicitly queued for manual conflict review.
3. `operative_episode_detail_v2` rebuilt from live entity source (not hardcoded FALSE).
4. TSH + RAI findings loaded from the LLM JSON tables into canonical.
5. `path_stage_raw` derived from `path_synoptics`.
6. VC complication tiering extended; no more rows stuck at 'absent_or_unconfirmed' where evidence flags are positive.
7. All stale/snapshot/versioned-predecessor objects moved to `archive_pub_v1_0`; reference-safety check passed; V1_0 slimmed to its canonical set.
8. **`main.canonical_us_nodule_master_v1` exists** — one row per (research_id, imaging_exam_id, laterality, nodule_index) integrating every scattered US/TIRADS source (canonical_us_nodule_characteristics_v1 + tirads_v2_nodules_raw + tirads_llm_extracted_v2 + parsed LLM note entities for TIRADS granular + parsed LLM note entities for nodule dynamics + imaging_fna_linkage_v3) with TIRADS 2017 category, component points, dynamics, FNA linkage, and provenance.
9. **`main.canonical_us_exam_master_v1` and `main.canonical_us_patient_master_v1` exist** — per-exam and per-patient rollups with worst/best TIRADS, n_nodules, bilateral/multifocal flags, longitudinal rank; CPM TIRADS columns backfilled conservatively from the patient master.
10. **`main.genetics_per_test_master_v1` exists** — one row per molecular test episode for the ~1,286 tested patients, joining molecular_test_episode_v2 + molecular_results (assay/panel/raw JSON) + molecular_variant_long rollup (per-variant drill-down as JSON array with gene/HGVS/AF/zygosity) + thyroseq_molecular_enrichment (CNA, GEP, fusion panel, allele fractions) + note_entities_genetics rollup. Per-gene flags, variant-level detail, and risk calls all present.
11. **`main.genetics_per_patient_master_v1` exists** — one row per patient (full 10,871 cohort), explicitly distinguishes `was_tested=FALSE` from negative gene status (no coercion of untested to FALSE). Variants, platforms, and molecular risk tier rolled up. Duplicate/legacy genetics tables (`canonical_molecular_tested_v1`, legacy US wide `us_nodules_tirads`, and `imaging_nodule_master_v1` if confirmed redundant) archived to `archive_pub_v1_0`.
12. Discordance queues (`tirads_v1_v2_discordance_v1`, `genetics_per_test_discordance_v1`, `n_surgeries_v1_v2_conflict_v1`, `vc_complication_tiering_v1`) populated and awaiting Logan's adjudication — none auto-resolved.
13. `manuscript_workspace.archive_move_log_v1` and `manuscript_workspace.cpm_backfill_log_v1` updated with full audit trail.
14. `scripts/output/298_postcleanup_audit.md` committed.
15. Every script committed individually and pushed to origin/main.
