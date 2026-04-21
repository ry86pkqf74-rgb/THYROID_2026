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

### Script 298 — Final V1_0 lint / verification pass

After Scripts 288–297 complete:

1. Re-run all four CPM invariants.
2. Re-run the missing-data audit from `scripts/285_cpm_missing_data_provenance.py` (or its equivalent) and diff against the pre-run snapshot — target is ≥60% of previously-empty columns now populated.
3. Re-check column types against a hard-coded whitelist (no INTEGER where DATE/VARCHAR is correct).
4. Re-query the view layer (`views_readable.*`) and confirm every view still resolves (`SELECT COUNT(*) FROM views_readable.<view>` doesn't error).
5. Write `scripts/output/298_postcleanup_audit.md` summarizing all changes with absolute row counts and column-fill deltas.
6. Print `git log --oneline scripts/28[6-9]*.py scripts/29[0-8]*.py` to confirm all commits landed.

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
8. `manuscript_workspace.archive_move_log_v1` and `manuscript_workspace.cpm_backfill_log_v1` updated with full audit trail.
9. `scripts/output/298_postcleanup_audit.md` committed.
10. Every script committed individually and pushed to origin/main.
