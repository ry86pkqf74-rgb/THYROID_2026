# Cursor Prompt — Operative Pathology Consolidation

**Context for Cursor:** You are working on the Emory thyroid cancer lakehouse in MotherDuck (database: `thyroid_canonical_publication_v1_0`, primary schema: `main`, view schema: `views_readable`, archive schema: `archive_pub_v1_0`). This is a consolidation build — read the decision record at `/Users/ros/THyroid 2026/op_path_consolidation_report_20260421.md` before writing any SQL.

Follow the canonical naming convention (`canonical_<domain>_events_v1` / `canonical_<domain>_patient_rollup_v1`, view suffix `_VIEW_v1`). Match the close-out pattern used by Script 360 (frozen section) and Script 347 (labs).

---

## Design Decisions (signed off 2026-04-21)

1. **Benign grain: WIDE** — one row per synoptic report (per `path_synoptics` row), with all benign histology flags as columns.
2. **Thyroiditis: KEEP SEPARATE FLAGS** — do not collapse `nlp_hashimotos_*`, `nlp_lymphocytic_thyroiditis`, `nlp_chronic_thyroiditis`. Carry each as its own column on the benign events table.
3. **Parathyroid + thyroid lobe morphology: ONE UNIFIED TABLE** — `canonical_path_gland_events_v1` covers both thyroid lobes (left/right/isthmus) AND parathyroid glands 1–6 as rows in the same table, keyed by `gland_type` + `gland_position`. Not on the benign events table.
4. *(merged into #3)*
5. **Malignant table: WHATEVER IS CLEANEST** — Claude's call. **Recommendation: rename-in-place** via `ALTER TABLE canonical_tumor_characteristics_v1 RENAME TO canonical_path_malignant_events_v1` after archiving a snapshot; merge in the 4 discordance flags from `tumor_episode_master_v2` as new columns, then archive v2.
6. **AJCC7/AJCC8 staging: KEEP on malignant events** — do not split out.

---

## Build Scope — Script Number: `361_op_path_consolidation.py`

Produce **one Python build script** under `scripts/` that executes as idempotent steps. Use the project's standard MotherDuck connection helper. At the top, define `SCRIPT_ID = "361"` and `BUILD_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")`.

### Step 0 — Pre-flight & archive
Before any writes:
```sql
-- Archive every table that will be modified or deprecated, all to archive_pub_v1_0
CREATE TABLE archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.canonical_tumor_characteristics_v1;
CREATE TABLE archive_pub_v1_0.canonical_benign_diagnosis_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.canonical_benign_diagnosis_v1;
CREATE TABLE archive_pub_v1_0.canonical_malignant_diagnosis_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.canonical_malignant_diagnosis_v1;
CREATE TABLE archive_pub_v1_0.canonical_diagnosis_unified_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.canonical_diagnosis_unified_v1;
CREATE TABLE archive_pub_v1_0.tumor_episode_master_v2_pre361_{BUILD_TS} AS
  SELECT * FROM main.tumor_episode_master_v2;
CREATE TABLE archive_pub_v1_0.synoptic_tumor_long_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.synoptic_tumor_long_v1;
CREATE TABLE archive_pub_v1_0.path_outcome_classification_v1_pre361_{BUILD_TS} AS
  SELECT * FROM main.path_outcome_classification_v1;
```

Assert row counts match live tables before proceeding. Abort with clear error if any count drops.

### Step 1 — Build `main.canonical_path_malignant_events_v1`

**Grain:** one row per tumor per surgery (inherits from `canonical_tumor_characteristics_v1`; expected ~6,700 rows / ~4,137 patients).

Approach:
1. `CREATE TABLE main.canonical_path_malignant_events_v1 AS SELECT * FROM main.canonical_tumor_characteristics_v1;` (then drop the original later in Step 7).
2. `ALTER TABLE` to add 4 discordance columns from `tumor_episode_master_v2`:
   - `discordance_histology_flag BOOLEAN`
   - `discordance_size_flag BOOLEAN`
   - `discordance_laterality_flag BOOLEAN`
   - `discordance_notes VARCHAR`
3. `UPDATE … FROM tumor_episode_master_v2` joining on `(research_id, surgery_episode_id, tumor_slot_ix)` — confirm join keys via `list_columns` before writing the UPDATE. If `tumor_slot_ix` is not present on both, fall back to `(research_id, surgery_episode_id)` and mark rows where v2 had multiple tumors.
4. **Pull linkage cols forward from `specimen_tumor_focus_v1`** — `ALTER TABLE` to add:
   - `specimen_focus_id VARCHAR`
   - `linkage_confidence_tier VARCHAR`
   - `linkage_score DECIMAL(29,3)`
   Then `UPDATE` joining on `(research_id, surgery_episode_id, tumor_ordinal)`. Verify column names via `list_columns` first — `canonical_tumor_characteristics_v1` may use `tumor_index` rather than `tumor_ordinal`. ~85% match expected (9,411/11,106); leave NULL where no match.
5. Keep all existing AJCC7/AJCC8 columns in place (per decision 6).
6. Add provenance columns if not present: `source_table VARCHAR`, `source_row_ix INTEGER`, `build_script VARCHAR DEFAULT '361'`, `build_ts TIMESTAMP`.

### Step 2 — Build `main.canonical_path_benign_events_v1`

**Grain:** one row per synoptic report (≈ one row per `path_synoptics` row; expected ~11,688 rows / ~10,871 patients, *including malignant patients with concomitant benign findings*).

Source: `main.path_synoptics` as the spine. Join `main.specimen_master_v1` on `(research_id, synoptic_row_ix)` for date/specimen metadata where available (77% coverage — leave NULL where missing, do NOT drop rows).

Columns to carry (all as separate flags — do not collapse):
- **Identity/linkage:** `research_id` (cast VARCHAR → BIGINT to match other canonicals), `synoptic_row_ix`, `surgery_episode_id` (nullable), `specimen_id` (nullable), `path_date`, `source_text_type`, `source_report_id`
- **Benign histology flags** (all from `path_synoptics`): `nlp_mng`, `nlp_multinodular_goiter`, `nlp_follicular_adenoma`, `nlp_hurthle_cell_adenoma`, `nlp_adenomatoid_nodule`, `nlp_colloid_nodule`, `nlp_cystic_change`, `nlp_graves`, `nlp_graves_disease`, `nlp_nifcp`, `nlp_nifp`, `nlp_nifpt`, `nlp_hyperplasia`, `nlp_normal_thyroid`, `nlp_nodular_hyperplasia`
- **Thyroiditis flags (kept separate per decision 2):** `nlp_hashimotos_thyroiditis`, `nlp_hashimotos`, `nlp_lymphocytic_thyroiditis`, `nlp_chronic_thyroiditis`, `nlp_riedels_thyroiditis`, `nlp_de_quervains_thyroiditis`, `nlp_granulomatous_thyroiditis`
- **Concomitant-malignancy indicator:** `has_concomitant_malignant_event BOOLEAN` — TRUE if the same `(research_id, surgery_episode_id)` has any row in `canonical_path_malignant_events_v1`; else FALSE.
- **Provenance:** `source_table = 'path_synoptics'`, `build_script = '361'`, `build_ts`

Before running the INSERT, `list_columns` on `path_synoptics` and validate every flag column exists — log warnings for any missing, coalesce to FALSE.

### Step 3 — Build `main.canonical_path_gland_events_v1` (UNIFIED — thyroid + parathyroid)

**Grain:** one row per anatomical gland per surgery. Long format. Covers thyroid lobes (left/right/isthmus/total) AND parathyroid glands (positions 1–6) in a single table, distinguished by `gland_type`.

Source:
- Thyroid lobes: `path_synoptics` `left_lobe_*`, `right_lobe_*`, `isthmus_*` size/weight/dimension columns + any specimen-level measurements from `specimen_master_v1`.
- Parathyroid: `path_synoptics` `parathyroid_gland_1..6_*` columns + any parathyroid tables found via `search_catalog('parathyroid')`. Pivot wide → long.

Columns:
- **Identity/linkage:** `research_id` (BIGINT), `surgery_episode_id`, `synoptic_row_ix`, `specimen_id` (nullable), `path_date`, `linkage_quality` ('full','synoptic_only','specimen_only','unlinked')
- **Gland identity:** `gland_type` VARCHAR — one of `'thyroid_lobe'` or `'parathyroid'`; `gland_position` VARCHAR — for thyroid: `'left'`,`'right'`,`'isthmus'`,`'total'`; for parathyroid: `'1'`–`'6'` (also accept laterality+position labels like `'right_upper'` if captured)
- **Measurements (shared schema, NULL where N/A):** `gland_length_cm` NUMERIC, `gland_width_cm` NUMERIC, `gland_depth_cm` NUMERIC, `gland_weight_g` NUMERIC, `gland_weight_mg` NUMERIC (parathyroid is typically reported in mg — keep both columns rather than coerce units)
- **Pathology:** `gland_pathology` VARCHAR (e.g., 'hyperplasia','adenoma','normal','malignant_involvement'), `gland_notes` VARCHAR
- **Specimen context:** `specimen_type` VARCHAR — hard-coded `'operative'` per decision 4
- **Provenance:** `source_table` VARCHAR (which source row this came from), `build_script = '361'`, `build_ts`

Build pattern:
1. Build a thyroid-lobe row set via UNPIVOT/UNION over the 3 lobe slots from `path_synoptics`.
2. Build a parathyroid row set via UNPIVOT/UNION over the 6 gland slots.
3. `UNION ALL` the two sets into the final table. Validate `gland_type` values are in {`'thyroid_lobe'`,`'parathyroid'`}.
4. Drop rows where ALL of `gland_length_cm`, `gland_width_cm`, `gland_depth_cm`, `gland_weight_g`, `gland_weight_mg`, `gland_pathology`, `gland_notes` are NULL (no data from that slot).

### Step 5 — Patient rollup tables (3 total)

Follow the `canonical_*_patient_rollup_v1` pattern from Scripts 347/360. One row per `research_id`. For each rollup:

**`canonical_path_malignant_patient_rollup_v1`:** any_malignant_event BOOLEAN, n_malignant_surgeries, n_tumors_total, earliest_malignant_path_date, latest_malignant_path_date, highest_stage_ajcc8, highest_stage_ajcc7, any_ett BOOLEAN, any_metastasis BOOLEAN, dominant_histology VARCHAR. **Merge forward from `path_outcome_classification_v1` (before it's dropped):** `bethesda_final` INTEGER (max value across reports per patient), `bethesda_final_name` VARCHAR (corresponding to max), `regex_path_outcome` VARCHAR (preserve original regex classification label), `poc_tumor_1_histologic_type` VARCHAR — join on research_id, left-merged so no rows lost.

**`canonical_path_benign_patient_rollup_v1`:** any_benign_event BOOLEAN, n_benign_synoptics, any_mng BOOLEAN, any_hashimotos BOOLEAN, any_lymphocytic_thyroiditis BOOLEAN, any_graves BOOLEAN, any_follicular_adenoma BOOLEAN, earliest_benign_path_date, latest_benign_path_date, any_concomitant_malignant BOOLEAN (derived from the event-grain `has_concomitant_malignant_event`).

**`canonical_path_gland_patient_rollup_v1`:** covers both thyroid and parathyroid from the unified events table — `any_thyroid_lobe_measured` BOOLEAN, `total_thyroid_weight_g` (SUM across thyroid_lobe rows for most recent surgery), `left_lobe_max_dim_cm`, `right_lobe_max_dim_cm`, `any_isthmus_documented` BOOLEAN, `any_parathyroid_documented` BOOLEAN, `n_parathyroid_glands_documented`, `n_parathyroid_glands_abnormal`, `any_parathyroid_hyperplasia` BOOLEAN, `any_parathyroid_adenoma` BOOLEAN.

### Step 6 — Views (6 total) in `views_readable`

```sql
CREATE OR REPLACE VIEW views_readable.path_malignant_events_VIEW_v1 AS
  SELECT * FROM main.canonical_path_malignant_events_v1;
CREATE OR REPLACE VIEW views_readable.path_benign_events_VIEW_v1 AS
  SELECT * FROM main.canonical_path_benign_events_v1;
CREATE OR REPLACE VIEW views_readable.path_gland_events_VIEW_v1 AS
  SELECT * FROM main.canonical_path_gland_events_v1;
CREATE OR REPLACE VIEW views_readable.path_malignant_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_path_malignant_patient_rollup_v1;
CREATE OR REPLACE VIEW views_readable.path_benign_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_path_benign_patient_rollup_v1;
CREATE OR REPLACE VIEW views_readable.path_gland_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_path_gland_patient_rollup_v1;
```

### Step 7 — Deprecate old tables

After Steps 1–6 verify row counts match expectations:
```sql
DROP TABLE main.canonical_tumor_characteristics_v1;      -- superseded by malignant_events
DROP TABLE main.canonical_benign_diagnosis_v1;
DROP TABLE main.canonical_malignant_diagnosis_v1;
DROP TABLE main.canonical_diagnosis_unified_v1;
DROP TABLE main.tumor_episode_master_v2;                 -- discordance flags merged into malignant_events
DROP TABLE main.synoptic_tumor_long_v1;
DROP TABLE main.path_outcome_classification_v1;          -- bethesda/regex_outcome merged into malignant_patient_rollup
```

Also drop any `views_readable` views that referenced these — `search_catalog` for dependencies first. Archive copies from Step 0 remain in `archive_pub_v1_0` as the history.

### Step 8 — Update `detail_table_registry_v1`

Remember the registry schema drift from the memory file — query `information_schema.columns` for the actual column names before the INSERT. The filter column is `detail_table_name` and there are 3 extra columns beyond Scripts 247/236.

- DELETE rows where `detail_table_name` IN the 7 dropped tables.
- INSERT one row per new canonical (3 event tables + 3 rollups = 6 rows), with `domain = 'operative_pathology'`, `schema_name`, `detail_table_name`, `build_script = '361'`, `status = 'active'`, plus whatever the 3 extra columns are (fill from an existing active row as template).

### Step 9 — CPM feeder audit (report only, no writes in this script)

Print a report of every `nlp_*` column on `main.canonical_patient_master` that was previously fed by the 7 deprecated tables. Use `git grep` over `scripts/` to find the feeder — do not modify CPM in this script. Output the list to stdout and also append to `/Users/ros/THyroid 2026/op_path_cpm_feeder_audit_20260421.md`. A follow-up script will re-point CPM feeders.

### Step 10 — Zero-drift QA

Verify:
- `SUM(any_malignant_event)` on new rollup == patient count on old `canonical_malignant_diagnosis_v1` ± 0 (must match exactly).
- `SUM(any_benign_event)` on new rollup >= patient count on old `canonical_benign_diagnosis_v1` (will be GREATER because concomitant-benign-in-malignant-pts now captured — expected delta ~1,804 patients per the report).
- Row count on `canonical_path_malignant_events_v1` == row count on archived `canonical_tumor_characteristics_v1`.
- No `research_id` values lost anywhere — compare DISTINCT research_id set before/after across the union of old and new tables.
- **Path outcome preservation:** `SELECT COUNT(*)` on archived `path_outcome_classification_v1` == count of rows in `canonical_path_malignant_patient_rollup_v1` ∪ `canonical_path_benign_patient_rollup_v1` where `bethesda_final IS NOT NULL`. Verify Bethesda values exactly match the archive row-by-row (sample 100 patients).
- **Linkage column population:** `SELECT COUNT(*) FROM canonical_path_malignant_events_v1 WHERE specimen_focus_id IS NOT NULL` should be ≥ 9,000 (~85% of ~11,106 rows).

Write QA output to `qa_script_361_op_path_consolidation.json` under `qa/`.

---

## Gotchas (from the decision report — do not skip)

1. **Linkage completeness:** only 7,816/10,139 specimens (77%) have both `surgery_episode_id` AND `synoptic_row_ix`. Use LEFT JOIN and keep rows with NULL linkage. Flag these via a `linkage_quality` column: 'full', 'synoptic_only', 'specimen_only', 'unlinked'.
2. **Type coercion:** `research_id` is VARCHAR in `path_synoptics`, BIGINT in `specimen_master_v1`, INTEGER in `canonical_tumor_characteristics_v1`. Cast every INSERT target to BIGINT. Wrap in `TRY_CAST` with a warning log for any non-numeric research_ids.
3. **Raw-source rule:** do not re-extract from raw path notes in this script. All data already exists in `path_synoptics` and the inherited canonical — this is materialization, not NLP.
4. **Registry schema:** use `information_schema.columns` to introspect before the INSERT, per the `reference_detail_table_registry_schema.md` memory.
5. **Views layer:** views in `views_readable` may currently point at the about-to-be-dropped tables. Drop dependent views before dropping base tables; recreate in Step 6.
6. **PHI:** do not print any clinical notes or path report text to stdout. Research_id only.

---

## Git workflow

Per `feedback_commit_workflow.md`:
- Lint with `ruff check scripts/361_op_path_consolidation.py` before staging.
- Commit message: `361: consolidate operative pathology — 4 canonical event + 4 rollup tables, 6 deprecations`
- Push to `origin/main` after tests pass.

## Success criteria

- Script runs to completion idempotently (second run should be a no-op aside from timestamp updates).
- `qa_script_361_op_path_consolidation.json` all checks pass.
- Registry clean (7 out, 6 in).
- `archive_pub_v1_0` contains 7 pre-361 snapshots with matching row counts.
- CPM feeder audit report written.
- All 6 views resolve.
