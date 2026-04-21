# Cursor Prompt — Script 362: Operative Procedure Canonicalization (Narrow)

**Context for Cursor:** You are working on the Emory thyroid cancer lakehouse in MotherDuck (`thyroid_canonical_publication_v1_0`). This is the second of four planned consolidations. **Do not start this until Script 361 is fully deployed and QA-green.** Read `/Users/ros/THyroid 2026/op_procedure_consolidation_scoping_362.md` before writing any SQL — it captures the sign-off decisions.

Follow the canonical naming convention (`canonical_<domain>_events_v1` / `canonical_<domain>_patient_rollup_v1`, view suffix `_VIEW_v1`). Match the close-out pattern used by Scripts 347 / 360 / 361.

---

## Scope & decisions (locked)

Build 3 new canonicals from the operative procedure domain:
1. `main.canonical_operative_events_v1` — rename of `operative_episode_detail_v2` + enrichment; **keeps** invasion-adjacent flags for now (`gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `local_invasion_flag`) because Script 363 will strip them when the invasion canonical lands. `strap_muscle_involvement_flag` stays permanently (op-note / synoptic only per scoping Q3).
2. `main.canonical_operative_patient_rollup_v1` — NEW patient-grain.
3. `main.canonical_operative_procedure_codes_v1` — NEW mention-grain procedure table, one row per procedure mention from `note_entities_procedures` (21,691 present-mentioned rows across 4,723 patients; 9 normalized procedure values).

**Deprecations (single, with archive):** `operative_episode_detail_v2` (renamed).

**Keep as-is (upstream feeders):** `note_entities_procedures`, `note_entities_operative_detail`, `note_entities_llm_past_surgical_hx`.

---

## Build Scope — Script File: `362_operative_consolidation.py`

Define `SCRIPT_ID = "362"` and `BUILD_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")` at top. Idempotent steps. Use the project's standard MotherDuck connection helper.

### Step 0 — Pre-flight & archive

```sql
CREATE TABLE archive_pub_v1_0.operative_episode_detail_v2_pre362_{BUILD_TS} AS
  SELECT * FROM main.operative_episode_detail_v2;
```

Assert row count matches live table. Abort with clear error if count drops.

Also assert that Script 361's canonicals exist (`main.canonical_path_malignant_events_v1`, `main.canonical_path_benign_events_v1`, `main.canonical_path_gland_events_v1`) — if any are missing, abort.

### Step 1 — Build `main.canonical_operative_events_v1`

**Grain:** one row per surgery episode per patient (~11,773 rows / ~10,871 pts — same shape as source).

1. `CREATE TABLE main.canonical_operative_events_v1 AS SELECT * FROM main.operative_episode_detail_v2;` (then drop the original in Step 7).
2. Cast `research_id` to BIGINT if not already (it's currently INTEGER on source). Wrap in `TRY_CAST`.
3. `ALTER TABLE` to add enrichment columns from `note_entities_operative_detail`:
   - `op_detail_nerve_monitoring_n INT` — count of `entity_type='nerve_monitoring'` mentions for this surgery_episode_id
   - `op_detail_ebl_n INT` — count of `entity_type='ebl'` mentions
   - `op_detail_parathyroid_mgmt_n INT` — count of `entity_type='parathyroid_management'` mentions
   - `op_detail_intraop_complication_n INT` — count of `entity_type='intraop_complication'` mentions
   - `op_detail_reoperative_field_n INT`
   - `op_detail_total_mentions INT` — SUM across all 15 op_detail entity types
4. Populate enrichment columns via `UPDATE ... FROM (SELECT ... FROM note_entities_operative_detail GROUP BY research_id)` — link by `research_id`; if `episode_id` is populated on both, prefer that key. Confirm via `list_columns` first.
5. Add provenance columns: `build_script VARCHAR DEFAULT '362'`, `build_ts TIMESTAMP`, `source_table VARCHAR DEFAULT 'operative_episode_detail_v2'`.

### Step 2 — Build `main.canonical_operative_patient_rollup_v1`

**Grain:** one row per `research_id` (~10,871 rows).

Columns:
- `research_id` BIGINT PRIMARY KEY
- `n_surgeries` INT
- `n_total_thyroidectomies` INT — count where `procedure_normalized ILIKE '%total thyroidectomy%'`
- `n_hemithyroidectomies` INT — where `procedure_normalized ILIKE '%hemithyroidectomy%'` OR `'%lobectomy%'`
- `n_completion_thyroidectomies` INT
- `n_central_neck_dissections` INT (use `central_neck_dissection_flag`)
- `n_lateral_neck_dissections` INT (use `lateral_neck_dissection_flag`)
- `any_reoperative_field` BOOLEAN
- `any_parathyroid_autograft` BOOLEAN
- `total_parathyroid_autograft_count` INT (SUM of `parathyroid_autograft_count`)
- `total_parathyroid_identified_count` INT
- `total_parathyroid_resection` INT (SUM of `parathyroid_resection_flag`::INT across surgeries)
- `any_rln_monitoring` BOOLEAN
- `any_frozen_section` BOOLEAN (derived from `frozen_section_flag`)
- `any_frozen_section_malignant` BOOLEAN (from `frozen_section_any_malignant_flag`)
- `earliest_surgery_date` DATE
- `latest_surgery_date` DATE
- `mean_ebl_ml` DOUBLE
- `max_ebl_ml` DOUBLE
- `any_drain_placed` BOOLEAN
- `build_script VARCHAR DEFAULT '362'`, `build_ts TIMESTAMP`

Build via `CREATE TABLE ... AS SELECT research_id, COUNT(*) AS n_surgeries, ... FROM main.canonical_operative_events_v1 GROUP BY research_id`.

### Step 3 — Build `main.canonical_operative_procedure_codes_v1`

**Grain:** one row per procedure mention (~21,691 rows where `present_or_negated='present'`; exclude negated mentions).

Columns:
- `procedure_mention_id` VARCHAR — generated as `sha256(research_id || note_row_id || evidence_start)` for stability
- `research_id` BIGINT
- `note_row_id` VARCHAR
- `note_date` DATE (cast from VARCHAR via TRY_CAST)
- `note_type` VARCHAR
- `procedure_raw` VARCHAR (from `entity_value_raw`)
- `procedure_normalized` VARCHAR (from `entity_value_norm`) — expected values: `hemithyroidectomy`, `total_thyroidectomy`, `central_neck_dissection`, `completion_thyroidectomy`, `tracheostomy`, `laryngoscopy`, `modified_radical_neck_dissection`, `lateral_neck_dissection`, `parathyroid_autotransplant`
- `confidence` DOUBLE
- `evidence_span` VARCHAR
- `extraction_run_id` VARCHAR
- `linked_surgery_episode_id` BIGINT — nullable; link by `research_id` + temporal proximity to `canonical_operative_events_v1.surgery_date_native` within ±30 days; prefer exact `episode_id` match if populated on both
- `linkage_method` VARCHAR — 'exact_episode' | 'temporal_30d' | 'unlinked'
- `build_script`, `build_ts`

**No CPT mapping in this script** (deferred — `note_entities_procedures` doesn't have CPT codes). Leave `cpt_code` OUT for now; can be added later.

Source:
```sql
SELECT ...
FROM main.note_entities_procedures
WHERE present_or_negated = 'present'
```

### Step 4 — Views (3) in `views_readable`

```sql
CREATE OR REPLACE VIEW views_readable.operative_events_VIEW_v1 AS
  SELECT * FROM main.canonical_operative_events_v1;
CREATE OR REPLACE VIEW views_readable.operative_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_operative_patient_rollup_v1;
CREATE OR REPLACE VIEW views_readable.operative_procedure_codes_VIEW_v1 AS
  SELECT * FROM main.canonical_operative_procedure_codes_v1;
```

Before running these CREATE OR REPLACE, `search_catalog` for any existing views referencing `operative_episode_detail_v2` in `views_readable` and drop them first (they'll reference the about-to-be-renamed table).

### Step 5 — Deprecate old table

After Steps 1–4 verify row counts match expectations:

```sql
DROP TABLE main.operative_episode_detail_v2;   -- renamed to canonical_operative_events_v1 (archive snapshot preserved)
```

Also drop any `views_readable` views that still reference `operative_episode_detail_v2` — `search_catalog` for dependencies first.

### Step 6 — Update `detail_table_registry_v1`

Query `information_schema.columns` first to introspect the 3 extra columns (per the registry schema drift memory).

- DELETE row where `detail_table_name = 'operative_episode_detail_v2'`.
- INSERT 3 rows, one per new canonical, with `domain = 'operative_procedure'`, `schema_name = 'main'`, `detail_table_name`, `build_script = '362'`, `status = 'active'`, plus the 3 extra columns (fill from an existing active row as template).

### Step 7 — CPM feeder audit (report only)

Print a report of every `nlp_*` / `op_*` column on `main.canonical_patient_master` that was previously fed by `operative_episode_detail_v2`. Use `git grep -E 'operative_episode_detail_v2|from operative_episode' scripts/` to find the feeders. Output to stdout AND append to `/Users/ros/THyroid 2026/operative_cpm_feeder_audit_20260421.md`. Do not modify CPM in this script.

### Step 8 — Zero-drift QA

Verify and write results to `qa/qa_script_362_operative.json`:

1. Row count on `canonical_operative_events_v1` == row count on archived `operative_episode_detail_v2` (exact match).
2. DISTINCT `research_id` set on new events table == DISTINCT set on archive (no patient lost).
3. Row count on `canonical_operative_patient_rollup_v1` == DISTINCT `research_id` on events.
4. Row count on `canonical_operative_procedure_codes_v1` == `SELECT COUNT(*) FROM note_entities_procedures WHERE present_or_negated='present'` (expected ~21,691).
5. Linkage coverage report: `SELECT linkage_method, COUNT(*) FROM canonical_operative_procedure_codes_v1 GROUP BY 1` — print to stdout.
6. Procedure normalization sanity: `SELECT procedure_normalized, COUNT(*) FROM canonical_operative_procedure_codes_v1 GROUP BY 1 ORDER BY 2 DESC` — expect ~9 values matching: hemithyroidectomy, total_thyroidectomy, central_neck_dissection, completion_thyroidectomy, tracheostomy, laryngoscopy, modified_radical_neck_dissection, lateral_neck_dissection, parathyroid_autotransplant.
7. All 3 views resolve (`SELECT COUNT(*)` each).

---

## Gotchas

1. **research_id type on `operative_episode_detail_v2` is INTEGER, not BIGINT** — cast throughout.
2. **`note_entities_procedures.research_id` is BIGINT** — aligns with our target. Good.
3. **`note_date` on `note_entities_procedures` is VARCHAR** — TRY_CAST to DATE; log % of rows that fail to cast.
4. **Linkage precedence:** prefer `episode_id` column on `note_entities_procedures` if populated (match against `canonical_operative_events_v1.surgery_episode_id`). Fallback: same-research_id + note_date within ±30 days of `surgery_date_native`. Mark `linkage_method` accordingly. Expect roughly 60–75% exact_episode, rest temporal or unlinked.
5. **Registry schema drift:** use `information_schema.columns` to introspect before the INSERT (per `reference_detail_table_registry_schema.md` memory).
6. **Views dependency order:** must drop dependent views before `DROP TABLE operative_episode_detail_v2`.
7. **No invasion-flag removal in this script.** `gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `local_invasion_flag` stay on `canonical_operative_events_v1` for now — Script 363 will strip them when the cross-modal invasion canonical lands.
8. **PHI:** do not print any clinical notes or narrative text to stdout. research_id + aggregate counts only.

---

## Git workflow

Per `feedback_commit_workflow.md`:
- Lint with `ruff check scripts/362_operative_consolidation.py` before staging.
- Commit: `362: consolidate operative procedure — 3 canonicals, 1 deprecation, procedure_codes mention-grain`
- Push to `origin/main` after QA passes.

## Success criteria

- Script runs to completion idempotently (re-run is a no-op besides timestamp updates).
- `qa/qa_script_362_operative.json` all checks pass.
- Registry clean (1 out, 3 in).
- `archive_pub_v1_0` contains 1 pre-362 snapshot with matching row count.
- CPM feeder audit report written.
- All 3 views resolve.

---

## Next up after this

Script 363 — Cross-modal invasion findings canonical. Before writing that prompt, you will verify `note_entities_llm_airway_invasion` + `note_entities_llm_vascular_invasion` coverage across CT/MRI/nucmed modalities (per Q10). Script 363 will also strip invasion columns from `canonical_path_malignant_events_v1` (built in 361) and `canonical_operative_events_v1` (built here) to hit the "no redundancy" goal from Q9. Keep invasion columns intact in 362 — 363 handles the removal.
