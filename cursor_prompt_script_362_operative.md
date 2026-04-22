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

### CLI flag contract (match Script 361)

Implement phase-gated execution with `argparse`:

- `--dry-run` (default) — plan + print SQL, no writes.
- `--commit` — actually execute writes.
- `--skip-drop` — run Steps 0–4, 6, 7, 8 but skip Step 5 (drop of deprecated table). For the initial build-and-verify commit.
- `--phase N` — run ONLY step N. `--commit --phase 5` is the isolated drop run that ties to a clean second commit.

Typical flow:
1. `python scripts/362_operative_consolidation.py --dry-run` → inspect planned SQL.
2. `python scripts/362_operative_consolidation.py --commit --skip-drop` → build everything, run QA, leave old table in place.
3. Verify QA JSON green, then `python scripts/362_operative_consolidation.py --commit --phase 5` → drop old table. Separate git commit tying drop to green QA SHA.

### Step 0 — Pre-flight & archive

**Script 361 dependency check.** Assert that Script 361's canonicals exist (`main.canonical_path_malignant_events_v1`, `main.canonical_path_benign_events_v1`, `main.canonical_path_gland_events_v1`) — if any are missing, abort.

**Column existence pre-flight** (per Script 361 Pattern 7 — slot-name validation). Before any writes, query `information_schema.columns` for `operative_episode_detail_v2` and confirm the following columns exist:

```
['research_id', 'surgery_episode_id', 'surgery_date_native',
 'procedure_normalized', 'central_neck_dissection_flag',
 'lateral_neck_dissection_flag', 'parathyroid_autograft_count',
 'parathyroid_identified_count', 'parathyroid_resection_flag',
 'frozen_section_flag', 'frozen_section_any_malignant_flag',
 'reoperative_field_flag', 'parathyroid_autograft_flag',
 'rln_monitoring_flag', 'drain_placed', 'ebl_ml',
 'gross_ete_flag', 'tracheal_involvement_flag',
 'esophageal_involvement_flag', 'strap_muscle_involvement_flag',
 'local_invasion_flag']
```

For any missing columns, emit `log_warn(f"source column missing: {col}")` and either (a) DROP that dependent rollup column or (b) emit a placeholder column with `placeholder_flag_cols VARCHAR[]` annotation (per Script 361 Pattern 5). Do NOT silently skip — log and document.

**Idempotent archive** (per Script 360 bd0d713 pattern). Check `information_schema.tables` in `"Thyroid 2026 UPdated".archive_pub_v1_0` for existing `operative_episode_detail_v2_pre362_%` snapshots:

```python
archive_name = f"operative_episode_detail_v2_pre362_{BUILD_TS}"
existing = conn.execute("""
    SELECT table_name FROM "Thyroid 2026 UPdated".information_schema.tables
    WHERE table_schema = 'archive_pub_v1_0'
      AND table_name = ?
""", [archive_name]).fetchone()
if existing:
    log_info(f"archive {archive_name} already exists — skipping")
else:
    conn.execute(f"""
        CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.{archive_name} AS
        SELECT * FROM main.operative_episode_detail_v2
    """)
```

Assert row count matches live table. Abort with clear error if count drops.

**Date-column type probe.** Query `information_schema.columns` for `surgery_date_native` type. If VARCHAR (not DATE/TIMESTAMP), downstream comparisons in Step 3 linkage need `TRY_CAST`. Log the detected type.

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
- `linkage_method` VARCHAR — 'exact_episode' | 'temporal_30d' | 'temporal_30d_ambiguous' | 'unlinked'
- `n_candidate_episodes` INT — count of surgery episodes within ±30 days for this (research_id, note_date) tuple. Computed via `COUNT(*) OVER (PARTITION BY research_id, note_date)` BEFORE the rn=1 pick filter (per Script 361 Pattern 2 — never `GROUP BY` after a rn=1 filter, it always returns 1).
- `linkage_ambiguous_multi_episode` BOOLEAN — `TRUE` when `n_candidate_episodes > 1` and `linkage_method != 'exact_episode'`. When TRUE, `linked_surgery_episode_id` is the deterministic pick (lowest `surgery_episode_id` among candidates) — consumers needing precision should filter on this flag (per Script 361 Pattern 1).
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

### Step 5 — Deprecate old table (runs only under `--phase 5` or full `--commit` without `--skip-drop`)

**Autonomous archive lookup** (per Script 361 Pattern 6 — do NOT depend on this run's `BUILD_TS`; the drop step should work even if invoked in a fresh session days later). Find the most recent matching archive snapshot:

```python
archive = conn.execute("""
    SELECT table_name
    FROM "Thyroid 2026 UPdated".information_schema.tables
    WHERE table_schema = 'archive_pub_v1_0'
      AND table_name LIKE 'operative_episode_detail_v2_pre362_%'
    ORDER BY table_name DESC
    LIMIT 1
""").fetchone()
assert archive, "no pre362 archive found — abort drop"
```

**Pre-drop safety gates (ALL must pass):**
1. `main.canonical_operative_events_v1` exists and `COUNT(*) > 0`.
2. `main.canonical_operative_patient_rollup_v1` exists and `COUNT(*) > 0`.
3. `main.canonical_operative_procedure_codes_v1` exists and `COUNT(*) > 0`.
4. Archive parity: `COUNT(*)` on `archive_pub_v1_0.<archive_name>` == `COUNT(*)` on current `main.operative_episode_detail_v2` (guard against drift between archive and live since Step 0 archive was taken).
5. No dependent views in `views_readable` still reference `operative_episode_detail_v2` — `search_catalog` for `FROM.*operative_episode_detail_v2` / `JOIN.*operative_episode_detail_v2` patterns; drop any found FIRST.

Then:
```sql
DROP TABLE main.operative_episode_detail_v2;   -- renamed to canonical_operative_events_v1 (archive snapshot preserved)
```

### Step 6 — Update `detail_table_registry_v1`

Query `information_schema.columns` first to introspect the 3 extra columns (per the registry schema drift memory).

- DELETE row where `detail_table_name = 'operative_episode_detail_v2'`.
- INSERT 3 rows, one per new canonical, with `domain = 'operative_procedure'`, `schema_name = 'main'`, `detail_table_name`, `build_script = '362'`, `status = 'active'`, plus the 3 extra columns (fill from an existing active row as template).

### Step 7 — CPM feeder audit (report only)

Print a report of every `nlp_*` / `op_*` column on `main.canonical_patient_master` that was previously fed by `operative_episode_detail_v2`. Use `git grep -E 'operative_episode_detail_v2|from operative_episode' scripts/` to find the feeders. Output to stdout AND append to `/Users/ros/THyroid 2026/operative_cpm_feeder_audit_20260421.md`. Do not modify CPM in this script.

### Step 8 — Zero-drift QA

Verify and write results to `qa/qa_script_362_operative.json`. Each check is a hard gate (blocks commit) unless marked INFORMATIONAL:

**Hard gates:**
1. `events_rowcount_matches_archive`: `COUNT(*)` on `canonical_operative_events_v1` == `COUNT(*)` on the pre362 archive (exact match).
2. `no_research_ids_lost`: DISTINCT `research_id` set on new events table == DISTINCT set on archive.
3. `patient_rollup_parity`: `COUNT(*)` on `canonical_operative_patient_rollup_v1` == `COUNT(DISTINCT research_id)` on events (per Script 360 Pattern 2 — rollup↔events hard gate; without this a filter-too-narrow bug silently drops patients).
4. `procedure_codes_rowcount_matches_source`: `COUNT(*)` on `canonical_operative_procedure_codes_v1` == `SELECT COUNT(*) FROM note_entities_procedures WHERE present_or_negated='present'` (expected ~21,691, exact match).
5. `procedure_linkage_exact_episode_geq_60pct`: `SUM(linkage_method='exact_episode') / COUNT(*) >= 0.60` on procedure_codes. If `episode_id` column doesn't exist on `note_entities_procedures`, fall back to `exact_episode OR temporal_30d (unambiguous) >= 0.70`.
6. `view_resolves_operative_events_VIEW_v1`, `view_resolves_operative_patient_rollup_VIEW_v1`, `view_resolves_operative_procedure_codes_VIEW_v1`: each view returns `COUNT(*) > 0`.

**Informational (log, don't gate):**
7. `procedure_linkage_method_distribution`: `SELECT linkage_method, COUNT(*) FROM canonical_operative_procedure_codes_v1 GROUP BY 1`.
8. `procedure_linkage_ambiguity_rate`: `SUM(linkage_ambiguous_multi_episode) / COUNT(linked_surgery_episode_id IS NOT NULL)`. Expect <20%; log if higher for follow-up (smarter tie-breakers warranted — e.g. procedure_normalized match against operative events procedure field).
9. `procedure_normalization_sanity`: `SELECT procedure_normalized, COUNT(*) FROM canonical_operative_procedure_codes_v1 GROUP BY 1 ORDER BY 2 DESC`. Expect ~9 values.
10. `placeholder_flag_cols`: print the list of columns that were placeholder-FALSE due to missing source columns (from Step 0 pre-flight). Should be empty for a clean build.
11. `varchar_date_parse_failures`: % of `note_date` rows where `TRY_CAST(note_date AS DATE) IS NULL`. Log for each VARCHAR date column.

---

## Gotchas

1. **research_id type on `operative_episode_detail_v2` is INTEGER, not BIGINT** — cast throughout.
2. **`note_entities_procedures.research_id` is BIGINT** — aligns with our target. Good.
3. **VARCHAR date fields need `TRY_CAST` everywhere, not `CAST`.** Per Script 361 Pattern 3: `note_date` on `note_entities_procedures` is VARCHAR; some rows are empty strings (`''`); plain `CAST(... AS DATE)` raises `Conversion Error: invalid date field format` and aborts the build. Use `TRY_CAST(note_date AS DATE) IS NOT NULL` filter to silently drop unparseable rows. ALSO check `surgery_date_native` type in Step 0 probe — if VARCHAR, apply same discipline in the Step 3 temporal join.
4. **Linkage precedence:** prefer `episode_id` column on `note_entities_procedures` if populated (match against `canonical_operative_events_v1.surgery_episode_id`). Fallback: same-research_id + note_date within ±30 days of `surgery_date_native`. Mark `linkage_method` accordingly. Expect roughly 60–75% exact_episode, rest temporal or unlinked. **Multi-candidate ambiguity flag required** (per Script 361 Pattern 1) — see `n_candidate_episodes` and `linkage_ambiguous_multi_episode` columns in Step 3.
5. **`COUNT(*) OVER (PARTITION BY ...)` for ambiguity counts, never `GROUP BY` after a rn=1 filter** (per Script 361 Pattern 2). A `GROUP BY (research_id, note_date)` placed after `WHERE rn = 1` always returns 1 — the ambiguity signal would be silently broken without a probe catching it. Window-function the count BEFORE the filter.
6. **Registry schema drift:** use `information_schema.columns` to introspect before the INSERT (per `reference_detail_table_registry_schema.md` memory).
7. **Views dependency order:** must drop dependent views before `DROP TABLE operative_episode_detail_v2`. Use `search_catalog` LIKE pattern.
8. **No invasion-flag removal in this script.** `gross_ete_flag`, `tracheal_involvement_flag`, `esophageal_involvement_flag`, `local_invasion_flag` stay on `canonical_operative_events_v1` for now — Script 363 will strip them when the cross-modal invasion canonical lands.
9. **`NULLIF(CONCAT_WS(...), '')` discipline** (per Script 361 Pattern 4). 362 doesn't currently build CONCAT_WS-derived narrative columns, but if you add one during enrichment (e.g. concatenating evidence spans) wrap in `NULLIF(CONCAT_WS('; ', ...), '')` so empty concats become NULL instead of `''`. Downstream `IS NULL` filters will silently miss `''` and cause row-count inflation.
10. **PHI:** do not print any clinical notes or narrative text to stdout. research_id + aggregate counts only.
11. **Surgical git add only** (per `feedback_surgical_git_add.md`). When committing, stage by explicit path: `git add scripts/362_operative_consolidation.py qa/qa_script_362_operative.json operative_cpm_feeder_audit_20260421.md`. Never `git add scripts/output/` or `git add -A`.

---

## Git workflow

Per `feedback_commit_workflow.md` and `feedback_surgical_git_add.md`:
- Lint with `ruff check scripts/362_operative_consolidation.py` before staging.
- Stage by explicit path only: `git add scripts/362_operative_consolidation.py qa/qa_script_362_operative.json operative_cpm_feeder_audit_*.md`. **NEVER** `git add scripts/output/` or `git add -A`.
- Two-commit pattern (per Script 361 close-out):
  1. After `--commit --skip-drop` + green QA: `Script 362: build operative canonicals (3 new, 13/N QA gates green)`
  2. After `--commit --phase 5` drops: `Script 362: drop operative_episode_detail_v2 after green QA <build_sha>`
- Push to `origin/main` after each commit.

## Success criteria

- Script runs to completion idempotently across separate sessions (re-run of `--commit` is a no-op besides timestamp updates; archive skip-if-exists; `--phase 5` autonomous archive lookup).
- `qa/qa_script_362_operative.json` all hard gates pass; informational metrics logged.
- Registry clean (1 out, 3 in).
- `archive_pub_v1_0` contains exactly 1 pre-362 snapshot with matching row count (prune any stale dry-run snapshots after final --phase 5 succeeds, per Script 361 close-out pattern).
- CPM feeder audit report written.
- All 3 views resolve.
- `placeholder_flag_cols` empty (or each entry justified by a missing-source-column log line from Step 0).
- Procedure linkage: ≥60% exact_episode (or ≥70% combined exact + unambiguous temporal).
- Procedure ambiguity rate <20% (informational; if higher, log carry-forward for smarter tie-breaker).
- Two clean git commits on `main`: build commit + drop commit, each with its own QA SHA reference.

---

## Next up after this

Script 363 — Cross-modal invasion findings canonical. Before writing that prompt, you will verify `note_entities_llm_airway_invasion` + `note_entities_llm_vascular_invasion` coverage across CT/MRI/nucmed modalities (per Q10). Script 363 will also strip invasion columns from `canonical_path_malignant_events_v1` (built in 361) and `canonical_operative_events_v1` (built here) to hit the "no redundancy" goal from Q9. Keep invasion columns intact in 362 — 363 handles the removal.
