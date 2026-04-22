# Cursor Prompt — Script 363: Cross-Modal Invasion Findings Canonical (NEW DOMAIN)

**Context for Cursor:** You are working on the Emory thyroid cancer lakehouse in MotherDuck (`thyroid_canonical_publication_v1_0`). This is the third of four planned consolidations — and the only one building a genuinely **new** Tier-2 domain (not a rename-plus-enrichment). **Do not start this until Scripts 361 and 362 are fully deployed and QA-green.**

Read first:
- `/Users/ros/THyroid 2026/op_procedure_consolidation_scoping_362.md` (Q-decisions that drove 363's shape — especially Q3 cross-modal provenance, Q4 per-patient+date+source, Q9 no redundancy).
- `/Users/ros/Library/Application Support/Claude/local-agent-mode-sessions/.../memory/project_op_path_consolidation_script_361.md` (the 7 reusable patterns you MUST apply).

Follow the canonical naming convention (`canonical_<domain>_events_v1` / `canonical_<domain>_patient_rollup_v1`, view suffix `_VIEW_v1`). Match the close-out pattern used by Scripts 347 / 360 / 361 / 362.

---

## Why this script exists (read before designing)

Logan's requirement (Q3, verbatim): *"I think some of those flags have duplicate pulls; those flags should trace back independently across different reports. For example. I want to know if gross ete flag was seen in op note versus pathology report versus US versus CT Vs. pet CT, vs MRI etc. Tracheal/esophageal involvements similar parsing to ete. Strap muscle is likely only operative note/synoptic."*

And Q4: *"Should be per patient with date and source noted form parsed."*

And Q9: *"Just get all info into one clean table, no redundancy."*

Translation:
- **Single source of truth** for invasion findings. After 363 is green, `canonical_path_malignant_events_v1` and `canonical_operative_events_v1` will have their invasion columns ALTER-DROPPED (Step 7 cascade strip).
- **Event grain, not patient-collapsed.** Every mention of invasion gets its own row with `source_modality` and `finding_date`. Consumers needing a single boolean do a query-time MAX.
- **Every modality that could plausibly document the finding is included as a source**, even if coverage is sparse. Sparse coverage gets logged as carry-forward; it doesn't block the build.

**Strap muscle is explicitly excluded** from this canonical (per Q3). `strap_muscle_involvement_flag` stays permanently on `canonical_operative_events_v1`. Do NOT strip it in Step 7.

---

## Scope & decisions (locked)

Build 2 new canonicals:
1. `main.canonical_invasion_events_v1` — event grain (one row per invasion-finding mention per modality).
2. `main.canonical_invasion_patient_rollup_v1` — patient grain (one row per research_id).

Plus a **cascade strip** (Step 7) that ALTER-DROPs invasion columns from `canonical_path_malignant_events_v1` + `canonical_operative_events_v1`. Runs only under `--phase 7`.

**Invasion types (7):**
- `gross_ete` — gross extrathyroidal extension (AJCC pT3b trigger)
- `microscopic_ete` — microscopic ETE (AJCC pT3a; histopathology-only typically)
- `tracheal` — tracheal invasion / involvement
- `esophageal` — esophageal invasion / involvement
- `vascular_microscopic` — microscopic vascular invasion (pathology-only typically)
- `airway` — airway invasion (laryngeal, hypopharyngeal — broader than tracheal)
- `local` — generic local invasion / "locally advanced disease" without specific structure

**Source modalities (9, census-gated in Step 0):**
- `op_note` (from `canonical_operative_events_v1` + `note_entities_operative_detail`)
- `synoptic_path` (from `canonical_path_malignant_events_v1` fields sourced from path_synoptics)
- `narrative_path` (from `canonical_path_malignant_events_v1` fields sourced from narrative_path + any `note_entities_llm_*` with path source)
- `frozen_section` (from `canonical_frozen_section_events_v1`)
- `ultrasound` (from imaging entity tables — exists per cunc_v1 + gland/LN v2 builds)
- `ct` (from CT imaging entity tables)
- `mri` (from MRI imaging entity tables)
- `pet_ct` (from PET/CT imaging entity tables)
- `nucmed` (from nuclear medicine imaging entity tables)
- Plus the two NLP-specific tables: `note_entities_llm_airway_invasion`, `note_entities_llm_vascular_invasion` — these feed into whichever modality their `note_type` indicates.

**Keep as-is (upstream feeders — read-only):** all `note_entities_llm_*` tables, `canonical_frozen_section_events_v1`, all imaging canonicals.

**Deprecations:** none in the usual sense. Step 7 is a column-level ALTER DROP on two existing canonicals, with pre-step archive snapshots.

---

## Build Scope — Script File: `363_invasion_canonical.py`

Define `SCRIPT_ID = "363"` and `BUILD_TS = datetime.utcnow().strftime("%Y%m%d_%H%M%S")` at top. Idempotent steps. Use the project's standard MotherDuck connection helper.

### CLI flag contract (match Scripts 361/362)

Implement phase-gated execution with `argparse`:

- `--dry-run` (default) — plan + print SQL, no writes.
- `--commit` — actually execute writes.
- `--skip-strip` — run Steps 0–6, 8 but skip Step 7 (cascade strip). For the initial build-and-verify commit.
- `--phase N` — run ONLY step N. `--commit --phase 7` is the isolated cascade-strip run that ties to a clean second commit.

Typical flow:
1. `python scripts/363_invasion_canonical.py --dry-run` → inspect planned SQL + coverage census output.
2. `python scripts/363_invasion_canonical.py --commit --skip-strip` → build invasion canonicals, run QA, leave invasion columns on 361/362 canonicals intact.
3. Verify QA JSON green + spot-check against current invasion columns on 361/362 canonicals, then `python scripts/363_invasion_canonical.py --commit --phase 7` → ALTER DROP invasion columns from 361/362 canonicals. Separate git commit tying strip to green QA SHA.

### Step 0 — Pre-flight, coverage census, and column existence checks

**(0.a) Dependency check.** Assert all of these exist with `COUNT(*) > 0`:
- `main.canonical_path_malignant_events_v1` (Script 361)
- `main.canonical_path_benign_events_v1` (Script 361)
- `main.canonical_operative_events_v1` (Script 362)
- `main.canonical_frozen_section_events_v1` (Script 360)
- `main.note_entities_llm_airway_invasion`
- `main.note_entities_llm_vascular_invasion`

If any missing, abort with clear error.

**(0.b) Imaging modality coverage census (per user Q10 — verify before build).** For each of `ultrasound`, `ct`, `mri`, `pet_ct`, `nucmed`, probe `information_schema.tables` + `information_schema.columns` to find:
1. Which `note_entities_llm_*` or `canonical_<modality>_*` tables exist for that modality.
2. Do any of them have `entity_type` / `finding_type` / column values indicating invasion? Use `SELECT DISTINCT entity_type FROM <table>` and grep for `ete|extrathyroidal|tracheal|esophageal|airway|vascular|invasion|invasive|involvement`.
3. `COUNT(*)` of matching mentions per modality. `COUNT(DISTINCT research_id)` of patients per modality.

Output a coverage matrix table to stdout AND to `/Users/ros/THyroid 2026/invasion_coverage_census_20260421.md`:

```
| modality     | source_table                         | n_mentions | n_patients | % of cohort |
|--------------|--------------------------------------|-----------:|-----------:|------------:|
| op_note      | canonical_operative_events_v1        | ~X         | ~Y         | ~Z%         |
| synoptic_path| canonical_path_malignant_events_v1   | ~X         | ~Y         | ~Z%         |
| ...          | ...                                  | ...        | ...        | ...         |
```

**Coverage gates:**
- If a modality has `n_patients = 0`, DROP that modality from the build (log and document; add to `placeholder_modalities VARCHAR[]` on the events table).
- If a modality has `n_patients > 0` but `< 1% of cohort`, INCLUDE with a warning — carry-forward item for upstream NLP improvement.
- If `op_note` or `synoptic_path` or `narrative_path` show `n_patients = 0`, ABORT — those are the invasion backbone and something is broken upstream.

**(0.c) Column existence pre-flight** (per Script 361 Pattern 7). For each source table identified in (0.b), query `information_schema.columns` and confirm the expected columns. Specifically confirm on `canonical_path_malignant_events_v1`:
```
['gross_ete_flag', 'tracheal_involvement_flag',
 'esophageal_involvement_flag', 'local_invasion_flag',
 'vascular_invasion_flag', 'microscopic_ete_flag']
```
And on `canonical_operative_events_v1`:
```
['gross_ete_flag', 'tracheal_involvement_flag',
 'esophageal_involvement_flag', 'local_invasion_flag']
```
Missing columns → `log_warn` + add to `placeholder_invasion_types VARCHAR[]`. Do NOT silently skip.

**(0.d) Date-column type probes.** For each source table, query `information_schema.columns` for the date column used in Step 1 (e.g. `surg_date`, `surgery_date_native`, `note_date`, `fs_day`, imaging `exam_date`). If VARCHAR, apply `TRY_CAST` discipline in Step 1 UNION (per Script 361 Pattern 3).

### Step 1 — Build `main.canonical_invasion_events_v1`

**Grain:** one row per (research_id × invasion_type × source_modality × source_row_id) invasion-finding mention. No collapsing across modalities — that's the whole point of this table.

**Construction pattern:** one CTE per (source_modality, invasion_type) combo, then UNION ALL. Per Script 361 Pattern 7, validate each CTE's source columns exist BEFORE building the CTE — skip CTEs whose source columns are missing and log to `placeholder_cte_combos VARCHAR[]`.

**Columns:**
- `invasion_event_id` VARCHAR — `md5(research_id || source_modality || source_table || source_row_id || invasion_type)`. Stable + deterministic.
- `research_id` BIGINT
- `invasion_type` VARCHAR — one of the 7 values above (plus `placeholder_invasion_types` entries if any column was missing)
- `finding_status` VARCHAR — `present` | `absent` | `suspected` | `indeterminate`. For BOOLEAN source columns, map TRUE→`present`, FALSE→`absent`, NULL→`indeterminate`. For free-text NLP sources, use the extractor's `present_or_negated` / `status` field.
- `source_modality` VARCHAR — one of the 9 modality values above (only modalities that passed the 0.b coverage gate)
- `source_table` VARCHAR — fully qualified (e.g. `main.canonical_path_malignant_events_v1`)
- `source_row_id` VARCHAR — best-available row identifier (surgery_episode_id, fs_event_id, us_exam_id, note_row_id, etc.)
- `finding_date` DATE — `TRY_CAST` from whichever date column the source uses. NULL if unavailable.
- `linked_surgery_episode_id` BIGINT — nullable. For non-operative modalities, link by `research_id` + temporal proximity to `canonical_operative_events_v1.surgery_date_native` within ±90 days (wider than 362's ±30d because imaging can precede surgery by weeks). Prefer exact `episode_id` match if source has it.
- `linked_path_malignant_event_id` BIGINT — nullable. Same-day `(research_id, surg_date)` match to `canonical_path_malignant_events_v1` where applicable.
- `linkage_method` VARCHAR — `exact_episode` | `temporal_90d` | `temporal_90d_ambiguous` | `unlinked` | `na_source_is_surgical`
- `n_candidate_episodes` INT — `COUNT(*) OVER (PARTITION BY research_id, finding_date)` against `canonical_operative_events_v1` BEFORE any rn=1 pick (per Script 361 Pattern 2).
- `linkage_ambiguous_multi_episode` BOOLEAN — `TRUE` when `n_candidate_episodes > 1` AND `linkage_method NOT IN ('exact_episode', 'na_source_is_surgical')`. Deterministic pick = lowest `surgery_episode_id`.
- `confidence` DOUBLE — NULL for BOOLEAN-sourced rows; populated for NLP-sourced rows.
- `evidence_span_hash` VARCHAR — `md5(evidence_span)` IF source has evidence text. **Do NOT store the raw text in this canonical** (PHI). The hash lets consumers dedupe identical evidence spans across rerun extractions.
- `extraction_run_id` VARCHAR — NULL for BOOLEAN-sourced rows; populated for NLP-sourced.
- `build_script VARCHAR DEFAULT '363'`
- `build_ts TIMESTAMP`

**CTE pattern for each source (example for gross_ete from synoptic_path):**

```sql
WITH cte_gross_ete_synoptic_path AS (
    SELECT
        'gross_ete' AS invasion_type,
        CASE
            WHEN gross_ete_flag = TRUE THEN 'present'
            WHEN gross_ete_flag = FALSE THEN 'absent'
            ELSE 'indeterminate'
        END AS finding_status,
        'synoptic_path' AS source_modality,
        'main.canonical_path_malignant_events_v1' AS source_table,
        CAST(path_event_id AS VARCHAR) AS source_row_id,
        research_id,
        TRY_CAST(surg_date AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        NULL::VARCHAR AS extraction_run_id
    FROM main.canonical_path_malignant_events_v1
    WHERE gross_ete_flag IS NOT NULL
       OR surg_date IS NOT NULL   -- keep indeterminate rows that have a date anchor
)
```

Do this for each (modality, invasion_type) combo that passed the (0.b) and (0.c) gates. Then UNION ALL. Then wrap in a final SELECT that:
1. Adds `invasion_event_id` = md5 concat.
2. LEFT JOIN to `canonical_operative_events_v1` via (research_id, temporal_90d) to populate linkage cols.
3. LEFT JOIN to `canonical_path_malignant_events_v1` via (research_id, finding_date) to populate `linked_path_malignant_event_id`.
4. `COUNT(*) OVER (PARTITION BY research_id, finding_date)` for `n_candidate_episodes`.

**Expected row count:** depends on coverage census output. For reference, current invasion-column populated rows on 361/362:
- `canonical_path_malignant_events_v1`: ~3,500 rows with at least one invasion flag set (gross_ete + micro_ete + tracheal + esoph + vasc + local)
- `canonical_operative_events_v1`: ~2,000 rows with at least one invasion flag set
- `note_entities_llm_airway_invasion`: probe with `SELECT COUNT(*) FROM main.note_entities_llm_airway_invasion` — should be non-trivial
- `note_entities_llm_vascular_invasion`: same
- Imaging modalities: unknown until census runs

Target total: **~8,000–25,000 rows** across all modalities, spanning ~4,000–5,000 distinct patients. Actual numbers depend on coverage census; record expected bands in QA JSON as informational only, not gated.

### Step 2 — Build `main.canonical_invasion_patient_rollup_v1`

**Grain:** one row per `research_id` (expected ~4,000–5,000 rows — only patients with at least one invasion finding).

**Columns — for each invasion_type (7) × source_modality (up to 9):**
- `any_<type>_in_<modality>` BOOLEAN — e.g. `any_gross_ete_in_synoptic_path`, `any_tracheal_in_ct`. Skip combos that were placeholder-rejected in Step 1.

**Cross-modal aggregates for each invasion_type:**
- `any_<type>_anywhere` BOOLEAN — TRUE if `present` in any modality
- `any_<type>_in_op_or_path` BOOLEAN — TRUE if `present` in op_note OR synoptic_path OR narrative_path OR frozen_section
- `any_<type>_in_imaging` BOOLEAN — TRUE if `present` in ultrasound OR ct OR mri OR pet_ct OR nucmed
- `earliest_<type>_date` DATE
- `latest_<type>_date` DATE
- `n_modalities_with_<type>` INT — count of distinct modalities where `finding_status='present'`

**Discordance flags per invasion_type (informational, not gated):**
- `<type>_path_imaging_concordant` BOOLEAN — TRUE if (any in op/path) == (any in imaging); NULL if neither side has data. Useful for manuscript-level concordance tables.

**Provenance:**
- `build_script VARCHAR DEFAULT '363'`
- `build_ts TIMESTAMP`

Build via `CREATE TABLE ... AS SELECT research_id, BOOL_OR(invasion_type='gross_ete' AND source_modality='synoptic_path' AND finding_status='present') AS any_gross_ete_in_synoptic_path, ... FROM canonical_invasion_events_v1 GROUP BY research_id`. Only emit columns for (type, modality) combos that survived the census gate.

### Step 3 — Views (2) in `views_readable`

```sql
CREATE OR REPLACE VIEW views_readable.invasion_events_VIEW_v1 AS
  SELECT * FROM main.canonical_invasion_events_v1;
CREATE OR REPLACE VIEW views_readable.invasion_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_invasion_patient_rollup_v1;
```

### Step 4 — Update `detail_table_registry_v1`

Query `information_schema.columns` first to introspect the 3 extra columns.

- INSERT 2 rows (one per canonical) with `domain = 'invasion_findings'`, `schema_name = 'main'`, `detail_table_name`, `build_script = '363'`, `status = 'active'`, plus the 3 extra columns (fill from existing active row as template).
- No DELETE (nothing deprecated at the table level).

### Step 5 — CPM feeder audit (report only)

Print a report of every `nlp_*` column on `main.canonical_patient_master` that was previously fed by an invasion column on `canonical_path_malignant_events_v1` or `canonical_operative_events_v1`. Use `git grep -E 'gross_ete_flag|tracheal_involvement_flag|esophageal_involvement_flag|local_invasion_flag|vascular_invasion_flag|microscopic_ete_flag' scripts/` to find the feeders.

**Each feeder will need to be repointed to `canonical_invasion_patient_rollup_v1`** in a follow-up script (NOT in 363). Output the repointing recommendations to `/Users/ros/THyroid 2026/invasion_cpm_feeder_repoint_plan.md` with the target rollup column name for each CPM feeder (e.g., `cpm.nlp_gross_ete` → `canonical_invasion_patient_rollup_v1.any_gross_ete_anywhere`).

**This is critical:** Step 7's cascade strip will delete the source columns the CPM feeders currently read from. The repointing script must run BEFORE Step 7, OR the CPM feeders must be coded to fall back gracefully. Log this as a blocker in the close-out memory if not yet addressed.

### Step 6 — Zero-drift QA

Verify and write results to `qa/qa_script_363_invasion.json`. Each check is a hard gate unless marked INFORMATIONAL:

**Hard gates:**
1. `events_rowcount_nonzero`: `COUNT(*)` on `canonical_invasion_events_v1` > 0.
2. `rollup_parity_with_events`: `COUNT(*)` on `canonical_invasion_patient_rollup_v1` == `COUNT(DISTINCT research_id)` on events (per Script 360 Pattern 2).
3. `all_required_modalities_present`: op_note, synoptic_path, narrative_path, frozen_section each have at least 1 row in events. Fails if the invasion backbone is missing.
4. `invasion_type_coverage`: all 7 invasion types appear in at least one row (unless explicitly listed in `placeholder_invasion_types`).
5. `preservation_path_malignant`: `SUM(gross_ete_flag IS TRUE)` on `canonical_path_malignant_events_v1` == `COUNT(DISTINCT research_id || '|' || path_event_id)` on `canonical_invasion_events_v1 WHERE invasion_type='gross_ete' AND source_modality='synoptic_path' AND finding_status='present'`. Zero drift. Repeat for each (source, invasion_type) combo.
6. `preservation_operative`: same as above for `canonical_operative_events_v1` invasion flags → `source_modality='op_note'` rows in events.
7. `no_research_ids_lost_vs_source_union`: DISTINCT `research_id` set on events ⊇ DISTINCT set across all source tables where any invasion column was TRUE.
8. `view_resolves_invasion_events_VIEW_v1`, `view_resolves_invasion_patient_rollup_VIEW_v1`.

**Informational:**
9. `coverage_census_matrix`: the Step 0.b output, attached to QA JSON.
10. `linkage_method_distribution`: SELECT linkage_method, COUNT(*) GROUP BY 1.
11. `linkage_ambiguity_rate`: SUM(linkage_ambiguous_multi_episode) / COUNT(linked_surgery_episode_id IS NOT NULL). Expect <30% (wider than 362 because ±90d window).
12. `discordance_rate_per_type`: % of patients where `<type>_path_imaging_concordant = FALSE`. High rates (>25%) flag upstream NLP quality issues.
13. `placeholder_modalities`, `placeholder_invasion_types`, `placeholder_cte_combos`: all three lists logged.
14. `varchar_date_parse_failures`: per source table.

### Step 7 — Cascade strip (runs only under `--phase 7` or full `--commit` without `--skip-strip`)

**Pre-strip archive snapshots** (per Script 361 Pattern 6 — autonomous lookup, idempotent):

```python
for table in ['canonical_path_malignant_events_v1', 'canonical_operative_events_v1']:
    archive_name = f"{table}_pre363strip_{BUILD_TS}"
    existing = conn.execute("""
        SELECT table_name FROM "Thyroid 2026 UPdated".information_schema.tables
        WHERE table_schema = 'archive_pub_v1_0'
          AND table_name LIKE ?
    """, [f"{table}_pre363strip_%"]).fetchone()
    if existing:
        log_info(f"pre363strip archive for {table} exists — skipping")
    else:
        conn.execute(f'''
            CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.{archive_name} AS
            SELECT * FROM main.{table}
        ''')
```

**Pre-strip safety gates (ALL must pass):**
1. `main.canonical_invasion_events_v1` exists with `COUNT(*) > 0`.
2. `main.canonical_invasion_patient_rollup_v1` exists with `COUNT(*) > 0`.
3. The Step 6 `preservation_*` gates all passed on the last QA run (check the QA JSON).
4. CPM feeder repoint plan exists at `/Users/ros/THyroid 2026/invasion_cpm_feeder_repoint_plan.md`. **If CPM feeders are still pointing at the about-to-be-dropped columns**, require `--force-strip` flag OR abort with a clear message pointing to the repoint plan.
5. No views in `views_readable` select `gross_ete_flag` / `tracheal_involvement_flag` / `esophageal_involvement_flag` / `local_invasion_flag` / `vascular_invasion_flag` / `microscopic_ete_flag` from the target tables — `search_catalog` first; drop/recreate any found.

**Columns to DROP from `main.canonical_path_malignant_events_v1`:**
```
gross_ete_flag, microscopic_ete_flag, tracheal_involvement_flag,
esophageal_involvement_flag, local_invasion_flag, vascular_invasion_flag
```

**Columns to DROP from `main.canonical_operative_events_v1`:**
```
gross_ete_flag, tracheal_involvement_flag,
esophageal_involvement_flag, local_invasion_flag
```

**DO NOT DROP** `strap_muscle_involvement_flag` from `canonical_operative_events_v1` (per Q3 — strap muscle stays on 362 permanently, not cross-modal).

Execute `ALTER TABLE ... DROP COLUMN ...` one column at a time with a log line per drop. After all drops, re-run the Step 4 views CREATE OR REPLACE to refresh their compiled column lists (per `feedback_alter_view_dependents.md` memory — ALTER doesn't cascade into dependent view bodies).

**Post-strip verification:**
1. `canonical_path_malignant_events_v1` row count unchanged.
2. `canonical_operative_events_v1` row count unchanged.
3. None of the dropped column names appear in `information_schema.columns` for those tables.
4. `canonical_invasion_events_v1` row count unchanged.

### Step 8 — Close-out summary

Print to stdout AND append to `/Users/ros/THyroid 2026/script_363_closeout_20260421.md`:
- Row/patient counts on both new canonicals.
- Coverage census matrix (from Step 0.b).
- All 14 QA gate results.
- Cascade strip outcome (if `--phase 7` ran): columns dropped per table, archive SHA.
- Full SHA chain on `main` for 363 commits.
- Carry-forward items (sparse-coverage modalities, discordance rates above threshold, CPM repoint plan status).
- Reusable patterns discovered during the build.

---

## Gotchas

1. **`strap_muscle_involvement_flag` is NOT cross-modal.** Per Q3, strap muscle stays only on `canonical_operative_events_v1` (op-note + synoptic only in practice). Do NOT include in Step 1 CTEs. Do NOT drop in Step 7.
2. **PHI: do not store raw evidence spans.** Store `md5(evidence_span)` only. Consumers who need the text can join back to the source table via `source_row_id`.
3. **VARCHAR date fields need `TRY_CAST` everywhere** (Script 361 Pattern 3). Different source tables use different date column names — probe each in Step 0.d before the CTE UNION.
4. **`COUNT(*) OVER (PARTITION BY ...)` for ambiguity counts, never `GROUP BY` after rn=1 filter** (Script 361 Pattern 2).
5. **`NULLIF(CONCAT_WS(...), '')` if you build any narrative column** (Script 361 Pattern 4). Particularly relevant if you add an optional `finding_narrative` column from multiple source fields.
6. **ALTER DROP COLUMN does NOT cascade into view bodies** (`feedback_alter_view_dependents.md`). Step 7 must CREATE OR REPLACE the 6 views in `views_readable` that point at the two tables after the drops, otherwise the next query against those views will fail at compile.
7. **Imaging coverage may be 0% for some modalities.** Per user Q10 guidance and carry-forward #4 from Script 361, imaging NLP coverage is known-sparse. The census gate in Step 0.b is what decides which modalities go in. Don't assume pet_ct / nucmed / mri have any invasion extractions — they might not.
8. **Registry schema drift** — use `information_schema.columns` to introspect before INSERT (per `reference_detail_table_registry_schema.md`).
9. **CPM feeder repoint is a HARD blocker for Step 7.** The cascade strip will break CPM feeders that are still reading from the dropped columns. Gate 7.4 enforces this — do not override without understanding the blast radius.
10. **Surgical git add only** (per `feedback_surgical_git_add.md`). Stage explicit paths: `git add scripts/363_invasion_canonical.py qa/qa_script_363_invasion.json invasion_coverage_census_*.md invasion_cpm_feeder_repoint_plan.md script_363_closeout_*.md`. Never `git add scripts/output/` or `git add -A`.
11. **PHI: research_id + aggregate counts only in stdout.** No clinical notes, no raw evidence text.
12. **Discordance is informational, not a bug.** A patient with `gross_ete` present in op_note but absent in synoptic_path is a legitimate clinical finding (op-note describes gross appearance; synoptic captures microscopic fields). Do NOT treat discordance as a QA failure. Log rates; don't gate.

---

## Git workflow

Per `feedback_commit_workflow.md` and `feedback_surgical_git_add.md`:
- Lint with `ruff check scripts/363_invasion_canonical.py` before staging.
- Stage by explicit path only. **NEVER** `git add scripts/output/` or `git add -A`.
- Three-commit pattern:
  1. After `--commit --skip-strip` + green QA: `Script 363: build invasion canonicals (2 new, N QA gates green, cross-modal)`
  2. (Optional interim commit if CPM feeder repoint lands here before the strip): `Script 363: repoint CPM feeders to invasion_patient_rollup_v1`
  3. After `--commit --phase 7`: `Script 363: strip invasion columns from 361/362 canonicals after green QA <build_sha>`
- Push to `origin/main` after each commit.

## Success criteria

- Script runs to completion idempotently across separate sessions.
- `qa/qa_script_363_invasion.json` all hard gates pass; informational metrics logged.
- Registry clean (0 out, 2 in).
- `archive_pub_v1_0` contains pre363strip snapshots for both target tables at time of Step 7.
- Coverage census output documents which modalities were included vs excluded with rationale.
- CPM feeder repoint plan written (blocker gate 7.4 either satisfied or explicit `--force-strip` with documented consumer acknowledgment).
- All 2 views resolve.
- Strap muscle NOT touched on 362.
- `preservation_*` gates exact — zero invasion data lost going from 361/362 columns to invasion_events rows.
- Three clean git commits (or two + force-strip note).

---

## Next up after this

Script 364 — Complications canonical (event-grain, source-attributed; RLN injury folded in). Per Q3, RLN injury is parsed by how it was noted across op report / CT / MRI / US / path / nucmed with per-patient date + source — essentially the same cross-modal pattern 363 just established. 364 will re-use the CTE-per-(modality, complication_type) UNION pattern, the md5 `invasion_event_id`-style stable key, the `source_modality` column, and the `COUNT(*) OVER` ambiguity flag. The Step-7 cascade strip pattern will also recur if any complication columns currently live on 361/362/360 canonicals that should be single-sourced.
