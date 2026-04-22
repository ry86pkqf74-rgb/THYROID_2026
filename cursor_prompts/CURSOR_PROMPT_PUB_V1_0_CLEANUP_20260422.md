# Cursor Prompt — `thyroid_canonical_publication_v1_0` full cleanup & dedup (Script 387)

**Date:** 2026-04-22
**Target DB:** `md:thyroid_canonical_publication_v1_0` (alias `PUB`)
**Archive DB:** `md:"Thyroid 2026 UPdated"`
**Author context:** Logan Glosser (Emory thyroid surgery research)
**Precedents:** Scripts 358 (planned, never built), 360 (frozen section), 361 (op path), 362 (op procedure) — same patterns apply here
**Sibling prompt:** `CURSOR_PROMPT_LLM_INTEGRATION_AND_V1_0_DEDUP_20260422.md` (Script 386, LLM integration Phase D) — this prompt (387) is a **pure dedup/archive pass**, no LLM integration work

---

## Objective

After investigation against the live catalog, execute a single-commit cleanup that:

1. Collapses the `tier2` schema entirely (12 objects) by archiving to `"Thyroid 2026 UPdated".tier2_legacy_20260422`
2. Drops the `verify` schema (2 objects) by archiving to `"Thyroid 2026 UPdated".verify_legacy_20260422`
3. Removes the `views_readable.survival_followup_VIEW_v1` duplicate (keeping `views_readable.Survival_Followup`)
4. Archives 12 stale `manuscript_workspace` artifacts (pre_s376 snapshot, candidate_* views, prompt5/6/7 logs) to `"Thyroid 2026 UPdated".manuscript_workspace_legacy_20260422`
5. Runs a within-canonical event-row dedup QA phase across every `canonical_*_events_v1` / `canonical_*_patient_rollup_v1` table and flags any silent collapse

Nothing in `main`, `raw`, `views_readable` (except the one duplicate), or the active `manuscript_workspace` governance tables is touched.

**End-state expectation:** `tier2` and `verify` schemas cease to exist in `PUB`. `views_readable` has no convention violations. `manuscript_workspace` contains only the live governance/audit layer.

---

## Safety rules (must hold for entire run)

- **PHI:** Never print clinical note text, `evidence_text`, MRN, DOB, or any note content to logs. `research_id` only.
- **No cross-DB canonical sourcing:** All live canonicals stay in `PUB.main`. Archives are point-in-time copies in the archive DB — never read from the archive DB in a live view.
- **ALTER VIEW trap:** If a rename cascades into dependent views, `CREATE OR REPLACE` dependents in the same transaction with updated `FROM` clauses. (Script 360 close-out pattern.)
- **TIMESTAMP TZ trap:** For any new `build_ts` column, `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` — DuckDB returns TIMESTAMPTZ otherwise, which silently pulls in pytz.
- **No destructive drops before archive verification:** Every object to be dropped in `PUB` must first be confirmed row-count-equal in the archive copy.
- **Surgical `git add`:** Never `git add -A` or `git add scripts/output/`. Stage by explicit path.
- **Lint first:** `ruff check scripts/387_pub_v1_0_cleanup.py` must pass before commit.
- **No cloud PHI:** Archive copies go only to `md:"Thyroid 2026 UPdated"` — already an internal MotherDuck DB, not public.

---

## Precondition: pre-state snapshot tables

Before any archive/drop, the script writes to `manuscript_workspace`:

- `script_387_prestate_v1` — schema, name, type, row_count for every object being touched (26 objects: 12 tier2 + 2 verify + 1 views_readable dup + 12 manuscript_workspace legacy)
- `script_387_dedup_probe_v1` — empty, schema only (populated in Phase 6)

---

## Phase structure

### Phase 1 — Pre-state snapshot (read-only)

```python
# Enumerate the 26 targets with live row counts and capture into script_387_prestate_v1
# Compare against the investigation manifest below; abort if any target is unexpectedly missing or newly referenced by a live view (dep scan re-run).
```

**Dep scan re-run (must print 0 rows for rows that aren't the frozen_section_event_v1 self-ref):**

```sql
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND (view_definition ILIKE '%tier2.%' OR view_definition ILIKE '%verify.%')
  AND NOT (table_schema = 'tier2' AND table_name = 'frozen_section_event_v1');
```

If this returns any rows, **abort** — someone added a new dependency since 2026-04-22. Investigate before proceeding.

### Phase 2 — tier2 schema archive (12 objects)

Archive destination: `md:"Thyroid 2026 UPdated".tier2_legacy_20260422`

Targets (all 12 — the split between "has canonical replacement" vs "orphan" is documented but both groups archive the same way):

Has canonical replacement:
- `tier2.airway_invasion_event_v1` → `canonical_invasion_events_v1`
- `tier2.frozen_section_event_v1` (VIEW) → `canonical_frozen_section_*` (materialize the VIEW before archive)
- `tier2.past_surgical_hx_event_v1` → `canonical_psh_events_v1`
- `tier2.patient_tier2_master_v1` → `canonical_patient_master`
- `tier2.vascular_invasion_event_v1` → `canonical_invasion_events_v1`

Orphan (no canonical replacement yet — confirmed archive per 2026-04-22 decision):
- `tier2.dynamic_risk_response_event_v1`
- `tier2.functional_outcomes_event_v1`
- `tier2.parathyroid_detail_event_v1`
- `tier2.patient_decision_adherence_event_v1`
- `tier2.physical_exam_event_v1`
- `tier2.presenting_symptoms_event_v1`
- `tier2.rad_treatment_event_v1`

Per-object flow (implement as a helper):

```python
def archive_and_drop(pub_con, archive_con, src_schema, src_name, archive_schema):
    # 1. Create archive schema if not exists
    archive_con.execute(f'CREATE SCHEMA IF NOT EXISTS "{archive_schema}"')
    # 2. CREATE TABLE AS SELECT into archive DB (materialize VIEWs to tables)
    archive_con.execute(
        f'CREATE TABLE "{archive_schema}"."{src_name}" AS '
        f'SELECT * FROM thyroid_canonical_publication_v1_0.{src_schema}.{src_name}'
    )
    # 3. Row-count verify
    src_n = pub_con.execute(
        f'SELECT COUNT(*) FROM {src_schema}.{src_name}'
    ).fetchone()[0]
    arc_n = archive_con.execute(
        f'SELECT COUNT(*) FROM "{archive_schema}"."{src_name}"'
    ).fetchone()[0]
    assert src_n == arc_n, f'{src_schema}.{src_name}: src={src_n} arc={arc_n}'
    # 4. Drop from PUB
    kind = 'VIEW' if <is view> else 'TABLE'
    pub_con.execute(f'DROP {kind} {src_schema}.{src_name}')
    # 5. Log to archive_move_log_v1
    pub_con.execute('INSERT INTO manuscript_workspace.archive_move_log_v1 ...')
```

At end of Phase 2, verify:

```sql
SELECT COUNT(*) FROM information_schema.tables
WHERE table_catalog = 'thyroid_canonical_publication_v1_0' AND table_schema = 'tier2';
-- expected: 0
```

Then `DROP SCHEMA tier2 CASCADE` (should be empty already — CASCADE is belt-and-suspenders).

### Phase 3 — verify schema archive (2 objects)

Archive destination: `md:"Thyroid 2026 UPdated".verify_legacy_20260422`

Targets:
- `verify.concordance_master_v1`
- `verify.verify_long_v1`

Same archive_and_drop flow. Then `DROP SCHEMA verify CASCADE`.

### Phase 4 — `views_readable.survival_followup_VIEW_v1` drop

Confirm no other view references it:

```sql
SELECT table_schema, table_name FROM information_schema.views
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND view_definition ILIKE '%survival_followup_VIEW_v1%'
  AND NOT (table_schema = 'views_readable' AND table_name = 'survival_followup_VIEW_v1');
-- expected: 0 rows
```

Then `DROP VIEW views_readable.survival_followup_VIEW_v1`.

No archive needed (it's a pure `SELECT *` wrapper around `main.canonical_survival_followup_v1`, which persists).

### Phase 5 — manuscript_workspace stale artifact archive (12 objects)

Archive destination: `md:"Thyroid 2026 UPdated".manuscript_workspace_legacy_20260422`

Targets:
- `manuscript_workspace.canonical_us_nodule_v2_pre_s376_snapshot` (BASE TABLE)
- `manuscript_workspace.candidate_us_exam_master_v2` (VIEW)
- `manuscript_workspace.candidate_us_patient_master_v2` (VIEW)
- `manuscript_workspace.prompt5_remediation_log_v1` (BASE TABLE)
- `manuscript_workspace.prompt5_remediation_summary_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_completion_audit_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_defer_log_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_older_master_decisions_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_poststate_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_prestate_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_view_rebuild_log_v1` (BASE TABLE)
- `manuscript_workspace.prompt6_wiring_gap_remediation_v1` (BASE TABLE)
- `manuscript_workspace.prompt7_handoff_v1` (BASE TABLE)

(That's 13 — recount: 1 snapshot + 2 candidate views + 2 prompt5 + 7 prompt6 + 1 prompt7 = 13, correcting the 12 stated in investigation findings.)

Same archive_and_drop flow. **Do NOT drop the manuscript_workspace schema** — the live governance tables (`archive_move_log_v1`, `archive_candidate_review_v1`, `canonical_cleanup_audit_v1`, `main_schema_keep_list_v1`, `object_domain_map_v1`, `detail_table_registry_v1`, `__conventions`, `script_387_prestate_v1`, `script_387_dedup_probe_v1`) stay.

### Phase 6 — Within-canonical event-row dedup QA (~30 tables)

For every table matching `main.canonical_*_events_v1` or `main.canonical_*_patient_rollup_v1` or `main.canonical_labs_*_v1` or `main.canonical_*_v2` (enumerate at runtime), probe the partition key and flag any silent collapse.

Canonical tables to probe (from investigation, 33 tables):

```
canonical_complications_events_v1, canonical_complications_patient_rollup_v1,
canonical_fna_events_v1, canonical_fna_patient_rollup_v1,
canonical_frozen_section_events_v1, canonical_frozen_section_patient_rollup_v1,
canonical_invasion_events_v1, canonical_invasion_patient_rollup_v1,
canonical_labs_calcium_v1, canonical_labs_pth_v1, canonical_labs_thyroglobulin_v1,
canonical_labs_tsh_v1, canonical_labs_vitamin_d_v1,
canonical_medications_events_v1, canonical_medications_patient_rollup_v1,
canonical_molecular_genetics_v2, canonical_molecular_genetics_from_notes_v2,
canonical_operative_events_v1, canonical_operative_patient_rollup_v1,
canonical_operative_procedure_codes_v1,
canonical_path_benign_events_v1, canonical_path_benign_patient_rollup_v1,
canonical_path_gland_events_v1, canonical_path_gland_patient_rollup_v1,
canonical_path_malignant_events_v1, canonical_path_malignant_patient_rollup_v1,
canonical_patient_master,
canonical_pmh_events_v1, canonical_pmh_patient_rollup_v1,
canonical_psh_events_v1, canonical_psh_patient_rollup_v1,
canonical_recurrence_v1, canonical_survival_followup_v1,
canonical_us_lymph_node_v2, canonical_us_nodule_v2, canonical_us_thyroid_gland_v2
```

For each, determine the expected partition key (patient_rollup → `research_id`; events → `(research_id, event_date, value_raw)` or the table's actual event key; labs → `(research_id, collection_date, value_raw)`; US → `(research_id, us_exam_id, <structure_id>)`). Then:

```sql
INSERT INTO manuscript_workspace.script_387_dedup_probe_v1
SELECT
  '{table}' AS canonical_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT {partition_key}) AS distinct_keys,
  COUNT(*) - COUNT(DISTINCT {partition_key}) AS collapse_count,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM main.{table};
```

**Rule:** `collapse_count > 0` means duplicate rows per partition key. For `*_patient_rollup_v1` tables, collapse_count MUST be 0 (rollups are 1 row per patient). For `*_events_v1`, collapse_count > 0 is a red flag — log and stop for review before the commit.

Use mention-grain partition-key probe learnings from Script 362: the richer key `(research_id, note_row_id, value_raw, evidence_start)` should be the default for note-derived event tables, not the narrower `(rid, date, value_raw)`. Verify each table's key against its `note_row_id` column existence at runtime.

**Do not auto-fix collapses.** If probe flags any table, halt and surface the table + key + collapse_count in a markdown report at `scripts/output/387_dedup_probe_report.md` for manual review. That becomes a carry-forward item if found.

### Phase 7 — Post-state verification

- `information_schema.tables` shows 0 rows for `table_schema IN ('tier2','verify')`
- Total object count dropped by **28**: 313 → 285 (12 tier2 + 2 verify + 1 views_readable + 13 manuscript_workspace)
- `archive_move_log_v1` has 28 new INSERT rows with `script_name='387_pub_v1_0_cleanup'`
- `script_387_prestate_v1` shows source `row_count` == archive copy `row_count` for all 28 archived objects (join on `(schema, name)`)
- `script_387_dedup_probe_v1` has 36 rows (one per canonical probed) with `collapse_count=0` on all `*_patient_rollup_v1` and `canonical_patient_master`

### Phase 8 — Close-out

Write `/Users/ros/THyroid 2026/scripts/output/387_close_out.md`:

- Title, date, commit hash (post-commit)
- Object counts: 313 → 285 in PUB
- Archive DB delta: 3 new schemas added to `"Thyroid 2026 UPdated"`:
  - `tier2_legacy_20260422` (12 tables)
  - `verify_legacy_20260422` (2 tables)
  - `manuscript_workspace_legacy_20260422` (13 tables)
- Dedup probe outcomes summary (flags by table if any, otherwise "all clean")
- Reusable patterns to memory (for next Tier-2 domain close-out)
- Carry-forward items (if any dedup collapses found)

Then:
```
ruff check scripts/387_pub_v1_0_cleanup.py
git add scripts/387_pub_v1_0_cleanup.py scripts/output/387_close_out.md scripts/output/387_dedup_probe_report.md cursor_prompts/CURSOR_PROMPT_PUB_V1_0_CLEANUP_20260422.md
git commit -m "Script 387: archive tier2 + verify + manuscript_workspace legacy; dedup probe across canonicals; PUB 313→285 objects"
git push
```

---

## Implementation notes

- **Connection pattern:** two MotherDuck connections (one for `PUB`, one for the archive DB). Cross-DB CTAS works in a single MotherDuck session with both attached; confirm by `ATTACH` or by using fully-qualified names with a single `duckdb.connect('md:')` session.
- **frozen_section_event_v1 is a VIEW:** materialize to a table in the archive (`CREATE TABLE AS SELECT *`) — don't try to archive the view definition.
- **Schema drop CASCADE:** `DROP SCHEMA tier2 CASCADE` and `DROP SCHEMA verify CASCADE` should be no-ops since all tables are archived individually first. CASCADE is defensive.
- **Script 358 was the original plan for this:** it was drafted but never executed. 387 supersedes that prompt. Remove `cursor_prompts/prompt6_358_schema_consolidation.md` after close-out if Logan wants the historical prompt folder tidy.

---

## Do NOT in this pass

- **No LLM integration work** — Script 386 (`CURSOR_PROMPT_LLM_INTEGRATION_AND_V1_0_DEDUP_20260422.md`) handles that.
- **No new canonicals** for the 7 orphan tier2 domains — those build in future scripts against `note_entities_llm_*` raw tables.
- **No changes to `main.data_dictionary_v279`, `main.ete_adjudication_v1`, `main.cupm_v2_canonical_backfill_v1`** — all three are load-bearing or on the keep list.
- **No changes to `raw.*`, `views_readable.*` (except the one duplicate drop), or the remaining active manuscript_workspace governance tables.**
- **No archive-DB reads in live views.** Archives are reference-only.

---

## Expected end-state object counts

| Schema | Before | After | Change |
|---|---|---|---|
| main (BASE) | 90 | 90 | 0 |
| main (VIEW) | 6 | 6 | 0 |
| manuscript_workspace (BASE) | 70 | 59 | -11 |
| manuscript_workspace (VIEW) | 68 | 66 | -2 |
| raw (BASE) | 2 | 2 | 0 |
| tier2 (BASE) | 11 | 0 | -11 |
| tier2 (VIEW) | 1 | 0 | -1 |
| verify (BASE) | 2 | 0 | -2 |
| views_readable (VIEW) | 63 | 62 | -1 |
| **TOTAL** | **313** | **285** | **-28** |

Schemas `tier2` and `verify` should no longer appear in `information_schema.schemata` for `thyroid_canonical_publication_v1_0` after this script runs.
