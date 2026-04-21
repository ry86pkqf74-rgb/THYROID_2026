# US final-drop + absorption pass — Cursor prompt

**Target DB:** `thyroid_canonical_publication_v1_0`
**Archive DB:** `"Thyroid 2026 UPdated".us_legacy_20260421`
**Date:** 2026-04-21
**Pattern:** absorb → verify → archive → drop

## Goal

Remove the 4 tables still living in `main` after the 2026-04-21 cleanup pass. Each table must have its genuinely-useful data absorbed into `canonical_us_nodule_v2` FIRST. Non-absorbable rows are written to a small gap stub in `manuscript_workspace`, then the source table is archived and dropped.

Held tables to resolve (all currently in `main`):

1. `note_entities_llm_tirads_granular`
2. `note_entities_llm_us_nodule_dynamics`
3. `note_entities_llm_us_nodule_tirads`  *(primary gap driver — 928 LLM-only patients)*
4. `tirads_v2_nodules_raw` *(11,914 rows, 5,082 at `nodule_index = 0` → 3,021 patients)*

Plus: `manuscript_workspace.us_llm_absorption_gap_v1` gets rebuilt at the end and dropped if empty.

**Constraints**
- Do NOT drop any source table until absorption + MD5 content-hash verification passes.
- Do NOT mutate `canonical_us_nodule_v2` outside the absorption UPDATE/INSERTs specified below.
- Keep `source_modality = 'US'` intact on every new row written to v2.
- `us_exam_id` for any new v2 row must be computed with the EXACT same recipe used by Script 363/364 — query it from gland/LN v2 if unsure, do NOT invent a new recipe.
- PHI: never SELECT clinical_note_text into output. Only research_id, exam_date, and structured fields.

---

## Phase 1 — Map LLM entity types → canonical columns

For each of the 3 LLM entity tables, list every distinct `entity_type` (or equivalent discriminator column) with row count and a sample of 5 rows (research_id + structured payload only, NO note text).

Then produce a mapping table (write as `manuscript_workspace.us_llm_absorption_mapping_v1`):

```
entity_source_table      | entity_type              | target_v2_column         | absorption_rule
-------------------------|--------------------------|--------------------------|----------------
llm_us_nodule_tirads     | composition              | composition              | COALESCE existing
llm_us_nodule_tirads     | echogenicity             | echogenicity             | COALESCE existing
llm_us_nodule_tirads     | shape                    | shape                    | COALESCE existing
llm_us_nodule_tirads     | margins                  | margins                  | COALESCE existing
llm_us_nodule_tirads     | echogenic_foci           | echogenic_foci           | COALESCE existing
llm_tirads_granular      | size_mm                  | longest_dimension_mm     | COALESCE existing
llm_tirads_granular      | location                 | nodule_location          | COALESCE existing
llm_us_nodule_dynamics   | change_vs_prior          | change_vs_prior_v2       | COALESCE existing (create col if absent)
...
```

For any `entity_type` that has NO reasonable target column in `canonical_us_nodule_v2`, mark `target_v2_column = '<none>'` and `absorption_rule = 'gap — document only'`. Do not invent columns.

Show me the mapping table contents before executing Phase 2.

---

## Phase 2 — Absorb LLM entities into canonical_us_nodule_v2

Scope: the 928 patients in the current `us_llm_absorption_gap_v1` whose LLM entities never made it into v2.

For each mapped entity_type, do one of:

**Case A — patient has exactly one v2 nodule row:**
```sql
UPDATE main.canonical_us_nodule_v2 v2
SET <target_col> = COALESCE(v2.<target_col>, e.<entity_value>)
FROM main.<entity_source_table> e
WHERE e.research_id = v2.research_id
  AND e.entity_type = '<type>'
  AND v2.source_modality = 'US'
  AND (SELECT COUNT(*) FROM main.canonical_us_nodule_v2
       WHERE research_id = v2.research_id) = 1;
```

**Case B — patient has ZERO v2 nodule rows but LLM has an exam's worth of entities:**
INSERT a new v2 row per exam using the LLM entities as the primary source. Use the gland/LN v2 `us_exam_id` recipe (join on `research_id` + `exam_date`, fall back to COALESCE as `canonical_us_exam_master_VIEW_v2` does). Set `source_modality = 'US'` and `nodule_index = 1`.

**Case C — patient has multiple v2 nodules and multiple LLM entities:**
Skip for this pass — these need a separate nodule-matching logic. Write these research_ids into `manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1` with entity counts, then continue.

After the UPDATE/INSERTs, re-run the gap query and show me:
- Rows absorbed via Case A (UPDATE count)
- Rows inserted via Case B (INSERT count)
- Rows deferred via Case C (row count in `us_llm_absorption_deferred_multi_nodule_v1`)
- Remaining gap count after this pass

---

## Phase 3 — Absorb `tirads_v2_nodules_raw` nodule_index = 0 rows

Target: the 5,082 rows at `nodule_index = 0` representing 3,021 patients.

For each such row:

```sql
-- Does this patient already have a v2 nodule for this exam_date?
SELECT research_id, exam_date, COUNT(*)
FROM main.canonical_us_nodule_v2
WHERE (research_id, exam_date) IN (
  SELECT research_id, exam_date FROM main.tirads_v2_nodules_raw WHERE nodule_index = 0
)
GROUP BY research_id, exam_date;
```

**Case A — no existing v2 row for that (research_id, exam_date):**
INSERT as a new v2 row with `nodule_index = 1`, using the locked `us_exam_id` recipe.

**Case B — v2 already has 1+ rows for that (research_id, exam_date):**
Compare the raw row's feature columns against the existing v2 row(s). If every non-NULL raw field matches (or is NULL in v2 and non-NULL in raw), UPDATE v2 via COALESCE. If any field conflicts, log to `manuscript_workspace.us_raw_index0_conflict_v1` and do nothing.

Report counts: inserted / updated / conflicted / skipped.

Also handle the `nodule_index >= 1` rows (6,832 rows): verify they already exist in `canonical_us_nodule_v2` with matching payload via MD5 content hash on the shared column set. If all match → no-op. If any don't match → log to `us_raw_index_mismatch_v1` and show me before proceeding.

---

## Phase 4 — Verify zero regression on canonical_us_nodule_v2

Before dropping anything:

```sql
-- Row count delta
SELECT
  (SELECT COUNT(*) FROM main.canonical_us_nodule_v2) AS v2_now,
  <v2_count_before_this_pass> AS v2_before,
  (SELECT COUNT(*) FROM main.canonical_us_nodule_v2) - <v2_count_before_this_pass> AS delta;

-- Patient coverage delta
SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_nodule_v2;

-- source_modality integrity
SELECT source_modality, COUNT(*)
FROM main.canonical_us_nodule_v2
GROUP BY source_modality;   -- must be 100% 'US'

-- No duplicate nodule_index per (research_id, exam_date)
SELECT research_id, exam_date, nodule_index, COUNT(*)
FROM main.canonical_us_nodule_v2
GROUP BY 1,2,3
HAVING COUNT(*) > 1
LIMIT 20;   -- must be empty
```

If any check fails, STOP and show me output. Do not proceed to Phase 5.

---

## Phase 5 — Archive + drop the 4 held tables

For each of the 4 tables:

```sql
-- 1. Content hash before
SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR)))
FROM main.<table> t;

-- 2. Copy to archive with archived_ prefix
CREATE TABLE "Thyroid 2026 UPdated".us_legacy_20260421.archived_<table> AS
SELECT * FROM main.<table>;

-- 3. Content hash after
SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR)))
FROM "Thyroid 2026 UPdated".us_legacy_20260421.archived_<table> t;

-- 4. If hashes match AND no referencing view exists → DROP
DROP TABLE main.<table>;
```

Also check `information_schema.view_table_usage` (or equivalent) for any view that references each table before dropping. If any view references survive, rewrite the view to point at the archive copy first, then drop.

---

## Phase 6 — Close out the gap stub

```sql
-- Rebuild gap from scratch
-- (if Phase 2 absorbed everything, this should be 0 rows or only Case C deferrals)
CREATE OR REPLACE TABLE manuscript_workspace.us_llm_absorption_gap_v1 AS
<new gap query — LLM-only patients minus the ones absorbed or deferred>;

SELECT COUNT(*) FROM manuscript_workspace.us_llm_absorption_gap_v1;
```

- If 0 rows → `DROP TABLE manuscript_workspace.us_llm_absorption_gap_v1`.
- If rows remain (expected: Case C deferrals + any truly non-absorbable types) → keep the table and show me what's in it.

---

## Phase 7 — Commit

Commit message:

```
US v2 final-drop + LLM absorption (2026-04-21)

Absorbed LLM entity tables into canonical_us_nodule_v2:
- note_entities_llm_tirads_granular        (<N> rows updated / <M> rows inserted)
- note_entities_llm_us_nodule_dynamics     (<N> rows updated)
- note_entities_llm_us_nodule_tirads       (<N> rows updated / <M> rows inserted)

Absorbed tirads_v2_nodules_raw nodule_index=0 rows:
- <N> inserted, <M> updated, <K> conflicts logged

All 4 held tables archived to "Thyroid 2026 UPdated".us_legacy_20260421
and dropped from main.

Gap stub: <closed | <N> deferred Case C / non-absorbable rows remain>

canonical_us_nodule_v2 now:
- <N> total rows / <M> distinct patients
- source_modality = 'US' for 100% of rows
- no duplicate (research_id, exam_date, nodule_index) keys
```

Push to origin/main.

---

## Report back

Before you start Phase 2, show me the Phase 1 mapping table.
Before you start Phase 5, show me the Phase 4 verification output.
After the whole pass, show me the final `main` schema object list filtered to US/TIRADS-related names, the final archive table list, and the commit SHA.

---

## Next-step queue (do NOT do these in this pass)

1. `Script 376_feature_string_to_pts_recompute.py` — normalize feature-string variants ("wider than tall" vs "wider_than_tall", "ill-defined" vs "ill_defined", bracketed JSON arrays in echogenic_foci), then recompute `acr2017_tirads_points` + `acr2017_tirads_category` for the ~14K rows with all 5 features but no points. Expected fill: 18,187 rows (18,684 − 497).
2. CPM column audit — classify each `tirads_*_v12` / `tirads_*_v271` / `tirads_*_v271b` column on `canonical_patient_master` as "identical to _v2 → drop" or "genuinely different → rename with context suffix." ~17 columns.
