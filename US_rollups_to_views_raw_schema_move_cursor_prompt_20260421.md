# US rollups → views, raw feeds → `raw` schema

**Target DB:** `thyroid_canonical_publication_v1_0`
**Date:** 2026-04-21
**Pattern:** verify-derivability → build view → parity-check → drop table → replace with view

## Goal

Reduce `main` to exactly 3 canonical US tables. Convert the 2 rollup "masters" to views (they contain zero unique data — every column is derivable from the 3 granular masters). Move the 2 raw Excel feeds to a new `raw` schema.

**End state on `main`:**

```
main.canonical_us_nodule_v2           BASE TABLE  (master)
main.canonical_us_thyroid_gland_v2    BASE TABLE  (master)
main.canonical_us_lymph_node_v2       BASE TABLE  (master)
main.canonical_us_exam_master_VIEW_v2      VIEW        (derived from 3 masters)
main.canonical_us_patient_master_VIEW_v2   VIEW        (derived from 3 masters, via exam_master view)
```

**Moved out of `main`:**

```
raw.ultrasound_reports    (was main.ultrasound_reports, 4,074 patients, 223 cols)
raw.us_nodules_tirads     (was main.us_nodules_tirads, 10,859 patients, 36 cols)
```

---

## Phase 0 — Safety: codebase grep for writes to the rollup tables

Before touching anything, grep the repo for scripts that INSERT/UPDATE/CREATE either rollup table. If any pipeline currently writes to `canonical_us_exam_master_VIEW_v2` or `canonical_us_patient_master_VIEW_v2`, those scripts will break when the table becomes a view.

```bash
grep -rn -iE "(insert\s+into|update|create\s+(or\s+replace\s+)?table)\s+[^;]*canonical_us_exam_master_VIEW_v2" scripts/
grep -rn -iE "(insert\s+into|update|create\s+(or\s+replace\s+)?table)\s+[^;]*canonical_us_patient_master_VIEW_v2" scripts/
```

Report findings. Expected outcome: 1-2 "builder" scripts per rollup (likely `Script_363_*` / `Script_364_*` era). Those scripts need their CREATE TABLE statements converted to CREATE OR REPLACE VIEW and moved into this prompt's Phase 2 SQL, OR archived if the view definition fully supersedes them.

Do NOT proceed to Phase 2 until we've agreed on the disposition of every writer script you find.

---

## Phase 1 — Column-by-column derivability audit

For each rollup, confirm every column maps to a derivation from the 3 masters. Write the mapping as SQL that we'll use verbatim in the view DDL.

### `canonical_us_exam_master_VIEW_v2` — derivation spec

Grain: one row per `(research_id, exam_date)` where any US happened (any gland, nodule, or LN record exists for that exam).

```sql
-- Phase 1 output: show 5 sample rows for each field to verify the logic matches
-- the current table's values before replacing it.
```

| view column | derivation |
|---|---|
| `research_id`, `exam_date` | from the union of exam keys across gland_v2/nodule_v2/lymph_node_v2 |
| `us_exam_id` | COALESCE of the per-modality ids — use the SAME recipe that the current exam_master table uses; look at Script 363/364 to confirm |
| `n_nodules_on_exam` | `COUNT(*)` on `canonical_us_nodule_v2` GROUP BY `(research_id, exam_date)` |
| `largest_nodule_cm` | `MAX(longest_dimension_mm)/10.0` on nodule_v2 |
| `second_largest_nodule_cm` | `NTH_VALUE(..., 2) OVER (...)` on nodule_v2 ordered DESC by size |
| `bilateral_flag` | `COUNT(DISTINCT side) >= 2` on nodule_v2 |
| `isthmus_nodule_flag` | `BOOL_OR(LOWER(nodule_location) LIKE '%isthmus%')` on nodule_v2 |
| `worst_tirads_category_this_exam` | MAX by band order (TR5>TR4>TR3>TR2>TR1) on `acr2017_tirads_category` (or `updated_tirads_category` — pick the one the current table uses and keep consistent) |
| `worst_tirads_points_this_exam` | `MAX(acr2017_tirads_points)` on nodule_v2 |
| `best_tirads_category_this_exam` | MIN by band order |
| `count_tr1..tr5` | `COUNT(*) FILTER (WHERE <category_col> = 'TR<n>')` on nodule_v2 |
| `has_gland_findings` | `EXISTS` row in gland_v2 for that exam key |
| `has_us_ln_findings` | `EXISTS` row in lymph_node_v2 for that exam key |
| `n_us_ln_total_on_exam` | `COUNT(*)` on lymph_node_v2 GROUP BY exam key |
| `n_abnormal_us_ln_on_exam` | `COUNT(*) FILTER (suspicious/abnormal flag)` on lymph_node_v2 — check the exact flag column the current table uses |
| `exam_rank_for_patient` | `DENSE_RANK() OVER (PARTITION BY research_id ORDER BY exam_date)` |
| `is_preop_exam` | derivation depends on how the current table populates this; if it joins to surgery date from a clinical table, the view has to do the same JOIN |
| `any_nlp_backfill_pending_on_exam` | `BOOL_OR(nlp_backfill_pending)` across the 3 masters for that exam key |

### `canonical_us_patient_master_VIEW_v2` — derivation spec

Grain: one row per `research_id` that has any US record.

```sql
-- Can be defined against the exam_master view + the 3 masters.
```

| view column | derivation |
|---|---|
| `research_id` | DISTINCT patients |
| `has_any_us` | always TRUE (filtered on any US existence) |
| `n_us_exams` | `COUNT(DISTINCT exam_date)` from exam_master view |
| `first_us_date`, `last_us_date` | MIN/MAX exam_date from exam_master |
| `preop_us_available_flag` | `BOOL_OR(is_preop_exam)` from exam_master |
| `max_tirads_category_ever` | worst-band over all exams |
| `max_tirads_points_ever` | `MAX(acr2017_tirads_points)` across all nodules |
| `tirads_category_at_first_exam` | worst band at MIN(exam_date) |
| `tirads_category_at_last_preop_exam` | worst band at MAX(exam_date) filtered to preop |
| `n_nodules_total_across_exams` | `SUM(n_nodules_on_exam)` from exam_master |
| `bilateral_disease_flag_ever` | `BOOL_OR(bilateral_flag)` from exam_master |
| `multifocal_flag_ever` | `BOOL_OR(n_nodules_on_exam >= 2)` from exam_master |
| `first_high_risk_tirads_date` | MIN(exam_date) WHERE `worst_tirads_category_this_exam IN ('TR4','TR5')` |
| `has_us_ln_findings_ever` | `BOOL_OR(has_us_ln_findings)` from exam_master |
| `any_suspicious_us_ln_ever` | `BOOL_OR(n_abnormal_us_ln_on_exam > 0)` from exam_master |
| `first_abnormal_us_ln_date` | MIN(exam_date) WHERE `n_abnormal_us_ln_on_exam > 0` |
| `has_gland_findings_ever` | `BOOL_OR(has_gland_findings)` from exam_master |
| `any_nlp_backfill_pending_for_patient` | `BOOL_OR(any_nlp_backfill_pending_on_exam)` |

**Derivability trap to check:** `is_preop_exam` / `preop_us_available_flag` / `tirads_category_at_last_preop_exam` — if these depend on surgery date from a clinical table (e.g., `canonical_patient_master` or a pathology table), the view definition must include that JOIN. If the current table's preop logic is a moving target (e.g., depends on data that gets updated), a view will automatically stay current — which is usually what you want, but worth calling out. Report what the current logic uses.

---

## Phase 2 — Build candidate views in `manuscript_workspace`

Do NOT touch `main` yet. Build both views in `manuscript_workspace` first so we can parity-check them against the live tables:

```sql
CREATE OR REPLACE VIEW manuscript_workspace.candidate_us_exam_master_v2 AS
<derivation SQL from Phase 1>;

CREATE OR REPLACE VIEW manuscript_workspace.candidate_us_patient_master_v2 AS
<derivation SQL from Phase 1>;
```

---

## Phase 3 — Parity check (view vs. current table)

For each rollup, every column must match between the current table and the candidate view. Acceptable drift: zero.

```sql
-- Row count
SELECT
  (SELECT COUNT(*) FROM main.canonical_us_exam_master_VIEW_v2) AS table_rows,
  (SELECT COUNT(*) FROM manuscript_workspace.candidate_us_exam_master_v2) AS view_rows;

-- Full content diff (anti-joins in both directions)
SELECT 'in_table_not_view' AS side, COUNT(*)
FROM (SELECT * FROM main.canonical_us_exam_master_VIEW_v2
      EXCEPT
      SELECT * FROM manuscript_workspace.candidate_us_exam_master_v2)
UNION ALL
SELECT 'in_view_not_table', COUNT(*)
FROM (SELECT * FROM manuscript_workspace.candidate_us_exam_master_v2
      EXCEPT
      SELECT * FROM main.canonical_us_exam_master_VIEW_v2);
-- Both counts MUST be 0.
```

If either side > 0, show me the first 20 differing rows grouped by which columns disagree. Do not proceed to Phase 4 until the diff is zero OR we've explicitly agreed that the drift is a view-side correction (e.g., fixing a stale rollup that missed the Script 376 feature-normalization update).

Expected case: the rollup tables were built before Script 376 ran, so the view may have MORE populated TIRADS categories than the table. If that's the only diff, it's acceptable — the view is the corrected version.

Run the same parity check for `patient_master`.

---

## Phase 4 — Replace tables with views

```sql
-- Archive the pre-replacement tables first (safety net, drop in 2 weeks)
CREATE TABLE "Thyroid 2026 UPdated".us_legacy_20260421.archived_canonical_us_exam_master_VIEW_v2 AS
SELECT * FROM main.canonical_us_exam_master_VIEW_v2;

CREATE TABLE "Thyroid 2026 UPdated".us_legacy_20260421.archived_canonical_us_patient_master_VIEW_v2 AS
SELECT * FROM main.canonical_us_patient_master_VIEW_v2;

-- Drop the base tables
DROP TABLE main.canonical_us_exam_master_VIEW_v2;
DROP TABLE main.canonical_us_patient_master_VIEW_v2;

-- Create the views in place (re-using the parity-verified SQL from Phase 2)
CREATE VIEW main.canonical_us_exam_master_VIEW_v2 AS
<SQL>;

CREATE VIEW main.canonical_us_patient_master_VIEW_v2 AS
<SQL>;
```

Verify:

```sql
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main'
  AND table_name IN ('canonical_us_exam_master_VIEW_v2','canonical_us_patient_master_VIEW_v2');
-- Expected: both rows show table_type = 'VIEW'
```

---

## Phase 5 — Move raw feeds to `raw` schema

```sql
CREATE SCHEMA IF NOT EXISTS raw;

-- Copy then drop (MotherDuck/DuckDB doesn't cleanly support ALTER SCHEMA across schemas)
CREATE TABLE raw.ultrasound_reports AS SELECT * FROM main.ultrasound_reports;
CREATE TABLE raw.us_nodules_tirads  AS SELECT * FROM main.us_nodules_tirads;

-- Content-hash verify
SELECT
  (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR))) FROM main.ultrasound_reports t) AS src_hash,
  (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR))) FROM raw.ultrasound_reports t) AS dst_hash;
-- Expected: equal.

SELECT
  (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR))) FROM main.us_nodules_tirads t) AS src_hash,
  (SELECT MD5(STRING_AGG(CAST(t AS VARCHAR) ORDER BY CAST(t AS VARCHAR))) FROM raw.us_nodules_tirads t) AS dst_hash;
-- Expected: equal.

-- Only drop from main if hashes match
DROP TABLE main.ultrasound_reports;
DROP TABLE main.us_nodules_tirads;
```

---

## Phase 6 — Update referencing scripts

Grep the repo for anything reading from `main.ultrasound_reports` or `main.us_nodules_tirads`:

```bash
grep -rn -E "main\.ultrasound_reports|FROM\s+ultrasound_reports" scripts/
grep -rn -E "main\.us_nodules_tirads|FROM\s+us_nodules_tirads" scripts/
```

Rewrite each match to `raw.ultrasound_reports` / `raw.us_nodules_tirads`. If any unqualified reference (no schema prefix, relying on search_path) exists, fix those too.

Also check `views_readable` — if any view there joins to the raw feeds, update the view definitions.

Report every file touched.

---

## Phase 7 — Final verification

```sql
-- Final main.US-related object count
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main'
  AND (LOWER(table_name) LIKE '%us%' OR LOWER(table_name) LIKE '%tirads%'
       OR LOWER(table_name) LIKE '%ultrasound%' OR LOWER(table_name) LIKE '%nodule%')
ORDER BY table_type, table_name;
-- Expected:
--   BASE TABLE: canonical_us_lymph_node_v2, canonical_us_nodule_v2, canonical_us_thyroid_gland_v2
--               (plus the 3 false-positive keyword matches: manuscript_cohort_v1,
--                molecular_fusions_unnested_VIEW_v2, specimen_tumor_focus_v1)
--   VIEW:       canonical_us_exam_master_VIEW_v2, canonical_us_patient_master_VIEW_v2

-- Confirm raw schema
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'raw'
ORDER BY table_name;
-- Expected: ultrasound_reports, us_nodules_tirads — both BASE TABLE.

-- Smoke test the views return sensible data
SELECT COUNT(*) AS exam_master_rows FROM main.canonical_us_exam_master_VIEW_v2;
SELECT COUNT(*) AS patient_master_rows FROM main.canonical_us_patient_master_VIEW_v2;
-- Expected: 18,102 and 10,859 (or updated counts if Script 376's new TIRADS categorizations shifted things).
```

---

## Phase 8 — Commit

Commit message:

```
US schema hygiene: rollups → views, raw feeds → raw schema

main now holds exactly 3 canonical US tables:
- canonical_us_nodule_v2 (master, 37,579 rows)
- canonical_us_thyroid_gland_v2 (master, 13,578 rows)
- canonical_us_lymph_node_v2 (master, 6,801 rows)

Converted to views (derived from the 3 masters):
- canonical_us_exam_master_VIEW_v2 (was table, 18,102 rows)
- canonical_us_patient_master_VIEW_v2 (was table, 10,859 rows)

Pre-replacement snapshots archived to
"Thyroid 2026 UPdated".us_legacy_20260421.archived_canonical_us_*_master_v2.

Raw Excel feeds moved out of main:
- ultrasound_reports  -> raw.ultrasound_reports (6,793 rows, 223 cols)
- us_nodules_tirads   -> raw.us_nodules_tirads (10,859 rows, 36 cols)

Updated <N> scripts to reference raw.* instead of main.* for the moved feeds.
<N> scripts that previously wrote to canonical_us_{exam,patient}_master_v2
archived/refactored (see commit for list).
```

Push to origin/main.

---

## Report back

1. Phase 0 grep output — every script that writes to either rollup, with disposition decided before proceeding.
2. Phase 3 parity check output (both diffs must be 0 before Phase 4).
3. Phase 5 hash comparison (src and dst MD5 must match).
4. Phase 6 list of every file touched in the scripts/ grep.
5. Phase 7 final table inventory for both schemas.
6. Commit SHA.

---

## Out of scope (queued)

1. CPM column audit: `tirads_*_v12` / `tirads_*_v271` / `tirads_*_v271b` on `canonical_patient_master`.
2. 4-band concordance outlier investigation (88 ACR-TR5 / updated-TR1 rows).
3. Audit backlog triage: `us_raw_index0_conflict_v1` (32,146), `us_raw_index_mismatch_v1` (13,166).
4. `us_llm_absorption_deferred_multi_nodule_v1` (825 patients).
5. 1-point → TR1 fix (3,044 rows with valid points but NULL category).
