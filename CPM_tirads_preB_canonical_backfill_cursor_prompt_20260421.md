# CPM TIRADS pre-B — canonical backfill of `cupm_v2`

**Target DB:** `thyroid_canonical_publication_v1_0`
**Target view:** `main.canonical_us_patient_master_VIEW_v2` (currently 19 cols, will become 28)
**Backing table (new):** `main.cupm_v2_canonical_backfill_v1`
**Archive DB:** `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421` (created if absent)
**Date:** 2026-04-21
**Predecessor:** Part A audit (complete) + Part B Phase 1 coverage audit (STOP-gated; `manuscript_workspace.cpm_tirads_canonical_coverage_v1` has 13 `gap_ABORT` rows)
**Successor:** Part B re-run from Phase 1 (expected: 0 `gap_ABORT`, 0 `gap_other_v2_table`)

## Architecture context (Logan's clarification, 2026-04-21)

Option C-soft is the operative architecture: canonical truth lives on the `canonical_us_*_v2` surface **at the right grain** — patient-grain on `cupm_v2`, exam-grain on `cuem_v2`, nodule-grain on `cunc_v2`. Consumers JOIN the right-grain table with aggregation where needed.

Two sub-rules pulled forward from Logan's adjudication:

1. The 13 `gap_ABORT` cols from Part B Phase 1 split: **6 DROP without replacement**, **7 PORT to `cupm_v2`** (rename-on-move).
2. Of the 8 `gap_other_v2_table` rows, **2 are pre-rolled to `cupm_v2`** because they're hot paths (4+ readers each). The other 6 stay as inline JOINs at the consumer.

This pre-B micro-script handles only the **9 column additions to `cupm_v2`**. Part B itself remains the larger commit that drops the CPM columns + migrates 8+ cohort views + freezes 6 writer scripts.

## The 9 columns

### 7 port-from-CPM (rename-on-move)

| new column on `cupm_v2`                                | type    | sourced from `main.canonical_patient_master`  |
|---                                                     |---      |---                                            |
| `imaging_laterality_rollup_v2`                         | VARCHAR | `imaging_laterality_rollup_v271b`             |
| `pathology_vs_imaging_laterality_concordant_v2`        | VARCHAR | `pathology_vs_imaging_laterality_concordant_v271b` |
| `tumor_pathology_laterality_v2`                        | VARCHAR | `tumor_pathology_laterality_v271b`            |
| `any_fna_recommended_report_ever`                      | BOOLEAN | `tirads_v2_any_fna_recommended_report`        |
| `any_fna_recommended_report_source`                    | VARCHAR | `tirads_v2_any_fna_recommended_report_source` |
| `tirads_worst_rank_ever`                               | INTEGER | `tirads_v2_worst_rank`                        |
| `tirads_worst_rank_source`                             | VARCHAR | `tirads_v2_worst_rank_source`                 |

Naming convention:

- Laterality cols → `_v2` suffix (consistent with the other v2-suffixed analytic cols on `cupm_v2`).
- "ever" rollups → `_ever` suffix; drop the `tirads_v2_` prefix.
- `_source` cols → bare `_source` suffix.
- **All `_v271b` and `tirads_v2_` prefixes are deleted on move. This is the moment to lose generation suffixes for good.**

### 2 compute-from-`cunc_v2` (per-RID aggregations)

| new column on `cupm_v2` | type   | derivation                                                                                        |
|---                      |---     |---                                                                                                |
| `max_nodule_size_mm`    | DOUBLE | `MAX(GREATEST(COALESCE(length_mm,0), COALESCE(width_mm,0), COALESCE(height_mm,0)))` per `research_id`, fallback to `MAX(size_cm_max)*10` when the three individual dims are all NULL. |
| `n_nodule_records`      | BIGINT | `COUNT(*)` per `research_id` over `canonical_us_nodule_v2`.                                       |

> ⚠️ Logan's original recipe referenced `longest_dimension_mm` — that column does not exist on `cunc_v2`. The size columns available are `length_mm`, `width_mm`, `height_mm`, `size_cm_max` (DOUBLE in **cm**), `extracted_size_cm`. The `GREATEST(...)`-with-`size_cm_max`-fallback recipe above preserves the spirit of "longest dimension in mm" without inventing a column. Confirm or override.

## Architectural sub-decision: backing-snapshot table

`cupm_v2` is a VIEW. The 7 port cols cannot be sourced directly from `main.canonical_patient_master` inside the view body, because Part B Phase 5 will DROP those columns from CPM and the view would break.

**Solution:** persist the 7 ports into a small backing table:

```sql
CREATE TABLE main.cupm_v2_canonical_backfill_v1 (
    research_id INTEGER PRIMARY KEY,
    imaging_laterality_rollup_v2                   VARCHAR,
    pathology_vs_imaging_laterality_concordant_v2  VARCHAR,
    tumor_pathology_laterality_v2                  VARCHAR,
    any_fna_recommended_report_ever                BOOLEAN,
    any_fna_recommended_report_source              VARCHAR,
    tirads_worst_rank_ever                         INTEGER,
    tirads_worst_rank_source                       VARCHAR,
    backfilled_at_utc                              TIMESTAMP DEFAULT now(),
    source_snapshot_note                           VARCHAR
                                                   DEFAULT 'snapshot from main.canonical_patient_master 2026-04-21; upstream writer scripts (271b, 221) frozen by CPM TIRADS Part B'
);
```

The new `cupm_v2` view definition then `LEFT JOIN`s `cupm_v2_canonical_backfill_v1` on `research_id`. This also matches the Part B Phase 4 freeze of Scripts 271b / 271a / 271 (laterality writers) and is consistent with Script 221 (fna_recommended_report writer) staying alive but no longer needing CPM as an output sink.

The 2 compute cols (`max_nodule_size_mm`, `n_nodule_records`) live in the view body directly via a CTE on `cunc_v2` and need no backing table.

## Constraints

- **Do NOT modify `canonical_us_exam_master_VIEW_v2` or `canonical_us_nodule_v2`.** Only `cupm_v2`'s view body changes; the per-exam and per-nodule grains stay as-is.
- **Additive only.** Every existing column on `cupm_v2` keeps its name, type, and ordinal position relative to the others. The 9 new columns append at the end (positions 20-28).
- **Row count must not change.** `cupm_v2` is currently 10,859 rows (1 per RID). After the change it must still be 10,859.
- **Lossless backfill.** For every RID currently populated in CPM for the 7 port cols, the backing table must have the same value. Verify cell-by-cell.
- **No drop of CPM columns in this micro-script.** Part B Phase 5 owns those drops; pre-B is purely additive on the canonical side.
- **Lint:** `ruff check scripts/_preB_*.py` before each commit.

## Phases

### Phase 1 — Reconnaissance (read-only)

1. Confirm `main.canonical_us_patient_master_VIEW_v2` is a VIEW; pull current `view_definition`.
2. Confirm `main.canonical_us_nodule_v2` schema includes `length_mm`, `width_mm`, `height_mm`, `size_cm_max`, `research_id` (Logan: don't pull from CPM — pull straight from `cunc_v2`).
3. Pull populated counts on CPM for the 7 source cols (sanity check matches Part A inventory):
   - `imaging_laterality_rollup_v271b` → 3,439
   - `pathology_vs_imaging_laterality_concordant_v271b` → 10,871
   - `tumor_pathology_laterality_v271b` → 3,986
   - `tirads_v2_any_fna_recommended_report` → 4,073
   - `tirads_v2_any_fna_recommended_report_source` → 4,073
   - `tirads_v2_worst_rank` → 2,465
   - `tirads_v2_worst_rank_source` → 2,465
4. Pull cunc_v2 row count grouped by RID — confirm distribution.
5. **Stop and report** the recon table to Logan if any source column count drifts from Part A by >0.5%; otherwise auto-continue to Phase 2.

### Phase 2 — Build the backfill snapshot

```sql
CREATE OR REPLACE TABLE main.cupm_v2_canonical_backfill_v1 AS
SELECT
    research_id,
    imaging_laterality_rollup_v271b                     AS imaging_laterality_rollup_v2,
    pathology_vs_imaging_laterality_concordant_v271b    AS pathology_vs_imaging_laterality_concordant_v2,
    tumor_pathology_laterality_v271b                    AS tumor_pathology_laterality_v2,
    tirads_v2_any_fna_recommended_report                AS any_fna_recommended_report_ever,
    tirads_v2_any_fna_recommended_report_source         AS any_fna_recommended_report_source,
    tirads_v2_worst_rank                                AS tirads_worst_rank_ever,
    tirads_v2_worst_rank_source                         AS tirads_worst_rank_source,
    now()                                               AS backfilled_at_utc,
    'snapshot from main.canonical_patient_master 2026-04-21; upstream writers frozen by CPM TIRADS Part B'
                                                        AS source_snapshot_note
FROM main.canonical_patient_master
WHERE imaging_laterality_rollup_v271b IS NOT NULL
   OR pathology_vs_imaging_laterality_concordant_v271b IS NOT NULL
   OR tumor_pathology_laterality_v271b IS NOT NULL
   OR tirads_v2_any_fna_recommended_report IS NOT NULL
   OR tirads_v2_any_fna_recommended_report_source IS NOT NULL
   OR tirads_v2_worst_rank IS NOT NULL
   OR tirads_v2_worst_rank_source IS NOT NULL;
```

Verify:

- Row count of `cupm_v2_canonical_backfill_v1` ≤ 10,871 (CPM total).
- For each of the 7 port cols, populated count on the backfill table matches the CPM source.
- `research_id` is unique on the backfill table.
- A 50-RID random spot check confirms every value matches CPM exactly.

### Phase 3 — Replace `cupm_v2` view definition

The new view body is the existing body **plus**:

- a third CTE `nodule_agg` that computes `max_nodule_size_mm` and `n_nodule_records` from `canonical_us_nodule_v2`, and
- a `LEFT JOIN main.cupm_v2_canonical_backfill_v1 AS bf USING (research_id)`,
- a `LEFT JOIN nodule_agg AS na USING (research_id)`,
- 9 new projected columns at the end of the SELECT list.

Proposed full new view body — see "View diff" section below for the precise SQL.

Deploy via:

```sql
CREATE OR REPLACE VIEW main.canonical_us_patient_master_VIEW_v2 AS
<new body>;
```

Note: DuckDB/MotherDuck `CREATE OR REPLACE VIEW` is atomic; downstream consumers see either the old definition or the new one, never a partial state.

### Phase 4 — Verify view shape

```sql
-- 1. Column count
SELECT COUNT(*) AS n_cols FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_us_patient_master_VIEW_v2';
-- expect: 28 (was 19, +9)

-- 2. Row count unchanged
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rids
FROM main.canonical_us_patient_master_VIEW_v2;
-- expect: 10859 / 10859 (must match pre-change)

-- 3. New column populated counts
SELECT
  COUNT(imaging_laterality_rollup_v2)                      AS n_lat_rollup,
  COUNT(pathology_vs_imaging_laterality_concordant_v2)     AS n_lat_concord,
  COUNT(tumor_pathology_laterality_v2)                     AS n_path_lat,
  COUNT(any_fna_recommended_report_ever)                   AS n_fna_rep,
  COUNT(any_fna_recommended_report_source)                 AS n_fna_rep_src,
  COUNT(tirads_worst_rank_ever)                            AS n_worst_rank,
  COUNT(tirads_worst_rank_source)                          AS n_worst_rank_src,
  COUNT(max_nodule_size_mm)                                AS n_max_size,
  COUNT(n_nodule_records)                                  AS n_n_records
FROM main.canonical_us_patient_master_VIEW_v2;
-- expect: matches CPM source counts for the 7 ports;
--          n_max_size + n_n_records match cunc_v2 distinct RID count.

-- 4. Existing columns intact (regression)
-- Pull any 5 RIDs and verify the 19 original columns return identical values
-- before vs after.
```

### Phase 5 — Rerun coverage audits

1. Re-run Part A inventory script (`scripts/output/_cpm_tirads_audit_phase0_inventory.py`) — CPM TIRADS column count unchanged (53), populations unchanged.
2. Re-run Part B Phase 1 coverage script (`scripts/output/_cpm_tirads_partB_phase1_coverage.py`), updating the MAPPING table to reflect:
   - The 7 port cols now have `mapped_cupm_v2` status pointing to the renamed targets.
   - The 2 compute cols now have `mapped_cupm_v2` status (not `gap_other_v2_table`).
   - The 6 DROP-without-replacement cols stay as `mapped_DROP` (or new status code) — they're slated for Part B Phase 5 with no canonical equivalent by design.
3. Verify `manuscript_workspace.cpm_tirads_canonical_coverage_v1` shows **0 `gap_ABORT` and 0 `gap_other_v2_table`** rows.

### Phase 6 — QA + commit

Write `qa/qa_script_cpm_tirads_preB.json`:

```json
{
  "view_column_count": {"before": 19, "after": 28},
  "view_row_count":    {"before": 10859, "after": 10859},
  "backfill_table_rows": <int>,
  "backfill_table_distinct_rids": <int>,
  "port_col_match_check": {
    "imaging_laterality_rollup_v2":                    {"cpm_n": 3439, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "pathology_vs_imaging_laterality_concordant_v2":   {"cpm_n": 10871, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "tumor_pathology_laterality_v2":                   {"cpm_n": 3986, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "any_fna_recommended_report_ever":                 {"cpm_n": 4073, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "any_fna_recommended_report_source":               {"cpm_n": 4073, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "tirads_worst_rank_ever":                          {"cpm_n": 2465, "cupm_v2_n": <int>, "cell_match_pct": 100.0},
    "tirads_worst_rank_source":                        {"cpm_n": 2465, "cupm_v2_n": <int>, "cell_match_pct": 100.0}
  },
  "compute_col_check": {
    "max_nodule_size_mm": {"n_populated": <int>, "min": <float>, "max": <float>, "p50": <float>},
    "n_nodule_records":   {"n_populated": <int>, "min": <int>, "max": <int>, "p50": <int>}
  },
  "coverage_audit_after": {"gap_ABORT": 0, "gap_other_v2_table": 0}
}
```

Commit message:

```
CPM TIRADS pre-B: backfill 9 columns into canonical_us_patient_master_VIEW_v2 (2026-04-21)

Adds 7 port-from-CPM columns (laterality + fna_recommended_report + worst_rank
families, rename-on-move to drop _v271b / tirads_v2_ generation suffixes) and
2 compute-from-cunc_v2 columns (max_nodule_size_mm, n_nodule_records).

The 7 port cols are persisted in main.cupm_v2_canonical_backfill_v1 (new) so
the cupm_v2 view survives the CPM column drops in Part B Phase 5. Backing-
table semantics: snapshot from CPM 2026-04-21; upstream writer scripts (271b,
221) get frozen by CPM TIRADS Part B Phase 4. Future re-derivations require
a deliberate refresh of the backfill table.

cupm_v2 column count: 19 -> 28
Backfill table rows: <N>
Coverage audit after: 0 gap_ABORT, 0 gap_other_v2_table.

Resolves the STOP gate from Part B Phase 1; clears the path for Part B
Phases 2-7 to proceed.
```

Push to `origin/main`.

### Phase 7 — Hand-off

Confirm to Logan:

1. cupm_v2 column list (must show all 28).
2. Backfill table SHA (or row count + populated counts).
3. Refreshed `manuscript_workspace.cpm_tirads_canonical_coverage_v1` showing 0 gaps.
4. Commit SHA + push confirmation.

Then Part B can resume from its Phase 1 (re-run, expect 0 gaps), then Phases 2-7 as originally specified — with the additional Phase 3 reader-migration regex amendments noted at the bottom of this prompt.

## View diff (proposed `CREATE OR REPLACE VIEW` body)

Original cupm_v2 body (existing, 19 cols, single SELECT off `exam_agg` + `nodule_first_last`):

```sql
WITH exam_agg AS (
    SELECT research_id, ...,
           max(worst_tirads_category_this_exam) AS max_tirads_category_ever,
           ..., bool_or(any_nlp_backfill_pending_on_exam) AS any_nlp_backfill_pending_for_patient
    FROM main.canonical_us_exam_master_VIEW_v2
    GROUP BY 1
),
nodule_first_last AS (
    SELECT e.research_id,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date)
             FILTER (WHERE e.exam_rank_for_patient = 1) AS tirads_category_at_first_exam,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date DESC)
             FILTER (WHERE CAST(e.is_preop_exam AS BOOLEAN) IS NOT DISTINCT FROM TRUE)
             AS tirads_category_at_last_preop_exam,
           min(CASE WHEN upper(e.worst_tirads_category_this_exam) IN ('TR4','TR5')
                    THEN e.exam_date END) AS first_high_risk_tirads_date
    FROM main.canonical_us_exam_master_VIEW_v2 AS e
    GROUP BY 1
)
SELECT e.research_id, e.has_any_us, e.n_us_exams, e.first_us_date, e.last_us_date,
       e.preop_us_available_flag,
       e.max_tirads_category_ever, e.max_tirads_points_ever,
       nfl.tirads_category_at_first_exam, nfl.tirads_category_at_last_preop_exam,
       e.n_nodules_total_across_exams, e.bilateral_disease_flag_ever, e.multifocal_flag_ever,
       nfl.first_high_risk_tirads_date,
       e.has_us_ln_findings_ever, e.any_suspicious_us_ln_ever, e.first_abnormal_us_ln_date,
       e.has_gland_findings_ever, e.any_nlp_backfill_pending_for_patient
FROM exam_agg AS e
LEFT JOIN nodule_first_last AS nfl USING (research_id);
```

New cupm_v2 body (additive; existing portions unchanged):

```sql
CREATE OR REPLACE VIEW main.canonical_us_patient_master_VIEW_v2 AS
WITH exam_agg AS (
    -- (unchanged from current definition)
    SELECT research_id, CAST('t' AS BOOLEAN) AS has_any_us,
           count_star() AS n_us_exams,
           min(exam_date) AS first_us_date, max(exam_date) AS last_us_date,
           bool_or(is_preop_exam) AS preop_us_available_flag,
           max(worst_tirads_category_this_exam) AS max_tirads_category_ever,
           max(worst_tirads_points_this_exam)   AS max_tirads_points_ever,
           sum(n_nodules_on_exam)               AS n_nodules_total_across_exams,
           bool_or(bilateral_flag)              AS bilateral_disease_flag_ever,
           (sum(CASE WHEN n_nodules_on_exam >= 2 THEN 1 ELSE 0 END) > 0)
                                                AS multifocal_flag_ever,
           bool_or(has_us_ln_findings)          AS has_us_ln_findings_ever,
           bool_or(has_gland_findings)          AS has_gland_findings_ever,
           (sum(COALESCE(n_abnormal_us_ln_on_exam, 0)) > 0) AS any_suspicious_us_ln_ever,
           min(CASE WHEN n_abnormal_us_ln_on_exam IS NOT NULL
                     AND n_abnormal_us_ln_on_exam > 0 THEN exam_date END)
                                                AS first_abnormal_us_ln_date,
           bool_or(any_nlp_backfill_pending_on_exam)
                                                AS any_nlp_backfill_pending_for_patient
    FROM main.canonical_us_exam_master_VIEW_v2
    GROUP BY 1
),
nodule_first_last AS (
    -- (unchanged from current definition)
    SELECT e.research_id,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date)
             FILTER (WHERE e.exam_rank_for_patient = 1) AS tirads_category_at_first_exam,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date DESC)
             FILTER (WHERE CAST(e.is_preop_exam AS BOOLEAN) IS NOT DISTINCT FROM TRUE)
             AS tirads_category_at_last_preop_exam,
           min(CASE WHEN upper(e.worst_tirads_category_this_exam) IN ('TR4','TR5')
                    THEN e.exam_date END) AS first_high_risk_tirads_date
    FROM main.canonical_us_exam_master_VIEW_v2 AS e
    GROUP BY 1
),
nodule_agg AS (
    -- NEW: per-RID nodule rollups for max_nodule_size_mm + n_nodule_records
    SELECT research_id,
           MAX(
             COALESCE(
               NULLIF(GREATEST(COALESCE(length_mm, 0),
                               COALESCE(width_mm,  0),
                               COALESCE(height_mm, 0)), 0),
               size_cm_max * 10.0  -- fallback when individual dims all NULL
             )
           ) AS max_nodule_size_mm,
           COUNT(*) AS n_nodule_records
    FROM main.canonical_us_nodule_v2
    GROUP BY 1
)
SELECT
    -- (existing 19 columns, unchanged)
    e.research_id,
    e.has_any_us,
    e.n_us_exams,
    e.first_us_date,
    e.last_us_date,
    e.preop_us_available_flag,
    e.max_tirads_category_ever,
    e.max_tirads_points_ever,
    nfl.tirads_category_at_first_exam,
    nfl.tirads_category_at_last_preop_exam,
    e.n_nodules_total_across_exams,
    e.bilateral_disease_flag_ever,
    e.multifocal_flag_ever,
    nfl.first_high_risk_tirads_date,
    e.has_us_ln_findings_ever,
    e.any_suspicious_us_ln_ever,
    e.first_abnormal_us_ln_date,
    e.has_gland_findings_ever,
    e.any_nlp_backfill_pending_for_patient,
    -- 7 new port-from-CPM cols (via backfill snapshot table)
    bf.imaging_laterality_rollup_v2,
    bf.pathology_vs_imaging_laterality_concordant_v2,
    bf.tumor_pathology_laterality_v2,
    bf.any_fna_recommended_report_ever,
    bf.any_fna_recommended_report_source,
    bf.tirads_worst_rank_ever,
    bf.tirads_worst_rank_source,
    -- 2 new compute-from-cunc_v2 cols
    na.max_nodule_size_mm,
    na.n_nodule_records
FROM exam_agg AS e
LEFT JOIN nodule_first_last           AS nfl USING (research_id)
LEFT JOIN main.cupm_v2_canonical_backfill_v1 AS bf  USING (research_id)
LEFT JOIN nodule_agg                  AS na  USING (research_id);
```

## QA plan summary

| check | expected | failure means |
|---|---|---|
| `cupm_v2` is a VIEW after Phase 3 | yes | `CREATE OR REPLACE` got mis-typed |
| `cupm_v2` column count | 28 (was 19) | view body lost columns |
| `cupm_v2` row count | 10,859 | aggregation logic regressed; investigate the new LEFT JOINs (should be inner-set-preserving) |
| 7 port-col populated counts on `cupm_v2` | match CPM exactly | backfill snapshot drift |
| 50-RID spot check on each port col | 100% cell match vs CPM | snapshot SQL is wrong |
| `max_nodule_size_mm` populated count | ≈ count of distinct RIDs in `cunc_v2` (one per RID with at least one nodule) | nodule_agg CTE missing rows |
| `n_nodule_records` ≥ `n_nodules_total_across_exams` | TRUE for every RID where both populated | the COUNT(*) over cunc_v2 should equal or exceed the SUM over exam-master (one is per-record, the other is per-exam-roll) — flag if reverse |
| Coverage audit after rerun | 0 `gap_ABORT`, 0 `gap_other_v2_table` | mapping table not updated, OR a column we expected to port is missing |
| Existing 19 columns: 5-RID regression | 100% match before vs after | view body broke an existing aggregation |

## Stop gates

- **Phase 1 stop:** if any source column count drifts from Part A inventory by >0.5%, stop and report.
- **Phase 2 stop:** if `cupm_v2_canonical_backfill_v1` row count > 10,871, stop (impossible duplicate RIDs).
- **Phase 3 stop:** if the new view body fails to compile (`CREATE OR REPLACE VIEW` errors), stop and report the SQL error.
- **Phase 4 stop:** if any QA check fails, **roll back the view definition** to the pre-change body (it's still in `information_schema.views` — re-run `CREATE OR REPLACE VIEW` with the original body) and report.
- **Phase 5 stop:** if the rerun of `_cpm_tirads_partB_phase1_coverage.py` shows any `gap_ABORT` or `gap_other_v2_table` row that wasn't expected, stop and report (the MAPPING table has a bug).

## What this prompt does NOT do (out of scope)

- No drops on `main.canonical_patient_master`. Part B Phase 5 owns that.
- No edits to cohort views. Part B Phase 2 owns that.
- No edits to `cuem_v2` or `cunc_v2`. Their definitions stay.
- No freezing of writer scripts. Part B Phase 4 owns that.
- No rewrite of script-level readers. Part B Phase 3 owns that.

## Amendments to the original Part B prompt (for the agent that runs Part B after this micro-script)

Phase 3 reader-migration regex needs to additionally cover the legacy column names that map to the **2 newly pre-rolled cupm_v2 columns**, plus the 6 `gap_other_v2_table` columns Logan said stay as inline JOINs (so the Phase 3 audit catches them and the agent JOINs `cunc_v2` at the consumer):

```bash
git grep -nE 'tirads_(best|worst|n_nodule|nodule_size|n_sources|reliability|concordant_count|mismatch_count|has_acr_recalc|source)_v12|tirads_\w+_v271(?!b)|tirads_\w+_v271b|pathology_vs_imaging_laterality_concordant(?!_v271b|_v2)|imaging_laterality_rollup(?!_v271b|_v2)|max_tirads_ever(?!_v2)|worst_tirads_category(?!_v2)|tirads_v2_(any_fna_recommended(?!_report)|any_interval_growth|n_nodules_scored|largest_nodule_cm|max_points|worst_category)|tirads_best_points_v271|tirads_nodules_scored_combined' scripts/ sql/ manuscripts/ studies/ lakehouse/ utils/ app/
```

Specifically the new groups:

- `tirads_v2_any_fna_recommended` (no `_report`) — 0 readers but listed for completeness
- `tirads_v2_any_interval_growth` — 0 readers; same
- `tirads_v2_n_nodules_scored` — 1 reader
- `tirads_v2_largest_nodule_cm` — 0 readers
- `tirads_v2_max_points`, `tirads_v2_worst_category` — already covered by the `tirads_v2_*` pattern? Note: these have known cupm_v2 maps (`max_tirads_points_ever`, `max_tirads_category_ever`); migration is rename-not-drop but Phase 3 still touches them.
- `tirads_best_points_v271` — 0 readers
- `tirads_nodules_scored_combined` — 1 reader

Otherwise Part B proceeds as originally specified. The Phase 1 coverage audit re-runs at the start of Part B and is expected to show 0 gaps.
