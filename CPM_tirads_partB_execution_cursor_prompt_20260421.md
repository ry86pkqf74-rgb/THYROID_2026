# CPM TIRADS Part B — execution prompt

**Target DB:** `thyroid_canonical_publication_v1_0`
**Archive DB:** `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421`
**Date:** 2026-04-21
**Follow-on to:** Part A (read-only audit, complete)
**Architecture decision:** Option C — `canonical_patient_master` carries ZERO TIRADS columns. Canonical TIRADS truth lives on `main.canonical_us_patient_master_v2`; downstream consumers JOIN that table explicitly.

## Goal

Remove all TIRADS-related columns from `main.canonical_patient_master` (52 columns across `_v12` / `_v271` / `_v271b` / unsuffixed / `_v2` generations) in a single commit that also migrates every consumer. Views-first, scripts-second, CPM-last.

## Locked decisions (Logan, 2026-04-21)

1. **Retired-concept columns — REDESIGN cohort views, do NOT port to canonical.**
   Columns `tirads_concordant_count_v12`, `tirads_mismatch_count_v12`, `tirads_n_sources_v12`, `tirads_reliability_v12` are retired. `cohort_m025_tirads_performance_v1` and `cohort_m075_tirads_multi_nodule_v1` get their TIRADS analysis redesigned to use what `canonical_us_patient_master_v2` provides (`tirads_v2_n_reports`, `tirads_v2_worst_category`, `tirads_v2_worst_rank`, `tirads_v2_worst_rank_source`). If a metric has no v2 surrogate, flag it and STOP for sign-off — do not silently drop the metric.
2. **Writer-script freeze is in-scope for Part B, not a follow-on.**
   Scripts 207 / 271 / 271a / 271b / 265 / 273 freeze in the same commit that drops CPM columns. Match the Script 113 freeze pattern from the lab consolidation close-out.

## Inputs from Part A (re-read at top of script; do not cache)

- `manuscript_workspace.cpm_tirads_audit_classification_v1` (32 rows) — legacy column canonical list
- `scripts/output/_cpm_tirads_audit_writers_readers.json` — writer/reader map
- `scripts/output/_cpm_tirads_audit_view_readers.json` — view dependency map
- `scripts/output/_cpm_tirads_audit_FINAL_REPORT.md` — narrative
- `scripts/output/_cpm_tirads_audit_inventory.json` — full 53-column inventory (includes v2-suffixed columns not in the 32-row classification)

The 32-row classification covers legacy-only. Part B must also drop any `tirads_v2_*` column on CPM — pull those from `_cpm_tirads_audit_inventory.json` and merge into the drop list.

## Constraints

- **Do NOT touch `canonical_us_patient_master_v2`.** If Phase 1 finds a gap, STOP. Any column port is a separate pre-B micro-script with its own review.
- **Drop order is strict:** cohort views → script readers → writer-script freeze → CPM columns. Breaking this order breaks live consumers mid-run.
- **PHI:** research_id + structured fields only. No `clinical_note_text` into output anywhere.
- **Archive everything before dropping.** Pre-drop CPM snapshot + all 8 old view definitions go to `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421`.
- **Git workflow:** one commit per phase (for rollback granularity), push to `origin/main` at the end. Lint with `ruff check` before each commit.

---

## Phase 1 — Canonical coverage audit

Goal: prove every CPM TIRADS column either (a) has a semantically-equivalent column on `canonical_us_patient_master_v2`, or (b) is explicitly retired under the Q1 redesign decision.

Build `manuscript_workspace.cpm_tirads_canonical_coverage_v1`:

```
column_name                   | cpm_dtype | canonical_column                  | canonical_dtype | coverage_status    | notes
------------------------------|-----------|-----------------------------------|-----------------|--------------------|------
tirads_worst_score_v12        | BIGINT    | tirads_v2_worst_category          | BIGINT          | mapped             | 45.3% agreement; canonical wins
tirads_concordant_count_v12   | BIGINT    | <none>                            | —               | retired_redesign   | cohort_m025 redesigned to tirads_v2_n_reports
tirads_mismatch_count_v12     | BIGINT    | <none>                            | —               | retired_redesign   | cohort_m025 drops concept
tirads_n_sources_v12          | BIGINT    | <none>                            | —               | retired_redesign   | cohort_m025/m075 drops concept
tirads_reliability_v12        | VARCHAR   | <none>                            | —               | retired_redesign   | cohort_m075 drops concept
max_tirads_ever               | BIGINT    | tirads_v2_max_category_ever       | ?               | mapped_category    | category-form on canonical
max_tirads_ever_v2            | DOUBLE    | tirads_v2_max_points_ever         | ?               | mapped_points      | points-form on canonical
imaging_laterality_rollup     | VARCHAR   | imaging_laterality_rollup_v2      | VARCHAR         | mapped             | legacy unsuffixed
imaging_laterality_rollup_v271b | VARCHAR | imaging_laterality_rollup_v2      | VARCHAR         | mapped             | intermediate version
pathology_vs_imaging_laterality_concordant     | BOOLEAN | <5-value _v2 equivalent> | VARCHAR | mapped_5valued | BOOLEAN collapses; consumers coerce
pathology_vs_imaging_laterality_concordant_v271b | VARCHAR | <5-value _v2 equivalent> | VARCHAR | mapped        | direct rename
tirads_nodule_size_max_mm_v12 | DOUBLE    | tirads_v2_largest_nodule_cm       | DOUBLE          | mapped_unit_convert | 2% agreement flagged in Part A — different pipelines; canonical wins
...
```

`coverage_status` ∈ {`mapped`, `mapped_category`, `mapped_points`, `mapped_unit_convert`, `mapped_5valued`, `retired_redesign`, `gap_ABORT`}.

Verify every mapped canonical column actually exists:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_us_patient_master_v2'
  AND column_name ILIKE '%tirads%' OR column_name ILIKE '%laterality%';
```

**STOP gate:** if any row ends up `gap_ABORT` — i.e. a column has consumers AND no canonical equivalent AND is not in the Q1 redesign list — show Logan the list and abort Part B. Do not proceed to Phase 2.

Commit message: `CPM TIRADS Part B / Phase 1: canonical coverage audit`

---

## Phase 2 — Cohort view redesign

The 8 cohort views flagged by Part A as reading `_v12` columns:

- `manuscript_workspace.cohort_descriptive_full_cohort_v1`
- `manuscript_workspace.cohort_m011_*`
- `manuscript_workspace.cohort_m025_tirads_performance_v1`
- `manuscript_workspace.cohort_m045_*`
- `manuscript_workspace.cohort_m053_*`
- `manuscript_workspace.cohort_m064_*`
- `manuscript_workspace.cohort_m075_tirads_multi_nodule_v1`
- `manuscript_workspace.cohort_m076_*`

Resolve the `*` wildcards via the Part A view_readers inventory before writing SQL.

For each view:

1. **Snapshot definition:**
   ```sql
   CREATE TABLE "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.view_def_<view_name> AS
   SELECT '<view_name>' AS view_name,
          '<full CREATE OR REPLACE VIEW ... body>' AS definition_sql,
          now() AS archived_at;
   ```
   Or use `SELECT view_definition FROM information_schema.views WHERE ...` to pull the body.

2. **Rewrite CREATE OR REPLACE VIEW:**
   - Replace every CPM TIRADS column reference with a JOIN to `main.canonical_us_patient_master_v2` on `research_id` (1:1).
   - For `cohort_m025_tirads_performance_v1` and `cohort_m075_tirads_multi_nodule_v1`: drop `tirads_concordant_count_v12` / `tirads_mismatch_count_v12` / `tirads_n_sources_v12` / `tirads_reliability_v12`. Replace where possible:
     - concordant_count → `tirads_v2_n_reports` (if intent was "how many TIRADS-scored reports for this patient")
     - mismatch_count / n_sources / reliability → drop entirely; add a view comment noting the retirement; surface `tirads_v2_worst_rank_source` where a nearest-signal substitute is needed.
   - If a specific m025/m075 metric has no v2 surrogate AND is load-bearing for the manuscript, STOP and ask Logan before dropping it silently.

3. **Shape verification:**
   - `SELECT COUNT(*)` before vs after — must match exactly.
   - `SELECT COUNT(DISTINCT research_id)` before vs after — must match exactly.
   - Column count before vs after — dropped columns logged.

4. **Spot check:** 20 random `research_id` values, column-by-column comparison on columns that still exist in both versions.

Report per view:

```
view_name | row_count_before | row_count_after | col_count_before | col_count_after | cols_dropped | cols_replaced_join | spot_check_agree
```

Commit message: `CPM TIRADS Part B / Phase 2: migrated 8 cohort views to canonical_us_patient_master_v2`

---

## Phase 3 — Script-level reader migration

Enumerate readers:

```bash
git grep -nE 'tirads_(best|worst|n_nodule|nodule_size|n_sources|reliability|concordant_count|mismatch_count|has_acr_recalc|source)_v12|tirads_\w+_v271(?!b)|tirads_\w+_v271b|pathology_vs_imaging_laterality_concordant(?!_v271b|_v2)|imaging_laterality_rollup(?!_v271b|_v2)|max_tirads_ever(?!_v2)|worst_tirads_category(?!_v2)' scripts/ sql/ manuscripts/ studies/ lakehouse/ utils/ app/
```

For each hit (expect ~20–40 based on Part A reader inventory):

- If the file already JOINs `canonical_us_patient_master_v2`: swap the column name in place.
- If not: add a LEFT JOIN to `canonical_us_patient_master_v2 cupm ON cupm.research_id = <existing>.research_id` and swap the reference.
- For BOOLEAN → 5-valued laterality concordance: coerce at the consumer (e.g., `cupm.pathology_vs_imaging_laterality_concordant_v2 IN ('concordant', ...)` where the old code expected TRUE).
- For `max_tirads_ever` dual-form: pick category OR points form based on what the specific script is doing; check the column comment and usage context.
- `ruff check <file>` after each Python change; abort that file if lint fails.

Log every change to `scripts/output/_cpm_tirads_partB_reader_migrations.md`:

```
| file | line | before | after | lint_status |
```

Commit message: `CPM TIRADS Part B / Phase 3: migrated <N> script readers to canonical_us_patient_master_v2`

---

## Phase 4 — Freeze writer scripts

Scripts to freeze: **207, 271, 271a, 271b, 265, 273**.

For each:

1. Create `scripts/frozen/` if missing.
2. Move the file: `git mv scripts/Script_NNN_*.py scripts/frozen/Script_NNN_*.py`
3. Prepend a FROZEN header block at the top:
   ```python
   # =====================================================================
   # FROZEN — 2026-04-21 — Script NNN
   # =====================================================================
   # Reason: CPM TIRADS columns dropped per Option C (CPM TIRADS Part B).
   # Replacement: canonical TIRADS values live on main.canonical_us_patient_master_v2.
   #             Rebuild via the US v2 pipeline (canonical_us_nodule_v2 + canonical_us_exam_master_v2 + canonical_us_patient_master_v2).
   # Do NOT re-enable without a new column plan and CPM-schema decision.
   # =====================================================================
   ```
4. Update (or create) `scripts/frozen/README.md` with a one-line entry per frozen script:
   ```
   - Script_207_*.py — frozen 2026-04-21 — CPM TIRADS Part B — superseded by US v2 pipeline
   - Script_271_*.py — frozen 2026-04-21 — CPM TIRADS Part B — ...
   ...
   ```
5. Match the Script 113 freeze pattern verbatim for structure (header block placement, README format).

Commit message: `CPM TIRADS Part B / Phase 4: froze writer scripts 207/271/271a/271b/265/273`

---

## Phase 5 — Archive + DROP TIRADS columns from CPM

```sql
-- 1. Pre-drop snapshot (full table, not just TIRADS columns)
CREATE TABLE "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB AS
SELECT * FROM main.canonical_patient_master;

-- 2. Content hash + row count for post-drop verification
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT research_id) AS distinct_patients
FROM main.canonical_patient_master;
-- save these to the QA file

-- 3. Enumerate drop list
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND (column_name ILIKE '%tirads%'
       OR column_name IN (
         'imaging_laterality_rollup',
         'imaging_laterality_rollup_v271b',
         'imaging_laterality_rollup_v2',
         'pathology_vs_imaging_laterality_concordant',
         'pathology_vs_imaging_laterality_concordant_v271b',
         'worst_tirads_category',
         'max_tirads_ever',
         'max_tirads_ever_v2',
         'preop_tirads_best',
         'preop_tirads_category'
       ))
ORDER BY column_name;
-- cross-check this list against manuscript_workspace.cpm_tirads_audit_classification_v1
-- + v2-suffixed columns from _cpm_tirads_audit_inventory.json;
-- every column in the drop list must have coverage_status != 'gap_ABORT' in Phase 1's table.

-- 4. Drop each column (one ALTER per column for clean error reporting)
ALTER TABLE main.canonical_patient_master DROP COLUMN tirads_best_category_v12;
ALTER TABLE main.canonical_patient_master DROP COLUMN tirads_worst_category_v12;
-- ...repeat for every column in the drop list

-- 5. Drop Part A sample tables (retention kept on classification + coverage)
DROP TABLE manuscript_workspace.cpm_tirads_audit_sample_imaging_laterality_rollup_v1;
-- ...repeat for all 19 cpm_tirads_audit_sample_*_v1 tables.
-- KEEP: cpm_tirads_audit_classification_v1, cpm_tirads_canonical_coverage_v1 (2-week retention).

-- 6. Post-drop verification
SELECT COUNT(*) AS row_count_after FROM main.canonical_patient_master;
-- must equal pre-drop row_count exactly.

SELECT column_name FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND column_name ILIKE '%tirads%';
-- must return 0 rows.
```

Commit message: `CPM TIRADS Part B / Phase 5: archived CPM + dropped <N> TIRADS columns`

---

## Phase 6 — QA

Write results to `qa/qa_script_cpm_tirads_partB.json`. Every check must pass:

1. `canonical_patient_master` row count unchanged from pre-drop.
2. `canonical_patient_master` has 0 columns matching `ILIKE '%tirads%'`.
3. `canonical_patient_master` has 0 laterality-concordance columns (the BOOLEAN and `_v271b` VARCHAR are gone; only canonical-side versions remain).
4. All 8 migrated cohort views resolve (`SELECT COUNT(*) FROM <view>`) and return same row count as pre-migration.
5. `git grep` for the legacy column regex (Phase 3 list) returns empty across `scripts/`, `sql/`, `manuscripts/`, `studies/`, `lakehouse/`, `utils/`, `app/`, excluding `scripts/frozen/`, `scripts/output/`, and `archive_*` paths.
6. `manuscript_workspace.cpm_tirads_canonical_coverage_v1` has 0 rows with `coverage_status = 'gap_ABORT'`.
7. Archive sanity: `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB` row count + column count match the pre-drop live table.
8. Archive sanity: one `view_def_*` row per migrated cohort view.
9. Frozen scripts: all 6 present in `scripts/frozen/`, each has FROZEN header, `scripts/frozen/README.md` has 6 entries.
10. Canonical spot check: 10 random research_ids per mapped column — value in `canonical_us_patient_master_v2` matches value in pre-drop archive after type coercion.

---

## Phase 7 — Commit + push

If you committed per-phase, push all 7 commits:

```bash
git push origin main
```

If you squashed, use this commit message:

```
CPM TIRADS Part B — drop all TIRADS columns from canonical_patient_master (2026-04-21)

Architecture: Option C. canonical_patient_master carries ZERO TIRADS columns.
Canonical TIRADS truth lives on main.canonical_us_patient_master_v2; downstream
consumers JOIN that table explicitly.

Phases:
1. Canonical coverage audit -> manuscript_workspace.cpm_tirads_canonical_coverage_v1
2. Rewrote 8 cohort views to JOIN canonical_us_patient_master_v2;
   cohort_m025_tirads_performance_v1 + cohort_m075_tirads_multi_nodule_v1
   redesigned to drop retired concordance / reliability / n_sources concepts
   (no canonical surrogate; per Logan, redesign not port).
3. Migrated <N> script-level readers; log at scripts/output/_cpm_tirads_partB_reader_migrations.md
4. Froze writer scripts 207 / 271 / 271a / 271b / 265 / 273 to scripts/frozen/.
5. Archived pre-drop CPM + 8 view definitions to
   "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421; dropped <N> TIRADS columns
   from main.canonical_patient_master.
6. QA: 10/10 checks pass. 0 TIRADS columns on CPM. All 8 cohort views resolve.

canonical_patient_master column count: <before> -> <after>
Dropped 19 Part A cpm_tirads_audit_sample_* tables from manuscript_workspace.
Retained: cpm_tirads_audit_classification_v1, cpm_tirads_canonical_coverage_v1 (2-week retention).
```

---

## Report back at each gate

**Before Phase 2:** show the Phase 1 coverage table. Abort on any `gap_ABORT`.
**Before Phase 5:** show Phase 2 + 3 + 4 summary — view migration report, reader-migration log, frozen-scripts listing. Drop is irreversible outside archive restore; one last confirmation.
**After Phase 7:** show the commit SHA(s), the final CPM column list filtered to TIRADS/laterality keywords (must be empty), the frozen-scripts directory listing, and the QA JSON.

---

## Gotchas

1. **Drop order.** Views before scripts before CPM. Dropping CPM columns before migrating readers breaks views and scripts mid-run; DuckDB/MotherDuck will error on column references to non-existent columns when the view is re-executed.
2. **BOOLEAN vs 5-valued laterality concordance.** BOOLEAN is strictly less informative than `_v271b`. Under Option C both drop from CPM; the 5-valued form lives on canonical. Any script expecting BOOLEAN must coerce at the consumer side.
3. **`max_tirads_ever` dual form.** Category-form AND points-form must both exist on canonical before the drop. Verify in Phase 1 — if one is missing, STOP (per the "do not touch canonical" constraint, porting is a pre-B micro-script).
4. **`tirads_nodule_size_max_mm_v12` 2% agreement.** Part A flagged this as different pipelines. Under Option C the canonical version wins by fiat; note in Phase 6 QA that legacy-vs-canonical for this column will NOT match — that's expected, not a bug.
5. **Cohort view redesign for m025/m075.** Q1 directive is "redesign, do not port". If a specific m025/m075 metric has no v2 surrogate AND is load-bearing for the manuscript, STOP and ask Logan — the redesign assumes a redesign is feasible.
6. **Archive DB name.** `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421` — exact spacing + quoting. Matches the `us_legacy_20260421` / `molecular_legacy_20260421` convention.
7. **`git mv` not `mv`.** Frozen scripts must move via `git mv` so git tracks the rename; a plain `mv` + `git add` loses history.
8. **Do NOT modify `canonical_us_patient_master_v2`.** If Phase 1 finds a gap, STOP. Port is a separate script.
9. **Part A workspace retention.** `cpm_tirads_audit_classification_v1` and `cpm_tirads_canonical_coverage_v1` stay (2-week retention). The 19 `cpm_tirads_audit_sample_*_v1` tables drop in Phase 5.
10. **PHI safety.** No `clinical_note_text` into any output, log, or commit diff. research_id + structured fields only.

---

## Success criteria

- 0 TIRADS-related columns on `main.canonical_patient_master`.
- All 8 cohort views resolve and return same row count as pre-migration.
- All script readers migrated; `git grep` for the legacy column regex returns empty (excluding `scripts/frozen/` and `scripts/output/`).
- 6 writer scripts present in `scripts/frozen/` with FROZEN headers and README entries.
- Coverage audit shows 0 `gap_ABORT` rows.
- Archive DB contains pre-drop CPM snapshot + 8 old view definitions.
- One commit (or one-per-phase) on `origin/main`, QA JSON committed.
