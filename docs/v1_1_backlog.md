# v1_1 Backlog — Non-blocking follow-ups from v1_0 trailing gaps closure

_Created 2026-04-17 after PR #5 (Scripts 267/268/269) merged. v1_0 is publication-ready;
items below are quality-of-life improvements deferred to v1_1._

---

## 1. Bethesda index-nodule consistency assertion

**Status**: Low-priority data-quality check. Single patient affected today.

**Observation** (post-Script 268): Exactly 1 patient has
`bethesda_index_nodule > bethesda_final`. By construction `bethesda_final` is
the per-patient max of `bethesda_calculated` across **preop** FNAs, so the
index-nodule value (drawn from any FNA in the linkage chain) should never
exceed it. The single violation is a probable date-window edge — the
index-nodule chain in `_bethesda_index_nodule_268` may resolve a
different `fna_date_parsed` window than the strict preop CTE used for
`bethesda_final`.

**Action for v1_1**:
1. Add a SQL assertion (e.g., as Phase 7 invariant in the next finalization script):
   ```sql
   SELECT COUNT(*) = 0 AS pass_index_nodule_le_final
   FROM canonical_patient_master
   WHERE bethesda_index_nodule IS NOT NULL
     AND bethesda_final IS NOT NULL
     AND bethesda_index_nodule > bethesda_final;
   ```
2. When count > 0, surface the offending `research_id` list to a
   `scripts/output/<NNN>_index_nodule_violations.json` audit file.
3. Investigate whether the index-nodule CTE should adopt the same strict
   preop date filter as the rollup CTE, or whether the linkage chain should
   prefer the FNA already used for the per-patient max.

**Find the violator** (drop-in query for v1_1):
```sql
SELECT research_id, bethesda_final, bethesda_index_nodule,
       bethesda_index_nodule_linkage_source
FROM canonical_patient_master
WHERE bethesda_index_nodule IS NOT NULL
  AND bethesda_final IS NOT NULL
  AND bethesda_index_nodule > bethesda_final;
```

---

## 2. `legacy_column_sweep_v1_1` schema cleanup — add `notes` column

**Status**: Low-priority schema refactor. Workaround in place.

**Current schema**:
```
column_name        VARCHAR
version            INTEGER
stem               VARCHAR
max_version_in_cpm INTEGER
successor_column   VARCHAR
inventoried_at     TIMESTAMP WITH TIME ZONE
```

**Problem**: Script 268 needed to log a *semantic rebuild* (not a drop) of
`bethesda_final`. There is no dedicated free-form column for this, so the
explanation was crammed into `successor_column` —
`"bethesda_final (rebuilt under preop_worst_calculated_from_morphology_era_preserved, ..."`.
That works for one entry but breaks the column's intended semantics
(should hold a successor column name, not prose). Future semantic
rebuilds will compound the kludge.

**Proposed v1_1 change**:
```sql
ALTER TABLE manuscript_workspace.legacy_column_sweep_v1_1
ADD COLUMN notes VARCHAR;

ALTER TABLE manuscript_workspace.legacy_column_sweep_v1_1
ADD COLUMN dropped_by_script VARCHAR;

-- Backfill: extract embedded prose from successor_column where it isn't
-- a clean column reference, move into notes, restore successor_column
-- to a clean canonical name (or NULL for semantic rebuilds).

UPDATE manuscript_workspace.legacy_column_sweep_v1_1
SET notes = successor_column,
    successor_column = NULL,
    dropped_by_script = 'script_268'
WHERE column_name = 'bethesda_final';

-- Repeat for the four Script 267 rows: split out script tag from prose,
-- leave the canonical successor name in successor_column.
```

**Acceptance**:
- `successor_column` contains only canonical CPM column names or NULL.
- `notes` carries the free-form rebuild/drop explanation.
- `dropped_by_script` carries `script_NNN` tag for audit grouping.
- All existing rows backfilled without information loss.

---

## Backlog ownership

Owner: project_lead
Target window: opportunistic during v1_1 sprint (no blocker on v1_0 publication)
Tracking: this file; promote to issues when v1_1 work begins.
