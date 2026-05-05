# Script 392 — ete_grade_final_v2 Boolean-String Normalization

**Stamp:** 2026-04-22
**Type:** Surgical in-place UPDATE on `canonical_patient_master` (CPM)
**DB:** `thyroid_canonical_publication_v1_0`
**Target columns:** `main.canonical_patient_master.ete_grade_final_v2`, `main.canonical_patient_master.ete_grade`
**Prereqs:** 391 closed (tag `v1_0-t-stage-reconciled-20260422_223618`); 390 queue table exists at `manuscript_workspace.cpm_ete_self_contradiction_queue_v1`
**Ends at:** `v1_0-ete-bool-strings-normalized-<stamp>` commit + tag

---

## Problem

`canonical_patient_master.ete_grade_final_v2` contains **183 rows with the raw string literals `'false'` (179) or `'true'` (4)** instead of the enum values `none`/`microscopic`/`gross`/`unable_to_determine`. Script 391's legacy-sync faithfully propagated them into `ete_grade` as well (so both columns now hold the junk strings at the same 183 research_ids).

The upstream table that emitted these (`main.tumor_episode_master_v2`) has been deprecated and no longer exists in the live DB — so the 183 rows are **frozen in the CPM materialization**, and a surgical in-place fix is both safe and terminal (no rebuild will regenerate the bug).

## Live-state baselines (2026-04-22, this session)

```
Junk string distribution:           false=179, true=4 (total 183)
ete_grade_source (all 183):         tumor_episode_master_v2
Legacy `ete_grade` on same 183:     matches 1:1 (same junk values — 391 sync preserved it)

Evidence cross-check on the 179 'false' rows:
  op_intraop_gross_ete_any = TRUE  : 0
  path_gross_ete_flag      = TRUE  : 0
  any_microscopic_ete_anywhere=TRUE: 0
  all three flags FALSE            : 179  ← 100% corroborated-negative

Evidence cross-check on the 4 'true' rows:
  op_intraop_gross_ete_any = TRUE  : 1
  path_gross_ete_flag      = TRUE  : 1  (same row as the op one? verify in Phase 0)
  any_microscopic_ete_anywhere=TRUE: 0
  all three flags FALSE            : 2  ← route to queue
```

**Implication: the cohort splits into three clean buckets by evidence:**

| bucket | n | target `ete_grade_final_v2` | target `ete_grade` | route |
|---|---|---|---|---|
| `false` + all flags negative | 179 | `'none'` | `'none'` | in-place UPDATE |
| `true` + gross corroboration | 2 | `'gross'` | `'gross'` | in-place UPDATE + T-stage cascade |
| `true` + no corroborating flags | 2 | keep `'true'` (preserve signal) | keep `'true'` | route to `cpm_ete_self_contradiction_queue_v1` with reason=`boolean_string_no_corroboration` |

Total rows updated in CPM: **181** (179 → 'none' + 2 → 'gross'). Total rows routed to queue: **2**.

## Scope

1. UPDATE `canonical_patient_master` — normalize the 179 corroborated-negative rows (`'false'` → `'none'`) and the 2 corroborated-positive rows (`'true'` → `'gross'`) in both `ete_grade_final_v2` AND `ete_grade`.
2. Route the 2 uncorroborated `'true'` rows to `manuscript_workspace.cpm_ete_self_contradiction_queue_v1` with reason `boolean_string_no_corroboration`.
3. Cascade T-stage for the 2 rows that flip to `'gross'` (apply 391-style logic: re-derive `ajcc8_t_stage` + `ajcc8_stage_group` + `microscopic_ete_t3b_corrected` if applicable).
4. Archive snapshot of pre-392 CPM ete columns.
5. `__readme` provenance row.

### Out-of-scope
- No rebuild of CPM
- No touching of `tumor_episode_master_v2` (already deprecated/absent)
- No other upstream extractor changes
- No touching of 389.1 registry columns
- No mass rewrite of `ete_grade_source` values

## Execution phases

### Phase 0 — Probe (idempotent, read-only)
Re-confirm the baselines live (should match this prompt). Produce `scripts/output/392_prestate_probe_report.md` with:

```sql
-- Q0-A. Junk cohort count + distribution
SELECT ete_grade_final_v2, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true','false')
GROUP BY 1 ORDER BY 2 DESC;
-- EXPECT: false=179, true=4

-- Q0-B. Evidence bucketing
SELECT
  LOWER(TRIM(ete_grade_final_v2)) AS junk_val,
  CASE
    WHEN op_intraop_gross_ete_any = TRUE OR path_gross_ete_flag = TRUE
      OR gross_ete_flag = TRUE THEN 'gross_corroborated'
    WHEN any_microscopic_ete_anywhere = TRUE THEN 'micro_corroborated'
    WHEN op_intraop_gross_ete_any = FALSE
      AND (path_gross_ete_flag = FALSE OR path_gross_ete_flag IS NULL)
      AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL)
      THEN 'all_flags_negative'
    ELSE 'mixed_or_null'
  END AS evidence_bucket,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true','false')
GROUP BY 1, 2 ORDER BY 1, 3 DESC;
-- EXPECT:
--   false, all_flags_negative : 179
--   true,  gross_corroborated :   2
--   true,  all_flags_negative :   2  (the queue-routed pair)

-- Q0-C. Legacy ete_grade parity check
SELECT
  COUNT(*) AS n_junk_with_matching_legacy
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true','false')
  AND LOWER(TRIM(ete_grade)) = LOWER(TRIM(ete_grade_final_v2));
-- EXPECT: 183 (100% parity)

-- Q0-D. T-stage state of the 2 gross-flip candidates
SELECT research_id, diagnosis_primary, ete_grade_final_v2,
       ete_grade, ete_ordinal_worst, ajcc8_t_stage,
       ajcc8_stage_group, tumor_size_cm_dominant
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
  AND (op_intraop_gross_ete_any = TRUE OR path_gross_ete_flag = TRUE OR gross_ete_flag = TRUE);
-- INSPECT: will these 2 rows flip T? probably yes if AJCC8 T3b applies per 391 logic.

-- Q0-E. Queue table membership — are any 183 rows already queued?
SELECT COUNT(*) AS n_already_queued
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
WHERE research_id IN (
  SELECT research_id FROM main.canonical_patient_master
  WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true','false')
);
-- NOTE: if >0, 390 already routed some; dedup before insert.
```

**Halt gate:** if any of Q0-A/B/C results differ from `EXPECT` by more than ±2 rows, halt and re-probe. Do not proceed to Phase 2 writes.

### Phase 1 — Plan-review gate
No approval file needed. The three buckets are deterministic from evidence flags. Logan reviews the Phase 0 report, confirms the 2 gross-corroborated rows look clean, then greenlights Phase 2.

### Phase 2 — Apply

#### 2A. Archive snapshot (PUB-resident, matches 387/388/389/390 pattern)
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_ete_pre392_<STAMP> AS
SELECT research_id, ete_grade, ete_grade_final_v2, ete_grade_source,
       ete_ordinal_worst, ajcc8_t_stage, ajcc8_stage_group,
       microscopic_ete_t3b_corrected, tumor_size_cm_dominant,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('true','false')
   OR research_id IN (
     -- include the 2 cascade-affected rows even if final value isn't junk post-update
     SELECT research_id FROM main.canonical_patient_master
     WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
       AND (op_intraop_gross_ete_any = TRUE OR path_gross_ete_flag = TRUE
            OR gross_ete_flag = TRUE)
   );
-- EXPECT row count: 183 (the cascade set is a subset).
```

#### 2B. Normalize 'false' → 'none' (179 rows)
```sql
UPDATE main.canonical_patient_master
SET ete_grade_final_v2 = 'none',
    ete_grade          = 'none'
WHERE LOWER(TRIM(ete_grade_final_v2)) = 'false'
  AND (op_intraop_gross_ete_any = FALSE OR op_intraop_gross_ete_any IS NULL)
  AND (path_gross_ete_flag      = FALSE OR path_gross_ete_flag      IS NULL)
  AND (gross_ete_flag           = FALSE OR gross_ete_flag           IS NULL)
  AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL);
-- EXPECT: 179 rows updated.
```

#### 2C. Normalize 'true' → 'gross' (2 rows with corroboration)
```sql
UPDATE main.canonical_patient_master
SET ete_grade_final_v2 = 'gross',
    ete_grade          = 'gross',
    ete_ordinal_worst  = GREATEST(COALESCE(ete_ordinal_worst, 0), 3)  -- 3 = gross on the 0-3 ordinal
WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
  AND (op_intraop_gross_ete_any = TRUE
       OR path_gross_ete_flag = TRUE
       OR gross_ete_flag = TRUE);
-- EXPECT: 2 rows updated.
```

**Verify the ordinal scale before applying** — inspect live distribution of `ete_ordinal_worst` to confirm `3 = gross`. If the scale differs, adjust `GREATEST(..., 3)` to match. From 391: ordinal 2 = micro, 3 = gross is the expected mapping; confirm in probe.

#### 2D. T-stage cascade on the 2 gross-flip rows
Apply 391-style logic for DTC-only (PTC/FTC/HCC), skip MTC/ATC:
```sql
-- For the 2 rows that flipped to gross, re-derive T-stage if they were T1/T2.
-- AJCC 8: gross ETE upgrades T to T3b regardless of size for DTC.
UPDATE main.canonical_patient_master
SET ajcc8_t_stage = 'T3b',
    microscopic_ete_t3b_corrected = FALSE  -- no longer micro-ETE
WHERE research_id IN (
  -- exact 2-row set identified in 2C (re-filter from post-state)
  SELECT research_id FROM main.canonical_patient_master
  WHERE ete_grade_final_v2 = 'gross'
    AND ete_grade_source = 'tumor_episode_master_v2'
    AND diagnosis_primary IN ('PTC','FTC','HCC','other_malignant','follicular_adenoma')
    -- restrict to the cohort whose ajcc8_t_stage was T1/T2/T3a pre-392
    AND ajcc8_t_stage IN ('T1','T1a','T1b','T2','T3a')
);
-- EXPECT: 0-2 rows (depends on current T per Q0-D; 0 is fine — they may already be T3b/T4).

-- Stage-group cascade on the same set (AJCC8 DTC table — age<55 → I, age>=55 → II for T3b)
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = CASE
      WHEN age_at_surgery < 55 THEN 'I'
      WHEN age_at_surgery >= 55 THEN 'II'
      ELSE ajcc8_stage_group
    END
WHERE research_id IN (/* same 2 IDs as above UPDATE */)
  AND ajcc8_t_stage = 'T3b'
  AND n_stage IS NOT NULL
  AND m_stage IS NOT NULL;
```

**Guard:** if the 2 gross-flip rows already have `ajcc8_t_stage = 'T3b'` or `'T4a/b'` (they've been upgraded by other evidence), 2D is a no-op. Report 0 rows cascaded. That's fine.

#### 2E. Queue the 2 uncorroborated 'true' rows
```sql
INSERT INTO manuscript_workspace.cpm_ete_self_contradiction_queue_v1
  (research_id, ete_grade_final_v2, reason, inserted_at, source_script)
SELECT research_id, 'true', 'boolean_string_no_corroboration',
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP), '392'
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true'
  AND (op_intraop_gross_ete_any = FALSE OR op_intraop_gross_ete_any IS NULL)
  AND (path_gross_ete_flag      = FALSE OR path_gross_ete_flag      IS NULL)
  AND (gross_ete_flag           = FALSE OR gross_ete_flag           IS NULL)
  AND (any_microscopic_ete_anywhere = FALSE OR any_microscopic_ete_anywhere IS NULL)
  AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1 q
    WHERE q.research_id = canonical_patient_master.research_id
      AND q.reason = 'boolean_string_no_corroboration'
  );
-- EXPECT: 2 rows inserted. Leaves these 2 rows with ete_grade_final_v2='true' in CPM
-- (preserving upstream signal) pending manual review/adjudication.
```

**Design note — why preserve 'true' in CPM rather than force it to 'unable_to_determine':** the queue entry IS the audit trail; the raw 'true' in the column is a ticking reminder that downstream stage calcs should treat it as suspect. Forcing a normalization would obscure the signal. This is the same pattern 390 used for the 194 boolean-string rows it first saw.

#### 2F. `__readme` provenance row
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 392: canonical_patient_master.ete_grade_final_v2 boolean-string normalization — 179 ''false''→''none'' (corroborated-negative), 2 ''true''→''gross'' (corroborated-positive w/ T-stage cascade), 2 routed to cpm_ete_self_contradiction_queue_v1 as boolean_string_no_corroboration. Legacy ete_grade synced in same UPDATE. Source ete_grade_source=tumor_episode_master_v2 (upstream table deprecated, no rebuild). Snapshot: archive_pub_v1_0.cpm_ete_pre392_<STAMP>.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

### Phase 3 — Verify
```sql
-- V1. No 'true'/'false' literals remain in the corroborated buckets
SELECT COUNT(*) AS n_remaining_bool_strings
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) IN ('false')
   OR (LOWER(TRIM(ete_grade_final_v2)) = 'true'
       AND (op_intraop_gross_ete_any = TRUE OR path_gross_ete_flag = TRUE OR gross_ete_flag = TRUE));
-- EXPECT: 0

-- V2. Exactly 2 'true' literals remain (the queue-routed pair)
SELECT COUNT(*) AS n_remaining_true_literal
FROM main.canonical_patient_master
WHERE LOWER(TRIM(ete_grade_final_v2)) = 'true';
-- EXPECT: 2

-- V3. Queue grew by exactly 2 under reason=boolean_string_no_corroboration
SELECT COUNT(*) AS n_queue_392
FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
WHERE source_script = '392' AND reason = 'boolean_string_no_corroboration';
-- EXPECT: 2

-- V4. Legacy `ete_grade` stays in sync with final_v2 on all 183 original rows
SELECT COUNT(*) AS n_out_of_sync
FROM main.canonical_patient_master
WHERE research_id IN (/* original 183 research_id list — load from archive snapshot */)
  AND LOWER(TRIM(COALESCE(ete_grade,''))) != LOWER(TRIM(COALESCE(ete_grade_final_v2,'')));
-- EXPECT: 0

-- V5. Distribution of ete_grade_final_v2 post-update (normal enum values only, + the 2 queue-'true')
SELECT ete_grade_final_v2, COUNT(*) AS n
FROM main.canonical_patient_master
GROUP BY 1 ORDER BY 2 DESC;
-- EXPECT: enum values dominate; literal 'true' = 2; literal 'false' = 0.

-- V6. Stage-group invariant still holds (no rows with T/N/M all set but stage_group NULL)
SELECT COUNT(*) AS n_orphan_stage_groups
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NOT NULL
  AND n_stage IS NOT NULL
  AND m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: equal to 391's post-state value (no regression).

-- V7. __readme row landed
SELECT COUNT(*) FROM main.__readme WHERE content LIKE 'Script 392:%';
-- EXPECT: 1
```

### Phase 4 — Commit + tag
Staged files (**explicit paths only — no `git add -A` or `git add scripts/output/`**):
- `scripts/392_ete_boolean_string_normalization.py`
- `scripts/output/392_prestate_probe_report.md`
- `scripts/output/392_run.log`
- `scripts/output/392_close_out_report.md`

Commit message: `Script 392: ete_grade_final_v2 boolean-string normalization — 179→none, 2→gross+cascade, 2→queue`
Tag: `v1_0-ete-bool-strings-normalized-<stamp>`

## Idempotency
Detection strategy (same pattern as 389/390/391):
1. If `archive_pub_v1_0.cpm_ete_pre392_*` snapshot exists AND `main.__readme` has a row with `content LIKE 'Script 392:%'` → exit 0, NO-OP.
2. If snapshot exists but no `__readme` row → halt with `partial apply detected; manual review required`.
3. If `__readme` exists but no snapshot → halt with `missing snapshot; manual review required`.

## Non-goals / deferred
- **Upstream extractor trace** — `tumor_episode_master_v2` is already deprecated; no extractor to fix. If a future rebuild ever resurrects a similar source, re-probe and repatch.
- **`ete_grade_source` rewrite** — the 181 normalized rows still carry `ete_grade_source='tumor_episode_master_v2'`, which is now a misleading provenance tag. Consider appending `_normalized_392` suffix ONLY if manuscript tables need the distinction; otherwise leave for future cleanup.
- **Mass re-grade of all `source=tumor_episode_master_v2` rows** — only the 183 junk-string rows are in scope. Other rows from that source may have valid enum grades already.

## First action for the agent

1. Run Phase 0 probe. Write `scripts/output/392_prestate_probe_report.md`. Halt.
2. Confirm counts match expected (179 false, 4 true, 2 gross-corroborated, 2 uncorroborated).
3. **Inspect the 4 'true' rows by research_id** — confirm which 2 have op/path gross corroboration (these are the gross-flip set) and which 2 don't (queue set). Print both sets to the Phase 0 report explicitly with their full flag vectors.
4. Wait for Logan's greenlight (verbal in chat — no approval file).
5. Apply Phases 2A–2F. Run Phase 3. Commit + tag.

**Do NOT rebuild any canonical tables. Do NOT touch `tumor_episode_master_v2` (it doesn't exist). Do NOT use `FROM archive_pub_v1_0.*` for canonical reads.**
