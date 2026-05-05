# Script 393 — DTC T3b Stage-Group Orphan Fill

**Stamp:** 2026-04-22
**Type:** Surgical in-place UPDATE on `canonical_patient_master` (CPM)
**DB:** `thyroid_canonical_publication_v1_0`
**Target column:** `main.canonical_patient_master.ajcc8_stage_group`
**Prereqs:** 392 closed (tag `v1_0-ete-bool-strings-normalized-20260422_234621`)
**Ends at:** `v1_0-dtc-stage-group-orphans-filled-<stamp>` commit + tag
**Scope size:** 9 rows — fully deterministic AJCC8 derivation

---

## Problem

9 DTC (PTC/FTC/HCC) patients have `ajcc8_t_stage = 'T3b'` with full T/N/M components populated but `ajcc8_stage_group IS NULL`. The 240-builder's stage-group derivation never triggered for these rows because their T came in as T3b from upstream (pre-391), rather than via 391's T3b flip which did trigger the derivation for its own cohort. This is a builder-logic gap at rest, not a data-quality issue.

## Live baseline (2026-04-22)

```
Orphan cohort (DTC, T/N/M non-null, stage_group NULL, T=T3b): 9 rows
All 9 are T3b with ete_grade_final_v2='gross' (post-392).
Diagnosis mix: 8 PTC + 1 FTC. No MTC/ATC.
```

Deterministic fill via AJCC8 DTC rules (age<55: M decides; age≥55: T+M decide):

| research_id | dx | T | N | M | age | → stage_group |
|---|---|---|---|---|---|---|
| 1412 | PTC | T3b | N0  | M0 | 47 | **I**    (age<55, M0) |
| 1546 | PTC | T3b | N0  | M0 | 36 | **I**    (age<55, M0) |
| 4430 | PTC | T3b | N1a | M1 | 27 | **II**   (age<55, M1) |
| 6087 | PTC | T3b | N1a | M0 | 54 | **I**    (age<55, M0) |
| 7566 | PTC | T3b | N1a | M1 | 44 | **II**   (age<55, M1) |
| 550  | PTC | T3b | N1a | M0 | 59 | **II**   (age≥55, T3b, M0) |
| 1908 | PTC | T3b | N0  | M0 | 57 | **II**   (age≥55, T3b, M0) |
| 5432 | FTC | T3b | N0  | M0 | 64 | **II**   (age≥55, T3b, M0) |
| 11108 | PTC | T3b | N1a | M1 | 56 | **IVB**  (age≥55, M1) |

Split: 3 × I, 5 × II, 1 × IVB.

## Scope

UPDATE the 9 DTC-orphan rows' `ajcc8_stage_group` via a single generic AJCC8 CASE expression. Also update `ajcc8_stage_group_corrected` (which the 240-builder treats as the canonical-corrected mirror column) when non-null so the two stay in sync.

### Out-of-scope
- No other table/column touched
- No rebuild of CPM
- No ATC/MTC staging (separate tables)
- No T-stage changes
- No touching of ajcc7_stage_group

## Execution phases

### Phase 0 — Probe (read-only, idempotent)
```sql
-- Q0-A. Confirm the 9-row cohort
SELECT COUNT(*) AS n_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 9

-- Q0-B. Derivation preview — confirm expected stage_group per row
SELECT research_id, diagnosis_primary, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
       age_at_surgery,
       CASE
         WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
         WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
         WHEN age_at_surgery >= 55 AND ajcc8_t_stage = 'T3b' AND ajcc8_m_stage = 'M0' THEN 'II'
         WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
         ELSE NULL  -- unexpected shape — halt if any row falls here
       END AS derived_stage_group
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
ORDER BY research_id;
-- EXPECT: 9 rows, all with non-NULL derived_stage_group matching the table above.

-- Q0-C. Parity check vs ajcc8_stage_group_corrected (the mirror column from 240-builder)
SELECT COUNT(*) AS n_corrected_also_null
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
  AND ajcc8_stage_group_corrected IS NULL;
-- EXPECT: ≤ 9 (tells us how many mirror columns also need filling).

-- Q0-D. Global DTC orphan count for any T-stage (sanity bound — 393 only fixes T3b)
SELECT ajcc8_t_stage, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
GROUP BY 1 ORDER BY 2 DESC;
-- NOTE: if any non-T3b rows appear, they are out-of-scope for 393. Flag for future script.
```

**Halt gate:** if Q0-A ≠ 9 OR any Q0-B row has NULL `derived_stage_group` OR Q0-D shows any non-T3b orphan with >0 count, halt. Do not proceed.

### Phase 1 — Plan-review gate
No approval file. The derivation is fully deterministic from AJCC8 DTC rules. Logan eyeballs the Phase 0 report, confirms Q0-A = 9 and the derivation column matches the 3×I / 5×II / 1×IVB split, and greenlights Phase 2 verbally in chat.

### Phase 2 — Apply

#### 2A. Archive snapshot
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_stage_group_pre393_<STAMP> AS
SELECT research_id, diagnosis_primary,
       ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, age_at_surgery,
       ajcc8_stage_group, ajcc8_stage_group_corrected,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT row count: 9
```

#### 2B. Fill `ajcc8_stage_group` (9 rows)
```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = CASE
      WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
      WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
      WHEN age_at_surgery >= 55 AND ajcc8_t_stage = 'T3b' AND ajcc8_m_stage = 'M0' THEN 'II'
      WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
    END
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 9 rows updated.
```

#### 2C. Mirror to `ajcc8_stage_group_corrected` (only if currently NULL — do not overwrite existing corrections)
```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group_corrected = ajcc8_stage_group
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group_corrected IS NULL
  AND ajcc8_stage_group IS NOT NULL   -- only mirror rows just filled by 2B (or previously filled)
  AND diagnosis_primary IN ('PTC','FTC','HCC')
  AND research_id IN (
    SELECT research_id
    FROM archive_pub_v1_0.cpm_stage_group_pre393_<STAMP>
  );
-- EXPECT: ≤ 9 rows updated (whatever Q0-C returned).
```

#### 2D. `__readme` provenance row
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 393: canonical_patient_master.ajcc8_stage_group DTC T3b orphan fill — 9 rows derived deterministically from AJCC8 rules (3×I, 5×II, 1×IVB). Mirror column ajcc8_stage_group_corrected synced where also NULL. Builder-logic gap (240-builder did not retrigger for upstream-T3b rows); no rebuild. Snapshot: archive_pub_v1_0.cpm_stage_group_pre393_<STAMP>.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

### Phase 3 — Verify
```sql
-- V1. Zero DTC T3b orphans remain
SELECT COUNT(*) AS n_orphans_remaining
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 0

-- V2. All 9 archived rows now have a non-NULL stage_group
SELECT COUNT(*) AS n_filled
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre393_<STAMP> s USING (research_id)
WHERE cpm.ajcc8_stage_group IS NOT NULL;
-- EXPECT: 9

-- V3. Stage-group distribution on the 9-row cohort matches expected split
SELECT ajcc8_stage_group, COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre393_<STAMP> s USING (research_id)
GROUP BY 1 ORDER BY 1;
-- EXPECT: I=3, II=5, IVB=1

-- V4. Non-T3b DTC orphan count is unchanged (we didn't accidentally touch anything else)
SELECT COUNT(*) AS n_non_t3b_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage != 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: equal to Q0-D's non-T3b sum (likely 0, but whatever it was pre-apply).

-- V5. CPM row count unchanged
SELECT COUNT(*) AS n_cpm FROM main.canonical_patient_master;
-- EXPECT: 10,871

-- V6. Snapshot table exists with 9 rows
SELECT COUNT(*) AS n_snapshot
FROM archive_pub_v1_0.cpm_stage_group_pre393_<STAMP>;
-- EXPECT: 9

-- V7. __readme row landed
SELECT COUNT(*) FROM main.__readme WHERE content LIKE 'Script 393:%';
-- EXPECT: 1

-- V8. Mirror column parity (ajcc8_stage_group == ajcc8_stage_group_corrected on the 9-row cohort,
--     except for rows where _corrected was previously non-NULL and differs for a documented reason)
SELECT research_id, ajcc8_stage_group, ajcc8_stage_group_corrected
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre393_<STAMP> s USING (research_id)
WHERE ajcc8_stage_group != ajcc8_stage_group_corrected;
-- EXPECT: 0 rows (no mismatches). If any appear, inspect — they had a pre-existing _corrected
-- value that 2C deliberately left alone.
```

### Phase 4 — Commit + tag
Staged files (**explicit paths only — no `git add -A` or `git add scripts/output/`**):
- `scripts/393_dtc_stage_group_orphan_fill.py`
- `scripts/output/393_prestate_probe_report.md`
- `scripts/output/393_run.log`
- `scripts/output/393_close_out_report.md`

Commit message: `Script 393: DTC T3b stage_group orphan fill — 9 rows (3×I, 5×II, 1×IVB)`
Tag: `v1_0-dtc-stage-group-orphans-filled-<stamp>`

## Idempotency
1. If `archive_pub_v1_0.cpm_stage_group_pre393_*` snapshot exists AND `__readme` has `content LIKE 'Script 393:%'` → exit 0, NO-OP.
2. If snapshot exists but no `__readme` row → halt with `partial apply detected; manual review required`.
3. If `__readme` exists but no snapshot → halt with `missing snapshot; manual review required`.

## Non-goals / deferred
- **Non-T3b DTC orphans (if any surface in Q0-D)** — separate script, different rules matrix (T4a/T4b need their own stage-group logic).
- **ATC/MTC stage_group** — separate tables, separate AJCC8 chapters; not in this cohort.
- **Rebuild of 240-builder's stage-group derivation** — the builder-logic gap is out-of-scope; this is a point-fix for the 9 known orphans.
- **ajcc8_stage_group_v2 column** — ignore; it's a parallel experimental column from phase-4.6 and not in scope here.

## First action for the agent

1. Run Phase 0 probe. Write `scripts/output/393_prestate_probe_report.md` with the exact Q0-A/B/C/D outputs.
2. Confirm Q0-A = 9 and the 9 rows in Q0-B match the 3×I / 5×II / 1×IVB split from the table above.
3. Print the Q0-D non-T3b orphan count. If it's > 0, surface the list to Logan with a note that 393 won't touch them (separate future script needed).
4. Wait for Logan's greenlight (verbal).
5. Apply Phases 2A–2D. Run Phase 3 V1–V8. Commit + tag.

**Do NOT rebuild any canonical tables. Do NOT touch non-DTC or non-T3b rows. Do NOT modify ajcc8_stage_group_corrected where it's already non-NULL.**
