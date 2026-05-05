# Script 394 — DTC NULL-T Stage-Group Fill (M-Decidable Subset)

**Stamp:** 2026-04-22
**Type:** Surgical in-place UPDATE on `canonical_patient_master` (CPM)
**DB:** `thyroid_canonical_publication_v1_0`
**Target column:** `main.canonical_patient_master.ajcc8_stage_group`
**Prereqs:** 393 closed (tag `v1_0-dtc-stage-group-orphans-filled-20260422_235819`)
**Ends at:** `v1_0-dtc-null-t-stage-groups-filled-<stamp>` commit + tag
**Scope size:** 20 rows (filled) + 13 rows carried forward to future 395

---

## Problem

33 DTC (PTC/FTC/HCC) patients have `ajcc8_t_stage IS NULL` but `ajcc8_n_stage` + `ajcc8_m_stage` populated AND `ajcc8_stage_group IS NULL`. These are the second half of the 240-builder stage-group sync gap (393 handled the T3b bucket; 394 handles the NULL-T bucket).

Of the 33, **20 are derivable from (age, M) alone under AJCC8 DTC rules** (age<55: M decides; age≥55 + M1: always IVB). The remaining 13 are age≥55 M0 and require T-stage to resolve — out-of-scope for 394.

## Live baseline (2026-04-22, this session, direct MD probe)

Cohort partition:

| bucket | n | `ajcc8_stage_group_corrected` (builder) | AJCC8-derived target | 394 action |
|---|---|---|---|---|
| age<55, M0 | 14 | `I` | `I` (age<55 M0 → I regardless of T/N) | **FILL** |
| age<55, M1 | 1 | `II` | `II` (age<55 M1 → II) | **FILL** |
| age≥55, M1 | 5 | `IVB` | `IVB` (any age≥55 M1 → IVB) | **FILL** |
| age≥55, M0 | 13 | `NULL` | requires T | **DEFER to 395** |

**Total fillable in 394: 20 rows (14×I + 1×II + 5×IVB). Builder `_corrected` values match AJCC8 derivation 20/20 — this is a clean builder-sync, same pattern as 393's 2B.**

The 13 deferred rows have `ajcc8_t_stage_v2` populated for 11 of them (2 have no T signal anywhere). Future Script 395 would sync `ajcc8_t_stage ← ajcc8_t_stage_v2` for those 11 and then re-derive stage group. Two rows (research_ids 1404, 12198) may stay unresolvable without manual chart review.

## Scope

Primary UPDATE: `ajcc8_stage_group` for the 20 M-decidable NULL-T DTC rows, keyed on (`ajcc8_t_stage IS NULL`, `ajcc8_stage_group_corrected IS NOT NULL`, diagnosis DTC). Source of truth = `ajcc8_stage_group_corrected` (which the builder computed correctly but never synced back).

### Out-of-scope
- The 13 age≥55 M0 rows (they need T; separate script)
- No touching of `ajcc8_t_stage` (no T-sync in 394)
- No rebuild of CPM, no other columns, no other diagnosis classes
- No touching of `ajcc8_t_stage_v2` or `ajcc8_stage_group_v2` parallel columns

## Execution phases

### Phase 0 — Probe (read-only, idempotent)
```sql
-- Q0-A. Confirm the 33-row cohort (total NULL-T DTC orphans)
SELECT COUNT(*) AS n_null_t_dtc_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 33

-- Q0-B. Confirm the 20-row fillable subset (builder _corrected is non-NULL)
SELECT
  CASE
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I (age<55 M0)'
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II (age<55 M1)'
    WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB (age>=55 M1)'
    ELSE 'NOT_DERIVABLE'
  END AS derivation_route,
  ajcc8_stage_group_corrected AS builder_corrected,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
GROUP BY 1,2 ORDER BY 1,2;
-- EXPECT:
--   I (age<55 M0)     | I   | 14
--   II (age<55 M1)    | II  | 1
--   IVB (age>=55 M1)  | IVB | 5
--   NOT_DERIVABLE     | NULL| 13

-- Q0-C. V8-preview — builder _corrected vs AJCC8 derivation for the 20-row fillable subset
SELECT
  SUM(CASE
    WHEN ajcc8_stage_group_corrected = CASE
      WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
      WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
      WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
    END THEN 1 ELSE 0 END) AS n_match,
  COUNT(*) AS n_total
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected IS NOT NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: n_match = n_total = 20

-- Q0-D. Per-row fillable cohort dump (for eyeball verification)
SELECT research_id, diagnosis_primary, age_at_surgery,
       ajcc8_n_stage, ajcc8_m_stage,
       ajcc8_stage_group_corrected AS builder_corrected,
       CASE
         WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
         WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
         WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
       END AS ajcc8_derived
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected IS NOT NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
ORDER BY age_at_surgery, research_id;
-- EXPECT: 20 rows, builder_corrected = ajcc8_derived on every row.

-- Q0-E. The 13 deferred rows — record them for CF-395 handoff
SELECT research_id, diagnosis_primary, age_at_surgery,
       ajcc8_n_stage, ajcc8_m_stage,
       ajcc8_t_stage_v2 AS t_v2_signal,
       COALESCE(CAST(path_t_stage_raw AS VARCHAR), '·') AS path_t_raw
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
ORDER BY age_at_surgery, research_id;
-- EXPECT: 13 rows, all age>=55 M0. 11/13 have t_v2 populated; 2 have t_v2 NULL
-- (research_ids 1404 and 12198). Record this list in the probe report and the
-- close-out CF-1 block so Script 395 can consume it.
```

**Halt gate:** halt if ANY of the following fail:
- Q0-A ≠ 33
- Q0-B doesn't produce exactly the expected split (14 + 1 + 5 + 13)
- Q0-C.n_match ≠ 20 OR Q0-C.n_total ≠ 20
- Q0-D count ≠ 20 OR any row shows `builder_corrected != ajcc8_derived`

### Phase 1 — Plan-review gate
No approval file. Deterministic derivation. Logan eyeballs the Phase 0 report, confirms 20/20 V8-preview MATCH, greenlights verbally.

### Phase 2 — Apply

#### 2A. Archive snapshot (capture both the fillable 20 AND the deferred 13 for CF-395 reference)
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_stage_group_pre394_<STAMP> AS
SELECT research_id, diagnosis_primary, age_at_surgery,
       ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
       ajcc8_t_stage_v2,
       ajcc8_stage_group, ajcc8_stage_group_corrected,
       CASE
         WHEN ajcc8_stage_group_corrected IS NOT NULL THEN '394_fillable'
         ELSE '395_deferred_needs_T'
       END AS cohort_tag,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT row count: 33 (20 tagged 394_fillable + 13 tagged 395_deferred_needs_T).
```

#### 2B. Fill `ajcc8_stage_group` on the 20-row fillable subset (builder-sync pattern)
```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = ajcc8_stage_group_corrected
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected IS NOT NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
  -- Belt-and-suspenders: validate the builder's value matches AJCC8 rules
  AND ajcc8_stage_group_corrected = CASE
        WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
        WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
        WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
      END;
-- EXPECT: 20 rows updated.
```

**Design note:** the `CASE` guard in the WHERE means any future row where builder and AJCC8 disagree will SKIP the update silently. That's the correct behavior — we want to halt-on-mismatch rather than overwrite. Post-update verification (V8) will catch any skipped row as a residual orphan.

#### 2C. `__readme` provenance row
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 394: canonical_patient_master.ajcc8_stage_group NULL-T DTC orphan fill (M-decidable subset) — 20 rows builder-synced from ajcc8_stage_group_corrected (14×I, 1×II, 5×IVB), all AJCC8-validated. 13 rows deferred to future Script 395 (age>=55 M0, needs T sync from ajcc8_t_stage_v2). Snapshot: archive_pub_v1_0.cpm_stage_group_pre394_<STAMP>. Builder-sync validated 20/20 — same pattern as 393.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

### Phase 3 — Verify
```sql
-- V1. NULL-T DTC orphan count dropped by exactly 20 (was 33, should be 13)
SELECT COUNT(*) AS n_orphans_remaining
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 13

-- V2. All 20 rows tagged 394_fillable in the snapshot now have non-NULL stage_group
SELECT COUNT(*) AS n_filled
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '394_fillable'
  AND cpm.ajcc8_stage_group IS NOT NULL;
-- EXPECT: 20

-- V3. Stage-group distribution on the 20-row filled cohort
SELECT cpm.ajcc8_stage_group, COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '394_fillable'
GROUP BY 1 ORDER BY 1;
-- EXPECT: I=14, II=1, IVB=5

-- V4. Deferred 13 rows were NOT touched
SELECT COUNT(*) AS n_deferred_unchanged
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_deferred_needs_T'
  AND cpm.ajcc8_stage_group IS NULL;
-- EXPECT: 13

-- V5. CPM rowcount unchanged
SELECT COUNT(*) AS n_cpm FROM main.canonical_patient_master;
-- EXPECT: 10,871

-- V6. Snapshot table exists with 33 rows total
SELECT cohort_tag, COUNT(*) AS n
FROM archive_pub_v1_0.cpm_stage_group_pre394_<STAMP>
GROUP BY 1 ORDER BY 1;
-- EXPECT: 394_fillable=20, 395_deferred_needs_T=13.

-- V7. __readme row landed
SELECT COUNT(*) FROM main.__readme WHERE content LIKE 'Script 394:%';
-- EXPECT: 1

-- V8. Mirror parity on the 20-row cohort (stage_group == _corrected post-apply)
SELECT SUM(CASE WHEN cpm.ajcc8_stage_group != cpm.ajcc8_stage_group_corrected THEN 1 ELSE 0 END) AS n_mismatch
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '394_fillable';
-- EXPECT: 0

-- V9. Global T3b DTC orphan count still zero (393's work preserved)
SELECT COUNT(*) AS n_t3b_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 0
```

### Phase 4 — Commit + tag
Staged files (**explicit paths only** — `git add -f` is permitted for paths that match `.gitignore` globs, as established on 393):
- `scripts/394_dtc_null_t_stage_group_fill.py`
- `scripts/output/394_prestate_probe_report.md`
- `scripts/output/394_run.log` (use `-f` if `.gitignore` excludes it)
- `scripts/output/394_close_out_report.md`

Commit message: `Script 394: DTC NULL-T stage_group fill — 20 rows (14×I, 1×II, 5×IVB); 13 deferred to 395`
Tag: `v1_0-dtc-null-t-stage-groups-filled-<stamp>`

## Idempotency
1. If `archive_pub_v1_0.cpm_stage_group_pre394_*` snapshot exists AND `__readme` has `content LIKE 'Script 394:%'` → exit 0, NO-OP.
2. If snapshot exists but no `__readme` row → halt `partial apply detected; manual review required`.
3. If `__readme` exists but no snapshot → halt `missing snapshot; manual review required`.

## Non-goals / deferred (carry-forward explicit)
**CF-394-1 (→ Script 395):** 13 age≥55 M0 DTC rows where `ajcc8_t_stage IS NULL` AND `ajcc8_stage_group_corrected IS NULL`. Target for 395:
 - Sync `ajcc8_t_stage ← ajcc8_t_stage_v2` for the 11 rows where t_v2 is populated
 - Re-derive stage_group from (T, N, M, age) for those 11
 - 2 residual rows (research_ids 1404, 12198) have NO T signal anywhere — route to a manual-review queue
 - Snapshot names and per-row dumps are preserved in `archive_pub_v1_0.cpm_stage_group_pre394_<STAMP>` under `cohort_tag = '395_deferred_needs_T'`

**CF-394-2:** Builder-logic root cause — the 240-builder computes correct values into `ajcc8_stage_group_corrected` but doesn't sync to `ajcc8_stage_group` in some code paths. 393 + 394 are point-fixes. A future script (or a builder PR) should close this loop structurally; otherwise the sync gap will reappear on any CPM rebuild. Not in 394 scope.

## First action for the agent

1. Run Phase 0. Write `scripts/output/394_prestate_probe_report.md` with all five Q0 outputs (A, B, C, D, E).
2. Confirm Q0-A=33, Q0-B split is 14+1+5+13, Q0-C n_match=20, Q0-D is 20 rows with 100% builder/AJCC8 agreement.
3. Include the 13-row Q0-E dump verbatim in the close-out's CF-394-1 section.
4. Wait for Logan's greenlight (verbal).
5. Apply Phases 2A–2C. Run Phase 3 V1–V9. Commit + tag.

**Do NOT touch `ajcc8_t_stage` in 394. Do NOT touch the 13 deferred rows' `ajcc8_stage_group`. Do NOT rebuild any canonical tables.**
