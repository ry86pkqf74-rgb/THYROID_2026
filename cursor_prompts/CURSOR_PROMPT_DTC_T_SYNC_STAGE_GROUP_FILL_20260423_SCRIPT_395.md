# Script 395 — DTC T-Sync + Stage-Group Fill (T-Requiring Residual)

**Stamp:** 2026-04-23
**Type:** Surgical in-place UPDATE on `canonical_patient_master` (CPM) + insert into new manual-review queue
**DB:** `thyroid_canonical_publication_v1_0`
**Target columns:** `main.canonical_patient_master.ajcc8_t_stage`, `ajcc8_stage_group`, `ajcc8_stage_group_corrected`
**Prereqs:** 394 closed (tag `v1_0-dtc-null-t-stage-groups-filled-20260423_000452`); carry-forward cohort preserved in `archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452` under `cohort_tag = '395_deferred_needs_T'`
**Ends at:** `v1_0-dtc-t-sync-stage-groups-filled-<stamp>` commit + tag
**Scope size:** 11 rows filled + 2 rows routed to new manual-review queue

---

## Problem

13 DTC (PTC/FTC/HCC) patients, all age≥55 M0, have `ajcc8_t_stage IS NULL` AND `ajcc8_stage_group IS NULL` — the T-requiring residual from 394. Under AJCC8 DTC age≥55 M0, stage group depends on T+N.

Direct MD probe (2026-04-23, this session) shows **11 of the 13 have `ajcc8_t_stage_v2` populated** (the phase-4.6 parallel T column, unused since added). Cross-column corroboration is strong: `dominant_tumor_ajcc8_t_stage` and `path_t_stage_raw` match `ajcc8_t_stage_v2` on the rows where those are populated. The T-sync is low-risk.

The remaining 2 rows (`research_id` ∈ {1404, 12198}) have **no T signal across any T column** but both have `path_stage_raw = 'III'` and `gm_path_stage_raw = 'III'` — pathology directly documented "Stage III" but the T didn't get extracted. These need manual chart review to determine AJCC edition (III under AJCC8 age≥55 M0 ⇒ T4a; III under AJCC7 ⇒ different T interpretation) and go to a new review queue.

## Live baseline (2026-04-23 probe)

### 11 T-syncable rows — full per-row data with projected stage

| research_id | dx | age | `t_v2` (→ new T) | N | M | projected stage | AJCC8 rationale |
|---|---|---|---|---|---|---|---|
| 165 | PTC | 57 | T1a | N0 | M0 | **I** | age≥55 M0 T1/T2 N0 → I |
| 1799 | PTC | 55 | T2 | N0 | M0 | **I** | age≥55 M0 T1/T2 N0 → I |
| 325 | PTC | 63 | T1a | N0 | M0 | **I** | age≥55 M0 T1/T2 N0 → I |
| 3790 | PTC | 76 | T2 | N0 | M0 | **I** | age≥55 M0 T1/T2 N0 → I |
| 1050 | FTC | 58 | T1b | N1a | M0 | **II** | age≥55 M0 T1/T2 N1 → II |
| 1074 | PTC | 85 | T3a | N1a | M0 | **II** | age≥55 M0 T3a → II |
| 1138 | FTC | 61 | T3a | N0 | M0 | **II** | age≥55 M0 T3a → II |
| 497 | PTC | 56 | T1 | N1a | M0 | **II** | age≥55 M0 T1/T2 N1 → II |
| 5569 | PTC | 58 | T1a | N1a | M0 | **II** | age≥55 M0 T1/T2 N1 → II |
| 5781 | PTC | 55 | T1b | N1a | M0 | **II** | age≥55 M0 T1/T2 N1 → II |
| 651 | PTC | 72 | T1a | N1a | M0 | **II** | age≥55 M0 T1/T2 N1 → II |

**Split: 4 × I + 7 × II = 11 rows**

### 2 manual-review rows — routed to queue, NOT updated

| research_id | dx | age | N | M | `path_stage_raw` | `gm_path_stage_raw` | reason |
|---|---|---|---|---|---|---|---|
| 1404 | PTC | 64 | N1a | M0 | III | III | T absent across all columns; pathology says "Stage III" (AJCC edition unknown — manual review to resolve T4a vs T3) |
| 12198 | PTC | 61 | N1a | M0 | III | III | same as above |

## Scope

1. **Sync T:** `ajcc8_t_stage ← ajcc8_t_stage_v2` on the 11-row cohort.
2. **Fill stage_group:** derive from (t_v2, N, M, age) under AJCC8 DTC rules on same 11 rows.
3. **Mirror:** `ajcc8_stage_group_corrected ← ajcc8_stage_group` on same 11 rows (both currently NULL).
4. **Queue:** insert 2 unresolvable rows into new table `manuscript_workspace.cpm_stage_group_manual_review_v1` with pathology-stage signal surfaced.

### Out-of-scope
- No other CPM columns touched
- No rebuild of CPM or any canonical table
- No touching of `ajcc8_t_stage_v2` (source column stays read-only)
- No backfill of `path_stage_raw` → ajcc8_* on other rows (that's a separate systematic problem)

## Execution phases

### Phase 0 — Probe (read-only, idempotent)
```sql
-- Q0-A. Re-confirm the 13-row cohort is still exactly as 394 left it
SELECT COUNT(*) AS n_remaining_null_t_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 13

-- Q0-B. Confirm the 13 are exactly the rows in 394's deferred snapshot
SELECT COUNT(*) AS n_match_394_deferred
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452 s USING (research_id)
WHERE s.cohort_tag = '395_deferred_needs_T'
  AND cpm.ajcc8_t_stage IS NULL
  AND cpm.ajcc8_stage_group IS NULL;
-- EXPECT: 13

-- Q0-C. Split — 11 T-syncable vs 2 unresolvable
SELECT
  CASE WHEN cpm.ajcc8_t_stage_v2 IS NOT NULL THEN 'T_syncable' ELSE 'manual_review' END AS route,
  COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452 s USING (research_id)
WHERE s.cohort_tag = '395_deferred_needs_T'
GROUP BY 1 ORDER BY 1;
-- EXPECT: T_syncable=11, manual_review=2

-- Q0-D. T-column corroboration on the 11 syncable rows
-- (Are ajcc8_t_stage_v2 and dominant_tumor_ajcc8_t_stage aligned where both populated?)
SELECT
  cpm.research_id,
  cpm.ajcc8_t_stage_v2 AS t_v2,
  COALESCE(CAST(cpm.dominant_tumor_ajcc8_t_stage AS VARCHAR), '·') AS t_dom,
  CASE
    WHEN cpm.dominant_tumor_ajcc8_t_stage IS NULL THEN 'v2_only_ok'
    WHEN CAST(cpm.dominant_tumor_ajcc8_t_stage AS VARCHAR) = CAST(cpm.ajcc8_t_stage_v2 AS VARCHAR) THEN 'corroborated'
    ELSE 'DISAGREEMENT_halt'
  END AS corroboration
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452 s USING (research_id)
WHERE s.cohort_tag = '395_deferred_needs_T'
  AND cpm.ajcc8_t_stage_v2 IS NOT NULL
ORDER BY cpm.research_id;
-- EXPECT: 11 rows, all 'corroborated' or 'v2_only_ok'. If any 'DISAGREEMENT_halt' — halt.

-- Q0-E. Projected stage_group distribution (expect 4×I + 7×II)
SELECT
  CASE
    WHEN cpm.ajcc8_t_stage_v2 IN ('T1','T1a','T1b','T2') AND cpm.ajcc8_n_stage = 'N0' THEN 'I'
    WHEN cpm.ajcc8_t_stage_v2 IN ('T1','T1a','T1b','T2') AND cpm.ajcc8_n_stage LIKE 'N1%' THEN 'II'
    WHEN cpm.ajcc8_t_stage_v2 IN ('T3a','T3b') THEN 'II'
    WHEN cpm.ajcc8_t_stage_v2 = 'T4a' THEN 'III'
    WHEN cpm.ajcc8_t_stage_v2 = 'T4b' THEN 'IVA'
    ELSE 'UNHANDLED_halt'
  END AS projected,
  COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452 s USING (research_id)
WHERE s.cohort_tag = '395_deferred_needs_T'
  AND cpm.ajcc8_t_stage_v2 IS NOT NULL
  AND cpm.age_at_surgery >= 55
  AND cpm.ajcc8_m_stage = 'M0'
GROUP BY 1 ORDER BY 1;
-- EXPECT: I=4, II=7. No 'UNHANDLED_halt'.

-- Q0-F. Confirm the 2 manual-review rows have path_stage_raw='III'
SELECT research_id, path_stage_raw, gm_path_stage_raw, ajcc8_n_stage, ajcc8_m_stage, age_at_surgery
FROM main.canonical_patient_master
WHERE research_id IN ('1404', '12198');
-- EXPECT: both rows return path_stage_raw='III' and gm_path_stage_raw='III'.

-- Q0-G. Check for existing manual-review queue table (idempotency)
SELECT COUNT(*) AS n_tbls
FROM information_schema.tables
WHERE table_catalog='thyroid_canonical_publication_v1_0'
  AND table_schema='manuscript_workspace'
  AND table_name='cpm_stage_group_manual_review_v1';
-- If 1: table exists, check row count for 395 entries before inserting.
-- If 0: create in Phase 2D.
```

**Halt gate:** halt if ANY of:
- Q0-A ≠ 13
- Q0-B ≠ 13
- Q0-C split ≠ (11, 2)
- Q0-D any row = 'DISAGREEMENT_halt'
- Q0-E any row = 'UNHANDLED_halt' OR distribution ≠ (I=4, II=7)
- Q0-F either row's path_stage_raw ≠ 'III'

### Phase 1 — Plan-review gate
**Use `--i-approve=<probe_report_sha256>` if the runner supports it** (established as a process improvement after 394). Otherwise verbal greenlight after probe report lands.

### Phase 2 — Apply

#### 2A. Archive snapshot
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> AS
SELECT research_id, diagnosis_primary, age_at_surgery,
       ajcc8_t_stage, ajcc8_t_stage_v2,
       dominant_tumor_ajcc8_t_stage,
       ajcc8_n_stage, ajcc8_m_stage,
       ajcc8_stage_group, ajcc8_stage_group_corrected,
       path_stage_raw, gm_path_stage_raw,
       CASE
         WHEN ajcc8_t_stage_v2 IS NOT NULL THEN '395_t_synced'
         ELSE '395_manual_review'
       END AS cohort_tag,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM main.canonical_patient_master cpm
WHERE research_id IN (
  SELECT research_id FROM archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452
  WHERE cohort_tag = '395_deferred_needs_T'
);
-- EXPECT row count: 13 (11 tagged 395_t_synced + 2 tagged 395_manual_review).
```

#### 2B. Sync T on the 11 T-syncable rows
```sql
UPDATE main.canonical_patient_master
SET ajcc8_t_stage = ajcc8_t_stage_v2
WHERE ajcc8_t_stage IS NULL
  AND ajcc8_t_stage_v2 IS NOT NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
  AND research_id IN (
    SELECT research_id FROM archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452
    WHERE cohort_tag = '395_deferred_needs_T'
  )
  -- Belt-and-suspenders: require corroboration where dominant_tumor_ajcc8_t_stage is populated
  AND (dominant_tumor_ajcc8_t_stage IS NULL
       OR CAST(dominant_tumor_ajcc8_t_stage AS VARCHAR) = CAST(ajcc8_t_stage_v2 AS VARCHAR));
-- EXPECT: 11 rows updated.
```

#### 2C. Derive and fill stage_group (and mirror to _corrected) on the 11 rows
```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = CASE
      WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage = 'N0'      THEN 'I'
      WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage LIKE 'N1%'  THEN 'II'
      WHEN ajcc8_t_stage IN ('T3a','T3b')                                         THEN 'II'
      WHEN ajcc8_t_stage = 'T4a'                                                  THEN 'III'
      WHEN ajcc8_t_stage = 'T4b'                                                  THEN 'IVA'
    END,
    ajcc8_stage_group_corrected = CASE
      WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage = 'N0'      THEN 'I'
      WHEN ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage LIKE 'N1%'  THEN 'II'
      WHEN ajcc8_t_stage IN ('T3a','T3b')                                         THEN 'II'
      WHEN ajcc8_t_stage = 'T4a'                                                  THEN 'III'
      WHEN ajcc8_t_stage = 'T4b'                                                  THEN 'IVA'
    END
WHERE ajcc8_stage_group IS NULL
  AND ajcc8_t_stage IS NOT NULL  -- was just filled by 2B
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage = 'M0'
  AND age_at_surgery >= 55
  AND diagnosis_primary IN ('PTC','FTC','HCC')
  AND research_id IN (
    SELECT research_id FROM archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452
    WHERE cohort_tag = '395_deferred_needs_T'
  );
-- EXPECT: 11 rows updated. Distribution I=4, II=7.
```

#### 2D. Create manual-review queue (if not exists) and insert the 2 unresolvable rows
```sql
CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_stage_group_manual_review_v1 (
  research_id VARCHAR,
  reason VARCHAR,
  path_stage_raw VARCHAR,
  gm_path_stage_raw VARCHAR,
  ajcc8_n_stage VARCHAR,
  ajcc8_m_stage VARCHAR,
  age_at_surgery INTEGER,
  diagnosis_primary VARCHAR,
  source_script VARCHAR,
  inserted_at TIMESTAMP
);

INSERT INTO manuscript_workspace.cpm_stage_group_manual_review_v1
  (research_id, reason, path_stage_raw, gm_path_stage_raw,
   ajcc8_n_stage, ajcc8_m_stage, age_at_surgery, diagnosis_primary,
   source_script, inserted_at)
SELECT cpm.research_id,
       'no_T_signal_path_stage_raw_III_ajcc_edition_unknown',
       CAST(cpm.path_stage_raw AS VARCHAR),
       CAST(cpm.gm_path_stage_raw AS VARCHAR),
       cpm.ajcc8_n_stage, cpm.ajcc8_m_stage,
       cpm.age_at_surgery, cpm.diagnosis_primary,
       '395', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM main.canonical_patient_master cpm
WHERE cpm.research_id IN ('1404', '12198')
  AND NOT EXISTS (
    SELECT 1 FROM manuscript_workspace.cpm_stage_group_manual_review_v1 q
    WHERE q.research_id = cpm.research_id AND q.source_script = '395'
  );
-- EXPECT: 2 rows inserted (0 on re-run).
```

#### 2E. `__readme` provenance row
```sql
INSERT INTO main.__readme(content, updated_at) VALUES
('Script 395: canonical_patient_master T-sync + stage_group fill for 394-deferred cohort — 11 rows ajcc8_t_stage<-ajcc8_t_stage_v2 (corroborated by dominant_tumor_ajcc8_t_stage where present) with ajcc8_stage_group derived via AJCC8 DTC age>=55 M0 rules (4xI, 7xII); ajcc8_stage_group_corrected mirrored. 2 rows (research_ids 1404, 12198) routed to manuscript_workspace.cpm_stage_group_manual_review_v1 as no_T_signal_path_stage_raw_III_ajcc_edition_unknown (need chart review to resolve T4a vs T3). Snapshot: archive_pub_v1_0.cpm_t_sync_pre395_<STAMP>.',
 CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

### Phase 3 — Verify
```sql
-- V1. DTC stage_group orphan count dropped from 13 to 2
SELECT COUNT(*) AS n_orphans_remaining
FROM main.canonical_patient_master
WHERE ajcc8_stage_group IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 2 (the research_ids 1404, 12198).

-- V2. All 11 T_synced rows now have non-NULL T, stage_group, and _corrected
SELECT COUNT(*) AS n_fully_filled
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_t_synced'
  AND cpm.ajcc8_t_stage IS NOT NULL
  AND cpm.ajcc8_stage_group IS NOT NULL
  AND cpm.ajcc8_stage_group_corrected IS NOT NULL;
-- EXPECT: 11

-- V3. Distribution on the 11-row filled cohort matches spec (4×I, 7×II)
SELECT cpm.ajcc8_stage_group, COUNT(*) AS n
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_t_synced'
GROUP BY 1 ORDER BY 1;
-- EXPECT: I=4, II=7.

-- V4. ajcc8_t_stage matches ajcc8_t_stage_v2 on all 11 rows (T-sync fidelity)
SELECT SUM(CASE WHEN CAST(cpm.ajcc8_t_stage AS VARCHAR) != CAST(cpm.ajcc8_t_stage_v2 AS VARCHAR) THEN 1 ELSE 0 END) AS n_mismatch
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_t_synced';
-- EXPECT: 0

-- V5. _primary matches _corrected on the 11 rows
SELECT SUM(CASE WHEN cpm.ajcc8_stage_group != cpm.ajcc8_stage_group_corrected THEN 1 ELSE 0 END) AS n_mismatch
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_t_synced';
-- EXPECT: 0

-- V6. The 2 manual-review rows were NOT touched in CPM
SELECT COUNT(*) AS n_review_still_null
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_t_sync_pre395_<STAMP> s USING (research_id)
WHERE s.cohort_tag = '395_manual_review'
  AND cpm.ajcc8_t_stage IS NULL
  AND cpm.ajcc8_stage_group IS NULL;
-- EXPECT: 2

-- V7. Manual-review queue has exactly 2 rows from 395
SELECT COUNT(*) AS n_in_queue
FROM manuscript_workspace.cpm_stage_group_manual_review_v1
WHERE source_script = '395';
-- EXPECT: 2

-- V8. CPM rowcount unchanged
SELECT COUNT(*) FROM main.canonical_patient_master;
-- EXPECT: 10,871

-- V9. T3b DTC orphan count still zero (393 preserved)
SELECT COUNT(*) AS n_t3b_orphans
FROM main.canonical_patient_master
WHERE ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC');
-- EXPECT: 0

-- V10. 394-filled rows still have stage_group (394 preserved)
SELECT COUNT(*) AS n_394_filled_lost
FROM main.canonical_patient_master cpm
JOIN archive_pub_v1_0.cpm_stage_group_pre394_20260423_000452 s USING (research_id)
WHERE s.cohort_tag = '394_fillable'
  AND cpm.ajcc8_stage_group IS NULL;
-- EXPECT: 0

-- V11. __readme row landed
SELECT COUNT(*) FROM main.__readme WHERE content LIKE 'Script 395:%';
-- EXPECT: 1
```

### Phase 4 — Commit + tag
Staged files (explicit paths only; `git add -f` permitted for `.gitignore`-excluded listed paths):
- `scripts/395_dtc_t_sync_stage_group_fill.py`
- `scripts/output/395_prestate_probe_report.md`
- `scripts/output/395_run.log`
- `scripts/output/395_close_out_report.md`

Commit message: `Script 395: DTC T-sync + stage_group fill — 11 rows (4×I, 7×II); 2 routed to manual-review queue`
Tag: `v1_0-dtc-t-sync-stage-groups-filled-<stamp>`

## Idempotency
1. If `archive_pub_v1_0.cpm_t_sync_pre395_*` snapshot exists AND `__readme` has `Script 395:%` → exit 0, NO-OP.
2. If snapshot exists but no `__readme` row → halt `partial apply detected; manual review required`.
3. If `__readme` exists but no snapshot → halt `missing snapshot; manual review required`.

**Carry forward the 394-era bug fix** (script must write the close-out report AFTER the idempotency check, not before, so the NO-OP branch doesn't blank an existing close-out).

## Non-goals / deferred (carry-forward explicit)
**CF-395-1:** 2 rows (research_ids 1404, 12198) remain in `manuscript_workspace.cpm_stage_group_manual_review_v1`. Chart-review decision needed: under AJCC8 age≥55 M0, path_stage_raw='III' ⇒ T4a (Stage III); under AJCC7 ⇒ different T mapping. The edition used by the pathologist on each case must be confirmed manually before stage_group is back-filled. Not a builder problem — a data-completeness problem at the extraction layer.

**CF-395-2:** Builder root-cause — the 240-builder still has a code path that can leave `ajcc8_stage_group_corrected` NULL when `ajcc8_t_stage` is NULL but `ajcc8_t_stage_v2` is populated. A structural fix in the builder (COALESCE or fallback) would prevent future rebuilds from regenerating this 13-row gap. Not in 395 scope; track for a builder PR.

## First action for the agent

1. Run Phase 0 Q0-A through Q0-G. Write `scripts/output/395_prestate_probe_report.md` with all outputs verbatim.
2. Halt if any halt gate fires (see Phase 0 Halt gate list).
3. **Use `--i-approve=<sha256 of probe report>` as the gate mechanism** if the runner supports it. Otherwise wait for Logan's verbal greenlight.
4. Apply Phases 2A–2E. Run Phase 3 V1–V11. Commit + tag.

**Do NOT touch the 2 manual-review rows' CPM columns. Do NOT backfill `path_stage_raw` → `ajcc8_*` on any other rows. Do NOT modify `ajcc8_t_stage_v2` (read-only source).**
