# Cursor Prompt — mig_163b ANY-RECURRENCE HYBRID Apply (Logan-ratified 2026-04-29)

**Lane:** 58 / mig_163b
**Batch_id:** `mig_163b_any_recurrence_hybrid_apply_20260429`
**Generated:** 2026-04-29 (late evening) — Logan ratified **HYBRID** definition
**Type:** Data write to PM. Pre-snapshot required.

**Logan's call (2026-04-29 evening):** **HYBRID** — `canonical_recurrence_v1.recurrence_confirmed=TRUE` ∪ `canonical_recurrence_resolved_v1.recurrence_status_final='path_proven'`. Cowork live verified the union equals **514 patients** today (path_proven ⊆ recurrence_confirmed → 0 extra patients vs STRICT), but HYBRID is the manuscript definition for resilience: if the recurrence_v1 vs recurrence_resolved_v1 split changes later, HYBRID still captures every path-proven recurrence.

---

## §0 Why this lane exists

mig_163 (Cursor commit `9c1fd68`) profiled the ANY-RECURRENCE undercount and surfaced 3 candidate definitions. Logan ratified **HYBRID** on 2026-04-29:

| Definition | ARF=TRUE N | Alignment vs current PM | Status |
|---|---|---|---|
| STRICT (recurrence_confirmed only) | 514 | 94.8% | not picked |
| WIDE (bioch ∨ struct ∨ distant ∨ confirmed) | 2,187 | 83.4% | not picked |
| **HYBRID** (confirmed ∨ resolved path_proven) | **514** | 94.8% | **RATIFIED** |

Tier-1 LLM probe found `note_entities_llm_recurrence` rows had `{"entities": []}` empty payloads on the "struct no canonical join" sample — Tier-1 is not the source of the structural_recurrence_flag PM-only positives. The 219 PM-only patients are dropped; the 349 canon-only patients are added.

This prompt is the **applier** for HYBRID. Cowork live cardinality probe (2026-04-29):
- `canonical_recurrence_v1.recurrence_confirmed=TRUE` → 514 distinct rids
- `canonical_recurrence_resolved_v1.recurrence_status_final='path_proven'` → 145 distinct rids
- UNION (HYBRID) → **514** rids (path_proven ⊆ recurrence_confirmed today)
- path_proven ∖ recurrence_confirmed = **0** rids

So the apply is data-equivalent to STRICT today, but **definitionally HYBRID** — the registry note records the lineage so future drift between recurrence_v1 and recurrence_resolved_v1 wouldn't silently change the manuscript flag definition.

## §1 Governance posture

- Cursor agent authors the SQL only. Cowork applies via Path C after Logan's ratification.
- Pre-snapshot the PM `any_recurrence_flag` slice + the registry slice for that col before any UPDATE.
- Output: `qc_framework_v1/migrations/163b_any_recurrence_strict_apply_20260429.sql` only (no Markdown report — mig_163's report has the analysis).

## §2 Required pre-flight probes (paste into SQL header)

```sql
-- §2a HYBRID cardinality reconcile — UNION of recurrence_confirmed + path_proven
WITH cr_conf AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed = TRUE
),
crr_path AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final = 'path_proven'
),
hybrid AS (
  SELECT rid FROM cr_conf UNION SELECT rid FROM crr_path
)
SELECT
  (SELECT COUNT(*) FROM cr_conf)   AS strict_n,
  (SELECT COUNT(*) FROM crr_path)  AS path_proven_n,
  (SELECT COUNT(*) FROM hybrid)    AS hybrid_union_n,
  (SELECT COUNT(*) FROM crr_path WHERE rid NOT IN (SELECT rid FROM cr_conf)) AS path_proven_added_by_hybrid;
-- Cowork live 2026-04-29: 514 / 145 / 514 / 0 (path_proven ⊆ recurrence_confirmed today).
-- If hybrid_union_n drifts from 514, document in header — definition is HYBRID, count is whatever the union returns.

-- §2b 2x2 reconcile vs current PM any_recurrence_flag (vs HYBRID)
WITH hybrid AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
  UNION
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'
),
pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag FROM main.canonical_patient_master)
SELECT
  SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS both,
  SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS pm_only_dropped,
  SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS hybrid_only_added,
  SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS neither
FROM pm LEFT JOIN hybrid USING (rid);
-- Expect today: 165 / 219 / 349 / 10138 (since HYBRID = STRICT in current data).

-- §2c Cohort parity invariant
SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids FROM main.canonical_patient_master;
-- Expect: 10871 / 10871

-- §2d Pre-state distribution of any_recurrence_flag
SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS pre_t,
       SUM(CASE WHEN NOT any_recurrence_flag THEN 1 ELSE 0 END) AS pre_f,
       SUM(CASE WHEN any_recurrence_flag IS NULL THEN 1 ELSE 0 END) AS pre_n
FROM main.canonical_patient_master;
-- Expect: 384 TRUE / 10487 FALSE / 0 NULL pre-flip (post-flip should be 514 / 10357 / 0).
```

## §3 SQL structure expected in `163b_any_recurrence_strict_apply_20260429.sql`

### Section A — Pre-snapshots

```sql
-- A1: Snapshot the column slice from PM (research_id + any_recurrence_flag) before mutation
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429 AS
SELECT research_id, any_recurrence_flag, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig163b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- A2: Snapshot the registry row for any_recurrence_flag
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_any_recurrence_flag_pre_mig163b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig163b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master' AND column_name='any_recurrence_flag';
```

### Section B — HYBRID redefinition (single transaction)

```sql
BEGIN TRANSACTION;

-- HYBRID = (canonical_recurrence_v1.recurrence_confirmed=TRUE)
--       OR (canonical_recurrence_resolved_v1.recurrence_status_final='path_proven')
UPDATE main.canonical_patient_master AS pm
SET any_recurrence_flag = (
  CAST(pm.research_id AS VARCHAR) IN (
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_v1
    WHERE recurrence_confirmed = TRUE
    UNION
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_resolved_v1
    WHERE recurrence_status_final = 'path_proven'
  )
);

-- Section B1 — Update the column registry note with the redefinition lineage
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_163b: any_recurrence_flag REDEFINED (HYBRID) = '
            || 'canonical_recurrence_v1.recurrence_confirmed=TRUE'
            || ' UNION canonical_recurrence_resolved_v1.recurrence_status_final=''path_proven''. '
            || 'Pre-snapshot canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429. '
            || 'Today path_proven subset of recurrence_confirmed (UNION=514); definition is HYBRID for resilience '
            || 'against future recurrence_v1 vs recurrence_resolved_v1 drift. '
            || 'Drops 219 PM-only patients; adds 349 canon-only patients (Logan-ratified manuscript definition 2026-04-29). '
            || 'Closes CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT.'
WHERE schema_name='main' AND table_name='canonical_patient_master' AND column_name='any_recurrence_flag';

COMMIT;
```

### Section C — Post-state verification (commented; Cowork runs after apply)

```sql
-- C1: Confirm new TRUE count matches HYBRID UNION cardinality (514 today)
-- SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS post_t
-- FROM main.canonical_patient_master;
-- Expect: 514

-- C2: Confirm 0 row mismatch vs HYBRID union
-- WITH pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag FROM main.canonical_patient_master),
--      hybrid AS (
--        SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
--        UNION
--        SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'
--      )
-- SELECT
--   SUM(CASE WHEN pm.any_recurrence_flag AND hybrid.rid IS NULL THEN 1 ELSE 0 END) AS pm_t_hybrid_f,
--   SUM(CASE WHEN NOT pm.any_recurrence_flag AND hybrid.rid IS NOT NULL THEN 1 ELSE 0 END) AS pm_f_hybrid_t
-- FROM pm LEFT JOIN hybrid USING (rid);
-- Expect: 0 / 0
```

## §4 Required CFs

- `CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT` → CLOSED (with link to mig_163b)
- `CF-mig163b-HYBRID-UNION-EQUALS-STRICT-TODAY` (informational) — note that path_proven ⊆ recurrence_confirmed today so HYBRID UNION = 514 = STRICT count; if a future recurrence_resolved_v1 rebuild adds path_proven patients absent from recurrence_v1, the HYBRID definition will pick them up automatically without code change.
- `CF-mig163b-WIDE-DEFINITION-DEFERRED` (informational) — WIDE definition (bioch ∨ struct ∨ distant ∨ confirmed = 2,187) was considered and rejected for primary endpoint but remains available for sensitivity analysis.

## §5 Git workflow

- File: `qc_framework_v1/migrations/163b_any_recurrence_hybrid_apply_20260429.sql`
- Commit: `qc: mig_163b ANY-RECURRENCE redefined HYBRID (Logan-ratified manuscript definition)`
- Push.

## §6 Out of scope

- Do NOT modify any other PM column.
- Do NOT modify `biochemical_recurrence_flag`, `structural_recurrence_flag`, `distant_mets_proxy*` — those are intentionally distinct concepts (suspicion proxies, not endpoint).
- Do NOT modify `recurrence_flag_v2`, `recurrence_flag_scoring`, `rec_structural_flag` — separate analytic flags.
- Do NOT touch `canonical_recurrence_v1` or `canonical_recurrence_resolved_v1` table data.
- Do NOT apply on MD; ship SQL only — Cowork applies via Path C after pre-snapshot.

## §7 Apply governance

This lane is now **Logan-ratified (HYBRID)**. Cursor agent ships the SQL; Cowork pre-snapshots and applies via `query_rw` per Path C protocol. No `STATUS: PENDING` banner needed — go straight to ship.
