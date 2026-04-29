# Cursor Prompt — mig_163b ANY-RECURRENCE STRICT Apply (PENDING LOGAN RATIFICATION)

**Lane:** 58 / mig_163b
**Batch_id:** `mig_163b_any_recurrence_strict_apply_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Data write to PM. Pre-snapshot required. **DO NOT EXECUTE this lane until Logan explicitly ratifies the STRICT (or HYBRID) clinical definition.**

---

## §0 Why this lane exists

mig_163 (Cursor commit `9c1fd68`) profiled the ANY-RECURRENCE undercount and surfaced 3 candidate definitions:

| Definition | ARF=TRUE N | Alignment vs current PM | Trade-off |
|---|---|---|---|
| **STRICT** (recurrence_confirmed only) | **514** | 94.8% | Drops 219 PM-only; adds 349 canon-only |
| **WIDE** (bioch ∨ struct ∨ distant ∨ confirmed) | 2,187 | 83.4% | +1,803 positives; suspicion envelope |
| **HYBRID** (confirmed ∨ resolved path_proven) | **514 (=STRICT)** | 94.8% | path_proven ⊆ recurrence_confirmed → 0 extra patients |

Tier-1 LLM probe found `note_entities_llm_recurrence` rows had `{"entities": []}` empty payloads on the "struct no canonical join" sample — Tier-1 is not the source of the structural_recurrence_flag PM-only positives.

mig_163's recommendation: **STRICT** (or HYBRID as alias — same N=514 today).

This prompt is the **applier**. It encodes STRICT, but is gated on Logan's explicit ratification before execution.

## §0.1 GATE — DO NOT PROCEED WITHOUT LOGAN'S RATIFICATION

Before this lane runs, Logan must confirm in writing:
- Pick definition: **STRICT** or **HYBRID** (or **WIDE** — would change all SQL below)
- Acknowledge the **219 PM-only patients dropped** (ARF flips TRUE→FALSE on these patients). Logan must confirm these are not the manuscript primary cohort.
- Acknowledge the **349 canon-only patients added** (ARF flips FALSE→TRUE on these patients).

If Logan picks WIDE, this prompt's SQL is wrong — re-author with the OR-ladder definition.

## §1 Governance posture

- Cursor agent authors the SQL only. Cowork applies via Path C after Logan's ratification.
- Pre-snapshot the PM `any_recurrence_flag` slice + the registry slice for that col before any UPDATE.
- Output: `qc_framework_v1/migrations/163b_any_recurrence_strict_apply_20260429.sql` only (no Markdown report — mig_163's report has the analysis).

## §2 Required pre-flight probes (paste into SQL header)

```sql
-- §2a Re-confirm the 2x2 reconcile (must match mig_163 report numbers: 165 / 219 / 349 / 10138)
WITH cr_conf AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
),
pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag FROM main.canonical_patient_master)
SELECT
  SUM(CASE WHEN pm.any_recurrence_flag AND cr_conf.rid IS NOT NULL THEN 1 ELSE 0 END) AS both,
  SUM(CASE WHEN pm.any_recurrence_flag AND cr_conf.rid IS NULL THEN 1 ELSE 0 END) AS pm_only,
  SUM(CASE WHEN NOT pm.any_recurrence_flag AND cr_conf.rid IS NOT NULL THEN 1 ELSE 0 END) AS canon_only,
  SUM(CASE WHEN NOT pm.any_recurrence_flag AND cr_conf.rid IS NULL THEN 1 ELSE 0 END) AS neither
FROM pm LEFT JOIN cr_conf USING (rid);
-- Expect: 165 / 219 / 349 / 10138 (or note material drift in header).

-- §2b Cohort parity invariant
SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids FROM main.canonical_patient_master;
-- Expect: 10871 / 10871

-- §2c Pre-state distribution of any_recurrence_flag
SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS pre_t,
       SUM(CASE WHEN NOT any_recurrence_flag THEN 1 ELSE 0 END) AS pre_f,
       SUM(CASE WHEN any_recurrence_flag IS NULL THEN 1 ELSE 0 END) AS pre_n
FROM main.canonical_patient_master;
-- Expect: 384 TRUE / N FALSE / N NULL pre-flip.
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

### Section B — STRICT redefinition (single transaction)

```sql
BEGIN TRANSACTION;

UPDATE main.canonical_patient_master AS pm
SET any_recurrence_flag = (
  CAST(pm.research_id AS VARCHAR) IN (
    SELECT DISTINCT CAST(research_id AS VARCHAR)
    FROM main.canonical_recurrence_v1
    WHERE recurrence_confirmed = TRUE
  )
);

-- Section B1 — Update the column registry note with the redefinition lineage
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_163b: any_recurrence_flag REDEFINED (STRICT) = canonical_recurrence_v1.recurrence_confirmed=TRUE.'
            || ' Pre-snapshot canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429.'
            || ' Drops 219 PM-only patients; adds 349 canon-only patients (Logan-ratified manuscript definition).'
            || ' Closes CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT.'
WHERE schema_name='main' AND table_name='canonical_patient_master' AND column_name='any_recurrence_flag';

COMMIT;
```

### Section C — Post-state verification (commented; Cowork runs after apply)

```sql
-- C1: Confirm new TRUE count matches canonical_recurrence_v1.recurrence_confirmed=TRUE count (514)
-- SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS post_t
-- FROM main.canonical_patient_master;
-- Expect: 514

-- C2: Confirm IS_DISTINCT_FROM canonical_recurrence_v1.recurrence_confirmed = 0 row mismatch
-- WITH pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, any_recurrence_flag FROM main.canonical_patient_master),
--      cr AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE)
-- SELECT
--   SUM(CASE WHEN pm.any_recurrence_flag AND cr.rid IS NULL THEN 1 ELSE 0 END) AS pm_t_canon_f,
--   SUM(CASE WHEN NOT pm.any_recurrence_flag AND cr.rid IS NOT NULL THEN 1 ELSE 0 END) AS pm_f_canon_t
-- FROM pm LEFT JOIN cr USING (rid);
-- Expect: 0 / 0
```

## §4 Required CFs

- `CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT` → CLOSED (with link to mig_163b)
- `CF-mig163b-WIDE-DEFINITION-DEFERRED` (if WIDE was Logan's secondary preference for sensitivity analysis) — informational note that mig_163b applied STRICT and WIDE remains as a future analytic alternative

## §5 Git workflow

- File: `qc_framework_v1/migrations/163b_any_recurrence_strict_apply_20260429.sql`
- Commit: `qc: mig_163b ANY-RECURRENCE redefined STRICT (Logan-ratified manuscript definition)`
- Push.

## §6 Out of scope

- Do NOT modify any other PM column.
- Do NOT modify `biochemical_recurrence_flag`, `structural_recurrence_flag`, `distant_mets_proxy*` — those are intentionally distinct concepts (suspicion proxies, not endpoint).
- Do NOT modify `recurrence_flag_v2`, `recurrence_flag_scoring`, `rec_structural_flag` — separate analytic flags.
- Do NOT touch the canonical_recurrence_v1 table itself.
- Do NOT apply on MD; ship SQL only — Cowork applies after Logan's ratification.

## §7 Reminder: STOP if not ratified

If Logan has not explicitly chosen STRICT (or HYBRID) at the time this lane runs, the agent must:
1. Author the prompt SQL anyway (in case Logan ratifies later)
2. Add a header banner: `-- STATUS: AWAITING LOGAN RATIFICATION — DO NOT APPLY UNTIL CONFIRMED`
3. Commit with message `qc: mig_163b ANY-RECURRENCE STRICT apply (PENDING ratification)`
4. Push.
