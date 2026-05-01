# Cursor Composer Dispatch — mig_255: Reconcile recurrence flag vs time-to-recurrence

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_255 — 740 patients have `any_recurrence_flag = FALSE` but `time_to_recurrence_days IS NOT NULL`. Either the flag is wrong (recurrence happened, days is correct) or `days` is a stale/spurious value that should be NULL.
**Recommended agent:** **Cursor Composer** — pattern matches existing recurrence-derivation migs. The rule choice is: trust the timing column (it has more nuance) or trust the flag (it's the rolled-up assertion).
**Estimated runtime:** 45–60 min
**Triggered by:** Snowflake validation Prompt 3 (survival/recurrence integrity).
**Severity:** MEDIUM-HIGH. M044, M032, and any survival/recurrence outcome paper.
**Opens carry-forward:** CF-mig255-RECUR-FLAG-TIMING.

---

## §0 — First message to paste into Cursor Composer

> mig_255 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_255_RECUR_FLAG_TIMING_20260501.md` end-to-end. MotherDuck MCP authed. The 740 patients in question have `any_recurrence_flag=FALSE` but `time_to_recurrence_days IS NOT NULL`. Run §2 probes first; surface the upstream source of `time_to_recurrence_days` (canonical_recurrence_events_v1? a derivation in CPM?) before deciding flip-flag vs null-days.

---

## §1 — Why this lane exists

Snowflake breakdown of the 740 mismatches:

| IS_MALIGNANT | FLAG | DAYS_BUCKET | N |
| --- | --- | --- | --- |
| True | false | days_pos | 681 |
| False | false | days_pos | 53 |
| True | false | days_0 | 6 |


The rolled-up patient-level flag conflicts with the days-since-recurrence numeric. Two fixes are possible:
- **Option A:** flip `any_recurrence_flag` to TRUE when `time_to_recurrence_days IS NOT NULL`. Treats timing as truth.
- **Option B:** null out `time_to_recurrence_days` when `any_recurrence_flag = FALSE`. Treats flag as truth.
- **Option C:** trace each upstream signal — `canonical_recurrence_events_v1` event count, NLP recurrence mentions, structured radiology — and derive both fields together from a unified source.

Option C is correct per Logan's Protocol v2 (no fix in isolation; rebuild from canonical events). A and B are quick patches.

---

## §2 — Pre-task probes

```sql
-- Probe 1: do these 740 have any source events?
WITH bad AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL
)
SELECT
  COUNT(*) AS n_bad_pts,
  COUNT(DISTINCT r.research_id) AS n_with_recur_event_in_canonical
FROM bad b LEFT JOIN main.canonical_recurrence_events_v1 r USING (research_id);

-- Probe 2: time_to_recurrence_days distribution
SELECT
  CASE WHEN time_to_recurrence_days < 0 THEN 'neg'
       WHEN time_to_recurrence_days = 0 THEN 'zero'
       WHEN time_to_recurrence_days BETWEEN 1 AND 365 THEN '1-365'
       WHEN time_to_recurrence_days BETWEEN 366 AND 1825 THEN '366-1825'
       ELSE '>1825' END AS days_bucket,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- Probe 3: registry lineage for both columns
SELECT column_name, last_signed_off_mig, verification_method
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('any_recurrence_flag', 'time_to_recurrence_days');
```

Surface to Logan; pick A/B/C.

---

## §3 — Apply (after disposition)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.canonical_patient_master_pre_mig255_20260501 AS
SELECT research_id, any_recurrence_flag, time_to_recurrence_days
FROM main.canonical_patient_master
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL;

-- Option A example
UPDATE main.canonical_patient_master
SET any_recurrence_flag = TRUE
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL;
```

Verify on Snowflake post-fix:
```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE ANY_RECURRENCE_FLAG = FALSE AND TIME_TO_RECURRENCE_DAYS IS NOT NULL;
```
(Should be 0 after Option A or B; Option C may leave a small residual.)

## §4 — Carry-forwards
- CF-mig255-RECUR-FLAG-TIMING (closed by this lane)
- CF-mig255-RECUR-RESOURCING-FROM-EVENTS (open if Option C deferred)

## §5 — Surgical git add paths
```
scripts/output/mig_255_*.md
scripts/output/mig_255_pre_snapshot_log.txt
```
