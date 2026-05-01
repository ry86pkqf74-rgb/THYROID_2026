# Cursor Composer Dispatch — mig_256: 6 benign patients flagged with recurrence

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_256 — 6 patients have `is_malignant = FALSE` but `any_recurrence_flag = TRUE`. By construction, benign disease cannot recur (it was never cancer). Either malignancy was upstaged at recurrence, or the recurrence flag is a false positive.
**Recommended agent:** **Cursor Composer** with explicit case-by-case review (N=6 is small enough for direct adjudication).
**Estimated runtime:** 30–45 min
**Triggered by:** Snowflake validation Prompt 3.
**Severity:** LOW (small N) but high data-quality optics.
**Opens carry-forward:** CF-mig256-BENIGN-RECUR-RECONCILE.

---

## §0 — First message to paste into Cursor Composer

> mig_256 dispatch. The 6 patients in §1 each need clinical adjudication — pull each `research_id` against `canonical_recurrence_events_v1`, `canonical_path_*_events_v1`, `clinical_notes_long`, and the path_synoptics. Decide per case: (A) was a malignancy event missed at index, mark `is_malignant=TRUE`; or (B) is the recurrence flag spurious, set `any_recurrence_flag=FALSE`. Surface table to Logan for sign-off before any UPDATE.

---

## §1 — Patient list

| RESEARCH_ID | HISTOLOGY_FINAL | IS_MALIGNANT | ANY_RECURRENCE_FLAG | TIME_TO_RECURRENCE_DAYS | FIRST_SURGERY_DATE | AJCC8_STAGE_GROUP | AJCC8_T_STAGE |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1262 |  | False | True | 204 | 2008-10-29 |  |  |
| 1927 |  | False | True | 3 | 2010-01-12 |  |  |
| 449 |  | False | True | 3559 | 2003-08-19 |  |  |
| 4563 |  | False | True | 2284 | 2014-09-08 |  |  |
| 6089 |  | False | True | 853 | 2017-06-27 |  |  |
| 8271 |  | False | True | 675 | 2020-08-11 |  |  |


## §2 — Per-patient probe template

For each `research_id` above:
```sql
-- Path events
SELECT * FROM main.canonical_path_malignant_events_v1 WHERE research_id = '<RID>';
SELECT * FROM main.canonical_path_gland_events_v1 WHERE research_id = '<RID>';

-- Recurrence events (the source of the flag)
SELECT * FROM main.canonical_recurrence_events_v1 WHERE research_id = '<RID>';

-- Note text — radiology / oncology follow-up
SELECT note_date, note_type, content
FROM main.clinical_notes_long
WHERE research_id = '<RID>'
  AND note_date >= (SELECT first_surgery_date FROM main.canonical_patient_master WHERE research_id = '<RID>')
ORDER BY note_date LIMIT 50;
```

## §3 — Apply

After per-case disposition, batch the UPDATEs:
```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig256_20260501 AS
SELECT research_id, is_malignant, any_recurrence_flag
FROM main.canonical_patient_master
WHERE research_id IN (<the 6 RIDs>);

-- Apply per disposition (example: 4 are upstage to malignant, 2 are spurious-flag)
-- (concrete UPDATEs to be drafted by Cursor after adjudication)
```

## §4 — Re-verify on Snowflake

```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = FALSE AND ANY_RECURRENCE_FLAG = TRUE;
```
(Should be 0.)

## §5 — Carry-forwards
- CF-mig256-BENIGN-RECUR-RECONCILE (closed)

## §6 — Surgical git add paths
```
scripts/output/mig_256_per_patient_disposition.csv
scripts/output/mig_256_apply_log.txt
```
