# Cursor Composer Dispatch — mig_257: Repair followup_years for deceased patients

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_257 — 100 deceased patients have `followup_years > overall_survival_years`. Followup cannot extend past death; either followup_years was sourced from a non-clinical contact (research-staff outreach post-mortem), or the survival/death-date pair is wrong.
**Recommended agent:** **Cursor Composer** — matches the mig_101 stale-rollup-after-events-repair pattern (memory: `project_path_gland_family_complete_2026-04-28.md`).
**Estimated runtime:** 45–60 min
**Triggered by:** Snowflake validation Prompt 3.
**Severity:** MEDIUM. Survival KM estimates and any time-to-event manuscript.
**Opens carry-forward:** CF-mig257-FU-POST-DEATH.

---

## §0 — First message to paste into Cursor Composer

> mig_257 dispatch. 100 deceased patients have followup_years > overall_survival_years. Per the mig_101 pattern, this is a stale-rollup; the rule is `followup_years = MIN(followup_years, overall_survival_years)` for deceased patients. Pre-snapshot to archive_pub_v1_0; apply UPDATE; verify Snowflake count drops to 0.

---

## §1 — Distribution of the gap

| GAP_BUCKET | N | MEAN_GAP_YRS |
| --- | --- | --- |
| 1-3yr_diff | 1 | 1.17 |
| <1yr_diff | 99 | 0.01 |


If the gap is mostly <1 year, this is likely a date-rounding artifact between death-date and last-followup-contact. If >3yr is common, an upstream date-encoding bug is likely.

## §2 — Apply (rule = clamp followup to survival for deceased)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig257_20260501 AS
SELECT research_id, followup_years, overall_survival_years, death_occurred, vital_status
FROM main.canonical_patient_master
WHERE death_occurred = TRUE AND followup_years > overall_survival_years;

-- Apply
UPDATE main.canonical_patient_master
SET followup_years = overall_survival_years
WHERE death_occurred = TRUE AND followup_years > overall_survival_years;

-- Registry signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_257', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
        'Followup_years clamped to overall_survival_years for 100 deceased patients (followup-post-death repair).');
```

## §3 — Re-verify on Snowflake

```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE DEATH_OCCURRED = TRUE AND FOLLOWUP_YEARS > OVERALL_SURVIVAL_YEARS;
```
(Should be 0.)

## §4 — Carry-forwards
- CF-mig257-FU-POST-DEATH (closed)
- CF-mig257-DATE-ENCODING (open if §1 shows >3yr-gap dominance — root cause investigation)

## §5 — Surgical git add paths
```
scripts/output/mig_257_apply_log.txt
```
