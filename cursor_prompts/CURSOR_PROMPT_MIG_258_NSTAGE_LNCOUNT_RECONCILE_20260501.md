# Cursor Composer Dispatch — mig_258: Reconcile N-stage assertion vs LN_TOTAL_POSITIVE count

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_258 — 1,501 patients have `ajcc8_n_stage = 'N1a'` (or N1b) but `ln_total_positive` is NULL or 0. The N-staging encodes positivity that the structured count doesn't reflect. This breaks any Table 1 that summarizes both columns (e.g. M037 LN predictors).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 or GPT-5)** for the rule design pass first → **Cursor Composer** for the apply. Decision is clinical/structural: do we trust the N-stage label and forward-fill the count, or treat the count as truth and downgrade the N-stage when missing?
**Estimated runtime:** 75–90 min
**Triggered by:** Snowflake M037 Table 1 generation (round 2).
**Severity:** HIGH. Affects M037 (LN predictors), M044 (ETE — uses N-stage), and any LN-counting paper.
**Opens carry-forward:** CF-mig258-NSTAGE-LNCOUNT-RECONCILE.

---

## §0 — First message to paste into Cursor Chat (decision pass)

> mig_258 decision pass. 1,501 patients have `ajcc8_n_stage` ∈ {'N1a', 'N1b'} but `ln_total_positive` is NULL or 0. The two fields disagree at scale. Three candidate rules:
>
> **Rule A — N-stage is truth:** when N1a/b but count is NULL/0, set count=`ln_total_examined` if examined>0, else fall back to 1 (sentinel for "at least one positive"). Pro: preserves N-staging. Con: conflates "≥1" with actual count.
>
> **Rule B — count is truth:** when count=0, set N-stage to N0; when NULL but staging implies positive, leave N-stage as-is but flag `ln_count_unknown=TRUE` in a new column. Pro: structurally honest. Con: loses 1,501 N1+ patients from manuscripts that filter by N1.
>
> **Rule C — separate truth domains:** keep both as-is, add `ln_status_source` ∈ {'staging','count','both'} so manuscripts can pick. Pro: lossless. Con: every downstream query must declare which source it trusts.
>
> Run §2 probes; surface counts to Logan; pick A/B/C. Only then move to Composer.

---

## §1 — Distribution

| AJCC8_N_STAGE | COUNT_BUCKET | N |
| --- | --- | --- |
| N1a | count_0 | 7 |
| N1a | count_NULL | 1494 |
| N1a | count_pos | 1051 |
| N1b | count_NULL | 8 |
| N1b | count_pos | 75 |


## §2 — Pre-task probes

```sql
-- Probe 1: do these patients have a path-events source?
WITH disagreement AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE is_malignant = TRUE AND ajcc8_n_stage IN ('N1a', 'N1b')
    AND (ln_total_positive IS NULL OR ln_total_positive = 0)
)
SELECT
  COUNT(*) AS n_disagreement,
  COUNT_IF(ln_total_examined > 0) AS n_with_lns_examined,
  COUNT_IF(ln_total_examined IS NULL OR ln_total_examined = 0) AS n_no_lns_examined
FROM main.canonical_patient_master cpm
JOIN disagreement d USING (research_id);

-- Probe 2: which mig wrote N-stage and ln_total_positive?
SELECT column_name, last_signed_off_mig, verification_method
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('ajcc8_n_stage', 'ln_total_positive', 'ln_total_examined', 'ln_positive_flag');

-- Probe 3: any patients with both signals?
SELECT
  COUNT(*) AS n,
  ROUND(AVG(ln_total_positive), 2) AS mean_count
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_n_stage = 'N1a' AND ln_total_positive > 0;
```

## §3 — Apply (template; concrete SQL after rule pick)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig258_20260501 AS
SELECT research_id, ajcc8_n_stage, ln_total_positive, ln_total_examined, ln_positive_flag
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_n_stage IN ('N1a', 'N1b');

-- Rule C example: add ln_status_source column
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS ln_status_source VARCHAR;
UPDATE main.canonical_patient_master
SET ln_status_source = CASE
  WHEN ajcc8_n_stage IN ('N1a','N1b') AND ln_total_positive > 0 THEN 'both'
  WHEN ajcc8_n_stage IN ('N1a','N1b') THEN 'staging'
  WHEN ln_total_positive > 0 THEN 'count'
  ELSE NULL
END
WHERE is_malignant = TRUE;
```

## §4 — Re-verify on Snowflake (post-mig)

```sql
-- Rule A or B: count should be 0
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_N_STAGE IN ('N1a', 'N1b')
  AND (LN_TOTAL_POSITIVE IS NULL OR LN_TOTAL_POSITIVE = 0);

-- Rule C: ln_status_source distribution
SELECT $1:ln_status_source::VARCHAR AS src, COUNT(*) FROM CANONICAL_PATIENT_MASTER GROUP BY 1;
-- (need to re-export CPM from MD post-mig and re-CTAS the Snowflake table)
```

## §5 — Carry-forwards
- CF-mig258-NSTAGE-LNCOUNT-RECONCILE (closed by chosen rule)
- CF-mig258-MANUSCRIPT-FILTER-UPDATE (M037 + M044 view definitions update to use chosen rule)

## §6 — Surgical git add paths
```
scripts/output/mig_258_*.md
scripts/output/mig_258_pre_snapshot_log.txt
```
