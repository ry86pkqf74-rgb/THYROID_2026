# Cursor Composer Dispatch — mig_275: M038 surgical-complexity column scaffold

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_275 — M038 Massive Goiter manuscript needs surgical-complexity proxies (operative time, blood loss, hospital LOS) as outcomes by weight strata. Discover which canonical tables hold this data, then either populate CPM with rolled-up columns or build a manuscript-side view that joins them.
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for column discovery + roll-up rule design → **Cursor Composer** for apply.
**Estimated runtime:** 90–120 min
**Triggered by:** Manuscript completion roadmap — M038 Tier 6 #2.
**Severity:** MED for M038 manuscript.

---

## §0 — First message to paste into Cursor Chat

> mig_275 discovery + design pass. M038 Massive Goiter manuscript needs operative time, estimated blood loss (EBL), and hospital length-of-stay (LOS) as patient-level columns to put in Table 1 + a complications-vs-weight regression model. Run §1 probes to find which tables hold this data. If found in `canonical_operative_events_v1` or `canonical_complications_events_v1` or NSQIP-derived columns on CPM, surface to Logan with a roll-up rule (e.g., MAX op time across multiple ops? sum or single op?). Then move to Composer to apply.

---

## §1 — Discovery probes

```sql
-- 1a. Operative time / blood loss / LOS columns on CPM
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND (LOWER(column_name) LIKE '%op%time%' OR LOWER(column_name) LIKE '%operative%time%'
       OR LOWER(column_name) LIKE '%blood_loss%' OR LOWER(column_name) LIKE '%ebl%'
       OR LOWER(column_name) LIKE '%los%' OR LOWER(column_name) LIKE '%length%stay%'
       OR LOWER(column_name) LIKE '%hospital%')
ORDER BY column_name;

-- 1b. Same columns on canonical_operative_events_v1
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_operative_events_v1'
  AND (LOWER(column_name) LIKE '%time%' OR LOWER(column_name) LIKE '%blood_loss%'
       OR LOWER(column_name) LIKE '%ebl%' OR LOWER(column_name) LIKE '%los%'
       OR LOWER(column_name) LIKE '%length%' OR LOWER(column_name) LIKE '%duration%'
       OR LOWER(column_name) LIKE '%hospital%')
ORDER BY column_name;

-- 1c. NSQIP-derived columns (Logan mentioned NSQIP_WEIGHT_LBS in earlier discovery)
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND LOWER(column_name) LIKE 'nsqip%'
ORDER BY column_name;

-- 1d. Coverage check — how many patients with non-null op time / EBL / LOS?
-- (Once columns are identified)
SELECT COUNT(*) AS n_total,
       COUNT_IF(<op_time_col> IS NOT NULL) AS n_with_op_time,
       COUNT_IF(<ebl_col> IS NOT NULL) AS n_with_ebl,
       COUNT_IF(<los_col> IS NOT NULL) AS n_with_los
FROM main.canonical_patient_master;
```

## §2 — Roll-up design (after discovery)

If data lives in `canonical_operative_events_v1` (multiple events per patient possible):
- **Op time:** SUM across all operative events on the same surgery date (or MAX if "longest single operation" is preferred clinically). Defer to Logan.
- **EBL:** SUM across same-date events.
- **LOS:** Use admission-to-discharge for the index-surgery hospitalization. May need linking back to admission events.

Export: add `cpm_op_time_min`, `cpm_ebl_ml`, `cpm_los_days` to canonical_patient_master.

## §3 — Apply

```sql
-- 3a. Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig275_20260502 AS
SELECT research_id, cpm_op_time_min, cpm_ebl_ml, cpm_los_days
FROM main.canonical_patient_master;

-- 3b. Add columns if missing
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS cpm_op_time_min DOUBLE,
  ADD COLUMN IF NOT EXISTS cpm_ebl_ml DOUBLE,
  ADD COLUMN IF NOT EXISTS cpm_los_days DOUBLE;

-- 3c. Populate from canonical_operative_events_v1
-- (concrete SQL after Logan ratifies roll-up rule)
UPDATE main.canonical_patient_master cpm
SET cpm_op_time_min = (
  SELECT SUM(op_time_min) FROM main.canonical_operative_events_v1 e
  WHERE e.research_id = cpm.research_id
    AND e.event_date = cpm.first_surgery_date
);
-- Repeat for EBL and LOS

-- 3d. Signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_275', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Added cpm_op_time_min / cpm_ebl_ml / cpm_los_days to canonical_patient_master via roll-up from canonical_operative_events_v1. Used by M038 Massive Goiter manuscript Table 1 + regression model.');
```

## §4 — Snowflake side

After mig_275 lands, re-export CPM → Snowflake and re-run M038 Table 1 to add the surgical-complexity rows.

## §5 — Surgical git add
```
qc_framework_v1/migrations/275_surgical_complexity_columns_20260502.sql
scripts/output/mig_275_apply_log.txt
```

## §6 — Carry-forwards
- CF-mig275-NSQIP-LIMITATION (if data only in NSQIP subset, manuscript footnote about coverage gap)
- CF-mig275-MULTI-OP-ROLLUP-RULE (document the chosen rule for reproducibility)
