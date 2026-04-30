-- mig_210 — ChatGPT P7 single-row data-correction: research_id 610 CPM first_surgery_date 1945→2004
-- Batch_id: mig_210_chatgpt_p7_rid_610_first_surgery_date_fix_20260430
-- Trigger: ChatGPT review flagged canonical_patient_master research_id 610 with first_surgery_date='1945-07-13'.
-- Investigation:
--   - canonical_operative_events_v1.resolved_surgery_date for rid 610 = '2004-07-13' (same month-day, year off by 59)
--   - canonical_psh_events_v1: 0 rows for rid 610 (no historical 1945 surgery to anchor on)
--   - System-wide check: rid 610 is the ONLY CPM row with pre-1990 first_surgery_date (1/10,871)
-- Conclusion: Pre-`reference_2digit_year_convention` (Logan ratified 2026-04-27) parsing failure where
--   '04' was incorrectly interpreted as '1945' instead of '2004'. Single-row corruption survived
--   subsequent rebuilds. Operative event is the source of truth.
-- Database: thyroid_canonical_publication_v1_0
-- COWORK-DIRECT APPLY (Logan-authorized 2026-04-30); single-row UPDATE with pre-snapshot.

USE thyroid_canonical_publication_v1_0;

-- §A pre-snapshot rid 610 row only (full PM snapshot would be 10,871 rows × 1,630 cols)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig210_rid610_20260430 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig210_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE research_id=610;

-- §B UPDATE — narrowly scoped to the single corrupted row
UPDATE main.canonical_patient_master
SET first_surgery_date = DATE '2004-07-13'
WHERE research_id=610 AND first_surgery_date = DATE '1945-07-13';

-- §C provenance
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_210_chatgpt_p7_rid_610_first_surgery_date_fix_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'cpm_rid610_first_surgery_date_1945_07_13_to_2004_07_13_match_operative_event',
   'CHATGPT-P7-RID610-PRE-1990-FIRST-SURGERY-DATE',
   'isolated_single_row_corruption_no_other_pre_1990_first_surgery_dates',
   '1_row_update_pre_snapshot_in_archive_pub_v1_0_canonical_patient_master_pre_mig210_rid610_20260430',
   'none');

-- §D verify (post-apply)
SELECT 'rid_610_post_fix' AS check_name, research_id, first_surgery_date FROM main.canonical_patient_master WHERE research_id=610;
SELECT 'pre_1990_count_post_fix' AS check_name, COUNT(*) AS n FROM main.canonical_patient_master WHERE first_surgery_date < DATE '1990-01-01';
