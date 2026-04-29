-- Migration: 147b_nucmed_dates_retype_and_dose_reclassify_20260429.sql
-- Purpose: Two-part mig_147 cleanup applied via query_rw 2026-04-29:
--          (a) Retype nucmed_first/last_scan_with_labs VARCHAR -> DATE.
--          (b) Reclassify nucmed_cumulative_therapeutic_dose verified -> na due to 83% drift
--              vs authoritative rai_total_cumulative_dose_mci (mig_148 SSOT).
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 37b (mig_147 cleanup)

-- ============================================================
-- PART (a): VARCHAR -> DATE retype for 2 cols
-- Source format MM/DD/YY (2-digit year); per Logan-ratified reference_2digit_year_convention.md
-- 20YY rule: '06' -> 2006, '25' -> 2025.
-- 447+447 / 447+447 non-null values parseable; 0 unparseable.
-- (Pre-snapshot bundled with mig_146b: archive_pub_v1_0.canonical_patient_master_pre_mig146b_147b_dates_20260429)
-- ============================================================

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN nucmed_first_scan_with_labs
  SET DATA TYPE DATE
  USING TRY_CAST(STRPTIME(REGEXP_REPLACE(nucmed_first_scan_with_labs, '^(\d{1,2})/(\d{1,2})/(\d{2})$', '\1/\2/20\3'), '%-m/%-d/%Y') AS DATE);

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN nucmed_last_scan_with_labs
  SET DATA TYPE DATE
  USING TRY_CAST(STRPTIME(REGEXP_REPLACE(nucmed_last_scan_with_labs, '^(\d{1,2})/(\d{1,2})/(\d{2})$', '\1/\2/20\3'), '%-m/%-d/%Y') AS DATE);

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_147b (2026-04-29): VARCHAR -> DATE retype (CF-mig147-PM-NUCMED-DATE-VARCHAR CLOSED). ' ||
            'Source format MM/DD/YY (2-digit year); per Logan-ratified reference_2digit_year_convention.md ' ||
            '20YY rule applied (06 -> 2006, 25 -> 2025). All 447+447 non-null values parseable. ' ||
            'Post-retype range: 2002-10-07 to 2025-01-31. ' ||
            'Pre-snapshot in archive_pub_v1_0.canonical_patient_master_pre_mig146b_147b_dates_20260429.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    data_type = 'DATE'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('nucmed_first_scan_with_labs', 'nucmed_last_scan_with_labs');

-- ============================================================
-- PART (c) — labelled mig_147c in registry notes:
-- nucmed_cumulative_therapeutic_dose verified -> na
-- 40/48 overlapping pts (83%) have >10% drift vs rai_total_cumulative_dose_mci.
-- Avg nucmed=190 mCi, avg RAI=514 mCi (2.7x systematic gap).
-- RAI canonical (rai_treatment_episode_v2 mig_148 verified) is authoritative SSOT.
-- ============================================================

UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verification_method = 'partial_dose_signal_supplanted_by_rai_canonical_authoritative',
    notes = COALESCE(notes,'') ||
            ' | mig_147c (2026-04-29): reclassified verified -> na. CF-mig147-NUCMED-VS-RAI-DOSE-SOURCE-SPLIT ' ||
            'investigation: 40/48 overlapping pts (83%) have >10% drift between this col and ' ||
            'rai_total_cumulative_dose_mci. Avg nucmed=190 mCi, avg RAI=514 mCi (2.7x systematic gap). ' ||
            'Nucmed is partial signal (sum of dose mentions in nucmed scan reports only); RAI canonical ' ||
            'rai_treatment_episode_v2 (mig_148 verified) is the authoritative cumulative-dose SSOT. ' ||
            'Use rai_total_cumulative_dose_mci for analytic cumulative dose; this nucmed col stays in ' ||
            'PM as a per-source diagnostic signal but is no longer claimed as analytically verified.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'canonical_patient_master'
  AND column_name = 'nucmed_cumulative_therapeutic_dose';

-- End of mig_147b/c. Already applied via query_rw 2026-04-29.
