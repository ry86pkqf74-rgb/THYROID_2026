-- Migration: 146b_pet_first_last_date_varchar_to_date_retype_20260429.sql
-- Purpose: Retype pet_first_date / pet_last_date VARCHAR -> DATE on canonical_patient_master.
--          Closes CF-mig146-PM-PET-FIRST-LAST-DATE-VARCHAR.
-- Trigger: Cowork verification of mig_146 found 2 of 49 cols stored as VARCHAR with format
--          'MM/DD/YYYY' instead of DATE. Calendar-only policy (feedback_clinical_dates_calendar_only.md)
--          requires DATE for clinical event dates.
-- Probe: 290 / 290 + 289 / 289 non-null values are TRY_CAST-able via STRPTIME('%-m/%-d/%Y'); 0 unparseable.
-- Author : Logan Glosser <logan.glosser@gmail.com>
-- Date   : 2026-04-29
-- Lane   : 36b (mig_146 cleanup; PM data write — pre-snapshot taken)

-- (Pre-snapshot was bundled with mig_147b; see archive_pub_v1_0.canonical_patient_master_pre_mig146b_147b_dates_20260429)

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN pet_first_date
  SET DATA TYPE DATE
  USING TRY_CAST(STRPTIME(pet_first_date, '%-m/%-d/%Y') AS DATE);

ALTER TABLE main.canonical_patient_master
  ALTER COLUMN pet_last_date
  SET DATA TYPE DATE
  USING TRY_CAST(STRPTIME(pet_last_date, '%-m/%-d/%Y') AS DATE);

-- Post-verify: 290+289 nonnull preserved, types DATE, range 2010-12-21 to 2025-02-07.

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') ||
            ' | mig_146b (2026-04-29): VARCHAR -> DATE retype (CF-mig146-PM-PET-FIRST-LAST-DATE-VARCHAR CLOSED). ' ||
            'Source format MM/DD/YYYY (4-digit year), all 290+289 non-null values parseable. ' ||
            'Pre-snapshot in archive_pub_v1_0.canonical_patient_master_pre_mig146b_147b_dates_20260429.',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    data_type = 'DATE'
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('pet_first_date', 'pet_last_date');

-- End of mig_146b. Already applied via query_rw 2026-04-29.
