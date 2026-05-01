-- mig_262 — Imaging exam_date outliers + LN suspicious rollup (canonical US v2 VIEWs)
--
-- Mechanical execution is scripted: scripts/mig_262_imaging_date_ln_flag.py
-- (MotherDuck RW). Summary:
--   • Archive selective rows on "Thyroid 2026 UPdated".archive_pub_v1_0
--     .raw_imaging_12_slots_v1_pre_mig262_20260501
--   • UPDATE raw_imaging_12_slots_v1 exam_date for rid 12048 (pre-1990) and 10511
--     (post-2030) per 2-digit-year / century OCR convention.
--   • Re-define n_abnormal_us_ln_on_exam in scripts/366_canonical_us_exam_master_v2.py,
--     then --commit Script 366 (exam master VIEW) + Script 367 (patient master VIEW).
--   • signoff_migration.mig_id = 'mig_262'
--
-- NULL bulk recovery (~2k rows): carry-forward CF-mig262-NULL-DATE-RECOVERY if needed.

SELECT 1 WHERE FALSE;
