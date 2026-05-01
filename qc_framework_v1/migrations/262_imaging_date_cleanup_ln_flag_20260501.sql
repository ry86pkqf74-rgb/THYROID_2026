-- mig_262 — Imaging exam_date outliers + LN suspicious rollup (canonical US v2 VIEWs)
--
-- scripts/mig_262_imaging_date_ln_flag.py (MotherDuck RW)
--
-- LN leg: signoff_migration.mig_id = 'mig_262' — script 366 LN heuristic + 366/367 VIEWs.
--
-- Imaging leg A (publication, default when raw table missing):
--   Archive + UPDATE main.imaging_exam_master_v1 and main.canonical_us_nodule_v2
--   rid 12048 YEAR=202 → DATE '2002-08-29'; rid 10511 YEAR=3022 → DATE '2022-03-03'.
--   signoff_migration.mig_id = 'mig_262_imaging'; then 366/367 --commit.
--
-- Imaging leg B (optional raw ingest):
--   Archive on ...raw_imaging_12_slots_v1_pre_mig262_20260501 + UPDATE that table
--   if main.raw_imaging_12_slots_v1 exists (multimodule script 50).
--
-- NULL bulk recovery (~2,050 rows on main.imaging_exam_master_v1): CF-mig262-NULL-DATE-RECOVERY
-- remains open — publication DB has no main.raw_imaging_12_slots_v1; legacy exam_id join recovers 0.
-- See studies/mig_262_null_date_recovery_20260501/README.md and scripts/mig_262_null_date_probe.py.

SELECT 1 WHERE FALSE;
