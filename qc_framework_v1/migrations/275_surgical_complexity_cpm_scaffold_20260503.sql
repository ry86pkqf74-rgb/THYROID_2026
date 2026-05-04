-- mig_275 — CPM surgical-complexity scaffold for M038 Table 1 (MotherDuck publication DB)
--
-- APPLY: .venv/bin/python scripts/mig_275_m038_surgical_complexity.py --apply
-- DRY : .venv/bin/python scripts/mig_275_m038_surgical_complexity.py --dry-run
--
-- Adds patient-level columns on main.canonical_patient_master:
--   cpm_op_time_min           — NSQIP operative duration (minutes); institutional subset (~1.3k).
--   cpm_ebl_ml                — COALESCE(index-surgery SUM(ebl_ml) from canonical_operative_events_v1,
--                                         ops_ebl_ml, op_nlp_ebl_ml). Roll-up rule CF-mig275-MULTI-OP-ROLLUP-RULE.
--   cpm_los_days              — COALESCE(nsqip_hospital_los_days, length_of_stay_days, surgical_los_days).
--   *_source VARCHAR           — provenance labels for manuscript Methods footnotes.
--
-- Archive (pre-overwrite): "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig275_20260503
--
-- Discovery (2026-05-03 live MotherDuck): canonical_operative_events_v1 carries ebl_ml only (no skin-to-skin /
-- operative duration). Coverage footnote: CF-mig275-NSQIP-LIMITATION for op time + LOS outside NSQIP linkage.
--
-- signoff_migration.mig_id = 'mig_275'

USE thyroid_canonical_publication_v1_0;

-- DDL is applied by scripts/mig_275_m038_surgical_complexity.py (ALTER … ADD COLUMN guards).
