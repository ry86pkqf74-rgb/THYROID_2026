-- ============================================================================
-- mig_318b signoff
-- Run AFTER independent_recompute_v4.py reports 7/7 PASS.
-- ============================================================================

-- First: sign off mig_318 (BQ DDL)
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.signoff_migration`
(mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_318',
  CURRENT_TIMESTAMP(),
  'cowork_session_2026_05_07',
  'mig_318: M048 v4 background-pathology bugfix — has_clt/has_mng/has_graves '
  're-derived from canonical_path_benign_patient_rollup_v1; expanded '
  'comorbidity panel from canonical_pmh_patient_rollup_v1; granular benign-'
  'diagnosis flags from canonical_benign_diagnosis_v1. v3 tables retained for '
  'traceability. Cascade re-fit happens in mig_318b (Cursor).'
);

-- Then: sign off mig_318b (Python pipeline re-fit)
INSERT INTO `thyroid-canonical-pub-2026.pub_signoff.signoff_migration`
(mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_318b',
  CURRENT_TIMESTAMP(),
  'cursor_composer_mig318b',
  'mig_318b: M048 v4 full recompute — M4 background-pathology step '
  'restored on corrected master (mig_318); cascade re-fit M0..M6; '
  'expanded comorbidity sensitivity arm to full PMH panel; Bethesda-'
  'stratified Model B + B-interaction re-fit; per-nodule cluster-robust '
  'Model F-Nodule re-fit with M4; 7 sensitivity arms re-fit (arm D '
  'no-CLT now non-trivial); mediation bootstrap extended to 8 mediators '
  '(adds has_mng, has_clt, has_graves) x 2 races. Manuscript regen to '
  'v1.3 in separate session.'
);
