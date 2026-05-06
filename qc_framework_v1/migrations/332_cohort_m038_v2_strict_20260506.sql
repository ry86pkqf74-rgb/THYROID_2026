-- =============================================================================
-- mig_332 — cohort_m038_massive_goiter_v2_strict (M038 working view)
--           pub_workspace.cohort_m038_massive_goiter_v2_strict
--
-- Date:    2026-05-06
-- Depends: mig_329 (complications_strict_v1), mig_330 (rln_signal_status_v1)
--
-- CONTEXT:
--   Adds strict-definition complication columns and BQ-canonical-enriched
--   operative-detail columns to the M038 cohort working view, without
--   altering the v1 view (preserved for backward compatibility).
--
-- VERIFY (post-apply):
--   SELECT
--     COUNTIF(comp_hypoparathyroidism_transient_strict)   AS strict_transient,    -- Expect 280
--     COUNTIF(comp_hypoparathyroidism_permanent_strict)   AS strict_permanent,    -- Expect 16
--     COUNTIF(comp_perioperative_tracheostomy_strict)     AS strict_perioperative,-- Expect 2
--     COUNTIF(rln_signal_status = 'loss_of_signal_los')   AS los_abnormal,        -- Expect 15
--     COUNTIF(rln_signal_status = 'signal_verified')      AS signal_verified      -- Expect 98
--   FROM `pub_workspace.cohort_m038_massive_goiter_v2_strict`;
-- =============================================================================

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_workspace.cohort_m038_massive_goiter_v2_strict` AS
SELECT
  m.*,
  -- mig_329 strict complications passthrough
  s.comp_hypoparathyroidism_transient_strict,
  s.comp_hypoparathyroidism_permanent_strict,
  s.comp_hypoparathyroidism_unconfirmed_evidence,
  s.comp_perioperative_tracheostomy_strict,
  s.comp_preexisting_tracheostomy,
  s.comp_late_postop_tracheostomy,
  s.comp_unanchored_tracheostomy_mention,
  -- BQ-canonical operative covariates (cohort-wide)
  cpm.op_nlp_nerve_monitoring_used,
  cpm.op_rln_monitoring_any,
  cpm.lateral_neck_dissected,
  cpm.syn_central_dissection,
  cpm.ops_nerve_stim_left,
  cpm.ops_nerve_stim_right,
  -- mig_330 signal-quality derivation
  rss.rln_signal_status,
  rss.rln_loss_of_signal_flag,
  rss.stim_left_uV,
  rss.stim_right_uV
FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m038_massive_goiter_v1` m
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.complications_strict_v1` s USING (research_id)
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` cpm USING (research_id)
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.rln_signal_status_v1` rss USING (research_id);
