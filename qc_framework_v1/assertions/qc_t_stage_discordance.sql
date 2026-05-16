-- qc_t_stage_discordance — reported AJCC8 T-stage disagrees with derived T-stage
-- Expected: ~207 cases per the second-pass gap analysis (canonical_ete_event_resolved_v1.t_stage_discordance_flag=TRUE)
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_t_stage_discordance';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
SELECT
  'qc_t_stage_discordance' AS assertion_id,
  research_id,
  surg_date AS event_date,
  CONCAT(
    'reported_t_stage_ajcc8=', COALESCE(reported_t_stage_ajcc8, '(null)'),
    ' derived_t_stage_ajcc8=', COALESCE(derived_t_stage_ajcc8, '(null)'),
    ' tumor_ordinal=', CAST(tumor_ordinal AS STRING)
  ) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_ete_event_resolved_v1`
WHERE t_stage_discordance_flag = TRUE;
