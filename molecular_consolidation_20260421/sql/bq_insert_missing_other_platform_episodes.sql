-- BigQuery: INSERT molecular episodes missing from canonical_molecular_genetics_v2
-- (platform='Other' with substantive episode text or driver-positive flags).
--
-- Run ONLY after validating column names match live
-- `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
-- (NOT NULL columns, defaults). Then re-run the genetics parser / builder on new rows.
--
-- Verification:
--   SELECT COUNT(*) FROM ... WHERE ingestion_source = 'retroactive_insert_missing_other_platform';

INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2` (
  research_id,
  molecular_episode_id,
  platform,
  platform_raw,
  overall_result_class,
  braf_flag,
  ras_flag,
  tert_flag,
  ntrk_flag,
  eif1ax_flag,
  tp53_flag,
  pax8_pparg_flag,
  cna_flag,
  fusion_flag,
  loh_flag,
  alk_flag,
  high_risk_marker_flag,
  inadequate_flag,
  cancelled_flag,
  gene_mutations_status,
  gene_fusions_status,
  parse_status,
  parse_status_v2,
  ingestion_source,
  built_at
)
SELECT
  e.research_id,
  e.molecular_episode_id,
  e.platform,
  e.platform,
  e.overall_result_class,
  COALESCE(e.braf_flag, FALSE),
  COALESCE(e.ras_flag, FALSE),
  COALESCE(e.tert_flag, FALSE),
  COALESCE(e.ntrk_flag, FALSE),
  COALESCE(e.eif1ax_flag, FALSE),
  COALESCE(e.tp53_flag, FALSE),
  COALESCE(e.pax8_pparg_flag, FALSE),
  COALESCE(e.cna_flag, FALSE),
  COALESCE(e.fusion_flag, FALSE),
  COALESCE(e.loh_flag, FALSE),
  COALESCE(e.alk_flag, FALSE),
  COALESCE(e.high_risk_marker_flag, FALSE),
  COALESCE(e.inadequate_flag, FALSE),
  COALESCE(e.cancelled_flag, FALSE),
  CASE
    WHEN COALESCE(e.braf_flag, FALSE)
      OR COALESCE(e.ras_flag, FALSE)
      OR COALESCE(e.tert_flag, FALSE)
      OR COALESCE(e.eif1ax_flag, FALSE)
      OR COALESCE(e.tp53_flag, FALSE)
      THEN 'Positive'
    ELSE NULL
  END,
  CAST(NULL AS STRING),
  'minimal',
  'minimal',
  'retroactive_insert_missing_other_platform',
  CURRENT_TIMESTAMP()
FROM `thyroid-canonical-pub-2026.pub_canonical.molecular_test_episode_v2` e
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2` c
  ON e.research_id = c.research_id
 AND e.molecular_episode_id = c.molecular_episode_id
WHERE c.molecular_episode_id IS NULL
  AND (
    (e.detailed_findings_raw IS NOT NULL AND LENGTH(e.detailed_findings_raw) > 50)
    OR (e.mutation IS NOT NULL AND LENGTH(e.mutation) > 3)
    OR e.braf_flag = TRUE
    OR e.ras_flag = TRUE
    OR e.tert_flag = TRUE
    OR e.overall_result_class IN ('positive', 'suspicious')
  );
