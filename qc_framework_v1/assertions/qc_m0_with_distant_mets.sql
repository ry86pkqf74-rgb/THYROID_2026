-- qc_m0_with_distant_mets — M0 staging but distant-mets findings in imaging tables
DELETE FROM `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
WHERE assertion_id = 'qc_m0_with_distant_mets';

INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1`
  (assertion_id, research_id, event_date, detail, detected_at)
WITH staged AS (
  SELECT research_id, surgery_date, histology_1_m_stage_ajcc8
  FROM `thyroid-canonical-pub-2026.pub_canonical.tumor_pathology`
  WHERE histology_1_m_stage_ajcc8 = 'M0'
),
distant_imaging AS (
  -- CT/MRI/NM with any distant disease indicator
  SELECT research_id, date_of_exam AS event_date,
         CONCAT('ct: ', COALESCE(thyroid_other_abnormality, ''), ' lymph_node_details: ',
                COALESCE(lymph_node_details, '')) AS finding
  FROM `thyroid-canonical-pub-2026.pub_canonical.ct_imaging`
  WHERE REGEXP_CONTAINS(LOWER(COALESCE(original_report, '')),
                        r'pulmonary metast|lung metast|bone metast|hepatic metast|brain metast|distant metast')
)
SELECT
  'qc_m0_with_distant_mets' AS assertion_id,
  s.research_id,
  d.event_date,
  CONCAT('m_stage=M0 but imaging on ', CAST(d.event_date AS STRING), ' shows: ', d.finding) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM staged s
INNER JOIN distant_imaging d USING (research_id);
