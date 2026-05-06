-- Mirror of Desktop migration: mig_082_mig004_vc_finding_source_20260506.sql
-- Authority: /Users/loganglosser/Desktop/Thyroid Motherduck To GC migration/bq_migrations/mig_082_mig004_vc_finding_source_20260506.sql
-- migration_id: mig_082_mig004_vc_finding_source_20260506
-- description: MIG-004 — rollup VC / RLN attribution source onto canonical_patient_master
-- DFL: DFL-20260506-082
-- =============================================================================

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.canonical_patient_master_vc_source_v1` AS
WITH src AS (
  SELECT
    research_id,
    COALESCE(mri_vocal_cords_described, FALSE)
      OR (mri_vocal_cords_normal IS NOT NULL) AS has_mri_vc,
    COALESCE(proc_nlp_laryngoscopy, FALSE)
      OR (
        ops_preop_laryngoscopy IS NOT NULL
        AND LENGTH(TRIM(ops_preop_laryngoscopy)) > 0
      ) AS has_laryngoscopy,
    COALESCE(op_nlp_rln_finding, FALSE)
      OR COALESCE(op_rln_monitoring_any, FALSE)
      OR COALESCE(syn_io_rln_monitoring, FALSE) AS has_op_rln,
    (COALESCE(nsqip_rln_injury_flag, 0) = 1)
      OR REGEXP_CONTAINS(
        LOWER(TRIM(COALESCE(nsqip_rln_injury, ''))),
        r'^yes'
      ) AS has_nsqip,
    proc_nlp_laryngoscopy_date AS laryngo_dt,
    op_nlp_rln_finding_date AS op_dt,
    mri_first_date AS mri_dt
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
),
ranked AS (
  SELECT
    research_id,
    ARRAY(
      SELECT AS STRUCT label AS source, d AS dt
      FROM UNNEST([
        STRUCT(has_laryngoscopy AS h, 'laryngoscopy' AS label, laryngo_dt AS d),
        STRUCT(has_mri_vc AS h, 'mri_vocal_cords' AS label, mri_dt AS d),
        STRUCT(has_op_rln AS h, 'operative_rln' AS label, op_dt AS d),
        STRUCT(has_nsqip AS h, 'nsqip_attribution' AS label, CAST(NULL AS DATE) AS d)
      ])
      WHERE h
    ) AS sources
  FROM src
)
SELECT
  research_id,
  COALESCE(
    (
      SELECT s.source
      FROM UNNEST(sources) AS s
      ORDER BY s.dt NULLS LAST, s.source
      LIMIT 1
    ),
    'none'
  ) AS vc_finding_source_first,
  ARRAY(
    SELECT s.source
    FROM UNNEST(sources) AS s
    ORDER BY s.dt NULLS LAST, s.source
  ) AS vc_finding_source_set,
  CASE
    WHEN ARRAY_LENGTH(sources) = 0 THEN 'none'
    WHEN ARRAY_LENGTH(sources) = 1 THEN 'single_source'
    ELSE 'concordant_multi'
  END AS vc_finding_source_concordance
FROM ranked;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS vc_finding_source_first STRING;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS vc_finding_source_set ARRAY<STRING>;

ALTER TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`
ADD COLUMN IF NOT EXISTS vc_finding_source_concordance STRING;

UPDATE `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` c
SET
  vc_finding_source_first = s.vc_finding_source_first,
  vc_finding_source_set = s.vc_finding_source_set,
  vc_finding_source_concordance = s.vc_finding_source_concordance
FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_patient_master_vc_source_v1` s
WHERE c.research_id = s.research_id;
