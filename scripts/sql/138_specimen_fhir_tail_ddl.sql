-- Genomic assay binding + analytic FHIR export (MotherDuck main)
-- Prerequisite: scripts/sql/139_specimen_identity_layer_ddl.sql (specimen_master_v1 / focus / xref / qa).

-- ═══════════════════════════════════════════════════════════════════════════
-- main.specimen_genomic_assay_v1 — bind molecular episodes via existing v3 linkage
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.specimen_genomic_assay_v1 AS
WITH mol AS (
  SELECT
    CAST(research_id AS BIGINT) AS research_id,
    CAST(molecular_episode_id AS BIGINT) AS molecular_episode_id,
    CAST(platform AS VARCHAR) AS platform,
    test_date_native
  FROM main.molecular_test_episode_v2
),
fm AS (
  SELECT
    research_id,
    molecular_episode_id,
    fna_episode_id,
    linkage_confidence_tier,
    linkage_score,
    score_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, molecular_episode_id
      ORDER BY score_rank NULLS LAST, linkage_score DESC NULLS LAST
    ) AS _rk
  FROM main.fna_molecular_linkage_v3
),
fm1 AS (SELECT * FROM fm WHERE _rk = 1),
ps AS (
  SELECT
    research_id,
    preop_episode_id,
    surgery_episode_id,
    linkage_confidence_tier AS preop_tier,
    score_rank AS preop_rank,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, preop_episode_id
      ORDER BY score_rank NULLS LAST
    ) AS _pr
  FROM main.preop_surgery_linkage_v3
),
ps1 AS (SELECT * FROM ps WHERE _pr = 1),
sp_agg AS (
  SELECT
    research_id,
    surgery_episode_id,
    min(specimen_id) AS specimen_id,
    min(specimen_focus_id) AS specimen_focus_id
  FROM main.specimen_tumor_focus_v1
  GROUP BY research_id, surgery_episode_id
),
bound AS (
  SELECT
    m.research_id,
    m.molecular_episode_id,
    m.platform,
    m.test_date_native,
    fm1.fna_episode_id,
    fm1.linkage_confidence_tier AS fm_tier,
    ps1.surgery_episode_id,
    ps1.preop_tier,
    sp.specimen_id,
    sp.specimen_focus_id,
    CASE
      WHEN sp.specimen_focus_id IS NOT NULL AND fm1.linkage_confidence_tier IN ('exact_match', 'high_confidence')
        THEN 'A_exact_high'
      WHEN sp.specimen_id IS NOT NULL AND fm1.linkage_confidence_tier IS NOT NULL
        THEN 'B_specimen_only'
      WHEN fm1.research_id IS NULL THEN 'D_unlinked'
      ELSE 'C_review'
    END AS binding_confidence_tier,
    (sp.specimen_focus_id IS NULL OR fm1.linkage_confidence_tier NOT IN ('exact_match', 'high_confidence'))
      AS review_flag
  FROM mol m
  LEFT JOIN fm1
    ON m.research_id = fm1.research_id
   AND m.molecular_episode_id = fm1.molecular_episode_id
  LEFT JOIN ps1
    ON fm1.research_id = ps1.research_id
   AND fm1.fna_episode_id = ps1.preop_episode_id
  LEFT JOIN sp_agg sp
    ON sp.research_id = ps1.research_id
   AND COALESCE(CAST(sp.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
)
SELECT
  ('sga_' || sha256(concat_ws(
    '|', CAST(research_id AS VARCHAR), CAST(molecular_episode_id AS VARCHAR), 'molecular_test_episode_v2'
  ))) AS genomic_assay_id,
  research_id,
  molecular_episode_id,
  platform,
  test_date_native,
  fna_episode_id,
  surgery_episode_id,
  specimen_id,
  specimen_focus_id,
  fm_tier,
  preop_tier,
  binding_confidence_tier,
  review_flag,
  'molecular_test_episode_v2+fna_molecular_linkage_v3+preop_surgery_linkage_v3'::VARCHAR AS binding_chain,
  current_timestamp AS materialized_at
FROM bound;

-- ═══════════════════════════════════════════════════════════════════════════
-- Analytic FHIR export (de-identified; NOT for clinical exchange)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.fhir_patient_deid_map_v1 AS
SELECT DISTINCT
  research_id,
  ('Patient/' || substring(sha256(concat('THYROID2026FHIR|', cast(research_id AS VARCHAR))), 1, 16))
    AS patient_fhir_id
FROM main.specimen_master_v1;

CREATE OR REPLACE TABLE main.fhir_specimen_v1 AS
SELECT
  ('Specimen/' || substring(specimen_fingerprint_sha256, 1, 16)) AS fhir_id,
  pm.patient_fhir_id,
  s.specimen_id,
  json_object(
    'resourceType', 'Specimen',
    'id', substring(s.specimen_fingerprint_sha256, 1, 16),
    'identifier', json_array(
      json_object(
        'system', 'urn:oid:thyroid2026:specimen-fingerprint',
        'value', s.specimen_fingerprint_sha256
      )
    ),
    'type', json_object(
      'text', s.specimen_role
    ),
    'subject', json_object('reference', 'Patient/' || pm.patient_fhir_id),
    'receivedTime', CASE
      WHEN regexp_matches(COALESCE(s.procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN s.procedure_date_day || 'T00:00:00Z'
      ELSE NULL
    END
  ) AS resource_json,
  current_timestamp AS built_at
FROM main.specimen_master_v1 s
JOIN main.fhir_patient_deid_map_v1 pm USING (research_id);

CREATE OR REPLACE TABLE main.fhir_procedure_collection_v1 AS
SELECT
  ('Procedure/' || substring(sha256(concat('proc|', specimen_id)), 1, 16)) AS fhir_id,
  pm.patient_fhir_id,
  s.specimen_id,
  json_object(
    'resourceType', 'Procedure',
    'id', substring(sha256(concat('proc|', s.specimen_id)), 1, 16),
    'subject', json_object('reference', 'Patient/' || pm.patient_fhir_id),
    'code', json_object('text', 'Thyroid specimen collection'),
    'performedDateTime', CASE
      WHEN regexp_matches(COALESCE(s.procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN s.procedure_date_day || 'T00:00:00Z'
      ELSE NULL
    END
  ) AS resource_json,
  current_timestamp AS built_at
FROM main.specimen_master_v1 s
JOIN main.fhir_patient_deid_map_v1 pm USING (research_id);

CREATE OR REPLACE TABLE main.fhir_encounter_v1 AS
SELECT
  ('Encounter/' || substring(sha256(concat('enc|', specimen_id)), 1, 16)) AS fhir_id,
  pm.patient_fhir_id,
  s.specimen_id,
  json_object(
    'resourceType', 'Encounter',
    'id', substring(sha256(concat('enc|', s.specimen_id)), 1, 16),
    'status', 'unknown',
    'class', json_object(
      'system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
      'code', 'AMB',
      'display', 'ambulatory'
    ),
    'subject', json_object('reference', 'Patient/' || pm.patient_fhir_id),
    'period', json_object(
      'start', CASE
        WHEN regexp_matches(COALESCE(s.procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
        THEN s.procedure_date_day || 'T00:00:00Z'
        ELSE NULL
      END
    )
  ) AS resource_json,
  current_timestamp AS built_at
FROM main.specimen_master_v1 s
JOIN main.fhir_patient_deid_map_v1 pm USING (research_id);

CREATE OR REPLACE TABLE main.fhir_episode_of_care_v1 AS
SELECT
  ('EpisodeOfCare/' || substring(sha256(concat('eoc|', coalesce(cast(surgery_episode_id AS VARCHAR), specimen_id))), 1, 16)) AS fhir_id,
  pm.patient_fhir_id,
  s.specimen_id,
  s.surgery_episode_id,
  json_object(
    'resourceType', 'EpisodeOfCare',
    'id', substring(sha256(concat('eoc|', coalesce(cast(s.surgery_episode_id AS VARCHAR), s.specimen_id))), 1, 16),
    'status', 'active',
    'type', json_array(json_object('text', 'Thyroid cancer care episode (analytic stub)')),
    'patient', json_object('reference', 'Patient/' || pm.patient_fhir_id)
  ) AS resource_json,
  current_timestamp AS built_at
FROM main.specimen_master_v1 s
JOIN main.fhir_patient_deid_map_v1 pm USING (research_id);

CREATE OR REPLACE TABLE main.fhir_bundle_specimen_export_v1 AS
SELECT
  row_number() OVER () AS bundle_ix,
  json_object(
    'resourceType', 'Bundle',
    'type', 'collection',
    'timestamp', cast(current_timestamp AS VARCHAR),
    'entry', json_array(
      json_object('resource', fs.resource_json),
      json_object('resource', fp.resource_json),
      json_object('resource', fe.resource_json),
      json_object('resource', fo.resource_json)
    )
  ) AS bundle_json,
  fs.specimen_id,
  current_timestamp AS built_at
FROM main.fhir_specimen_v1 fs
JOIN main.fhir_procedure_collection_v1 fp ON fs.specimen_id = fp.specimen_id
JOIN main.fhir_encounter_v1 fe ON fs.specimen_id = fe.specimen_id
JOIN main.fhir_episode_of_care_v1 fo ON fs.specimen_id = fo.specimen_id;
