-- Analytic FHIR export (MotherDuck main)
-- Prerequisite: scripts/sql/139_specimen_identity_layer_ddl.sql (specimen_master_v1 / focus / xref / qa).
--
-- Genomic assay binding lives in scripts/sql/140_specimen_genomics_binding_ddl.sql
-- (materialized by scripts/140_md_specimen_genomics_binding.py; orchestrated from
-- scripts/138_md_specimen_fhir_layer.py after identity + this FHIR tail).

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
