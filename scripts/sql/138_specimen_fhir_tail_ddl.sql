-- Analytic FHIR export (MotherDuck main) — de-identified, not for clinical exchange
-- Prerequisite: scripts/sql/139_specimen_identity_layer_ddl.sql (specimen_master_v1, focus, xref).
-- Optional enrichment: main.tumor_episode_master_v2 (oncology episode period). If absent, run fails;
--       materialization environments always include this table; tests create an empty/minimal stub.
--
-- Genomic assay: scripts/sql/140_specimen_genomics_binding_ddl.sql (script 140).

-- ═══════════════════════════════════════════════════════════════════════════
-- Patient logical id (bare); all references use "Patient/" || patient_fhir_id
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.fhir_patient_deid_map_v1 AS
SELECT DISTINCT
  research_id,
  substring(sha256(concat('THYROID2026FHIR|', cast(research_id AS VARCHAR))), 1, 16) AS patient_fhir_id
FROM main.specimen_master_v1;

-- ═══════════════════════════════════════════════════════════════════════════
-- Context: procedure text from synoptic link; episode dates from tumor_episode_master_v2
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.fhir_specimen_v1 AS
WITH focus_ctx AS (
  SELECT
    f.specimen_id,
    max(f.site_text) AS site_text_any
  FROM main.specimen_tumor_focus_v1 f
  LEFT JOIN main._specimen_path_surgery_link_v1 l
    ON f.research_id = l.research_id
   AND f.synoptic_row_ix = l.synoptic_row_ix
   AND COALESCE(cast(f.tumor_index AS VARCHAR), '') = COALESCE(cast(l.tumor_index AS VARCHAR), '')
  GROUP BY f.specimen_id
),
base AS (
  SELECT
    s.specimen_id,
    s.specimen_fingerprint_sha256,
    s.research_id,
    s.procedure_date_day,
    s.specimen_role,
    s.surgery_episode_id,
    pm.patient_fhir_id,
    fc.site_text_any,
    substring(s.specimen_fingerprint_sha256, 1, 16) AS spec_id_short,
    substring(sha256(concat('proc|', s.specimen_id)), 1, 16) AS proc_id_short,
    substring(sha256(concat('enc|', s.specimen_id)), 1, 16) AS enc_id_short,
    substring(sha256(concat(
      'eoc|', cast(s.research_id AS VARCHAR), '|',
      coalesce(cast(s.surgery_episode_id AS VARCHAR), 'none')
    )), 1, 16) AS eoc_id_short
  FROM main.specimen_master_v1 s
  INNER JOIN main.fhir_patient_deid_map_v1 pm USING (research_id)
  LEFT JOIN focus_ctx fc USING (specimen_id)
)
SELECT
  ('Specimen/' || spec_id_short) AS fhir_id,
  patient_fhir_id,
  specimen_id,
  proc_id_short AS procedure_fhir_id,
  enc_id_short AS encounter_fhir_id,
  eoc_id_short AS episode_fhir_id,
  json_object(
    'resourceType', 'Specimen',
    'id', spec_id_short,
    'identifier', json_array(
      json_object(
        'system', 'urn:oid:thyroid2026:specimen-fingerprint',
        'value', specimen_fingerprint_sha256
      ),
      json_object(
        'system', 'urn:oid:thyroid2026:specimen-id',
        'value', specimen_id
      )
    ),
    'status', 'available',
    'type', json_object(
      'text', coalesce(nullif(trim(specimen_role), ''), 'specimen')
    ),
    'subject', json_object('reference', 'Patient/' || patient_fhir_id),
    'receivedTime', CASE
      WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN procedure_date_day || 'T00:00:00Z'
      ELSE NULL
    END,
    'collection', json_object(
      'collectedDateTime', CASE
        WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
        THEN procedure_date_day || 'T00:00:00Z'
        ELSE NULL
      END,
      'bodySite', CASE
        WHEN site_text_any IS NOT NULL AND trim(cast(site_text_any AS VARCHAR)) <> ''
        THEN json_object('text', site_text_any)
        ELSE NULL
      END,
      'procedure', json_object('reference', 'Procedure/' || proc_id_short)
    )
  ) AS resource_json,
  current_timestamp AS built_at
FROM base;

CREATE OR REPLACE TABLE main.fhir_procedure_collection_v1 AS
WITH focus_ctx AS (
  SELECT
    f.specimen_id,
    max(l.thyroid_procedure) AS thyroid_procedure_any
  FROM main.specimen_tumor_focus_v1 f
  LEFT JOIN main._specimen_path_surgery_link_v1 l
    ON f.research_id = l.research_id
   AND f.synoptic_row_ix = l.synoptic_row_ix
   AND COALESCE(cast(f.tumor_index AS VARCHAR), '') = COALESCE(cast(l.tumor_index AS VARCHAR), '')
  GROUP BY f.specimen_id
),
base AS (
  SELECT
    s.specimen_id,
    s.research_id,
    s.procedure_date_day,
    s.specimen_role,
    pm.patient_fhir_id,
    fc.thyroid_procedure_any,
    substring(sha256(concat('proc|', s.specimen_id)), 1, 16) AS proc_id_short,
    substring(sha256(concat('enc|', s.specimen_id)), 1, 16) AS enc_id_short,
    substring(s.specimen_fingerprint_sha256, 1, 16) AS spec_id_short
  FROM main.specimen_master_v1 s
  INNER JOIN main.fhir_patient_deid_map_v1 pm USING (research_id)
  LEFT JOIN focus_ctx fc USING (specimen_id)
)
SELECT
  ('Procedure/' || proc_id_short) AS fhir_id,
  patient_fhir_id,
  specimen_id,
  enc_id_short AS encounter_fhir_id,
  spec_id_short AS specimen_ref_id,
  json_object(
    'resourceType', 'Procedure',
    'id', proc_id_short,
    'identifier', json_array(
      json_object(
        'system', 'urn:oid:thyroid2026:specimen-collection',
        'value', specimen_id
      )
    ),
    'status', CASE
      WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN 'completed'
      ELSE 'unknown'
    END,
    'code', json_object(
      'text', coalesce(
        nullif(trim(coalesce(thyroid_procedure_any, '')), ''),
        'Thyroid specimen collection'
      )
    ),
    'subject', json_object('reference', 'Patient/' || patient_fhir_id),
    'encounter', json_object('reference', 'Encounter/' || enc_id_short),
    'performedDateTime', CASE
      WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN procedure_date_day || 'T00:00:00Z'
      ELSE NULL
    END,
    'extension', CASE
      WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN json_array(
        json_object(
          'url', 'urn:thyroid2026:fhir:analytics:procedure-focus-specimen',
          'valueReference', json_object('reference', 'Specimen/' || spec_id_short)
        ),
        json_object(
          'url', 'urn:thyroid2026:fhir:analytics:procedure-occurrence-datetime',
          'valueDateTime', procedure_date_day || 'T00:00:00Z'
        )
      )
      ELSE json_array(
        json_object(
          'url', 'urn:thyroid2026:fhir:analytics:procedure-focus-specimen',
          'valueReference', json_object('reference', 'Specimen/' || spec_id_short)
        )
      )
    END
  ) AS resource_json,
  current_timestamp AS built_at
FROM base;

CREATE OR REPLACE TABLE main.fhir_encounter_v1 AS
WITH focus_ctx AS (
  SELECT
    f.specimen_id,
    max(l.thyroid_procedure) AS thyroid_procedure_any
  FROM main.specimen_tumor_focus_v1 f
  LEFT JOIN main._specimen_path_surgery_link_v1 l
    ON f.research_id = l.research_id
   AND f.synoptic_row_ix = l.synoptic_row_ix
   AND COALESCE(cast(f.tumor_index AS VARCHAR), '') = COALESCE(cast(l.tumor_index AS VARCHAR), '')
  GROUP BY f.specimen_id
),
base AS (
  SELECT
    s.specimen_id,
    s.research_id,
    s.procedure_date_day,
    s.specimen_role,
    s.surgery_episode_id,
    pm.patient_fhir_id,
    fc.thyroid_procedure_any,
    substring(sha256(concat('enc|', s.specimen_id)), 1, 16) AS enc_id_short,
    substring(sha256(concat(
      'eoc|', cast(s.research_id AS VARCHAR), '|',
      coalesce(cast(s.surgery_episode_id AS VARCHAR), 'none')
    )), 1, 16) AS eoc_id_short
  FROM main.specimen_master_v1 s
  INNER JOIN main.fhir_patient_deid_map_v1 pm USING (research_id)
  LEFT JOIN focus_ctx fc USING (specimen_id)
)
SELECT
  ('Encounter/' || enc_id_short) AS fhir_id,
  patient_fhir_id,
  specimen_id,
  eoc_id_short AS episode_fhir_id,
  json_object(
    'resourceType', 'Encounter',
    'id', enc_id_short,
    'identifier', json_array(
      json_object(
        'system', 'urn:oid:thyroid2026:encounter-specimen-anchor',
        'value', specimen_id
      )
    ),
    'status', CASE
      WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
      THEN 'finished'
      ELSE 'unknown'
    END,
    'class', CASE
      WHEN lower(coalesce(specimen_role, '')) = 'surgical_resection'
      THEN json_object(
        'system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
        'code', 'IMP',
        'display', 'inpatient encounter'
      )
      ELSE json_object(
        'system', 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
        'code', 'AMB',
        'display', 'ambulatory'
      )
    END,
    'type', CASE
      WHEN thyroid_procedure_any IS NOT NULL AND trim(cast(thyroid_procedure_any AS VARCHAR)) <> ''
      THEN json_array(json_object('text', thyroid_procedure_any))
      ELSE json_array(json_object('coding', json_array(json_object(
        'system', 'http://snomed.info/sct',
        'code', '185347001',
        'display', 'Encounter for problem (procedure)'
      ))))
    END,
    'subject', json_object('reference', 'Patient/' || patient_fhir_id),
    'period', json_object(
      'start', CASE
        WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
        THEN procedure_date_day || 'T00:00:00Z'
        ELSE NULL
      END,
      'end', CASE
        WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
        THEN procedure_date_day || 'T23:59:59Z'
        ELSE NULL
      END
    ),
    'episodeOfCare', json_array(
      json_object('reference', 'EpisodeOfCare/' || eoc_id_short)
    )
  ) AS resource_json,
  current_timestamp AS built_at
FROM base;

-- EpisodeOfCare spine is **encounter-driven** (join fhir_encounter_v1 → specimen_master_v1).
-- Root-cause fix for historical release FAILs (~10k× ``encounter_episode`` broken refs): partial
-- redeploys or spine drift left ``fhir_encounter_v1`` pointing at EpisodeOfCare ids that did not
-- exist in ``fhir_episode_of_care_v1`` when EoC was rebuilt only from a specimen GROUP BY that
-- diverged from per-specimen encounter hashes. Every encounter row implies exactly one EoC key
-- (research_id, surgery_episode_id, patient_fhir_id, eoc_id_short) — materialized here.
CREATE OR REPLACE TABLE main.fhir_episode_of_care_v1 AS
WITH tep AS (
  SELECT
    research_id,
    surgery_episode_id,
    min(try_cast(surgery_date AS DATE)) AS ep_period_start,
    max(try_cast(surgery_date AS DATE)) AS ep_period_end
  FROM main.tumor_episode_master_v2
  GROUP BY research_id, surgery_episode_id
),
enc_spine AS (
  SELECT
    s.research_id,
    s.surgery_episode_id,
    fe.patient_fhir_id,
    fe.episode_fhir_id AS eoc_id_short,
    max(s.procedure_date_day) AS procedure_date_day_any
  FROM main.fhir_encounter_v1 fe
  INNER JOIN main.specimen_master_v1 s ON fe.specimen_id = s.specimen_id
  GROUP BY s.research_id, s.surgery_episode_id, fe.patient_fhir_id, fe.episode_fhir_id
),
base AS (
  SELECT
    es.research_id,
    es.surgery_episode_id,
    es.patient_fhir_id,
    es.procedure_date_day_any AS procedure_date_day,
    tep.ep_period_start,
    tep.ep_period_end,
    es.eoc_id_short
  FROM enc_spine es
  LEFT JOIN tep
    ON es.research_id = tep.research_id
   AND es.surgery_episode_id IS NOT DISTINCT FROM tep.surgery_episode_id
)
SELECT
  ('EpisodeOfCare/' || eoc_id_short) AS fhir_id,
  patient_fhir_id,
  cast(NULL AS VARCHAR) AS specimen_id,
  surgery_episode_id,
  eoc_id_short AS episode_fhir_id,
  json_object(
    'resourceType', 'EpisodeOfCare',
    'id', eoc_id_short,
    'identifier', json_array(
      json_object(
        'system', 'urn:oid:thyroid2026:tumor-episode-master-v2',
        'value', concat(
          cast(research_id AS VARCHAR), ':',
          coalesce(cast(surgery_episode_id AS VARCHAR), 'null')
        )
      )
    ),
    'status', 'active',
    'type', json_array(
      json_object(
        'text',
        'Thyroid oncology longitudinal episode (tumor_episode_master_v2 anchor)'
      )
    ),
    'patient', json_object('reference', 'Patient/' || patient_fhir_id),
    'period', json_object(
      'start', CASE
        WHEN ep_period_start IS NOT NULL
        THEN strftime(ep_period_start, '%Y-%m-%d') || 'T00:00:00Z'
        WHEN regexp_matches(coalesce(procedure_date_day, ''), '^\d{4}-\d{2}-\d{2}$')
        THEN procedure_date_day || 'T00:00:00Z'
        ELSE NULL
      END,
      'end', CASE
        WHEN ep_period_end IS NOT NULL AND ep_period_end <> ep_period_start
        THEN strftime(ep_period_end, '%Y-%m-%d') || 'T23:59:59Z'
        ELSE NULL
      END
    )
  ) AS resource_json,
  current_timestamp AS built_at
FROM base;

CREATE OR REPLACE TABLE main.fhir_bundle_specimen_export_v1 AS
SELECT
  row_number() OVER (ORDER BY fs.specimen_id) AS bundle_ix,
  json_object(
    'resourceType', 'Bundle',
    'type', 'collection',
    'timestamp', cast(current_timestamp AS VARCHAR),
    'entry', json_array(
      json_object(
        'resource', fs.resource_json,
        'url', 'Specimen/' || json_extract_string(fs.resource_json, '$.id')
      ),
      json_object(
        'resource', fp.resource_json,
        'url', 'Procedure/' || json_extract_string(fp.resource_json, '$.id')
      ),
      json_object(
        'resource', fe.resource_json,
        'url', 'Encounter/' || json_extract_string(fe.resource_json, '$.id')
      ),
      json_object(
        'resource', fo.resource_json,
        'url', 'EpisodeOfCare/' || json_extract_string(fo.resource_json, '$.id')
      )
    )
  ) AS bundle_json,
  fs.specimen_id,
  current_timestamp AS built_at
FROM main.fhir_specimen_v1 fs
JOIN main.fhir_procedure_collection_v1 fp ON fs.specimen_id = fp.specimen_id
JOIN main.fhir_encounter_v1 fe ON fs.specimen_id = fe.specimen_id
JOIN main.fhir_episode_of_care_v1 fo
  ON fe.episode_fhir_id = fo.episode_fhir_id
 AND fe.patient_fhir_id = fo.patient_fhir_id;
