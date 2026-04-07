-- Specimen + analytic FHIR — QA diagnostic views (reviewer / release ops)
-- Deploy: appended by scripts/138_md_specimen_fhir_layer.py (MotherDuck UA specimen_fhir_release_ops_v1)
--         or scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py
-- Prereqs: main.specimen_* , main.fhir_* , qa.specimen_merge_review_queue_v1 ,
--          qa.specimen_genomic_link_review_v1 (after 139 + 138 tail + 140)

-- ═══════════════════════════════════════════════════════════════════════════
-- Duplicate fingerprints (should be empty when UNIQUE constraints hold)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW qa.v_diag_specimen_duplicate_master_fp_v1 AS
SELECT
  specimen_fingerprint_sha256 AS fingerprint,
  COUNT(*) AS row_count
FROM main.specimen_master_v1
GROUP BY 1
HAVING COUNT(*) > 1;

-- Note: full-table aggregates on main.specimen_tumor_focus_v1 have intermittently
-- raised internal errors on some MotherDuck catalogs. Focus-level duplicates / orphan
-- focus / genomic→focus orphans are checked in scripts/119_md_formalization_validate.py
-- via best-effort SQL (WARN if the scan is unavailable).

CREATE OR REPLACE VIEW qa.v_diag_specimen_orphan_genomic_master_v1 AS
SELECT
  g.genomic_assay_id,
  g.specimen_id,
  g.specimen_focus_id,
  g.research_id,
  'missing_master'::VARCHAR AS reason
FROM main.specimen_genomic_assay_v1 g
LEFT JOIN main.specimen_master_v1 m ON g.specimen_id = m.specimen_id
WHERE g.specimen_id IS NOT NULL AND m.specimen_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- Broken internal FHIR references (analytic export consistency)
-- Expectations work on **legacy** MotherDuck catalogs whose fhir_* tables only
-- expose fhir_id, patient_fhir_id, specimen_id, resource_json. Subject lines use
-- patient_fhir_id; Procedure/Specimen procedure refs use the sha256 short-id recipe from
-- 138_specimen_fhir_tail_ddl.sql. Encounter→Episode matches `fhir_episode_of_care_v1`
-- by **full** `fhir_id` + `patient_fhir_id` (same as bundle assembly), not via
-- `(patient, surgery_episode_id)` rollups that can false-positive across rebuilds.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW qa.v_diag_specimen_fhir_broken_refs_v1 AS
SELECT
  'specimen_subject'::VARCHAR AS issue,
  specimen_id AS anchor_id,
  json_extract_string(resource_json, '$.subject.reference') AS ref_value,
  ('Patient/' || regexp_replace(trim(coalesce(fs.patient_fhir_id, '')), '^(Patient/)+', '')) AS expected_ref
FROM main.fhir_specimen_v1 fs
WHERE regexp_replace(coalesce(json_extract_string(resource_json, '$.subject.reference'), ''), '^(Patient/)+', '')
  IS DISTINCT FROM regexp_replace(trim(coalesce(fs.patient_fhir_id, '')), '^(Patient/)+', '')
UNION ALL
SELECT
  'specimen_collection_procedure'::VARCHAR,
  specimen_id,
  json_extract_string(resource_json, '$.collection.procedure.reference'),
  'Procedure/' || substring(sha256(concat('proc|', fs.specimen_id)), 1, 16)
FROM main.fhir_specimen_v1 fs
WHERE json_extract_string(resource_json, '$.collection.procedure.reference') IS NOT NULL
  AND json_extract_string(resource_json, '$.collection.procedure.reference')
    IS DISTINCT FROM (
      'Procedure/' || substring(sha256(concat('proc|', fs.specimen_id)), 1, 16)
    )
UNION ALL
SELECT
  'procedure_subject'::VARCHAR,
  fp.specimen_id,
  json_extract_string(fp.resource_json, '$.subject.reference'),
  ('Patient/' || regexp_replace(trim(coalesce(fp.patient_fhir_id, '')), '^(Patient/)+', '')) AS expected_ref
FROM main.fhir_procedure_collection_v1 fp
WHERE regexp_replace(coalesce(json_extract_string(fp.resource_json, '$.subject.reference'), ''), '^(Patient/)+', '')
  IS DISTINCT FROM regexp_replace(trim(coalesce(fp.patient_fhir_id, '')), '^(Patient/)+', '')
UNION ALL
SELECT
  'procedure_encounter'::VARCHAR,
  fp.specimen_id,
  json_extract_string(fp.resource_json, '$.encounter.reference'),
  'Encounter/' || substring(sha256(concat('enc|', fp.specimen_id)), 1, 16)
FROM main.fhir_procedure_collection_v1 fp
WHERE json_extract_string(fp.resource_json, '$.encounter.reference') IS NOT NULL
  AND json_extract_string(fp.resource_json, '$.encounter.reference')
    IS DISTINCT FROM (
      'Encounter/' || substring(sha256(concat('enc|', fp.specimen_id)), 1, 16)
    )
UNION ALL
SELECT
  'encounter_subject'::VARCHAR,
  fe.specimen_id,
  json_extract_string(fe.resource_json, '$.subject.reference'),
  ('Patient/' || regexp_replace(trim(coalesce(fe.patient_fhir_id, '')), '^(Patient/)+', '')) AS expected_ref
FROM main.fhir_encounter_v1 fe
WHERE regexp_replace(coalesce(json_extract_string(fe.resource_json, '$.subject.reference'), ''), '^(Patient/)+', '')
  IS DISTINCT FROM regexp_replace(trim(coalesce(fe.patient_fhir_id, '')), '^(Patient/)+', '')
UNION ALL
SELECT
  'encounter_episode'::VARCHAR,
  fe.specimen_id,
  json_extract_string(fe.resource_json, '$.episodeOfCare[0].reference'),
  fo.fhir_id AS expected_ref
FROM main.fhir_encounter_v1 fe
LEFT JOIN main.fhir_episode_of_care_v1 fo
  ON fo.patient_fhir_id = fe.patient_fhir_id
 AND fo.fhir_id = json_extract_string(fe.resource_json, '$.episodeOfCare[0].reference')
WHERE json_extract_string(fe.resource_json, '$.episodeOfCare[0].reference') IS NOT NULL
  AND fo.fhir_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- Provenance completeness — one view per table (avoids multi-table CROSS JOIN
-- issues on some MotherDuck builds)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW qa.v_diag_specimen_provenance_master_v1 AS
SELECT
  COUNT(*) FILTER (WHERE TRIM(COALESCE(identity_build_run_id, '')) = '')::BIGINT
    AS n_missing_identity_run,
  COUNT(*)::BIGINT AS n_rows
FROM main.specimen_master_v1;

CREATE OR REPLACE VIEW qa.v_diag_specimen_provenance_genomic_v1 AS
SELECT
  COUNT(*) FILTER (
    WHERE linkage_confidence_tier IN ('exact', 'high_confidence')
      AND specimen_id IS NULL
  )::BIGINT AS n_high_tier_null_specimen,
  COUNT(*)::BIGINT AS n_rows
FROM main.specimen_genomic_assay_v1;

-- ═══════════════════════════════════════════════════════════════════════════
-- Review-queue burden — genomic link review (merge queue: query manually if catalog stable;
-- some MotherDuck builds have errored on full scans of qa.specimen_merge_review_queue_v1.)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW qa.v_diag_specimen_review_burden_v1 AS
SELECT
  'specimen_genomic_link_review'::VARCHAR AS queue_key,
  COALESCE(review_status, 'unknown')::VARCHAR AS review_status,
  COUNT(*)::BIGINT AS n_rows
FROM qa.specimen_genomic_link_review_v1
GROUP BY 1, 2;
