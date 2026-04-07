-- Specimen canonical layer + analytic FHIR export (MotherDuck main/qa)
-- Idempotent full rebuild via CREATE OR REPLACE (derived analytics surface).
--
-- specimen_master fingerprint ≡ utils/specimen_fingerprint.specimen_master_fingerprint_input
--   fields: research_id, source_system, procedure_date_day, accession_or_source_id,
--           specimen_role, anatomic_site, laterality, surgery_episode_id, encounter_synoptic_row_ix
--   (all lower/trim in SQL; day from surg_date_canonical when present)

-- ═══════════════════════════════════════════════════════════════════════════
-- Spine: synoptic tumor long ↔ encounter QC (deterministic tie-break)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main._specimen_synoptic_spine_v1 AS
WITH stl AS (
  SELECT *
  FROM main.synoptic_tumor_long_v1
  WHERE research_id IS NOT NULL
),
psq AS (
  SELECT *
  FROM main.path_synoptics_encounter_qc_v1
),
joined AS (
  SELECT
    stl.synoptic_row_ix,
    stl.research_id,
    stl.surg_date,
    stl.thyroid_procedure,
    stl.tumor_index,
    stl.site AS site_text,
    stl.histologic_type,
    psq.surg_date_canonical,
    psq.encounter_synoptic_row_ix,
    psq.surg_date_parse_tier,
    ROW_NUMBER() OVER (
      PARTITION BY stl.research_id, stl.synoptic_row_ix, stl.tumor_index
      ORDER BY
        CASE
          WHEN TRIM(LOWER(COALESCE(CAST(psq.tumor_1_histologic_type AS VARCHAR), '')))
            = TRIM(LOWER(COALESCE(CAST(stl.histologic_type AS VARCHAR), '')))
          THEN 0 ELSE 1
        END,
        psq.encounter_synoptic_row_ix NULLS LAST
    ) AS _enc_pick
  FROM stl
  LEFT JOIN psq
    ON stl.research_id = psq.research_id
   AND TRIM(COALESCE(CAST(stl.surg_date AS VARCHAR), ''))
       = TRIM(COALESCE(CAST(psq.surg_date AS VARCHAR), ''))
)
SELECT
  synoptic_row_ix,
  research_id,
  surg_date,
  thyroid_procedure,
  tumor_index,
  site_text,
  histologic_type,
  surg_date_canonical,
  encounter_synoptic_row_ix,
  surg_date_parse_tier
FROM joined
WHERE _enc_pick = 1;

CREATE OR REPLACE VIEW main._specimen_path_surgery_link_v1 AS
WITH spine AS (
  SELECT * FROM main._specimen_synoptic_spine_v1
),
sp AS (
  SELECT
    s.*,
    l.surgery_episode_id,
    l.path_surgery_id,
    l.tumor_ordinal,
    l.linkage_confidence_tier,
    l.linkage_score,
    l.score_rank,
    ROW_NUMBER() OVER (
      PARTITION BY s.research_id, s.synoptic_row_ix, s.tumor_index
      ORDER BY l.score_rank NULLS LAST, l.linkage_score DESC NULLS LAST
    ) AS _rk
  FROM spine s
  LEFT JOIN main.surgery_pathology_linkage_v3 l
    ON s.research_id = l.research_id
   AND TRY_CAST(s.tumor_index AS INTEGER) = TRY_CAST(l.tumor_ordinal AS INTEGER)
)
SELECT
  synoptic_row_ix,
  research_id,
  surg_date,
  thyroid_procedure,
  tumor_index,
  site_text,
  histologic_type,
  surg_date_canonical,
  encounter_synoptic_row_ix,
  surg_date_parse_tier,
  surgery_episode_id,
  path_surgery_id,
  tumor_ordinal,
  linkage_confidence_tier,
  linkage_score,
  score_rank
FROM sp
WHERE _rk = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- main.specimen_master_v1
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.specimen_master_v1 AS
WITH base AS (
  SELECT DISTINCT
    research_id,
    'pathology_synoptic_encounter'::VARCHAR AS source_system,
    CASE
      WHEN surg_date_canonical IS NOT NULL
        THEN strftime(CAST(surg_date_canonical AS DATE), '%Y-%m-%d')
      ELSE LOWER(TRIM(COALESCE(CAST(surg_date AS VARCHAR), '')))
    END AS procedure_date_day,
    LOWER(TRIM(COALESCE(CAST(path_surgery_id AS VARCHAR), ''))) AS accession_or_source_id,
    'surgical_resection'::VARCHAR AS specimen_role,
    'thyroid'::VARCHAR AS anatomic_site,
    ''::VARCHAR AS laterality,
    surgery_episode_id,
    encounter_synoptic_row_ix
  FROM main._specimen_path_surgery_link_v1
),
fp AS (
  SELECT
    *,
    sha256(
      concat_ws(
        '|',
        LOWER(TRIM(CAST(research_id AS VARCHAR))),
        LOWER(TRIM(source_system)),
        LOWER(TRIM(COALESCE(procedure_date_day, ''))),
        LOWER(TRIM(COALESCE(accession_or_source_id, ''))),
        LOWER(TRIM(specimen_role)),
        LOWER(TRIM(COALESCE(anatomic_site, ''))),
        LOWER(TRIM(COALESCE(laterality, ''))),
        LOWER(TRIM(COALESCE(CAST(surgery_episode_id AS VARCHAR), ''))),
        LOWER(TRIM(COALESCE(CAST(encounter_synoptic_row_ix AS VARCHAR), '')))
      )
    ) AS specimen_fingerprint_sha256
  FROM base
)
SELECT
  ('spm_' || specimen_fingerprint_sha256) AS specimen_id,
  specimen_fingerprint_sha256,
  research_id,
  source_system,
  procedure_date_day,
  accession_or_source_id,
  specimen_role,
  anatomic_site,
  laterality,
  surgery_episode_id,
  encounter_synoptic_row_ix,
  current_timestamp AS materialized_at
FROM fp;

-- ═══════════════════════════════════════════════════════════════════════════
-- main.specimen_tumor_focus_v1 (multi-tumor isolation: synoptic_row_ix + tumor_index)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.specimen_tumor_focus_v1 AS
WITH link AS (
  SELECT * FROM main._specimen_path_surgery_link_v1
),
joined AS (
  SELECT
    l.*,
    m.specimen_id,
    m.specimen_fingerprint_sha256 AS master_fingerprint_sha256
  FROM link l
  LEFT JOIN main.specimen_master_v1 m
    ON l.research_id = m.research_id
   AND COALESCE(CAST(l.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(m.surgery_episode_id AS VARCHAR), '')
   AND COALESCE(CAST(l.encounter_synoptic_row_ix AS VARCHAR), '')
       = COALESCE(CAST(m.encounter_synoptic_row_ix AS VARCHAR), '')
   AND (
     LOWER(TRIM(COALESCE(CAST(l.path_surgery_id AS VARCHAR), '')))
     = LOWER(TRIM(COALESCE(m.accession_or_source_id, '')))
     OR (l.path_surgery_id IS NULL AND m.accession_or_source_id = '')
   )
   AND LOWER(TRIM(COALESCE(
     CASE
       WHEN l.surg_date_canonical IS NOT NULL
         THEN strftime(CAST(l.surg_date_canonical AS DATE), '%Y-%m-%d')
       ELSE CAST(l.surg_date AS VARCHAR)
     END, '')))
   = LOWER(TRIM(COALESCE(m.procedure_date_day, '')))
),
fp AS (
  SELECT
    *,
    sha256(
      concat_ws(
        '|',
        master_fingerprint_sha256,
        LOWER(TRIM(CAST(synoptic_row_ix AS VARCHAR))),
        LOWER(TRIM(CAST(tumor_index AS VARCHAR))),
        LOWER(TRIM(COALESCE(CAST(site_text AS VARCHAR), ''))),
        LOWER(TRIM(COALESCE(CAST(histologic_type AS VARCHAR), '')))
      )
    ) AS focus_fingerprint_sha256
  FROM joined
  WHERE master_fingerprint_sha256 IS NOT NULL
)
SELECT
  ('spf_' || focus_fingerprint_sha256) AS specimen_focus_id,
  focus_fingerprint_sha256,
  specimen_id,
  master_fingerprint_sha256,
  synoptic_row_ix,
  research_id,
  surg_date,
  surg_date_canonical,
  encounter_synoptic_row_ix,
  tumor_index,
  site_text,
  histologic_type,
  surgery_episode_id,
  path_surgery_id,
  tumor_ordinal,
  linkage_confidence_tier,
  linkage_score,
  current_timestamp AS materialized_at
FROM fp;

-- ═══════════════════════════════════════════════════════════════════════════
-- main.specimen_source_xref_v1
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE main.specimen_source_xref_v1 AS
SELECT
  ('xrf_' || sha256(concat_ws(
    '|', 'path_synoptic_long', CAST(synoptic_row_ix AS VARCHAR),
    CAST(research_id AS VARCHAR), CAST(tumor_index AS VARCHAR)
  ))) AS xref_id,
  specimen_id,
  specimen_focus_id,
  'pathology'::VARCHAR AS domain,
  'synoptic_tumor_long_v1'::VARCHAR AS source_table,
  concat_ws(':',
    CAST(research_id AS VARCHAR),
    CAST(synoptic_row_ix AS VARCHAR),
    CAST(tumor_index AS VARCHAR)
  ) AS source_row_key,
  current_timestamp AS created_at
FROM main.specimen_tumor_focus_v1;

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
-- qa.specimen_merge_review_queue_v1 — near-duplicate candidates (no auto-merge)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE qa.specimen_merge_review_queue_v1 AS
WITH m AS (
  SELECT specimen_id, specimen_fingerprint_sha256, research_id, procedure_date_day, surgery_episode_id
  FROM main.specimen_master_v1
),
pairs AS (
  SELECT
    a.specimen_id AS specimen_id_a,
    b.specimen_id AS specimen_id_b,
    a.specimen_fingerprint_sha256 AS fp_a,
    b.specimen_fingerprint_sha256 AS fp_b,
    a.research_id,
    a.procedure_date_day,
    a.surgery_episode_id,
    'same_patient_day_diff_fp'::VARCHAR AS reason_code,
    concat(
      'distinct fingerprints for research_id=',
      CAST(a.research_id AS VARCHAR),
      ' day=', COALESCE(a.procedure_date_day, 'NULL'),
      ' surgery_episode_id=', COALESCE(CAST(a.surgery_episode_id AS VARCHAR), 'NULL')
    ) AS evidence_summary
  FROM m a
  JOIN m b
    ON a.research_id = b.research_id
   AND COALESCE(a.procedure_date_day, '') = COALESCE(b.procedure_date_day, '')
   AND COALESCE(CAST(a.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(b.surgery_episode_id AS VARCHAR), '')
   AND a.specimen_fingerprint_sha256 < b.specimen_fingerprint_sha256
)
SELECT
  row_number() OVER () AS queue_ix,
  *,
  current_timestamp AS queued_at
FROM pairs;

-- ═══════════════════════════════════════════════════════════════════════════
-- qa.val_specimen_contract_v1 — populated by scripts/138 (post-DDL checks)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE qa.val_specimen_contract_v1 (
  check_name VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  detail VARCHAR,
  measured_at TIMESTAMP NOT NULL
);

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
