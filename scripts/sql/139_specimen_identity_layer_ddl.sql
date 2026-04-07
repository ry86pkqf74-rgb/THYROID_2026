-- Canonical specimen identity layer (MotherDuck main/qa)
-- Idempotent full refresh per run: caller wraps in TRANSACTION + clears child→parent.
-- Fingerprint ≡ utils/specimen_fingerprint.specimen_master_fingerprint_input (incl. synoptic_row_ix).
-- Placeholder __BUILD_RUN_ID__ replaced by scripts/139_md_specimen_identity_layer.py

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
-- Physical tables (PK for ON CONFLICT; create if missing)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS main.specimen_master_v1 (
  specimen_id VARCHAR NOT NULL,
  specimen_fingerprint_sha256 VARCHAR NOT NULL,
  fingerprint_input_canonical VARCHAR NOT NULL,
  research_id BIGINT NOT NULL,
  source_system VARCHAR NOT NULL,
  procedure_date_day VARCHAR,
  accession_or_source_id VARCHAR,
  specimen_role VARCHAR NOT NULL,
  anatomic_site VARCHAR,
  laterality VARCHAR,
  surgery_episode_id BIGINT,
  encounter_synoptic_row_ix BIGINT,
  synoptic_row_ix BIGINT,
  source_candidate_kind VARCHAR NOT NULL,
  identity_build_run_id VARCHAR NOT NULL,
  identity_built_at TIMESTAMP NOT NULL,
  materialized_at TIMESTAMP NOT NULL,
  PRIMARY KEY (specimen_id),
  UNIQUE (specimen_fingerprint_sha256)
);

CREATE TABLE IF NOT EXISTS main.specimen_tumor_focus_v1 (
  specimen_focus_id VARCHAR NOT NULL,
  focus_fingerprint_sha256 VARCHAR NOT NULL,
  fingerprint_input_focus_canonical VARCHAR NOT NULL,
  specimen_id VARCHAR NOT NULL,
  master_fingerprint_sha256 VARCHAR NOT NULL,
  synoptic_row_ix BIGINT,
  research_id BIGINT NOT NULL,
  surg_date VARCHAR,
  surg_date_canonical DATE,
  encounter_synoptic_row_ix BIGINT,
  tumor_index BIGINT,
  site_text VARCHAR,
  histologic_type VARCHAR,
  surgery_episode_id BIGINT,
  path_surgery_id VARCHAR,
  tumor_ordinal BIGINT,
  linkage_confidence_tier VARCHAR,
  linkage_score DOUBLE,
  identity_build_run_id VARCHAR NOT NULL,
  identity_built_at TIMESTAMP NOT NULL,
  materialized_at TIMESTAMP NOT NULL,
  PRIMARY KEY (specimen_focus_id),
  UNIQUE (focus_fingerprint_sha256)
);

CREATE TABLE IF NOT EXISTS main.specimen_source_xref_v1 (
  xref_id VARCHAR NOT NULL,
  specimen_id VARCHAR NOT NULL,
  specimen_focus_id VARCHAR,
  domain VARCHAR NOT NULL,
  source_table VARCHAR NOT NULL,
  source_row_key VARCHAR NOT NULL,
  linkage_confidence_tier VARCHAR,
  identity_build_run_id VARCHAR NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (xref_id),
  UNIQUE (domain, source_table, source_row_key)
);

CREATE TABLE IF NOT EXISTS qa.specimen_merge_review_queue_v1 (
  queue_ix BIGINT NOT NULL,
  specimen_id_a VARCHAR NOT NULL,
  specimen_id_b VARCHAR NOT NULL,
  fp_a VARCHAR NOT NULL,
  fp_b VARCHAR NOT NULL,
  research_id BIGINT,
  compared_fields VARCHAR,
  similarity_score DOUBLE,
  secondary_score DOUBLE,
  reason_code VARCHAR NOT NULL,
  conflict_summary VARCHAR,
  evidence_context VARCHAR,
  review_priority INTEGER NOT NULL,
  review_status VARCHAR NOT NULL DEFAULT 'open',
  queued_at TIMESTAMP NOT NULL,
  identity_build_run_id VARCHAR NOT NULL,
  PRIMARY KEY (queue_ix)
);

CREATE TABLE IF NOT EXISTS qa.val_specimen_contract_v1 (
  check_name VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  detail VARCHAR,
  measured_at TIMESTAMP NOT NULL
);

-- Full-refresh pathology + molecular xrefs (specimen_detail and other kinds preserved)
DELETE FROM main.specimen_source_xref_v1 WHERE domain IN ('pathology', 'molecular');
DELETE FROM main.specimen_tumor_focus_v1
WHERE specimen_id IN (
  SELECT specimen_id FROM main.specimen_master_v1
  WHERE source_candidate_kind = 'pathology_synoptic'
);
DELETE FROM main.specimen_master_v1 WHERE source_candidate_kind = 'pathology_synoptic';

-- ═══════════════════════════════════════════════════════════════════════════
-- Staging: pathology-backed master rows
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP TABLE _specimen_master_path_staging AS
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
    encounter_synoptic_row_ix,
    synoptic_row_ix
  FROM main._specimen_path_surgery_link_v1
),
fp AS (
  SELECT
    *,
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
      LOWER(TRIM(COALESCE(CAST(encounter_synoptic_row_ix AS VARCHAR), ''))),
      LOWER(TRIM(COALESCE(CAST(synoptic_row_ix AS VARCHAR), '')))
    ) AS fingerprint_input_canonical,
    sha256(concat_ws(
      '|',
      LOWER(TRIM(CAST(research_id AS VARCHAR))),
      LOWER(TRIM(source_system)),
      LOWER(TRIM(COALESCE(procedure_date_day, ''))),
      LOWER(TRIM(COALESCE(accession_or_source_id, ''))),
      LOWER(TRIM(specimen_role)),
      LOWER(TRIM(COALESCE(anatomic_site, ''))),
      LOWER(TRIM(COALESCE(laterality, ''))),
      LOWER(TRIM(COALESCE(CAST(surgery_episode_id AS VARCHAR), ''))),
      LOWER(TRIM(COALESCE(CAST(encounter_synoptic_row_ix AS VARCHAR), ''))),
      LOWER(TRIM(COALESCE(CAST(synoptic_row_ix AS VARCHAR), '')))
    )) AS specimen_fingerprint_sha256
  FROM base
)
SELECT
  ('spm_' || specimen_fingerprint_sha256) AS specimen_id,
  specimen_fingerprint_sha256,
  fingerprint_input_canonical,
  research_id,
  source_system,
  procedure_date_day,
  accession_or_source_id,
  specimen_role,
  anatomic_site,
  laterality,
  surgery_episode_id,
  encounter_synoptic_row_ix,
  synoptic_row_ix,
  'pathology_synoptic'::VARCHAR AS source_candidate_kind,
  '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id,
  current_timestamp AS identity_built_at,
  current_timestamp AS materialized_at
FROM fp;

-- Upsert pathology masters
INSERT INTO main.specimen_master_v1 BY NAME
SELECT * FROM _specimen_master_path_staging s
ON CONFLICT (specimen_id) DO UPDATE SET
  specimen_fingerprint_sha256 = EXCLUDED.specimen_fingerprint_sha256,
  fingerprint_input_canonical = EXCLUDED.fingerprint_input_canonical,
  research_id = EXCLUDED.research_id,
  source_system = EXCLUDED.source_system,
  procedure_date_day = EXCLUDED.procedure_date_day,
  accession_or_source_id = EXCLUDED.accession_or_source_id,
  specimen_role = EXCLUDED.specimen_role,
  anatomic_site = EXCLUDED.anatomic_site,
  laterality = EXCLUDED.laterality,
  surgery_episode_id = EXCLUDED.surgery_episode_id,
  encounter_synoptic_row_ix = EXCLUDED.encounter_synoptic_row_ix,
  synoptic_row_ix = EXCLUDED.synoptic_row_ix,
  source_candidate_kind = EXCLUDED.source_candidate_kind,
  identity_build_run_id = EXCLUDED.identity_build_run_id,
  identity_built_at = EXCLUDED.identity_built_at,
  materialized_at = EXCLUDED.materialized_at;

-- ═══════════════════════════════════════════════════════════════════════════
-- Tumor focus rows (one per synoptic tumor slot)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP TABLE _specimen_focus_staging AS
WITH link AS (
  SELECT * FROM main._specimen_path_surgery_link_v1
),
joined AS (
  SELECT
    l.*,
    m.specimen_id,
    m.specimen_fingerprint_sha256 AS master_fingerprint_sha256
  FROM link l
  INNER JOIN main.specimen_master_v1 m
    ON l.research_id = m.research_id
   AND COALESCE(CAST(l.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(m.surgery_episode_id AS VARCHAR), '')
   AND COALESCE(CAST(l.encounter_synoptic_row_ix AS VARCHAR), '')
       = COALESCE(CAST(m.encounter_synoptic_row_ix AS VARCHAR), '')
   AND COALESCE(CAST(l.synoptic_row_ix AS VARCHAR), '')
       = COALESCE(CAST(m.synoptic_row_ix AS VARCHAR), '')
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
   AND m.source_system = 'pathology_synoptic_encounter'
),
fp AS (
  SELECT
    *,
    concat_ws(
      '|',
      LOWER(TRIM(COALESCE(master_fingerprint_sha256, ''))),
      LOWER(TRIM(CAST(synoptic_row_ix AS VARCHAR))),
      LOWER(TRIM(CAST(tumor_index AS VARCHAR))),
      LOWER(TRIM(COALESCE(CAST(site_text AS VARCHAR), ''))),
      LOWER(TRIM(COALESCE(CAST(histologic_type AS VARCHAR), '')))
    ) AS fingerprint_input_focus_canonical,
    sha256(concat_ws(
      '|',
      LOWER(TRIM(COALESCE(master_fingerprint_sha256, ''))),
      LOWER(TRIM(CAST(synoptic_row_ix AS VARCHAR))),
      LOWER(TRIM(CAST(tumor_index AS VARCHAR))),
      LOWER(TRIM(COALESCE(CAST(site_text AS VARCHAR), ''))),
      LOWER(TRIM(COALESCE(CAST(histologic_type AS VARCHAR), '')))
    )) AS focus_fingerprint_sha256
  FROM joined
  WHERE master_fingerprint_sha256 IS NOT NULL
)
SELECT
  ('spf_' || focus_fingerprint_sha256) AS specimen_focus_id,
  focus_fingerprint_sha256,
  fingerprint_input_focus_canonical,
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
  '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id,
  current_timestamp AS identity_built_at,
  current_timestamp AS materialized_at
FROM fp;

INSERT INTO main.specimen_tumor_focus_v1 BY NAME
SELECT * FROM _specimen_focus_staging s
ON CONFLICT (specimen_focus_id) DO UPDATE SET
  focus_fingerprint_sha256 = EXCLUDED.focus_fingerprint_sha256,
  fingerprint_input_focus_canonical = EXCLUDED.fingerprint_input_focus_canonical,
  specimen_id = EXCLUDED.specimen_id,
  master_fingerprint_sha256 = EXCLUDED.master_fingerprint_sha256,
  synoptic_row_ix = EXCLUDED.synoptic_row_ix,
  research_id = EXCLUDED.research_id,
  surg_date = EXCLUDED.surg_date,
  surg_date_canonical = EXCLUDED.surg_date_canonical,
  encounter_synoptic_row_ix = EXCLUDED.encounter_synoptic_row_ix,
  tumor_index = EXCLUDED.tumor_index,
  site_text = EXCLUDED.site_text,
  histologic_type = EXCLUDED.histologic_type,
  surgery_episode_id = EXCLUDED.surgery_episode_id,
  path_surgery_id = EXCLUDED.path_surgery_id,
  tumor_ordinal = EXCLUDED.tumor_ordinal,
  linkage_confidence_tier = EXCLUDED.linkage_confidence_tier,
  linkage_score = EXCLUDED.linkage_score,
  identity_build_run_id = EXCLUDED.identity_build_run_id,
  identity_built_at = EXCLUDED.identity_built_at,
  materialized_at = EXCLUDED.materialized_at;

-- ═══════════════════════════════════════════════════════════════════════════
-- Source xrefs: pathology + molecular (deterministic linkage tiers only)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TEMP TABLE _specimen_xref_path AS
SELECT
  ('xrf_' || sha256(concat_ws(
    '|', 'pathology', 'synoptic_tumor_long_v1', CAST(synoptic_row_ix AS VARCHAR),
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
  linkage_confidence_tier,
  '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id,
  current_timestamp AS created_at
FROM main.specimen_tumor_focus_v1;

CREATE OR REPLACE TEMP TABLE _specimen_xref_mol AS
WITH fm AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY research_id, molecular_episode_id
      ORDER BY score_rank NULLS LAST, linkage_score DESC NULLS LAST
    ) AS _fr
  FROM main.fna_molecular_linkage_v3
),
fm1 AS (SELECT * FROM fm WHERE _fr = 1),
ps AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY research_id, preop_episode_id ORDER BY score_rank NULLS LAST
    ) AS _pr
  FROM main.preop_surgery_linkage_v3
),
ps1 AS (SELECT * FROM ps WHERE _pr = 1),
mol AS (
  SELECT
    CAST(m.research_id AS BIGINT) AS research_id,
    CAST(m.molecular_episode_id AS BIGINT) AS molecular_episode_id,
    CAST(m.platform AS VARCHAR) AS platform
  FROM main.molecular_test_episode_v2 m
),
bound_raw AS (
  SELECT
    mol.research_id,
    mol.molecular_episode_id,
    mol.platform,
    fm1.linkage_confidence_tier AS fm_tier,
    ps1.surgery_episode_id,
    f.specimen_focus_id,
    f.specimen_id
  FROM mol
  INNER JOIN fm1
    ON mol.research_id = fm1.research_id
   AND mol.molecular_episode_id = fm1.molecular_episode_id
  INNER JOIN ps1
    ON fm1.research_id = ps1.research_id
   AND fm1.fna_episode_id = ps1.preop_episode_id
  INNER JOIN main.specimen_tumor_focus_v1 f
    ON f.research_id = ps1.research_id
   AND COALESCE(CAST(f.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(ps1.surgery_episode_id AS VARCHAR), '')
  WHERE fm1.linkage_confidence_tier IN ('exact_match', 'high_confidence')
),
bound AS (
  SELECT
    research_id,
    molecular_episode_id,
    platform,
    fm_tier,
    surgery_episode_id,
    specimen_focus_id,
    specimen_id
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id, molecular_episode_id
        ORDER BY specimen_focus_id
      ) AS _rk
    FROM bound_raw
  ) z
  WHERE z._rk = 1
)
SELECT
  ('xrf_' || sha256(concat_ws(
    '|', 'molecular', 'molecular_test_episode_v2',
    CAST(research_id AS VARCHAR), CAST(molecular_episode_id AS VARCHAR)
  ))) AS xref_id,
  specimen_id,
  specimen_focus_id,
  'molecular'::VARCHAR AS domain,
  'molecular_test_episode_v2'::VARCHAR AS source_table,
  concat_ws(':',
    CAST(research_id AS VARCHAR),
    CAST(molecular_episode_id AS VARCHAR)
  ) AS source_row_key,
  fm_tier AS linkage_confidence_tier,
  '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id,
  current_timestamp AS created_at
FROM bound;

INSERT INTO main.specimen_source_xref_v1 BY NAME
SELECT * FROM _specimen_xref_path
ON CONFLICT (domain, source_table, source_row_key) DO UPDATE SET
  specimen_id = EXCLUDED.specimen_id,
  specimen_focus_id = EXCLUDED.specimen_focus_id,
  linkage_confidence_tier = EXCLUDED.linkage_confidence_tier,
  identity_build_run_id = EXCLUDED.identity_build_run_id,
  created_at = EXCLUDED.created_at;

INSERT INTO main.specimen_source_xref_v1 BY NAME
SELECT * FROM _specimen_xref_mol
ON CONFLICT (domain, source_table, source_row_key) DO UPDATE SET
  specimen_id = EXCLUDED.specimen_id,
  specimen_focus_id = EXCLUDED.specimen_focus_id,
  linkage_confidence_tier = EXCLUDED.linkage_confidence_tier,
  identity_build_run_id = EXCLUDED.identity_build_run_id,
  created_at = EXCLUDED.created_at;

-- ═══════════════════════════════════════════════════════════════════════════
-- Review queue: structural ambiguity + fuzzy accession (no auto-merge)
-- ═══════════════════════════════════════════════════════════════════════════
DELETE FROM qa.specimen_merge_review_queue_v1;

INSERT INTO qa.specimen_merge_review_queue_v1 (
  queue_ix, specimen_id_a, specimen_id_b, fp_a, fp_b, research_id,
  compared_fields, similarity_score, secondary_score, reason_code,
  conflict_summary, evidence_context, review_priority, review_status,
  queued_at, identity_build_run_id
)
WITH m AS (
  SELECT *
  FROM main.specimen_master_v1
  WHERE source_system = 'pathology_synoptic_encounter'
),
structural AS (
  SELECT
    a.specimen_id AS specimen_id_a,
    b.specimen_id AS specimen_id_b,
    a.specimen_fingerprint_sha256 AS fp_a,
    b.specimen_fingerprint_sha256 AS fp_b,
    a.research_id,
    'procedure_date_day|surgery_episode_id|accession_or_source_id|synoptic_row_ix'::VARCHAR AS compared_fields,
    CAST(NULL AS DOUBLE) AS similarity_score,
    CAST(NULL AS DOUBLE) AS secondary_score,
    'same_accession_multi_synoptic'::VARCHAR AS reason_code,
    concat(
      'shared accession across distinct synoptic_row_ix; research_id=',
      CAST(a.research_id AS VARCHAR),
      '; day=',
      COALESCE(a.procedure_date_day, 'NULL')
    ) AS conflict_summary,
    concat(
      'synoptic_row_ix_a=', COALESCE(CAST(a.synoptic_row_ix AS VARCHAR), 'NULL'),
      '; synoptic_row_ix_b=', COALESCE(CAST(b.synoptic_row_ix AS VARCHAR), 'NULL'),
      '; encounter_ix_a=', COALESCE(CAST(a.encounter_synoptic_row_ix AS VARCHAR), 'NULL'),
      '; encounter_ix_b=', COALESCE(CAST(b.encounter_synoptic_row_ix AS VARCHAR), 'NULL'),
      '; accession=', COALESCE(a.accession_or_source_id, '')
    ) AS evidence_context,
    30 AS review_priority,
    'open'::VARCHAR AS review_status,
    current_timestamp AS queued_at,
    '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id
  FROM m a
  JOIN m b
    ON a.research_id = b.research_id
   AND COALESCE(a.procedure_date_day, '') = COALESCE(b.procedure_date_day, '')
   AND COALESCE(CAST(a.surgery_episode_id AS VARCHAR), '')
       = COALESCE(CAST(b.surgery_episode_id AS VARCHAR), '')
   AND COALESCE(a.accession_or_source_id, '') <> ''
   AND COALESCE(a.accession_or_source_id, '') = COALESCE(b.accession_or_source_id, '')
   AND COALESCE(CAST(a.synoptic_row_ix AS VARCHAR), '')
       <> COALESCE(CAST(b.synoptic_row_ix AS VARCHAR), '')
   AND a.specimen_fingerprint_sha256 < b.specimen_fingerprint_sha256
),
fuzzy AS (
  SELECT
    a.specimen_id AS specimen_id_a,
    b.specimen_id AS specimen_id_b,
    a.specimen_fingerprint_sha256 AS fp_a,
    b.specimen_fingerprint_sha256 AS fp_b,
    a.research_id,
    'accession_or_source_id|levenshtein|synoptic_row_ix'::VARCHAR AS compared_fields,
    (1.0 - CAST(levenshtein(
      COALESCE(a.accession_or_source_id, ''),
      COALESCE(b.accession_or_source_id, '')
    ) AS DOUBLE) / greatest(length(COALESCE(a.accession_or_source_id, '')), 1.0)) AS similarity_score,
    CAST(levenshtein(
      COALESCE(a.accession_or_source_id, ''),
      COALESCE(b.accession_or_source_id, '')
    ) AS DOUBLE) AS secondary_score,
    'near_duplicate_accession_candidate'::VARCHAR AS reason_code,
    concat(
      'levenshtein=',
      CAST(levenshtein(
        COALESCE(a.accession_or_source_id, ''),
        COALESCE(b.accession_or_source_id, '')
      ) AS VARCHAR),
      '; research_id=', CAST(a.research_id AS VARCHAR)
    ) AS conflict_summary,
    concat(
      'synoptic_row_ix_a=', COALESCE(CAST(a.synoptic_row_ix AS VARCHAR), 'NULL'),
      '; synoptic_row_ix_b=', COALESCE(CAST(b.synoptic_row_ix AS VARCHAR), 'NULL'),
      '; accession_a=', COALESCE(a.accession_or_source_id, ''),
      '; accession_b=', COALESCE(b.accession_or_source_id, '')
    ) AS evidence_context,
    20 AS review_priority,
    'open'::VARCHAR AS review_status,
    current_timestamp AS queued_at,
    '__BUILD_RUN_ID__'::VARCHAR AS identity_build_run_id
  FROM m a
  JOIN m b
    ON a.research_id = b.research_id
   AND COALESCE(a.procedure_date_day, '') = COALESCE(b.procedure_date_day, '')
   AND a.specimen_fingerprint_sha256 < b.specimen_fingerprint_sha256
   AND length(COALESCE(a.accession_or_source_id, '')) > 4
   AND length(COALESCE(b.accession_or_source_id, '')) > 4
   AND levenshtein(
     COALESCE(a.accession_or_source_id, ''),
     COALESCE(b.accession_or_source_id, '')
   ) BETWEEN 1 AND 2
   AND COALESCE(CAST(a.synoptic_row_ix AS VARCHAR), '')
       <> COALESCE(CAST(b.synoptic_row_ix AS VARCHAR), '')
),
combined AS (
  SELECT * FROM structural
  UNION ALL
  SELECT * FROM fuzzy
)
SELECT
  row_number() OVER () AS queue_ix,
  specimen_id_a,
  specimen_id_b,
  fp_a,
  fp_b,
  research_id,
  compared_fields,
  similarity_score,
  secondary_score,
  reason_code,
  conflict_summary,
  evidence_context,
  review_priority,
  review_status,
  queued_at,
  identity_build_run_id
FROM combined;
