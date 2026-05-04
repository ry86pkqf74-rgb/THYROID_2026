-- mig_284 — Legacy recurrence deprecation (mig_269b follow-up)
-- Database: thyroid_canonical_publication_v1_0.main
--
-- Notes:
-- 1) New SSOT remains:
--      - main.canonical_recurrence_events_v1
--      - main.canonical_recurrence_patient_rollup_v1
-- 2) Legacy TABLE objects are archived/dropped.
-- 3) Legacy names are recreated as compatibility VIEWs to avoid downstream breakage.

-- ---------------------------------------------------------------------------
-- Compatibility views from new SSOT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW semantic_publication.vw_recurrence_events_legacy_compat_v1 AS
SELECT
  CAST(e.research_id AS VARCHAR) AS research_id,
  CAST(e.recurrence_type AS VARCHAR) AS recurrence_type,
  CAST(e.recurrence_date AS TIMESTAMP) AS recurrence_date,
  CAST(e.recurrence_site AS INTEGER) AS recurrence_site,
  CAST(
    CASE
      WHEN e.recurrence_type = 'structural' THEN 'structural_confirmed'
      WHEN e.recurrence_type = 'biochemical' THEN 'biochemical_tg_rise'
      ELSE 'structural_date_unknown'
    END AS VARCHAR
  ) AS recurrence_definition,
  CAST(
    CASE
      WHEN e.evidence_strength = 'definitive' THEN 100.0
      WHEN e.evidence_strength = 'probable' THEN 80.0
      ELSE 50.0
    END AS DOUBLE
  ) AS source_priority,
  CAST(
    COALESCE(NULLIF(TRIM(CAST(e.evidence_source AS VARCHAR)), ''), 'canonical_recurrence_events_v1')
    AS VARCHAR
  ) AS source_table,
  CAST(e.recurrence_event_seq AS BIGINT) AS event_rank,
  CAST(e.recurrence_type = 'structural' AS BOOLEAN) AS structural_recurrence_flag,
  CAST(e.recurrence_type = 'biochemical' AS BOOLEAN) AS biochemical_recurrence_flag,
  CAST(e.build_ts AS TIMESTAMP WITH TIME ZONE) AS cleaned_at
FROM main.canonical_recurrence_events_v1 e;

CREATE OR REPLACE VIEW semantic_publication.vw_recurrence_rollup_legacy_compat_v1 AS
SELECT
  CAST(r.research_id AS VARCHAR) AS research_id,
  CAST(r.recurrence_confirmed AS BOOLEAN) AS recurrence_confirmed,
  CAST(r.recurrence_type_primary AS VARCHAR) AS recurrence_type,
  CAST(r.first_recurrence_date AS DATE) AS recurrence_date,
  CAST(r.recurrence_site_primary AS VARCHAR) AS recurrence_site,
  CAST(CASE WHEN r.recurrence_confirmed THEN 'path_proven' ELSE 'none' END AS VARCHAR) AS recurrence_definition,
  CAST(pm.first_surgery_date AS DATE) AS first_surgery_date,
  CAST(
    CASE
      WHEN r.first_recurrence_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
      ELSE DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.first_recurrence_date)
    END AS DOUBLE
  ) AS time_to_recurrence_days,
  CAST(NULL AS DOUBLE) AS biochemical_tg_nadir,
  CAST(NULL AS DOUBLE) AS biochemical_tg_at_recurrence
FROM main.canonical_recurrence_patient_rollup_v1 r
LEFT JOIN main.canonical_patient_master pm
  ON CAST(pm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR);

CREATE OR REPLACE VIEW semantic_publication.vw_recurrence_safe_VIEW_v1 AS
WITH img AS (
  SELECT
    CAST(e.research_id AS VARCHAR) AS research_id,
    BOOL_OR(
      CASE
        WHEN e.recurrence_type = 'structural' AND COALESCE(e.evidence_source, '') NOT ILIKE '%tg%' THEN TRUE
        ELSE FALSE
      END
    ) AS recurrence_imaging_suspicious,
    MIN(
      CASE
        WHEN e.recurrence_type = 'structural' AND COALESCE(e.evidence_source, '') NOT ILIKE '%tg%' THEN e.recurrence_date
        ELSE NULL
      END
    ) AS recurrence_imaging_suspicious_date,
    STRING_AGG(DISTINCT COALESCE(e.evidence_source, 'clinical_structural'), ', ') AS recurrence_imaging_modality_summary,
    STRING_AGG(DISTINCT COALESCE(e.evidence_source, 'clinical_structural'), ', ') AS recurrence_imaging_source,
    STRING_AGG(DISTINCT COALESCE(e.recurrence_site_text, e.recurrence_site, ''), '; ') AS recurrence_imaging_finding_text,
    COUNT(*) FILTER (WHERE e.recurrence_type = 'structural')::BIGINT AS recurrence_imaging_n_events
  FROM main.canonical_recurrence_events_v1 e
  GROUP BY 1
)
SELECT
  'pub_v1_0_20260430' AS release_id,
  CAST(r.research_id AS VARCHAR) AS research_id,
  CAST(pm.first_surgery_date AS DATE) AS first_surg_date,
  CAST(r.recurrence_confirmed AS BOOLEAN) AS recurrence_path_proven,
  CAST(r.first_recurrence_date AS DATE) AS recurrence_path_proven_date,
  CAST('canonical_recurrence_patient_rollup_v1' AS VARCHAR) AS recurrence_path_proven_source,
  CAST(
    CASE
      WHEN r.first_recurrence_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
      ELSE DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.first_recurrence_date)
    END AS BIGINT
  ) AS days_to_path_proven,
  CAST(COALESCE(i.recurrence_imaging_suspicious, FALSE) AS BOOLEAN) AS recurrence_imaging_suspicious,
  CAST(i.recurrence_imaging_suspicious_date AS DATE) AS recurrence_imaging_suspicious_date,
  CAST(NULL AS VARCHAR) AS recurrence_imaging_modality,
  CAST(i.recurrence_imaging_modality_summary AS VARCHAR) AS recurrence_imaging_modality_summary,
  CAST(i.recurrence_imaging_source AS VARCHAR) AS recurrence_imaging_source,
  CAST(i.recurrence_imaging_finding_text AS VARCHAR) AS recurrence_imaging_finding_text,
  CAST(COALESCE(i.recurrence_imaging_n_events, 0) AS BIGINT) AS recurrence_imaging_n_events,
  CAST(
    CASE
      WHEN i.recurrence_imaging_suspicious_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
      ELSE DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), i.recurrence_imaging_suspicious_date)
    END AS BIGINT
  ) AS days_to_imaging_suspicious,
  CAST(
    COALESCE(i.recurrence_imaging_suspicious, FALSE)
    AND r.recurrence_confirmed
    AND i.recurrence_imaging_suspicious_date IS NOT NULL
    AND r.first_recurrence_date IS NOT NULL
    AND i.recurrence_imaging_suspicious_date <= r.first_recurrence_date
    AS BOOLEAN
  ) AS recurrence_imaging_then_path_confirmed,
  CAST(
    CASE
      WHEN r.recurrence_confirmed THEN 'path_proven'
      WHEN COALESCE(i.recurrence_imaging_suspicious, FALSE) THEN 'imaging_only_unconfirmed'
      ELSE 'none'
    END AS VARCHAR
  ) AS recurrence_status_final,
  CAST('mig_284_ssot_compat' AS VARCHAR) AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE) AS build_ts,
  CAST(FALSE AS BOOLEAN) AS is_implausible_date_quarantine
FROM main.canonical_recurrence_patient_rollup_v1 r
LEFT JOIN main.canonical_patient_master pm
  ON CAST(pm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
LEFT JOIN img i
  ON i.research_id = CAST(r.research_id AS VARCHAR);

-- ---------------------------------------------------------------------------
-- Archive snapshots
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig284_20260503 AS
SELECT * FROM main.canonical_recurrence_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig284_20260503 AS
SELECT * FROM main.canonical_recurrence_resolved_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.recurrence_event_clean_v1_pre_mig284_20260503 AS
SELECT * FROM main.recurrence_event_clean_v1;

-- ---------------------------------------------------------------------------
-- Drop legacy tables, recreate as compatibility views
-- ---------------------------------------------------------------------------
DROP TABLE main.canonical_recurrence_v1;
DROP TABLE main.canonical_recurrence_resolved_v1;
DROP TABLE main.recurrence_event_clean_v1;

CREATE VIEW main.canonical_recurrence_v1 AS
SELECT * FROM semantic_publication.vw_recurrence_rollup_legacy_compat_v1;

CREATE VIEW main.canonical_recurrence_resolved_v1 AS
SELECT
  research_id,
  first_surg_date,
  recurrence_path_proven,
  recurrence_path_proven_date,
  recurrence_path_proven_source,
  CAST(NULL AS VARCHAR) AS recurrence_path_proven_evidence,
  days_to_path_proven,
  recurrence_imaging_suspicious,
  recurrence_imaging_suspicious_date,
  recurrence_imaging_modality,
  recurrence_imaging_modality_summary,
  recurrence_imaging_source,
  recurrence_imaging_finding_text,
  recurrence_imaging_n_events,
  days_to_imaging_suspicious,
  recurrence_imaging_then_path_confirmed,
  recurrence_status_final,
  build_script,
  build_ts,
  is_implausible_date_quarantine
FROM semantic_publication.vw_recurrence_safe_VIEW_v1;

CREATE VIEW main.recurrence_event_clean_v1 AS
SELECT * FROM semantic_publication.vw_recurrence_events_legacy_compat_v1;

-- ---------------------------------------------------------------------------
-- Signoff
-- ---------------------------------------------------------------------------
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_284',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig284',
  'mig_284: Legacy recurrence table deprecation. Archived + dropped legacy recurrence tables and recreated legacy object names as compatibility views backed by canonical_recurrence_patient_rollup_v1/canonical_recurrence_events_v1. Closes CF-mig269b-LEGACY-DEPRECATE.'
);
