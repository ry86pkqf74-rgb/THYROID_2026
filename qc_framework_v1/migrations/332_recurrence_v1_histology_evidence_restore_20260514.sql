-- mig_332 — Restore clinical recurrence columns on legacy-compat patient rollup view
-- Date: 2026-05-14
-- Database: thyroid_canonical_publication_v1_0
--
-- Context: mig_284 recreated main.canonical_recurrence_v1 as a facade over
-- semantic_publication.vw_recurrence_rollup_legacy_compat_v1, which projected
-- only 10 columns and dropped recurrence_histology + recurrence_evidence_source
-- (present on Script 203 / pre-mig_284 TABLE and on BQ legacy snapshot).
--
-- Provenance: join archive table captured immediately BEFORE mig_284 dropped the
-- 12-column TABLE (canonical_recurrence_v1_pre_mig284_20260503).
--
-- Apply: scripts/mig_332_recurrence_histology_evidence_apply.py --apply
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW semantic_publication.vw_recurrence_rollup_legacy_compat_v1 AS
SELECT
  CAST(r.research_id AS VARCHAR) AS research_id,
  CAST(r.recurrence_confirmed AS BOOLEAN) AS recurrence_confirmed,
  CAST(r.recurrence_type_primary AS VARCHAR) AS recurrence_type,
  CAST(r.first_recurrence_date AS DATE) AS recurrence_date,
  CAST(r.recurrence_site_primary AS VARCHAR) AS recurrence_site,
  CAST(leg.recurrence_histology AS VARCHAR) AS recurrence_histology,
  CAST(leg.recurrence_evidence_source AS VARCHAR) AS recurrence_evidence_source,
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
  ON CAST(pm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
LEFT JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig284_20260503 leg
  ON CAST(leg.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR);

CREATE OR REPLACE VIEW main.canonical_recurrence_v1 AS
SELECT * FROM semantic_publication.vw_recurrence_rollup_legacy_compat_v1;

INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_332',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig332',
  'mig_332: Restored recurrence_histology + recurrence_evidence_source on '
  || 'vw_recurrence_rollup_legacy_compat_v1 / main.canonical_recurrence_v1 via '
  || 'LEFT JOIN archive canonical_recurrence_v1_pre_mig284_20260503. '
  || 'Closes CPM feeder parity gap vs 12-col Script 203 / BQ legacy.'
);
