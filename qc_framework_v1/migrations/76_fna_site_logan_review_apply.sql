-- =============================================================================
-- Migration 76 -- apply Logan's fna_site review CSV decisions
-- =============================================================================
-- Date:   2026-04-27
-- Author: Logan Glosser (drafted with Claude / Cowork)
-- Plan:   apply Logan's filled fna_site_unclear_review.csv (round 1) and
--         flip fna_site to verified.
--
-- Source: verification_csvs/canonical_fna_events_v1/fna_site_unclear_review.csv
--         (Logan-filled and uploaded as fna_site_unclear_review.xlsx)
--
-- Net effect:
--   * 4 DELETEs of truly-empty phantom rows (Logan tagged 'Delete, no FNA'):
--       research_ids 10770, 2759, 6554, 7708 (all fna_index=1)
--   * 13 UPDATEs setting fna_site:
--       1313/2 -> thyroid_left_lobe
--       1904/2 -> thyroid_right_lobe
--       1967/1 -> thyroid_right_lobe
--       2071/1 -> thyroid_left_lobe
--       2634/2 -> thyroid_unspecified
--       2831/1 -> thyroid_unspecified
--       3717/1 -> neck_mass_right          (NEW vocab)
--       6521/1 -> midline_cyst             (NEW vocab; thyroglossal duct cyst territory)
--       734/2  -> thyroid_left_lobe
--       7569/2 -> thyroid_cyst             (NEW vocab)
--       9840/1 -> thyroid_cyst
--       9852/2 -> thyroid_isthmus
--       996/1  -> thyroid_unspecified
--   * Recompute derived rollups (fna_seq_n / fna_total_n_for_patient /
--     is_first_fna / is_last_fna / days_from_first_fna) since 4 rows were deleted
--   * fna_site flipped to verification_status='verified' (verification_method=
--     manual_source_review, batch_id=mig_76_fna_site)
--
-- Net row count: 8,054 -> 8,050.
-- canonical_fna_events_v1 n_verified: 20 -> 21 of 39.
--
-- Executed via Cowork query_rw 2026-04-27.
-- =============================================================================

DELETE FROM main.canonical_fna_events_v1
WHERE (research_id, fna_index) IN (
  ('10770', 1), ('2759', 1), ('6554', 1), ('7708', 1)
);

UPDATE main.canonical_fna_events_v1
SET fna_site = CASE
  WHEN research_id = '1313' AND fna_index = 2 THEN 'thyroid_left_lobe'
  WHEN research_id = '1904' AND fna_index = 2 THEN 'thyroid_right_lobe'
  WHEN research_id = '1967' AND fna_index = 1 THEN 'thyroid_right_lobe'
  WHEN research_id = '2071' AND fna_index = 1 THEN 'thyroid_left_lobe'
  WHEN research_id = '2634' AND fna_index = 2 THEN 'thyroid_unspecified'
  WHEN research_id = '2831' AND fna_index = 1 THEN 'thyroid_unspecified'
  WHEN research_id = '3717' AND fna_index = 1 THEN 'neck_mass_right'
  WHEN research_id = '6521' AND fna_index = 1 THEN 'midline_cyst'
  WHEN research_id = '734'  AND fna_index = 2 THEN 'thyroid_left_lobe'
  WHEN research_id = '7569' AND fna_index = 2 THEN 'thyroid_cyst'
  WHEN research_id = '9840' AND fna_index = 1 THEN 'thyroid_cyst'
  WHEN research_id = '9852' AND fna_index = 2 THEN 'thyroid_isthmus'
  WHEN research_id = '996'  AND fna_index = 1 THEN 'thyroid_unspecified'
END
WHERE (research_id, fna_index) IN (
  ('1313', 2), ('1904', 2), ('1967', 1), ('2071', 1), ('2634', 2),
  ('2831', 1), ('3717', 1), ('6521', 1), ('734', 2), ('7569', 2),
  ('9840', 1), ('9852', 2), ('996', 1)
);

-- Recompute rollups after deletes
UPDATE main.canonical_fna_events_v1 db
SET
  fna_seq_n               = sub.seqn,
  fna_total_n_for_patient = sub.tot,
  is_first_fna            = sub.is_first,
  is_last_fna             = sub.is_last,
  days_from_first_fna     = sub.days_from_first
FROM (
  SELECT fna_event_id,
    ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved, fna_event_id) AS seqn,
    COUNT(*) OVER (PARTITION BY research_id) AS tot,
    (ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved, fna_event_id) = 1) AS is_first,
    (ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY fna_date_resolved, fna_event_id) = COUNT(*) OVER (PARTITION BY research_id)) AS is_last,
    DATE_DIFF('day',
      MIN(fna_date_resolved) OVER (PARTITION BY research_id),
      fna_date_resolved
    ) AS days_from_first
  FROM main.canonical_fna_events_v1
) sub
WHERE db.fna_event_id = sub.fna_event_id;

-- Flip fna_site to verified
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'manual_source_review',
    batch_id            = 'mig_76_fna_site',
    verified_ts         = CURRENT_TIMESTAMP,
    notes               = COALESCE(notes,'')
                          || ' | mig_76: rule-based extraction + Logan adjudicated 13 unclear rows '
                          || '+ DELETEd 4 truly-empty phantom rows. New vocab: thyroid_cyst, '
                          || 'neck_mass_right, midline_cyst.'
WHERE schema_name='main' AND table_name='canonical_fna_events_v1' AND column_name='fna_site';

-- Recompute table signoff
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_fna_events_v1'
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;

-- =============================================================================
-- end of migration 76
-- =============================================================================
