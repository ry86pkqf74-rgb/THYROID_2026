-- ============================================================================
-- Script 237 — Document imaging↔FNA size concordance gap (no-op on data)
-- Date:    2026-04-16
-- Author:  THYROID_2026 canonical-finalization run (v1_0 lock)
-- Purpose: Document that imaging_fna_linkage_v3.fna_size_cm has no
--          independent source in the canonical DB. The only available
--          backfill path (imaging_nodule_long_v2 via nodule_id) is the
--          same source img_size_cm already uses, so any derived
--          size_score would be tautologically 1.0. Preserving the flat
--          0.5 fallback on size_score is the correct behavior in v1_0
--          until an independent FNA-side size value is extracted (v1_1).
--
-- Tables READ:
--   thyroid_canonical_publication_v1_0.main.imaging_fna_linkage_v3
--   thyroid_canonical_publication_v1_0.main.data_dictionary_v240
--   thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1
--
-- Tables WRITTEN (metadata-only — zero clinical-data row changes):
--   COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm
--   COMMENT ON COLUMN imaging_fna_linkage_v3.size_score
--   UPDATE manuscript_workspace.detail_table_registry_v1 (description for imaging_fna_linkage_v3)
--   INSERT 2 rows into data_dictionary_v240 (fna_size_cm, size_score; provisional)
--
-- Rollback plan:
--   COMMENTs are idempotent — re-issue with prior text from git history.
--   Registry description: see archive_pub_v1_0.detail_table_registry_v1_*
--   data_dictionary_v240 rows: DELETE WHERE column_name IN ('fna_size_cm','size_score')
--                              AND description LIKE 'PROVISIONAL (Script 237)%'
--
-- Why this is a no-op on data:
--   No row counts change. No column values change. Only DDL COMMENTs,
--   one registry description UPDATE, and two new dictionary rows.
-- ============================================================================

-- LOG: PHASE 1 — invariant capture (pre-state)
-- ASSERT: imaging_fna_linkage_v3 has 9911 rows
SELECT COUNT(*) = 9911 AS ok FROM imaging_fna_linkage_v3;

-- ASSERT: imaging_fna_linkage_v3.fna_size_cm fully NULL (gap confirmed)
SELECT COUNT(fna_size_cm) = 0 AS ok FROM imaging_fna_linkage_v3;

-- ASSERT: img_size_cm in linkage_v3 is identical to size_cm_max in long_v2 via nodule_id (the source coupling)
SELECT
  SUM(CASE WHEN fl.img_size_cm IS NOT NULL
                AND inl.size_cm_max IS NOT NULL
                AND fl.img_size_cm <> inl.size_cm_max THEN 1 ELSE 0 END) = 0 AS ok
FROM imaging_fna_linkage_v3 fl
LEFT JOIN imaging_nodule_long_v2 inl USING (nodule_id);

-- LOG: PHASE 2 — annotate columns with v1_0 design intent
COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm IS
  'NULL by design in v1_0 (Script 237, 2026-04-16): no independent FNA-side size source exists in the canonical DB. Do NOT backfill from imaging_nodule_long_v2 — that is the same source img_size_cm already uses, which produces tautological size_score = 1.0. v1_1 TODO: extract from note_entities_llm_us_nodule_dynamics or note_entities_llm_tirads_granular via a targeted NLP pass and re-score.';

COMMENT ON COLUMN imaging_fna_linkage_v3.size_score IS
  'Flat 0.5 in v1_0 by design (Script 237, 2026-04-16): neutral prior when fna_size_cm is unavailable. Do NOT interpret as a concordance measurement. Recompute in v1_1 after fna_size_cm has been backfilled from an independent source. Existing weighted formula remains: linkage_score = 0.5*temporal + 0.3*laterality + 0.2*size - ambiguity_penalty.';

-- LOG: PHASE 3 — update detail_table_registry_v1 description to surface the gap
UPDATE manuscript_workspace.detail_table_registry_v1
SET description = 'Imaging <-> FNA crosslink table. v1_0 scoring rests on temporal + laterality only; size_score is a neutral 0.5 prior because fna_size_cm has no independent source in the canonical DB (img_size_cm and the candidate backfill in imaging_nodule_long_v2 are the same source). v1_1 TODO: extract FNA-era nodule sizes via NLP (note_entities_llm_us_nodule_dynamics / note_entities_llm_tirads_granular) and re-score. See COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm and Script 237 (2026-04-16).'
WHERE detail_table_name = 'imaging_fna_linkage_v3';

-- LOG: PHASE 4 — record provisional dictionary entries (idempotent: delete-then-insert)
DELETE FROM data_dictionary_v240
WHERE column_name IN ('fna_size_cm', 'size_score')
  AND description LIKE 'PROVISIONAL (Script 237%';

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description)
SELECT
  'fna_size_cm' AS column_name,
  'DOUBLE'      AS data_type,
  NULL          AS ordinal_position,
  COUNT(fna_size_cm)::BIGINT AS n_non_null,
  100.0 * COUNT(fna_size_cm) / NULLIF(COUNT(*), 0) AS pct_non_null,
  COUNT(DISTINCT fna_size_cm)::BIGINT AS n_distinct,
  'PROVISIONAL (Script 237, 2026-04-16): table=imaging_fna_linkage_v3. v1_0 NULL by design. v1_1 TODO: backfill via NLP from note_entities_llm_us_nodule_dynamics / note_entities_llm_tirads_granular. See COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm.' AS description
FROM imaging_fna_linkage_v3;

INSERT INTO data_dictionary_v240
  (column_name, data_type, ordinal_position, n_non_null, pct_non_null, n_distinct, description)
SELECT
  'size_score'  AS column_name,
  'DECIMAL(2,1)' AS data_type,
  NULL          AS ordinal_position,
  COUNT(size_score)::BIGINT AS n_non_null,
  100.0 * COUNT(size_score) / NULLIF(COUNT(*), 0) AS pct_non_null,
  COUNT(DISTINCT size_score)::BIGINT AS n_distinct,
  'PROVISIONAL (Script 237, 2026-04-16): table=imaging_fna_linkage_v3. v1_0 flat 0.5 by design (neutral prior when fna_size_cm unavailable). Do not interpret as concordance. v1_1 TODO: recompute after fna_size_cm backfill.' AS description
FROM imaging_fna_linkage_v3;

-- LOG: PHASE 5 — assertions
-- ASSERT: row counts unchanged for clinical tables (strict no-op invariant)
SELECT COUNT(*) = 9911 AS ok FROM imaging_fna_linkage_v3;

-- ASSERT: canonical_patient_master untouched (still 10,871)
SELECT COUNT(*) = 10871 AS ok FROM canonical_patient_master;

-- ASSERT: COMMENT for fna_size_cm contains the v1_0-by-design marker
SELECT
  comment LIKE '%NULL by design in v1_0%' AS ok
FROM duckdb_columns()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name   = 'main'
  AND table_name    = 'imaging_fna_linkage_v3'
  AND column_name   = 'fna_size_cm';

-- ASSERT: COMMENT for size_score contains the neutral-prior marker
SELECT
  comment LIKE '%neutral prior%' AS ok
FROM duckdb_columns()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name   = 'main'
  AND table_name    = 'imaging_fna_linkage_v3'
  AND column_name   = 'size_score';

-- ASSERT: registry description for imaging_fna_linkage_v3 was updated
SELECT description LIKE '%v1_1 TODO%' AS ok
FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'imaging_fna_linkage_v3';

-- ASSERT: data_dictionary_v240 has both provisional rows (exactly one each)
SELECT
  (SELECT COUNT(*) FROM data_dictionary_v240
   WHERE column_name = 'fna_size_cm' AND description LIKE 'PROVISIONAL (Script 237%') = 1
  AND
  (SELECT COUNT(*) FROM data_dictionary_v240
   WHERE column_name = 'size_score' AND description LIKE 'PROVISIONAL (Script 237%') = 1
  AS ok;

-- LOG: Script 237 complete. NO clinical data was modified. NO scoring changed. Documentation only.
