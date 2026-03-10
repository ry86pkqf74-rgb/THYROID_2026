-- 27_date_provenance_formalization.sql
-- Formalizes the Date Provenance Layer on base note_entities_* tables.
-- Complements the enriched views in 15_date_association_views.sql by
-- materializing provenance columns directly on the 6 base tables.
--
-- Prerequisites: scripts 15-26 deployed; tables exist in thyroid_research_2026.
-- Deploy: USE thyroid_research_2026;
--
-- IMPORTANT: After ALTER TABLE adds columns to the base tables, the enriched
-- views in script 15 MUST use EXCLUDE to avoid duplicate column names:
--   SELECT e.* EXCLUDE (inferred_event_date, date_source, date_granularity, date_confidence), ...
--
-- Confidence scale (matches script 15 enriched views):
--   entity_date            → 100 (native, day-level)
--   note_date              →  70 (encounter-level, day)
--   surg_date              →  60 (surgical anchor, day)
--   molecular_testing.date →  60 (day-level) / 50 (year-only → YYYY-01-01)
--   fna_date_parsed        →  55 (cytology anchor, day)
--   unrecoverable          →   0
--
-- Precedence: entity_date > note_date > surg_date > molecular_testing.date > fna_date_parsed
--
-- Diagnostic findings (2026-03-10):
--   genetic_testing: DATE_1_year, DATE_2_year, DATE_3_year (BIGINT, year-only)
--   molecular_testing: "date" (VARCHAR, day-level or year-only)
--   Pipeline uses molecular_testing for date fallback (same Excel source as genetic_testing)

USE thyroid_research_2026;

-- ============================================================================
-- 1. ALTER TABLE — add provenance columns to all 6 note_entities_* tables
--    DuckDB requires one ALTER per statement (no multi-ADD in a single ALTER).
-- ============================================================================

ALTER TABLE note_entities_genetics ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_genetics ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_genetics ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_genetics ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

ALTER TABLE note_entities_staging ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_staging ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_staging ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_staging ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

ALTER TABLE note_entities_procedures ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_procedures ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_procedures ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_procedures ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

ALTER TABLE note_entities_complications ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_complications ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_complications ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_complications ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

ALTER TABLE note_entities_medications ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_medications ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_medications ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_medications ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

ALTER TABLE note_entities_problem_list ADD COLUMN IF NOT EXISTS inferred_event_date DATE;
ALTER TABLE note_entities_problem_list ADD COLUMN IF NOT EXISTS date_source VARCHAR;
ALTER TABLE note_entities_problem_list ADD COLUMN IF NOT EXISTS date_granularity VARCHAR;
ALTER TABLE note_entities_problem_list ADD COLUMN IF NOT EXISTS date_confidence INTEGER;

-- ============================================================================
-- 2. BACKFILL — Multi-step approach
--    Step A: Set all rows using entity_date and note_date (self-contained).
--    Step B: Upgrade unrecoverable rows with surg_date (4 tables).
--    Step C: Upgrade unrecoverable genetics rows with molecular_testing.
--    Step D: Upgrade unrecoverable genetics rows with fna_history.
--
--    This avoids the inner-join problem of a single UPDATE FROM with all
--    anchor tables (which misses patients not in every anchor table).
-- ============================================================================

-- ── Step A: entity_date + note_date for all 6 tables ──

UPDATE note_entities_genetics SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

UPDATE note_entities_staging SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

UPDATE note_entities_procedures SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

UPDATE note_entities_complications SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

UPDATE note_entities_medications SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

UPDATE note_entities_problem_list SET
    inferred_event_date = COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)),
    date_source = CASE
        WHEN entity_date IS NOT NULL AND TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN note_date IS NOT NULL AND TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable' END,
    date_granularity = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL END,
    date_confidence = CASE
        WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0 END;

-- ── Step B: Upgrade unrecoverable → surg_date (genetics, staging, procedures, complications) ──

UPDATE note_entities_genetics AS e SET
    inferred_event_date = ps.surg_date_parsed,
    date_source = 'surg_date',
    date_granularity = 'day',
    date_confidence = 60
FROM (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id
  AND e.date_source = 'unrecoverable';

UPDATE note_entities_staging AS e SET
    inferred_event_date = ps.surg_date_parsed,
    date_source = 'surg_date',
    date_granularity = 'day',
    date_confidence = 60
FROM (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id
  AND e.date_source = 'unrecoverable';

UPDATE note_entities_procedures AS e SET
    inferred_event_date = ps.surg_date_parsed,
    date_source = 'surg_date',
    date_granularity = 'day',
    date_confidence = 60
FROM (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id
  AND e.date_source = 'unrecoverable';

UPDATE note_entities_complications AS e SET
    inferred_event_date = ps.surg_date_parsed,
    date_source = 'surg_date',
    date_granularity = 'day',
    date_confidence = 60
FROM (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id
  AND e.date_source = 'unrecoverable';

-- ── Step C: Upgrade unrecoverable genetics → molecular_testing ──

UPDATE note_entities_genetics AS e SET
    inferred_event_date = mt.mt_date_parsed,
    date_source = 'molecular_testing_date',
    date_granularity = mt.mt_date_granularity,
    date_confidence = CASE WHEN mt.mt_date_granularity = 'year' THEN 50 ELSE 60 END
FROM (
    SELECT research_id,
        CASE
            WHEN TRY_CAST("date" AS DATE) IS NOT NULL THEN TRY_CAST("date" AS DATE)
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\d{4}$')
                THEN TRY_CAST(CAST("date" AS VARCHAR) || '-01-01' AS DATE)
            ELSE NULL
        END AS mt_date_parsed,
        CASE
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\d{4}$') THEN 'year'
            ELSE 'day'
        END AS mt_date_granularity
    FROM molecular_testing
    WHERE "date" IS NOT NULL
      AND CAST("date" AS VARCHAR) NOT IN ('x', 'X', '', 'None', 'maybe?')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY CASE WHEN TRY_CAST("date" AS DATE) IS NOT NULL THEN 0 ELSE 1 END,
                 TRY_CAST("date" AS DATE) DESC NULLS LAST, test_index DESC
    ) = 1
) mt
WHERE CAST(e.research_id AS BIGINT) = mt.research_id
  AND e.date_source = 'unrecoverable'
  AND mt.mt_date_parsed IS NOT NULL;

-- ── Step D: Upgrade unrecoverable genetics → fna_history ──

UPDATE note_entities_genetics AS e SET
    inferred_event_date = fna.fna_date,
    date_source = 'fna_date_parsed',
    date_granularity = 'day',
    date_confidence = 55
FROM (
    SELECT research_id, TRY_CAST(fna_date_parsed AS DATE) AS fna_date
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST, fna_index DESC
    ) = 1
) fna
WHERE CAST(e.research_id AS BIGINT) = fna.research_id
  AND e.date_source = 'unrecoverable'
  AND fna.fna_date IS NOT NULL;

-- ============================================================================
-- 3. VIEWS — enriched_master_timeline and date_rescue_rate_summary
-- ============================================================================

CREATE OR REPLACE VIEW enriched_master_timeline AS
SELECT
    entity_table, research_id, entity_date, note_date,
    inferred_event_date, date_source, date_granularity,
    date_confidence, date_anchor_type, date_anchor_table
FROM missing_date_associations_audit
WHERE date_source != 'unrecoverable';

CREATE OR REPLACE VIEW date_rescue_rate_summary AS
SELECT
    entity_table,
    COUNT(*) AS total_entities,
    COUNT(*) FILTER (WHERE date_source != 'unrecoverable') AS rescued,
    COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable,
    ROUND(100.0 * COUNT(*) FILTER (WHERE date_source != 'unrecoverable')
          / NULLIF(COUNT(*), 0), 2) AS rescue_rate_pct,
    ROUND(AVG(date_confidence), 1) AS avg_confidence,
    ROUND(AVG(date_confidence) FILTER (WHERE date_source != 'unrecoverable'), 1) AS avg_confidence_rescued
FROM missing_date_associations_audit
GROUP BY entity_table

UNION ALL

SELECT
    'ALL_DOMAINS' AS entity_table,
    COUNT(*) AS total_entities,
    COUNT(*) FILTER (WHERE date_source != 'unrecoverable') AS rescued,
    COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable,
    ROUND(100.0 * COUNT(*) FILTER (WHERE date_source != 'unrecoverable')
          / NULLIF(COUNT(*), 0), 2) AS rescue_rate_pct,
    ROUND(AVG(date_confidence), 1) AS avg_confidence,
    ROUND(AVG(date_confidence) FILTER (WHERE date_source != 'unrecoverable'), 1) AS avg_confidence_rescued
FROM missing_date_associations_audit
ORDER BY entity_table;

-- ============================================================================
-- 4. SMOKE TEST QUERIES — run after deployment to verify
-- ============================================================================

-- SELECT * FROM date_rescue_rate_summary;
--
-- SELECT COUNT(*) FROM missing_date_associations_audit WHERE date_source = 'unrecoverable';
--
-- SELECT 'genetics' AS tbl, COUNT(*) AS total, COUNT(inferred_event_date) AS has_date,
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable
-- FROM note_entities_genetics
-- UNION ALL
-- SELECT 'staging', COUNT(*), COUNT(inferred_event_date),
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable')
-- FROM note_entities_staging
-- UNION ALL
-- SELECT 'procedures', COUNT(*), COUNT(inferred_event_date),
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable')
-- FROM note_entities_procedures
-- UNION ALL
-- SELECT 'complications', COUNT(*), COUNT(inferred_event_date),
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable')
-- FROM note_entities_complications
-- UNION ALL
-- SELECT 'medications', COUNT(*), COUNT(inferred_event_date),
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable')
-- FROM note_entities_medications
-- UNION ALL
-- SELECT 'problem_list', COUNT(*), COUNT(inferred_event_date),
--        COUNT(*) FILTER (WHERE date_source = 'unrecoverable')
-- FROM note_entities_problem_list;
