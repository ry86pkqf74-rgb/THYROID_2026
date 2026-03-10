-- 27_date_provenance_formalization.sql
-- Formalizes the Date Provenance Layer on base note_entities_* tables.
-- Complements the enriched views in 15_date_association_views.sql by
-- materializing provenance columns directly on the 6 base tables.
--
-- Prerequisites: scripts 15-26 deployed; tables exist in thyroid_research_2026.
-- Deploy: USE thyroid_research_2026;
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

USE thyroid_research_2026;

-- ============================================================================
-- 0. DIAGNOSTIC — confirm genetic_testing/molecular_testing date columns
-- ============================================================================
-- Run these two queries first to verify column names before proceeding.
-- Expected: both tables have a "date" column (VARCHAR).
-- genetic_testing and molecular_testing share the same Excel source
-- (THYROSEQ_AFIRMA_12_5.xlsx); molecular_testing is used throughout the
-- existing pipeline (scripts 15-18) for date fallback.

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'genetic_testing'
--   AND (column_name ILIKE '%date%' OR column_name ILIKE '%year%');

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'molecular_testing'
--   AND (column_name ILIKE '%date%' OR column_name ILIKE '%year%');

-- ============================================================================
-- 1. ALTER TABLE — add provenance columns to all 6 note_entities_* tables
-- ============================================================================

ALTER TABLE note_entities_genetics
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

ALTER TABLE note_entities_staging
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

ALTER TABLE note_entities_procedures
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

ALTER TABLE note_entities_complications
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

ALTER TABLE note_entities_medications
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

ALTER TABLE note_entities_problem_list
    ADD COLUMN IF NOT EXISTS inferred_event_date DATE,
    ADD COLUMN IF NOT EXISTS date_source VARCHAR,
    ADD COLUMN IF NOT EXISTS date_granularity VARCHAR DEFAULT 'day',
    ADD COLUMN IF NOT EXISTS date_confidence INTEGER DEFAULT 0;

-- ============================================================================
-- 2. BACKFILL — note_entities_genetics (full fallback chain)
-- ============================================================================
-- genetics and staging get the full 5-source fallback chain;
-- complications/procedures get 3-source (entity, note, surg);
-- medications/problem_list get 2-source (entity, note).

UPDATE note_entities_genetics AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(n.note_date AS DATE),
        ps.surg_date_parsed,
        mt.mt_date_parsed,
        fna.fna_date
    ),
    date_source = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN n.note_date IS NOT NULL AND TRY_CAST(n.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
        WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular_testing_date'
        WHEN fna.fna_date IS NOT NULL THEN 'fna_date_parsed'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'day'
        WHEN n.note_date IS NOT NULL AND TRY_CAST(n.note_date AS DATE) IS NOT NULL
            THEN 'day'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
        WHEN mt.mt_date_parsed IS NOT NULL THEN mt.mt_date_granularity
        WHEN fna.fna_date IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 100
        WHEN n.note_date IS NOT NULL AND TRY_CAST(n.note_date AS DATE) IS NOT NULL
            THEN 70
        WHEN ps.surg_date_parsed IS NOT NULL THEN 60
        WHEN mt.mt_date_parsed IS NOT NULL
            THEN CASE WHEN mt.mt_date_granularity = 'year' THEN 50 ELSE 60 END
        WHEN fna.fna_date IS NOT NULL THEN 55
        ELSE 0
    END
FROM (
    SELECT DISTINCT ON (research_id)
        research_id, note_date
    FROM clinical_notes_long
    ORDER BY research_id, TRY_CAST(note_date AS DATE) DESC NULLS LAST
) n,
(
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
    ) = 1
) ps,
(
    SELECT
        research_id,
        CASE
            WHEN TRY_CAST("date" AS DATE) IS NOT NULL
                THEN TRY_CAST("date" AS DATE)
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\d{4}$')
                THEN TRY_CAST(CAST("date" AS VARCHAR) || '-01-01' AS DATE)
            ELSE NULL
        END AS mt_date_parsed,
        CASE
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\d{4}$')
                THEN 'year'
            ELSE 'day'
        END AS mt_date_granularity
    FROM molecular_testing
    WHERE "date" IS NOT NULL
      AND CAST("date" AS VARCHAR) NOT IN ('x', 'X', '', 'None', 'maybe?')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
            CASE WHEN TRY_CAST("date" AS DATE) IS NOT NULL THEN 0 ELSE 1 END,
            TRY_CAST("date" AS DATE) DESC NULLS LAST,
            test_index DESC
    ) = 1
) mt,
(
    SELECT
        research_id,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST,
                 fna_index DESC
    ) = 1
) fna
WHERE e.research_id = n.research_id
  AND e.research_id = ps.research_id
  AND e.research_id = mt.research_id
  AND e.research_id = fna.research_id;

-- Catch rows that missed the inner-join pass (patients lacking some anchor tables)
UPDATE note_entities_genetics AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END
WHERE e.inferred_event_date IS NULL AND e.date_source IS NULL;

-- ============================================================================
-- 3. BACKFILL — note_entities_staging (3-source: entity, note, surg)
-- ============================================================================

UPDATE note_entities_staging AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE),
        ps.surg_date_parsed
    ),
    date_source = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        WHEN ps.surg_date_parsed IS NOT NULL THEN 60
        ELSE 0
    END
FROM (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
    ) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id;

UPDATE note_entities_staging AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END
WHERE e.inferred_event_date IS NULL AND e.date_source IS NULL;

-- ============================================================================
-- 4. BACKFILL — note_entities_procedures (3-source: entity, note, surg)
-- ============================================================================

UPDATE note_entities_procedures AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE),
        ps.surg_date_parsed
    ),
    date_source = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        WHEN ps.surg_date_parsed IS NOT NULL THEN 60
        ELSE 0
    END
FROM (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
    ) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id;

UPDATE note_entities_procedures AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END
WHERE e.inferred_event_date IS NULL AND e.date_source IS NULL;

-- ============================================================================
-- 5. BACKFILL — note_entities_complications (3-source: entity, note, surg)
-- ============================================================================

UPDATE note_entities_complications AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE),
        ps.surg_date_parsed
    ),
    date_source = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        WHEN ps.surg_date_parsed IS NOT NULL THEN 60
        ELSE 0
    END
FROM (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
    ) = 1
) ps
WHERE CAST(e.research_id AS BIGINT) = ps.research_id;

UPDATE note_entities_complications AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END
WHERE e.inferred_event_date IS NULL AND e.date_source IS NULL;

-- ============================================================================
-- 6. BACKFILL — note_entities_medications (2-source: entity, note)
-- ============================================================================

UPDATE note_entities_medications AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END;

-- ============================================================================
-- 7. BACKFILL — note_entities_problem_list (2-source: entity, note)
-- ============================================================================

UPDATE note_entities_problem_list AS e
SET
    inferred_event_date = COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE)
    ),
    date_source = CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        ELSE 'unrecoverable'
    END,
    date_granularity = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        ELSE NULL
    END,
    date_confidence = CASE
        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        ELSE 0
    END;

-- ============================================================================
-- 8. ENRICHED MASTER TIMELINE — dashboard/analysis-ready view
-- ============================================================================

CREATE OR REPLACE VIEW enriched_master_timeline AS
SELECT
    entity_table,
    research_id,
    entity_date,
    note_date,
    inferred_event_date,
    date_source,
    date_granularity,
    date_confidence,
    date_anchor_type,
    date_anchor_table
FROM missing_date_associations_audit
WHERE date_source != 'unrecoverable';

-- ============================================================================
-- 9. DATE RESCUE RATE SUMMARY — KPI for dashboard
-- ============================================================================

CREATE OR REPLACE VIEW date_rescue_rate_summary AS
SELECT
    entity_table,
    COUNT(*) AS total_entities,
    COUNT(*) FILTER (WHERE date_source != 'unrecoverable') AS rescued,
    COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE date_source != 'unrecoverable')
        / NULLIF(COUNT(*), 0), 2
    ) AS rescue_rate_pct,
    ROUND(AVG(date_confidence), 1) AS avg_confidence,
    ROUND(
        AVG(date_confidence) FILTER (WHERE date_source != 'unrecoverable'), 1
    ) AS avg_confidence_rescued
FROM missing_date_associations_audit
GROUP BY entity_table

UNION ALL

SELECT
    'ALL_DOMAINS' AS entity_table,
    COUNT(*) AS total_entities,
    COUNT(*) FILTER (WHERE date_source != 'unrecoverable') AS rescued,
    COUNT(*) FILTER (WHERE date_source = 'unrecoverable') AS unrecoverable,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE date_source != 'unrecoverable')
        / NULLIF(COUNT(*), 0), 2
    ) AS rescue_rate_pct,
    ROUND(AVG(date_confidence), 1) AS avg_confidence,
    ROUND(
        AVG(date_confidence) FILTER (WHERE date_source != 'unrecoverable'), 1
    ) AS avg_confidence_rescued
FROM missing_date_associations_audit
ORDER BY entity_table;

-- ============================================================================
-- 10. SMOKE TEST — run after deployment to verify
-- ============================================================================

-- Verify provenance columns populated on base tables
-- SELECT 'genetics' AS tbl,
--        COUNT(*) AS total,
--        COUNT(inferred_event_date) AS has_date,
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

-- Overall rescue rate KPI
-- SELECT * FROM date_rescue_rate_summary;
