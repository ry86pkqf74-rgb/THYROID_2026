-- scripts/17_semantic_cleanup_v3.sql
-- Date Status Taxonomy V3 + Rescue Layer (exactly as described in AGENTS.md)
USE thyroid_research_2026;

-- Diagnostic (run first to confirm genetics columns exist — ~5 seconds):
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'genetic_testing'
-- ORDER BY ordinal_position;
--
-- Confirmed column facts (as of 2026-03-10):
--   genetic_testing:  DATE_1_year, DATE_2_year, DATE_3_year (BIGINT — year-level only)
--   path_synoptics:   surg_date only (no surgery_date column)
--   fna_history:      fna_date_parsed (VARCHAR YYYY-MM-DD)
--   fna_cytology:     fna_date only (no fna_date_parsed)

-- Master timeline rescue view (genetics domain; extend with UNION ALL for the other 5 tables)
CREATE OR REPLACE VIEW timeline_rescue_v2_mv AS
SELECT 
    'genetics' AS entity_type,
    e.research_id,
    e.entity_date,
    n.note_date,
    COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year) AS genetic_year,
    ps.surg_date,
    f.fna_date_parsed AS fna_date,
    CASE 
        WHEN e.entity_date IS NOT NULL
            THEN 'exact_source_date'
        WHEN n.note_date IS NOT NULL
            THEN 'inferred_day_level_date'
        WHEN COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year) IS NOT NULL
          OR ps.surg_date IS NOT NULL
          OR f.fna_date_parsed IS NOT NULL
            THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    e.entity_date IS NOT NULL AS date_is_source_native_flag,
    (n.note_date IS NOT NULL AND e.entity_date IS NULL) AS date_is_inferred_flag,
    (e.entity_date IS NULL 
     AND n.note_date IS NULL 
     AND COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year) IS NULL
     AND ps.surg_date IS NULL
     AND f.fna_date_parsed IS NULL)
     AS date_requires_manual_review_flag,
    -- TRY_CAST used throughout because surg_date/fna_date_parsed are VARCHAR,
    -- while DATE_1/2/3_year are BIGINT — cannot mix in a single COALESCE.
    COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(n.note_date AS DATE),
        TRY_CAST(
            FORMAT('%s-01-01', COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year))
            AS DATE),
        TRY_CAST(ps.surg_date AS DATE),
        TRY_CAST(f.fna_date_parsed AS DATE)
    ) AS inferred_event_date
FROM thyroid_share.note_entities_genetics e
LEFT JOIN thyroid_share.clinical_notes_long n ON e.research_id = n.research_id
LEFT JOIN genetic_testing gt ON e.research_id = gt.research_id
LEFT JOIN path_synoptics ps ON e.research_id = ps.research_id
LEFT JOIN fna_history f ON e.research_id = f.research_id;

-- Summary KPI view for dashboard & QA
CREATE OR REPLACE VIEW timeline_unresolved_summary_v2_mv AS
SELECT 
    date_status,
    COUNT(*) AS rows,
    COUNT(DISTINCT research_id) AS patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM timeline_rescue_v2_mv
GROUP BY 1;
