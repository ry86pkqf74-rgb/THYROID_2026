-- scripts/17_semantic_cleanup_v3.sql
-- Date Status Taxonomy V3 + Rescue Layer (exactly as described in AGENTS.md)
USE thyroid_research_2026;

-- Master timeline rescue view (genetics example — easy to UNION the other 5 tables)
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
        WHEN e.entity_date IS NOT NULL THEN 'exact_source_date'
        WHEN n.note_date IS NOT NULL THEN 'inferred_day_level_date'
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
    COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(n.note_date AS DATE),
        TRY_CAST(CAST(COALESCE(gt.DATE_1_year, gt.DATE_2_year, gt.DATE_3_year) AS VARCHAR) || '-01-01' AS DATE),
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
