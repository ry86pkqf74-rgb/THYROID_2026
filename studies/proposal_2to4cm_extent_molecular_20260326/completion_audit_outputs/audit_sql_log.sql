-- completion audit SQL log
-- run_utc=2026-03-27T02:39:26Z
-- token_mode=secrets.toml:LOCAL_DB_PATH
-- cohort_file=patient_level_dataset.csv primary_N=558 lobectomy_N=238

-- === operative_episode_detail_v2 ===

    SELECT research_id, surgery_episode_id, resolved_surgery_date,
           procedure_normalized, procedure_raw, laterality
    FROM operative_episode_detail_v2

-- === path_synoptics ===

    SELECT research_id, surg_date, thyroid_procedure, completion, thyroid_procedure AS procedure_text_raw
    FROM path_synoptics

-- === procedure_inv_oed_cohort_primary ===

    SELECT 'operative_episode_detail_v2' AS source_table,
           o.procedure_normalized AS normalized_label,
           o.procedure_raw AS raw_label,
           COUNT(*) AS n_row_instances
    FROM operative_episode_detail_v2 o
    INNER JOIN _coh_primary ON CAST(o.research_id AS BIGINT) = _coh_primary.research_id
    GROUP BY 1, 2, 3
    ORDER BY n_row_instances DESC

-- === procedure_inv_path_distinct_patterns ===

    SELECT DISTINCT 'path_synoptics' AS source_table,
      CASE
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%completion%' THEN 'TEXT_CONTAINS_completion'
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%total%thyroid%' OR LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%near-total%' THEN 'TEXT_CONTAINS_total_thyroid'
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%lobectomy%' OR LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%hemithyroid%' THEN 'TEXT_CONTAINS_lobectomy'
        ELSE 'TEXT_OTHER'
      END AS normalized_label,
      ps.thyroid_procedure AS raw_label,
      NULL::BIGINT AS n_row_instances
    FROM path_synoptics ps
    INNER JOIN _coh_lob ON CAST(ps.research_id AS BIGINT) = _coh_lob.research_id
    WHERE ps.thyroid_procedure IS NOT NULL AND TRIM(CAST(ps.thyroid_procedure AS VARCHAR)) <> ''

-- === procedure_inv_path_counts_lob_cohort ===

    SELECT 'path_synoptics' AS source_table,
           ps.thyroid_procedure AS raw_label,
           LOWER(COALESCE(CAST(ps.completion AS VARCHAR), '')) AS completion_column_value,
           COUNT(*) AS n_row_instances
    FROM path_synoptics ps
    INNER JOIN _coh_lob ON CAST(ps.research_id AS BIGINT) = _coh_lob.research_id
    WHERE ps.thyroid_procedure IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY n_row_instances DESC
