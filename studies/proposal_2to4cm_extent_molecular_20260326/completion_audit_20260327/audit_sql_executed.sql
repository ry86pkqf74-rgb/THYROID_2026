-- Independent completion audit 20260327
-- token_mode=secrets.toml:LOCAL_DB_PATH
-- primary_cohort_N=558

-- === operative_episode_detail_v2_full ===

    SELECT research_id, surgery_episode_id, resolved_surgery_date,
           procedure_normalized, procedure_raw, laterality
    FROM operative_episode_detail_v2

-- === path_synoptics_full ===
SELECT research_id, surg_date, thyroid_procedure, completion FROM path_synoptics

-- === operative_details_counts ===
SELECT research_id, COUNT(*) AS n FROM operative_details GROUP BY 1

-- === procedure_inventory_cohort ===

    SELECT o.procedure_normalized, o.procedure_raw, COUNT(*) AS n_rows
    FROM operative_episode_detail_v2 o
    INNER JOIN _coh ON CAST(o.research_id AS BIGINT) = _coh.research_id
    GROUP BY 1, 2
    ORDER BY n_rows DESC
