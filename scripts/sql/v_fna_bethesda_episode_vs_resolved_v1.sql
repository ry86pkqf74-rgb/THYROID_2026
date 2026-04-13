-- Pair episode-master Bethesda with resolved COALESCE(episode, fna_cytology) + reasons.
-- Deploy after v_fna_episode_bethesda_resolved_v1 (scripts/151_source_truth_confirmation_v1.py --md).
-- Analysts: use bethesda_resolved_num for numeric Bethesda; episode master alone is not SSOT.

CREATE OR REPLACE VIEW v_fna_bethesda_episode_vs_resolved_v1 AS
SELECT
    e.research_id,
    e.fna_episode_id,
    e.bethesda_category AS bethesda_in_episode_master,
    r.bethesda_resolved_num,
    r.bethesda_episode_num,
    r.bethesda_cytology_num,
    r.bethesda_value_source,
    r.bethesda_unscorable_reason,
    CASE
        WHEN e.bethesda_category IS NOT NULL THEN 'episode_master_numeric'
        WHEN r.bethesda_resolved_num IS NOT NULL THEN 'resolved_from_episode_or_cytology'
        ELSE 'unscorable_see_reason'
    END AS bethesda_analysis_bucket
FROM fna_episode_master_v2 e
LEFT JOIN v_fna_episode_bethesda_resolved_v1 r
    ON CAST(e.research_id AS BIGINT) = CAST(r.research_id AS BIGINT)
   AND CAST(e.fna_episode_id AS BIGINT) = CAST(r.fna_episode_id AS BIGINT);
