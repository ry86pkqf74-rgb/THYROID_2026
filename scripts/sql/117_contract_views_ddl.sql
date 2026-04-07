-- Contract Views DDL for MotherDuck "Thyroid 2026"
-- Promotes episode, linkage, and analysis assets into main schema as documented
-- contract tables.  Loaded from validated manuscript_freeze parquets.
-- Idempotent: safe to re-run.

-- ═══════════════════════════════════════════════════════════════════════════
-- Longitudinal lab consumption view (deduped over thyroglobulin_lab_canonical_v1)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.longitudinal_lab_deduped_v AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, lab_date, lab_type, lab_value
               ORDER BY research_id
           ) AS _rn
    FROM main.thyroglobulin_lab_canonical_v1
) sub
WHERE _rn = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- Episode contract tables
-- These are loaded from parquets by 117_md_contract_views.py, not defined
-- as SQL views.  The DDL here documents the expected schema shape.
-- ═══════════════════════════════════════════════════════════════════════════

-- tumor_episode_master_v2:     loaded from exports/manuscript_freeze_v1/data/
-- molecular_test_episode_v2:   loaded from exports/manuscript_freeze_v1/data/
-- rai_treatment_episode_v2:    loaded from exports/manuscript_freeze_v1/data/
-- operative_episode_detail_v2: loaded from exports/manuscript_freeze_v1/data/

-- ═══════════════════════════════════════════════════════════════════════════
-- Linkage summary view (over linkage-related episode tables)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.linkage_summary_v AS
SELECT
    t.research_id,
    COUNT(DISTINCT t.surgery_episode_id) AS n_tumor_episodes,
    (SELECT COUNT(*) FROM main.molecular_test_episode_v2 m
     WHERE m.research_id = t.research_id) AS n_molecular_episodes,
    (SELECT COUNT(*) FROM main.rai_treatment_episode_v2 r
     WHERE r.research_id = t.research_id) AS n_rai_episodes,
    (SELECT COUNT(*) FROM main.operative_episode_detail_v2 o
     WHERE o.research_id = t.research_id) AS n_operative_episodes
FROM main.tumor_episode_master_v2 t
GROUP BY t.research_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- Episode completeness summary (cross-domain)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW main.episode_completeness_summary_v AS
SELECT
    'tumor_episode_master_v2' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT research_id) AS unique_patients
FROM main.tumor_episode_master_v2
UNION ALL
SELECT
    'molecular_test_episode_v2',
    COUNT(*),
    COUNT(DISTINCT research_id)
FROM main.molecular_test_episode_v2
UNION ALL
SELECT
    'rai_treatment_episode_v2',
    COUNT(*),
    COUNT(DISTINCT research_id)
FROM main.rai_treatment_episode_v2
UNION ALL
SELECT
    'operative_episode_detail_v2',
    COUNT(*),
    COUNT(DISTINCT research_id)
FROM main.operative_episode_detail_v2;
