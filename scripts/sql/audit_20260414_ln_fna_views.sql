-- THYROID_2026 — Strict Audit Critical Fix Views (2026-04-14)
-- Deploy to: MotherDuck thyroid_ete_fix_20260413
-- Prerequisites: imaging_fna_linkage_v3, fna_cytology, fna_episode_master_v2,
--                v_fna_episode_bethesda_resolved_v1, ultrasound_reports, ct_imaging,
--                tumor_pathology

-- ============================================================================
-- VIEW 1: v_ln_imaging_separated_v1
-- Separated imaging LN surface (US + CT)
-- ============================================================================
CREATE OR REPLACE VIEW v_ln_imaging_separated_v1 AS
SELECT
    CAST(u.research_id AS BIGINT) AS research_id,
    'imaging' AS source_class,
    'ultrasound' AS source_modality,
    u.ultrasound_date AS exam_date,
    u.lymph_node_assessment AS ln_assessment_raw,
    CASE 
        WHEN LOWER(u.lymph_node_assessment) LIKE '%no abnormal%' THEN 'normal'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%normal cervical%' THEN 'normal'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%reactive%' THEN 'reactive'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%suspicious%' THEN 'suspicious'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%pathologic%' THEN 'suspicious'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%metasta%' THEN 'suspicious'
        WHEN LOWER(u.lymph_node_assessment) LIKE '%enlarged%' THEN 'indeterminate'
        WHEN u.lymph_node_assessment IS NULL THEN 'not_assessed'
        ELSE 'other'
    END AS ln_imaging_category,
    NULL::INTEGER AS ln_total_examined,
    NULL::INTEGER AS ln_total_positive,
    NULL::DOUBLE AS largest_short_axis_mm
FROM ultrasound_reports u
UNION ALL
SELECT
    CAST(c.research_id AS BIGINT),
    'imaging',
    'ct',
    c.date_of_exam,
    c.lymph_node_findings,
    CASE 
        WHEN c.lymph_nodes_suspicious IS TRUE THEN 'suspicious'
        WHEN c.pathologic_lymph_nodes IS TRUE THEN 'suspicious'
        WHEN c.lymph_nodes_enlarged IS TRUE THEN 'indeterminate'
        WHEN c.lymph_node_findings IS NOT NULL THEN 'assessed'
        ELSE 'not_assessed'
    END,
    NULL,
    NULL,
    TRY_CAST(c.largest_lymph_node_short_axis_mm AS DOUBLE)
FROM ct_imaging c
WHERE c.largest_lymph_node_short_axis_mm IS NOT NULL 
   OR c.lymph_node_findings IS NOT NULL
   OR c.lymph_nodes_suspicious IS NOT NULL;

-- ============================================================================
-- VIEW 2: v_ln_pathology_separated_v1
-- Separated pathology LN surface from tumor_pathology (per-level granularity)
-- ============================================================================
CREATE OR REPLACE VIEW v_ln_pathology_separated_v1 AS
SELECT
    CAST(tp.research_id AS BIGINT) AS research_id,
    'pathology' AS source_class,
    'tumor_pathology_excel' AS source_modality,
    TRY_CAST(tp.primary_ln_ln_total_examined AS INTEGER) AS ln_total_examined,
    TRY_CAST(tp.primary_ln_ln_total_positive AS INTEGER) AS ln_total_positive,
    tp.primary_ln_ln_ratio AS ln_ratio,
    tp.primary_ln_ln_extranodal_extension AS extranodal_extension,
    TRY_CAST(tp.primary_ln_ln_largest_deposit_cm AS DOUBLE) AS largest_deposit_cm,
    TRY_CAST(tp.ln_central_examined AS INTEGER) AS ln_central_examined,
    TRY_CAST(tp.ln_central_positive AS INTEGER) AS ln_central_positive,
    TRY_CAST(tp.ln_lateral_right_examined AS INTEGER) AS ln_lateral_right_examined,
    TRY_CAST(tp.ln_lateral_right_positive AS INTEGER) AS ln_lateral_right_positive,
    TRY_CAST(tp.ln_lateral_left_examined AS INTEGER) AS ln_lateral_left_examined,
    TRY_CAST(tp.ln_lateral_left_positive AS INTEGER) AS ln_lateral_left_positive,
    TRY_CAST(tp.ln_level_i_examined AS INTEGER) AS ln_level_i_examined,
    TRY_CAST(tp.ln_level_i_positive AS INTEGER) AS ln_level_i_positive,
    TRY_CAST(tp.ln_level_ii_examined AS INTEGER) AS ln_level_ii_examined,
    TRY_CAST(tp.ln_level_ii_positive AS INTEGER) AS ln_level_ii_positive,
    TRY_CAST(tp.ln_level_iii_examined AS INTEGER) AS ln_level_iii_examined,
    TRY_CAST(tp.ln_level_iii_positive AS INTEGER) AS ln_level_iii_positive,
    TRY_CAST(tp.ln_level_iv_examined AS INTEGER) AS ln_level_iv_examined,
    TRY_CAST(tp.ln_level_iv_positive AS INTEGER) AS ln_level_iv_positive,
    TRY_CAST(tp.ln_level_v_examined AS INTEGER) AS ln_level_v_examined,
    TRY_CAST(tp.ln_level_v_positive AS INTEGER) AS ln_level_v_positive,
    TRY_CAST(tp.ln_level_vi_examined AS INTEGER) AS ln_level_vi_examined,
    TRY_CAST(tp.ln_level_vi_positive AS INTEGER) AS ln_level_vi_positive,
    TRY_CAST(tp.ln_level_vii_examined AS INTEGER) AS ln_level_vii_examined,
    TRY_CAST(tp.ln_level_vii_positive AS INTEGER) AS ln_level_vii_positive,
    tp.ln_mets_ptc,
    tp.ln_mets_ftc,
    tp.ln_mets_mtc,
    tp.ln_mets_atc,
    tp.ln_mets_hurthle,
    tp.ln_mets_pdtc,
    tp.ln_mets_ptc_variant,
    tp.ln_mets_cystic,
    tp.ln_mets_micrometastasis,
    tp.ln_mets_extranodal_extension,
    TRY_CAST(tp.ln_total_levels_involved AS INTEGER) AS ln_total_levels_involved,
    tp.tumor_1_lymphatic_invasion
FROM tumor_pathology tp
WHERE tp.primary_ln_ln_total_examined IS NOT NULL;

-- ============================================================================
-- VIEW 3: v_ln_finalization_by_cancer_type_v1
-- LN metastasis by cancer histology type
-- ============================================================================
CREATE OR REPLACE VIEW v_ln_finalization_by_cancer_type_v1 AS
SELECT
    CAST(tp.research_id AS BIGINT) AS research_id,
    'pathology' AS source_class,
    TRY_CAST(tp.primary_ln_ln_total_examined AS INTEGER) AS ln_total_examined,
    TRY_CAST(tp.primary_ln_ln_total_positive AS INTEGER) AS ln_total_positive,
    tp.ln_mets_ptc AS ptc_positive_nodes,
    tp.ln_mets_ftc AS ftc_positive_nodes,
    tp.ln_mets_mtc AS mtc_positive_nodes,
    tp.ln_mets_atc AS atc_positive_nodes,
    tp.ln_mets_hurthle AS hurthle_positive_nodes,
    tp.ln_mets_pdtc AS pdtc_positive_nodes,
    tp.ln_mets_ptc_variant AS ptc_variant_detail,
    tp.ln_mets_cystic AS cystic_mets_count,
    tp.ln_mets_micrometastasis AS micromets_count,
    tp.ln_mets_extranodal_extension AS ene_count,
    TRY_CAST(tp.primary_ln_ln_largest_deposit_cm AS DOUBLE) AS largest_deposit_cm,
    TRY_CAST(tp.ln_total_levels_involved AS INTEGER) AS levels_involved_count,
    tp.source_workbook,
    tp.ln_histology_source
FROM tumor_pathology tp
WHERE tp.primary_ln_ln_total_examined IS NOT NULL
   OR tp.primary_ln_ln_total_positive IS NOT NULL;

-- ============================================================================
-- VIEW 4: v_fna_episode_bethesda_multiera_v1
-- Multi-era Bethesda (2010/2015/2023) + nodule linkage
-- Deduplicates fna_cytology and imaging_fna_linkage to prevent fan-out
-- ============================================================================
CREATE OR REPLACE VIEW v_fna_episode_bethesda_multiera_v1 AS
WITH cytology_dedup AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY research_id, fna_index 
        ORDER BY confidence DESC NULLS LAST, bethesda_2023_num DESC NULLS LAST
    ) as rn
    FROM fna_cytology
),
linkage_best AS (
    SELECT research_id, fna_episode_id, nodule_id, linkage_confidence_tier, linkage_score
    FROM imaging_fna_linkage_v3
    WHERE score_rank = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id, fna_episode_id ORDER BY linkage_score DESC) = 1
)
SELECT
    CAST(e.research_id AS BIGINT) AS research_id,
    e.fna_episode_id,
    e.resolved_fna_date AS fna_date,
    e.bethesda_category AS bethesda_episode_raw,
    COALESCE(c.bethesda_2023_num, c.category_num, r.bethesda_resolved_num) AS bethesda_current_resolved_num,
    c.bethesda_2010_num,
    c.bethesda_2010_name,
    c.bethesda_2015_num,
    c.bethesda_2015_name,
    c.bethesda_2023_num,
    c.bethesda_2023_name,
    r.bethesda_episode_num,
    r.bethesda_cytology_num,
    r.bethesda_value_source,
    r.bethesda_unscorable_reason,
    c.confidence AS cytology_confidence,
    c.rules_confidence,
    e.laterality,
    e.specimen_site_raw,
    l.nodule_id AS linked_nodule_id,
    l.linkage_confidence_tier AS nodule_linkage_confidence,
    l.linkage_score AS nodule_linkage_score,
    c.method AS scoring_method,
    c.source_workbook
FROM fna_episode_master_v2 e
LEFT JOIN cytology_dedup c 
    ON CAST(e.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
    AND CAST(e.fna_episode_id AS INTEGER) = CAST(c.fna_index AS INTEGER)
    AND c.rn = 1
LEFT JOIN v_fna_episode_bethesda_resolved_v1 r
    ON CAST(e.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
    AND CAST(e.fna_episode_id AS VARCHAR) = CAST(r.fna_episode_id AS VARCHAR)
LEFT JOIN linkage_best l 
    ON CAST(e.research_id AS VARCHAR) = CAST(l.research_id AS VARCHAR)
    AND CAST(e.fna_episode_id AS VARCHAR) = CAST(l.fna_episode_id AS VARCHAR);
