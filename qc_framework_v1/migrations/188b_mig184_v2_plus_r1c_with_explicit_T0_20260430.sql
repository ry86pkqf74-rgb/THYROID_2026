-- LOGAN RATIFIED 2026-04-30 (mig_192 patch); SUPERSEDES 8e2549c mig_188; READY FOR COWORK PATH-C APPLY
-- mig_188b — mig_184_v2 R1 AJCC + r1c LN-only prior-thy carry-forward + explicit T0 (Logan-ratified 2026-04-30)
-- Target DB: thyroid_canonical_publication_v1_0
-- Author: Logan Glosser <logan.glosser@gmail.com>
-- Posture: APPLY SKELETON ONLY; authored for Cowork Path-C review/apply. Cursor lane did NOT execute this file.
-- Predecessor: mig_184_v2 (`9702290` / `184_v2_r1_ajcc_derivation_ratified_20260430.sql`) + mig_188 (8e2549c)
-- Batch: mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430
--
-- COWORK APPLY ORDER (ratified): mig_188b → mig_186b → mig_185b → mig_187
--   This patch runs first. Explicit `t_stage_ajcc8_resolved='T0'` for no-primary / ambiguous
--   buckets (transparency vs inferring T0 only from `ajcc_resolution_source`).
-- No BEGIN TRANSACTION / COMMIT.

USE thyroid_canonical_publication_v1_0;

-- §0 pre-flight invariants
SELECT 'cpm_row_count' AS invariant_name, COUNT(*) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'cpm_distinct_research_id' AS invariant_name, COUNT(DISTINCT research_id) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'preexisting_mig188b_registry_rows_same_batch' AS invariant_name, COUNT(*) AS observed_value, 0 AS expected_value
FROM main.canonical_column_verification_registry_v1
WHERE batch_id = 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430';

-- §A pre-snapshot tables — TIMESTAMP columns without TZ drift (cf. reference_duckdb_timestamp_tz pattern).
CREATE SCHEMA IF NOT EXISTS manuscript_workspace;
CREATE TABLE IF NOT EXISTS manuscript_workspace.mig188_pre_snapshot_path_malignant AS
SELECT
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts,
    'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430' AS batch_id,
    research_id,
    surgery_episode_id,
    tumor_ordinal,
    t_stage_ajcc7,
    n_stage_ajcc7,
    m_stage_ajcc7,
    overall_stage_ajcc7,
    stage_group_ajcc7,
    t_stage_ajcc8,
    n_stage_ajcc8,
    m_stage_ajcc8,
    overall_stage_ajcc8,
    stage_group_ajcc8,
    staging_source_note
FROM main.canonical_path_malignant_events_v1;

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig188_pre_snapshot_patient_master AS
SELECT
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts,
    'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430' AS batch_id,
    research_id,
    ajcc7_t_stage,
    ajcc7_n_stage,
    ajcc7_m_stage,
    ajcc7_stage_group,
    ajcc8_t_stage,
    ajcc8_n_stage,
    ajcc8_m_stage,
    ajcc8_stage_group,
    dominant_tumor_ajcc8_t_stage,
    dominant_tumor_ajcc8_n_stage,
    dominant_tumor_ajcc8_m_stage,
    dominant_tumor_ajcc8_stage_group,
    age_at_surgery,
    histology_final,
    histologic_types_all
FROM main.canonical_patient_master;

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig188_pre_snapshot_registry AS
SELECT CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts, *
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §B canonical_path_malignant_events_v1 resolved columns (idempotent if mig_184_v2 already applied).
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;

-- §C canonical_patient_master resolved columns (idempotent).
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_t_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_n_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_m_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc8_stage_group_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_t_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_n_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_m_stage_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc7_stage_group_resolved VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;
ALTER TABLE main.canonical_patient_master ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;

-- §D T-stage UPDATE — Rules #1, #2, #6, #7 + §D-prime r1c LN-only prior-thy rule.
-- Injection site: AFTER gross-ete → T3b branch and BEFORE numeric size thresholds; resolves NULL event-size rows into:
--   (1) prior-thy carry-forward T from COALESCE(max_other_event_cm, path_tumor_size_cm),
--   (2) explicit unstaged NULL,
--   (3) ambiguous PM-only size pending Logan CSV review.
WITH t4_invasion AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        linked_surgery_episode_id AS surgery_episode_id,
        MAX(CASE
            WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), 'prevertebral|mediastinal|carotid|encas') THEN 2
            WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), 'laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway') THEN 1
            ELSE 0
        END) AS t4_rank
    FROM main.canonical_invasion_events_v1
    WHERE LOWER(COALESCE(finding_status,'')) NOT IN ('absent','negative','negated','not_present','not present')
    GROUP BY 1, 2
), pairwise_other_max AS (
    SELECT
        CAST(e1.research_id AS VARCHAR) AS research_id,
        e1.surgery_episode_id,
        e1.tumor_ordinal,
        MAX(COALESCE(e2.size_greatest_dimension_cm, e2.tumor_size_cm_per_surgery)) AS max_other_cm
    FROM main.canonical_path_malignant_events_v1 e1
    INNER JOIN main.canonical_path_malignant_events_v1 e2
        ON CAST(e1.research_id AS VARCHAR) = CAST(e2.research_id AS VARCHAR)
       AND COALESCE(e2.size_greatest_dimension_cm, e2.tumor_size_cm_per_surgery) IS NOT NULL
       AND (
            e1.surgery_episode_id IS DISTINCT FROM e2.surgery_episode_id
            OR e1.tumor_ordinal IS DISTINCT FROM e2.tumor_ordinal
           )
    GROUP BY 1, 2, 3
), event_derivation AS (
    SELECT
        CAST(e.research_id AS VARCHAR) AS research_id,
        e.surgery_episode_id,
        e.tumor_ordinal,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'niftp|non[- ]?invasive follicular thyroid neoplasm') THEN NULL
            WHEN COALESCE(t4.t4_rank, 0) = 2 THEN 'T4b'
            WHEN COALESCE(t4.t4_rank, 0) = 1 THEN 'T4a'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'anaplastic|\batc\b') THEN 'T4'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'prevertebral|mediastinal|carotid|encas') THEN 'T4b'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway') THEN 'T4a'
            -- §D-prime — BEFORE microscopic-ETE branch so NULL event-size rows honor prior-thy carry-forward first.
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) IS NOT NULL
            THEN
                CASE
                    WHEN COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) <= 1 THEN 'T1a'
                    WHEN COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) <= 2 THEN 'T1b'
                    WHEN COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) <= 4 THEN 'T2'
                    ELSE 'T3a'
                END
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND NOT (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND pm.path_tumor_size_cm IS NULL
            THEN 'T0'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND NOT (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND pm.path_tumor_size_cm IS NOT NULL
            THEN 'T0'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) IS NULL
            THEN 'T0'
            WHEN regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'micro|minimal|focal') THEN
                CASE
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 1 THEN 'T1a'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 2 THEN 'T1b'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 4 THEN 'T2'
                    WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) > 4 THEN 'T3a'
                    WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'T1a'
                    ELSE NULL
                END
            WHEN COALESCE(e.gross_ete, 0) = 1 OR regexp_matches(LOWER(COALESCE(e.extrathyroidal_extension,'')), 'gross|macroscopic|strap|skeletal\s+muscle|sternothyroid|sternohyoid|omohyoid|thyrohyoid') THEN 'T3b'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 1 THEN 'T1a'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 2 THEN 'T1b'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) <= 4 THEN 'T2'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) > 4 THEN 'T3a'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'T1a'
            ELSE NULL
        END AS t_stage_ajcc8_resolved,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'niftp|non[- ]?invasive follicular thyroid neoplasm') THEN 'niftp_excluded'
            WHEN COALESCE(t4.t4_rank, 0) > 0 THEN 'canonical_invasion_events_v1'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'anaplastic|\batc\b') THEN 'anaplastic_default_T4'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NOT NULL THEN 'coalesce_size_greatest_dimension_cm_tumor_size_cm_per_surgery'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) IS NOT NULL
                 AND (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
            THEN 'prior_thy_recurrence_T_from_prior_path'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND NOT (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND pm.path_tumor_size_cm IS NULL
            THEN 'no_primary_at_this_surgery_pT0_unstaged'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND NOT (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND pm.path_tumor_size_cm IS NOT NULL
            THEN 'ambiguous_pm_size_only_logan_pending'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
                 AND COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) IS NULL
            THEN 'no_primary_at_this_surgery_pT0_unstaged'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'microcarcinoma_without_size_default_T1a'
            ELSE 'size_residual_logan_pending'
        END AS t_resolution_source,
        CASE
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NOT NULL OR COALESCE(t4.t4_rank, 0) > 0 THEN 'high'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b|anaplastic|\batc\b') THEN 'medium'
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NULL
                 AND COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) IS NOT NULL
                 AND (
                     COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) = TRUE
                     OR pom.max_other_cm IS NOT NULL
                 )
            THEN 'medium'
            ELSE 'uncalculable'
        END AS t_resolution_confidence
    FROM main.canonical_path_malignant_events_v1 e
    LEFT JOIN t4_invasion t4
      ON CAST(e.research_id AS VARCHAR) = t4.research_id
     AND e.surgery_episode_id IS NOT DISTINCT FROM t4.surgery_episode_id
    LEFT JOIN main.canonical_patient_master pm
      ON CAST(e.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    LEFT JOIN pairwise_other_max pom
      ON CAST(e.research_id AS VARCHAR) = pom.research_id
     AND e.surgery_episode_id IS NOT DISTINCT FROM pom.surgery_episode_id
     AND e.tumor_ordinal IS NOT DISTINCT FROM pom.tumor_ordinal
)
UPDATE main.canonical_path_malignant_events_v1 AS tgt
SET
    t_stage_ajcc8_resolved = src.t_stage_ajcc8_resolved,
    t_stage_ajcc7_resolved = CASE
        WHEN src.t_stage_ajcc8_resolved = 'T3b' THEN 'T3'
        WHEN src.t_stage_ajcc8_resolved = 'T0' THEN 'T0'
        ELSE src.t_stage_ajcc8_resolved
    END,
    ajcc_resolution_source = src.t_resolution_source,
    ajcc_resolution_confidence = src.t_resolution_confidence
FROM event_derivation src
WHERE CAST(tgt.research_id AS VARCHAR) = src.research_id
  AND tgt.surgery_episode_id IS NOT DISTINCT FROM src.surgery_episode_id
  AND tgt.tumor_ordinal IS NOT DISTINCT FROM src.tumor_ordinal;

-- §E N-stage UPDATE — Rule #3 (unchanged).
-- Bucket `no_primary_at_this_surgery_pT0_unstaged` / `ambiguous_pm_size_only_logan_pending`: N copied from
-- existing `n_stage_ajcc8` / `n_stage_ajcc7` + LN heuristic via same statement (no T-stage override).
UPDATE main.canonical_path_malignant_events_v1
SET
    n_stage_ajcc8_resolved = COALESCE(n_stage_ajcc8,
        CASE
            WHEN COALESCE(ln_involved, 0) > 0 OR COALESCE(nodal_disease_positive_count, 0) > 0 THEN 'N1'
            WHEN COALESCE(ln_examined, 0) > 0 OR COALESCE(nodal_disease_total_count, 0) > 0 THEN 'N0'
            ELSE NULL
        END),
    n_stage_ajcc7_resolved = COALESCE(n_stage_ajcc7, n_stage_ajcc8,
        CASE
            WHEN COALESCE(ln_involved, 0) > 0 OR COALESCE(nodal_disease_positive_count, 0) > 0 THEN 'N1'
            WHEN COALESCE(ln_examined, 0) > 0 OR COALESCE(nodal_disease_total_count, 0) > 0 THEN 'N0'
            ELSE NULL
        END);

-- §F M-stage UPDATE — unchanged.
UPDATE main.canonical_path_malignant_events_v1
SET
    m_stage_ajcc8_resolved = COALESCE(m_stage_ajcc8, 'M0'),
    m_stage_ajcc7_resolved = COALESCE(m_stage_ajcc7, m_stage_ajcc8, 'M0');

-- §G PM rollup + PM N split + stage group update (§G CASE fall-through yields NULL sg when t8 NULL — preserves unstaged LN-only rows).
WITH event_rollup AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        arg_max(t_stage_ajcc8_resolved, CASE upper(t_stage_ajcc8_resolved) WHEN 'T4B' THEN 8 WHEN 'T4A' THEN 7 WHEN 'T4' THEN 6.5 WHEN 'T3B' THEN 5 WHEN 'T3A' THEN 4 WHEN 'T3' THEN 3 WHEN 'T2' THEN 2 WHEN 'T1B' THEN 1.2 WHEN 'T1A' THEN 1.1 ELSE 0 END) AS t8,
        arg_max(t_stage_ajcc7_resolved, CASE upper(t_stage_ajcc7_resolved) WHEN 'T4B' THEN 8 WHEN 'T4A' THEN 7 WHEN 'T4' THEN 6.5 WHEN 'T3B' THEN 5 WHEN 'T3A' THEN 4 WHEN 'T3' THEN 3 WHEN 'T2' THEN 2 WHEN 'T1B' THEN 1.2 WHEN 'T1A' THEN 1.1 ELSE 0 END) AS t7,
        arg_max(n_stage_ajcc8_resolved, CASE upper(n_stage_ajcc8_resolved) WHEN 'N1B' THEN 3 WHEN 'N1A' THEN 2 WHEN 'N1' THEN 1 ELSE 0 END) AS event_n,
        CASE WHEN SUM(CASE WHEN upper(COALESCE(m_stage_ajcc8_resolved, 'M0')) = 'M1' THEN 1 ELSE 0 END) > 0 THEN 'M1' ELSE 'M0' END AS m8,
        CASE WHEN SUM(CASE WHEN upper(COALESCE(m_stage_ajcc7_resolved, 'M0')) = 'M1' THEN 1 ELSE 0 END) > 0 THEN 'M1' ELSE 'M0' END AS m7
    FROM main.canonical_path_malignant_events_v1
    GROUP BY 1
), pm_derivation AS (
    SELECT
        CAST(pm.research_id AS VARCHAR) AS research_id,
        er.t8,
        er.t7,
        CASE
            WHEN upper(pm.ajcc8_n_stage) = 'N1' AND (
                COALESCE(pm.cnln_img_lateral_neck_present, FALSE)
                OR COALESCE(pm.cnln_img_left_present, FALSE)
                OR COALESCE(pm.cnln_img_right_present, FALSE)
                OR COALESCE(pm.cnln_img_bilateral_present, FALSE)
                OR COALESCE(pm.lateral_neck_dissected_structured_or_nlp, FALSE)
                OR COALESCE(pm.lateral_neck_dissected, FALSE)
                OR COALESCE(pm.ln_lateral_dissected, FALSE)
                OR COALESCE(pm.ln_rollup_lateral_left_positive, 0) > 0
                OR COALESCE(pm.ln_rollup_lateral_right_positive, 0) > 0
                OR COALESCE(pm.ln_rollup_bilateral_lateral_positive, 0) > 0
                OR COALESCE(pm.tp_ln_lateral_positive, 0) > 0
                OR regexp_matches(LOWER(COALESCE(pm.cnln_img_levels_mentioned,'') || ' ' || COALESCE(pm.cnln_surg_levels_mentioned,'')), 'lateral|level\s*[1-5ivx]+|jugular|retropharyngeal')
            ) THEN 'N1b'
            WHEN upper(pm.ajcc8_n_stage) = 'N1' AND (
                COALESCE(pm.cnln_img_central_present, FALSE)
                OR COALESCE(pm.ln_rollup_central_positive, 0) > 0
                OR COALESCE(pm.tp_ln_central_positive, 0) > 0
                OR COALESCE(pm.tp_central_positive_total, 0) > 0
                OR regexp_matches(LOWER(COALESCE(pm.cnln_img_levels_mentioned,'') || ' ' || COALESCE(pm.cnln_surg_levels_mentioned,'')), 'central|level\s*(vi|6|vii|7)|paratracheal|pretracheal|delphian|prelaryngeal')
            ) THEN 'N1a'
            ELSE COALESCE(pm.ajcc8_n_stage, er.event_n)
        END AS n8,
        COALESCE(pm.ajcc7_n_stage, pm.ajcc8_n_stage, er.event_n) AS n7,
        COALESCE(pm.ajcc8_m_stage, er.m8, 'M0') AS m8,
        COALESCE(pm.ajcc7_m_stage, pm.ajcc8_m_stage, er.m7, 'M0') AS m7,
        pm.age_at_surgery,
        CASE
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'anaplastic|\batc\b') THEN 'ATC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'medullary|\bmtc\b') THEN 'MTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'papillary|\bptc\b') THEN 'PTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'follicular|\bftc\b|hurthle|hürthle|oncocytic|\bhcc\b') THEN 'FTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'') || ' ' || COALESCE(pm.histology_final,'')), 'niftp') THEN 'NIFTP'
            ELSE 'DTC'
        END AS stage_component
    FROM main.canonical_patient_master pm
    LEFT JOIN event_rollup er ON CAST(pm.research_id AS VARCHAR) = er.research_id
), stage_derivation AS (
    SELECT
        *,
        CASE
            WHEN stage_component = 'NIFTP' THEN NULL
            WHEN stage_component = 'ATC' AND m8 = 'M1' THEN 'IVB'
            WHEN stage_component = 'ATC' AND t8 = 'T4b' THEN 'IVB'
            WHEN stage_component = 'ATC' THEN 'IVA'
            WHEN stage_component = 'MTC' AND m8 = 'M1' THEN 'IVC'
            WHEN stage_component = 'MTC' AND t8 IN ('T1','T1a','T1b') AND COALESCE(n8,'N0') IN ('N0','NX') THEN 'I'
            WHEN stage_component = 'MTC' AND t8 IN ('T2','T3','T3a','T3b') AND COALESCE(n8,'N0') IN ('N0','NX') THEN 'II'
            WHEN stage_component = 'MTC' AND t8 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n8 = 'N1a' THEN 'III'
            WHEN stage_component = 'MTC' AND (t8 = 'T4a' OR (t8 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n8 IN ('N1','N1b'))) THEN 'IVA'
            WHEN stage_component = 'MTC' AND t8 = 'T4b' THEN 'IVB'
            WHEN age_at_surgery < 55 AND m8 = 'M1' THEN 'II'
            WHEN age_at_surgery < 55 THEN 'I'
            WHEN m8 = 'M1' THEN 'IVB'
            WHEN t8 IN ('T1','T1a','T1b','T2') AND COALESCE(n8,'N0') IN ('N0','N0a','N0b','NX') THEN 'I'
            WHEN t8 IN ('T1','T1a','T1b','T2') AND n8 LIKE 'N1%' THEN 'II'
            WHEN t8 IN ('T3','T3a','T3b') THEN 'II'
            WHEN t8 = 'T4a' THEN 'III'
            WHEN t8 = 'T4b' THEN 'IVA'
            WHEN t8 = 'T4' THEN 'IVA'
            ELSE NULL
        END AS sg8,
        CASE
            WHEN stage_component = 'NIFTP' THEN NULL
            WHEN stage_component = 'ATC' AND m7 = 'M1' THEN 'IVC'
            WHEN stage_component = 'ATC' AND t7 = 'T4b' THEN 'IVB'
            WHEN stage_component = 'ATC' THEN 'IVA'
            WHEN stage_component = 'MTC' AND m7 = 'M1' THEN 'IVC'
            WHEN stage_component = 'MTC' AND t7 IN ('T1','T1a','T1b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'I'
            WHEN stage_component = 'MTC' AND t7 IN ('T2','T3','T3a','T3b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'II'
            WHEN stage_component = 'MTC' AND t7 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n7 = 'N1a' THEN 'III'
            WHEN stage_component = 'MTC' AND (t7 = 'T4a' OR (t7 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n7 IN ('N1','N1b'))) THEN 'IVA'
            WHEN stage_component = 'MTC' AND t7 = 'T4b' THEN 'IVB'
            WHEN age_at_surgery < 45 AND m7 = 'M1' THEN 'II'
            WHEN age_at_surgery < 45 THEN 'I'
            WHEN m7 = 'M1' THEN 'IVC'
            WHEN t7 IN ('T1','T1a','T1b') AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'I'
            WHEN t7 = 'T2' AND COALESCE(n7,'N0') IN ('N0','NX') THEN 'II'
            WHEN (t7 = 'T3' AND COALESCE(n7,'N0') IN ('N0','NX')) OR (t7 IN ('T1','T1a','T1b','T2','T3') AND n7 = 'N1a') THEN 'III'
            WHEN t7 = 'T4a' OR (t7 IN ('T1','T1a','T1b','T2','T3') AND n7 IN ('N1','N1b')) THEN 'IVA'
            WHEN t7 = 'T4b' THEN 'IVB'
            ELSE NULL
        END AS sg7
    FROM pm_derivation
)
UPDATE main.canonical_patient_master AS pm
SET
    ajcc8_t_stage_resolved = src.t8,
    ajcc8_n_stage_resolved = src.n8,
    ajcc8_m_stage_resolved = src.m8,
    ajcc8_stage_group_resolved = src.sg8,
    ajcc7_t_stage_resolved = src.t7,
    ajcc7_n_stage_resolved = src.n7,
    ajcc7_m_stage_resolved = src.m7,
    ajcc7_stage_group_resolved = src.sg7,
    ajcc_resolution_source = 'mig188b_mig184_v2_plus_r1c_explicit_T0_apply',
    ajcc_resolution_confidence = CASE WHEN src.t8 IS NULL OR src.sg8 IS NULL THEN 'uncalculable_or_pending' ELSE 'high' END,
    cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
FROM stage_derivation src
WHERE CAST(pm.research_id AS VARCHAR) = src.research_id;

-- §H Registry note appendix — CF-87-AJCC closure + mig_188 r1c prior-thy rule annotation.
UPDATE main.canonical_column_verification_registry_v1
SET
    verification_status = 'verified',
    verified_by = 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430',
    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'logan_ratified_AJCC8_R1_plus_r1c_prior_thy_carry_forward_explicit_T0',
    batch_id = 'mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430',
    notes = COALESCE(notes, '') || ' | mig188b+mig184_v2: CF-87-AJCC + r1c LN-only + explicit T0 for pT0/ambiguous buckets; manuscript SQL prefers *_resolved.'
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §I cpm_reconciliation_provenance_v1 row insert.
-- Idempotency: delete prior row for this run_id if re-applying.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
    (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
    ('mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
     'pre_snapshot_add_resolved_cols_event_tnm_pm_stage_group_registry_provenance_r1c_prior_thy_explicit_T0',
     'CF-87-AJCC', 'r1c_residual_ln_only_prior_thy_rule', 'resolved_columns_authored', 'ambiguous_pm_size_only_logan_pending_csv');

-- §J Post-state probes — incl. explicit T0 cohort (mig_188b) and t_resolution_source from §D-prime.
SELECT 'path_event_t8_resolved_T0_count' AS metric, CAST(NULL AS VARCHAR) AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1
WHERE t_stage_ajcc8_resolved = 'T0';
SELECT 'path_event_t0_by_resolution_source' AS metric, ajcc_resolution_source AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1
WHERE t_stage_ajcc8_resolved = 'T0'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
SELECT 'path_event_t8_resolved' AS metric, t_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1, 2 ORDER BY 1, 3 DESC;
SELECT 'path_event_t_resolution_source' AS metric, ajcc_resolution_source AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1, 2 ORDER BY 1, 3 DESC;
SELECT 'path_event_n8_resolved' AS metric, n_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1, 2 ORDER BY 1, 3 DESC;
SELECT 'pm_stage_group_ajcc8_resolved' AS metric, ajcc8_stage_group_resolved AS value, COUNT(*) AS n
FROM main.canonical_patient_master GROUP BY 1, 2 ORDER BY 1, 3 DESC;
SELECT
    COUNT(*) AS paired_pm_ajcc8_stage_group,
    SUM(CASE WHEN ajcc8_stage_group IS DISTINCT FROM ajcc8_stage_group_resolved THEN 1 ELSE 0 END) AS drifted_pm_ajcc8_stage_group
FROM main.canonical_patient_master
WHERE ajcc8_stage_group IS NOT NULL AND ajcc8_stage_group_resolved IS NOT NULL;

-- §K READONLY EXPORT — Path-C: run COPY ... TO or IDE export using these SELECT shells (adjust paths).
--
-- K1 strong prior-thy (~events ≈ patients targeted ~41):
-- COPY (
--   SELECT
--     CAST(e.research_id AS VARCHAR) AS research_id,
--     e.surgery_episode_id,
--     e.tumor_ordinal,
--     e.primary_histology,
--     e.histology_variant,
--     CASE WHEN COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE) THEN 'pshx_nlp_prior_thyroidectomy'
--          WHEN pom.max_other_cm IS NOT NULL THEN 'max_other_canonical_path_event_cm'
--          ELSE 'combined' END AS prior_thy_evidence_source,
--     COALESCE(pom.max_other_cm, TRY_CAST(pm.path_tumor_size_cm AS DOUBLE)) AS prior_path_size_cm_used,
--     e.t_stage_ajcc8_resolved AS t_stage_resolved,
--     e.n_stage_ajcc8_resolved AS n_stage_resolved,
--     pm.ajcc8_stage_group_resolved AS stage_group_resolved
--   FROM main.canonical_path_malignant_events_v1 e
--   INNER JOIN main.canonical_patient_master pm ON CAST(e.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
--   LEFT JOIN (
--     SELECT CAST(e1.research_id AS VARCHAR) AS research_id, e1.surgery_episode_id, e1.tumor_ordinal,
--            MAX(COALESCE(e2.size_greatest_dimension_cm, e2.tumor_size_cm_per_surgery)) AS max_other_cm
--     FROM main.canonical_path_malignant_events_v1 e1
--     INNER JOIN main.canonical_path_malignant_events_v1 e2
--       ON CAST(e1.research_id AS VARCHAR) = CAST(e2.research_id AS VARCHAR)
--      AND COALESCE(e2.size_greatest_dimension_cm, e2.tumor_size_cm_per_surgery) IS NOT NULL
--      AND (e1.surgery_episode_id IS DISTINCT FROM e2.surgery_episode_id OR e1.tumor_ordinal IS DISTINCT FROM e2.tumor_ordinal)
--     GROUP BY 1,2,3
--   ) pom ON CAST(e.research_id AS VARCHAR)=pom.research_id AND e.surgery_episode_id IS NOT DISTINCT FROM pom.surgery_episode_id AND e.tumor_ordinal IS NOT DISTINCT FROM pom.tumor_ordinal
--   WHERE e.ajcc_resolution_source = 'prior_thy_recurrence_T_from_prior_path'
-- ) TO 'exports/mig188_r1c_disposition_20260430/r1c_disposition_strong_prior_thy.csv' (HEADER, DELIMITER ',');
--
-- K2 weak/none (~6 events):
-- COPY ( SELECT ... same projection ... WHERE e.ajcc_resolution_source = 'no_primary_at_this_surgery_pT0_unstaged' )
-- TO 'exports/mig188_r1c_disposition_20260430/r1c_disposition_weak_or_none.csv' (HEADER, DELIMITER ',');
--
-- K3 ambiguous PM-only (~25 events):
-- COPY (
--   SELECT
--     ...,
--     TRY_CAST(pm.path_tumor_size_cm AS DOUBLE) AS pm_path_tumor_size_cm,
--     'canonical_patient_master.path_tumor_size_cm' AS pm_path_size_provenance_hint,
--     CAST(NULL AS VARCHAR) AS logan_review_recommended_t_stage,
--     CAST(NULL AS VARCHAR) AS logan_review_recommended_stage_group,
--     CAST(NULL AS VARCHAR) AS logan_notes
--   FROM ...
--   WHERE e.ajcc_resolution_source = 'ambiguous_pm_size_only_logan_pending'
-- ) TO 'exports/mig188_r1c_disposition_20260430/r1c_disposition_ambiguous_pm_only.csv' (HEADER, DELIMITER ',');
