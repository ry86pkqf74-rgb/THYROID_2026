-- mig_184_v2 R1 AJCC derivation RATIFIED (Logan-ratified 8 rules; supersedes 17b5d8a)
-- Target DB: thyroid_canonical_publication_v1_0
-- Posture: APPLY SKELETON ONLY; authored for Cowork Path-C review/apply. Cursor did NOT execute this file.
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY

USE thyroid_canonical_publication_v1_0;

-- §0 pre-flight invariants
SELECT 'cpm_row_count' AS invariant_name, COUNT(*) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'cpm_distinct_research_id' AS invariant_name, COUNT(DISTINCT research_id) AS observed_value, 10871 AS expected_value
FROM main.canonical_patient_master;
SELECT 'preexisting_mig184_v2_registry_rows' AS invariant_name, COUNT(*) AS observed_value, 0 AS expected_value
FROM main.canonical_column_verification_registry_v1
WHERE batch_id = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430';

-- §A pre-snapshot tables for 36 path-malignant CFs + 9 ETE event_resolved CFs + PM AJCC columns.
CREATE SCHEMA IF NOT EXISTS manuscript_workspace;
CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_path_malignant AS
SELECT
    CURRENT_TIMESTAMP AS snapshot_ts,
    'mig_184_v2_r1_ajcc_derivation_ratified_20260430' AS batch_id,
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

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_patient_master AS
SELECT
    CURRENT_TIMESTAMP AS snapshot_ts,
    'mig_184_v2_r1_ajcc_derivation_ratified_20260430' AS batch_id,
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

CREATE TABLE IF NOT EXISTS manuscript_workspace.mig184_v2_r1_pre_snapshot_registry AS
SELECT CURRENT_TIMESTAMP AS snapshot_ts, *
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §B canonical_path_malignant_events_v1 resolved columns (path-event grain holds T/N/M only; no stage group update here).
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc8_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS t_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS n_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS m_stage_ajcc7_resolved VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_source VARCHAR;
ALTER TABLE main.canonical_path_malignant_events_v1 ADD COLUMN IF NOT EXISTS ajcc_resolution_confidence VARCHAR;

-- §C canonical_patient_master resolved columns (PM grain computes stage group).
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

-- §D T-stage UPDATE — Rules #1, #2, #6, #7.
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
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b') THEN 'microcarcinoma_without_size_default_T1a'
            ELSE 'size_residual_logan_pending'
        END AS t_resolution_source,
        CASE
            WHEN COALESCE(e.size_greatest_dimension_cm, e.tumor_size_cm_per_surgery) IS NOT NULL OR COALESCE(t4.t4_rank, 0) > 0 THEN 'high'
            WHEN regexp_matches(LOWER(COALESCE(e.primary_histology,'') || ' ' || COALESCE(e.histology_variant,'')), 'microcarcinoma|\bptmc\b|anaplastic|\batc\b') THEN 'medium'
            ELSE 'uncalculable'
        END AS t_resolution_confidence
    FROM main.canonical_path_malignant_events_v1 e
    LEFT JOIN t4_invasion t4
      ON CAST(e.research_id AS VARCHAR) = t4.research_id
     AND e.surgery_episode_id IS NOT DISTINCT FROM t4.surgery_episode_id
)
UPDATE main.canonical_path_malignant_events_v1 AS tgt
SET
    t_stage_ajcc8_resolved = src.t_stage_ajcc8_resolved,
    t_stage_ajcc7_resolved = CASE WHEN src.t_stage_ajcc8_resolved = 'T3b' THEN 'T3' ELSE src.t_stage_ajcc8_resolved END,
    ajcc_resolution_source = src.t_resolution_source,
    ajcc_resolution_confidence = src.t_resolution_confidence
FROM event_derivation src
WHERE CAST(tgt.research_id AS VARCHAR) = src.research_id
  AND tgt.surgery_episode_id IS NOT DISTINCT FROM src.surgery_episode_id
  AND tgt.tumor_ordinal IS NOT DISTINCT FROM src.tumor_ordinal;

-- §E N-stage UPDATE — Rule #3.
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

-- §F M-stage UPDATE — copied from current verified patient/event stage where available; M0 default at PM grain unless M1 evidence exists.
UPDATE main.canonical_path_malignant_events_v1
SET
    m_stage_ajcc8_resolved = COALESCE(m_stage_ajcc8, 'M0'),
    m_stage_ajcc7_resolved = COALESCE(m_stage_ajcc7, m_stage_ajcc8, 'M0');

-- §E/F/G PM rollup + PM N split + stage group update.
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
    ajcc_resolution_source = 'mig184_v2_logan_ratified_R1_rules',
    ajcc_resolution_confidence = CASE WHEN src.t8 IS NULL OR src.sg8 IS NULL THEN 'uncalculable_or_pending' ELSE 'high' END,
    cpm_built_at = CURRENT_TIMESTAMP
FROM stage_derivation src
WHERE CAST(pm.research_id AS VARCHAR) = src.research_id;

-- §H Registry note appendix closing CF-87-AJCC on 45 CF rows.
UPDATE main.canonical_column_verification_registry_v1
SET
    verification_status = 'verified',
    verified_by = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430',
    verified_ts = CURRENT_TIMESTAMP,
    verification_method = 'logan_ratified_AJCC8_R1_resolved_derivation_legacy_columns_preserved',
    batch_id = 'mig_184_v2_r1_ajcc_derivation_ratified_20260430',
    notes = COALESCE(notes, '') || ' | mig184_v2: CF-87-AJCC closed by Logan-ratified 8-rule R1 resolved AJCC derivation; legacy stored columns preserved; manuscript SQL should prefer *_resolved.'
WHERE notes ILIKE '%CF-87-AJCC%'
   OR notes ILIKE '%CF-87%AJCC%'
   OR batch_id ILIKE '%CF-87%';

-- §I cpm_reconciliation_provenance_v1 row insert.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
    (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
    ('mig_184_v2_r1_ajcc_derivation_ratified_20260430', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
     'pre_snapshot_add_resolved_cols_event_tnm_pm_stage_group_registry_provenance_post_state',
     'CF-87-AJCC', '45_registry_rows_targeted', 'resolved_columns_authored', 'size_residual_and_csv_review_items');

-- §J Post-state probes.
SELECT 'path_event_t8_resolved' AS metric, t_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT 'path_event_n8_resolved' AS metric, n_stage_ajcc8_resolved AS value, COUNT(*) AS n
FROM main.canonical_path_malignant_events_v1 GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT 'pm_stage_group_ajcc8_resolved' AS metric, ajcc8_stage_group_resolved AS value, COUNT(*) AS n
FROM main.canonical_patient_master GROUP BY 1,2 ORDER BY 1,3 DESC;
SELECT
    COUNT(*) AS paired_pm_ajcc8_stage_group,
    SUM(CASE WHEN ajcc8_stage_group IS DISTINCT FROM ajcc8_stage_group_resolved THEN 1 ELSE 0 END) AS drifted_pm_ajcc8_stage_group
FROM main.canonical_patient_master
WHERE ajcc8_stage_group IS NOT NULL AND ajcc8_stage_group_resolved IS NOT NULL;
