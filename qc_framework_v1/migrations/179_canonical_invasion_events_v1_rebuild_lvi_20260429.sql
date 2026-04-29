-- mig_179: canonical_invasion_events_v1 rebuild for LVI extraction bug
-- CF: CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS
-- Batch: mig_177_events_rebuild_lvi_extraction_20260429
-- Authoring posture: SQL-only artifact. Do NOT execute from Cursor.
-- Execution target: MotherDuck database thyroid_canonical_publication_v1_0.
-- DuckDB/MotherDuck safety: one statement per call; no explicit transaction wrapper.

USE thyroid_canonical_publication_v1_0;

-- -------------------------------------------------------------------------
-- A. Preflight invariants and baseline counts (read-only)
-- -------------------------------------------------------------------------
SELECT
    'PRE_CPM_ROW_INVARIANT' AS check_name,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_distinct_research_id,
    CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_patient_master;

SELECT
    invasion_type,
    finding_status,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_patients
FROM main.canonical_invasion_events_v1
WHERE invasion_type IN ('vascular_microscopic', 'lymphatic_microscopic', 'capsular', 'perineural')
GROUP BY invasion_type, finding_status
ORDER BY invasion_type, finding_status;

-- Required full pre-snapshot. Path C apply should stop if this table already exists
-- unless the operator has verified it is the intended pre-mutation snapshot.
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre_mig177events_20260429 AS
SELECT *
FROM main.canonical_invasion_events_v1;

CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_patient_rollup_v1_pre_mig177events_20260429 AS
SELECT *
FROM main.canonical_invasion_patient_rollup_v1;

SELECT
    'SNAPSHOT_PARITY_EVENTS' AS check_name,
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1) AS live_rows,
    (SELECT COUNT(*) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre_mig177events_20260429) AS archive_rows,
    CASE
        WHEN (SELECT COUNT(*) FROM main.canonical_invasion_events_v1) =
             (SELECT COUNT(*) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre_mig177events_20260429)
        THEN 'PASS' ELSE 'FAIL'
    END AS status;

-- -------------------------------------------------------------------------
-- B. Re-extract LVI from path_synoptics and stage refreshed event table
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.__mig179_canonical_invasion_events_v1_stage AS
WITH existing_events AS (
    SELECT *
    FROM main.canonical_invasion_events_v1
    WHERE NOT (
        source_table = 'main.path_synoptics'
        AND source_kind = 'structured_mig179'
        AND extraction_run_id = 'mig_179_events_rebuild_lvi_extraction_20260429'
    )
),
path_syn_lvi_unpivot AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        TRY_CAST(surg_date AS DATE) AS finding_date,
        1::INTEGER AS tumor_index,
        CAST(tumor_1_lymphatic_invasion AS VARCHAR) AS lymphatic_raw,
        CAST(tumor_1_angioinvasion AS VARCHAR) AS angio_raw,
        CAST(tumor_1_angioinvasion_quantify AS VARCHAR) AS angio_qty_raw,
        CAST(tumor_1_margin_angiolymphatic_invasion_comment AS VARCHAR) AS comment_raw,
        COALESCE(CAST(synoptic_diagnosis AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(path_diagnosis_comment AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(microscopic_description AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(fs_pathology_frozen_section AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(path_special_studies AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(ancillary_studies AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(other_findings AS VARCHAR), '') || '\n' ||
        COALESCE(CAST(tumor_1_margin_angiolymphatic_invasion_comment AS VARCHAR), '') AS source_text
    FROM main.path_synoptics
    UNION ALL
    SELECT TRY_CAST(research_id AS BIGINT), TRY_CAST(surg_date AS DATE), 2::INTEGER,
           CAST(tumor_2_lymphatic_invasion AS VARCHAR), CAST(tumor_2_angioinvasion AS VARCHAR),
           CAST(tumor_2_angioinvasion_quantify AS VARCHAR), CAST(tumor_2_margin_comment AS VARCHAR),
           COALESCE(CAST(synoptic_diagnosis AS VARCHAR), '') || '\n' || COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(path_diagnosis_comment AS VARCHAR), '') || '\n' || COALESCE(CAST(microscopic_description AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(fs_pathology_frozen_section AS VARCHAR), '') || '\n' || COALESCE(CAST(path_special_studies AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(ancillary_studies AS VARCHAR), '') || '\n' || COALESCE(CAST(other_findings AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(tumor_2_margin_comment AS VARCHAR), '')
    FROM main.path_synoptics
    UNION ALL
    SELECT TRY_CAST(research_id AS BIGINT), TRY_CAST(surg_date AS DATE), 3::INTEGER,
           CAST(tumor_3_lymphatic_invasion AS VARCHAR), CAST(tumor_3_angioinvasion AS VARCHAR),
           CAST(tumor_3_angioinvasion_quantify AS VARCHAR), CAST(tumor_3_margin_comment AS VARCHAR),
           COALESCE(CAST(synoptic_diagnosis AS VARCHAR), '') || '\n' || COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(path_diagnosis_comment AS VARCHAR), '') || '\n' || COALESCE(CAST(microscopic_description AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(fs_pathology_frozen_section AS VARCHAR), '') || '\n' || COALESCE(CAST(path_special_studies AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(ancillary_studies AS VARCHAR), '') || '\n' || COALESCE(CAST(other_findings AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(tumor_3_margin_comment AS VARCHAR), '')
    FROM main.path_synoptics
    UNION ALL
    SELECT TRY_CAST(research_id AS BIGINT), TRY_CAST(surg_date AS DATE), 4::INTEGER,
           CAST(tumor_4_lymphatic_invasion AS VARCHAR), CAST(tumor_4_angioinvasion AS VARCHAR),
           CAST(tumor_4_angioinvasion_quantify AS VARCHAR), CAST(tumor_4_margin_comment AS VARCHAR),
           COALESCE(CAST(synoptic_diagnosis AS VARCHAR), '') || '\n' || COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(path_diagnosis_comment AS VARCHAR), '') || '\n' || COALESCE(CAST(microscopic_description AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(fs_pathology_frozen_section AS VARCHAR), '') || '\n' || COALESCE(CAST(path_special_studies AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(ancillary_studies AS VARCHAR), '') || '\n' || COALESCE(CAST(other_findings AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(tumor_4_margin_comment AS VARCHAR), '')
    FROM main.path_synoptics
    UNION ALL
    SELECT TRY_CAST(research_id AS BIGINT), TRY_CAST(surg_date AS DATE), 5::INTEGER,
           CAST(tumor_5_lymphatic_invasion AS VARCHAR), CAST(tumor_5_angioinvasion AS VARCHAR),
           CAST(tumor_5_angioinvasion_quantify AS VARCHAR), CAST(tumor_5_margin_comment AS VARCHAR),
           COALESCE(CAST(synoptic_diagnosis AS VARCHAR), '') || '\n' || COALESCE(CAST(path_diagnosis_summary AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(path_diagnosis_comment AS VARCHAR), '') || '\n' || COALESCE(CAST(microscopic_description AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(fs_pathology_frozen_section AS VARCHAR), '') || '\n' || COALESCE(CAST(path_special_studies AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(ancillary_studies AS VARCHAR), '') || '\n' || COALESCE(CAST(other_findings AS VARCHAR), '') || '\n' ||
           COALESCE(CAST(tumor_5_margin_comment AS VARCHAR), '')
    FROM main.path_synoptics
),
path_syn_lvi_flags AS (
    SELECT
        *,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(lymphatic_raw, ''), '[;.]+$', ''))) AS lymphatic_norm,
        LOWER(TRIM(REGEXP_REPLACE(COALESCE(angio_raw, ''), '[;.]+$', ''))) AS angio_norm,
        LOWER(source_text) AS source_text_lc
    FROM path_syn_lvi_unpivot
    WHERE research_id IS NOT NULL
),
path_syn_lvi_classified AS (
    SELECT
        *,
        (
            lymphatic_norm IN ('present', 'yes', 'true', 'identified', 'positive', 'focal', 'foacl', 'extensive', 'extrensive', 'extensivre', 'extensiver', 'estensive', 'extesive', 'minimal', 'microscopic')
            OR regexp_matches(lymphatic_norm, '^<\s*[0-9]+(\.[0-9]+)?\s*per\s*2\s*mm2?$')
        ) AS lymphatic_structured_present,
        (
            (
                regexp_matches(source_text_lc, '(lymph[ -]?vascular|lymphovascular)\s+invasion\s*:?\s*(is\s+)?(present|yes|identified)')
                OR regexp_matches(source_text_lc, 'angiolymphatic\s+invasion\s*(is\s*)?(present|yes|identified)')
            )
            AND NOT regexp_matches(source_text_lc, '(no|not identified|negative)\s+(lymph[ -]?vascular|lymphovascular|angiolymphatic)\s+invasion')
            AND NOT regexp_matches(source_text_lc, '(lymph[ -]?vascular|lymphovascular|angiolymphatic)\s+invasion\s*:?\s*(not identified|negative|absent|no)')
        ) AS combined_lymphovascular_present,
        (
            regexp_matches(source_text_lc, 'lymphangitic\s+invasion\s*(is\s*)?(present|identified)')
            OR regexp_matches(source_text_lc, 'multifocal\s+lymphangitic\s+invasion\s+present')
        ) AS lymphangitic_present,
        (
            regexp_matches(source_text_lc, 'lymphatic\s+invasion\s*:?\s*(present|yes|identified|<\s*[0-9]+(\.[0-9]+)?\s*per\s*2\s*mm2?)')
            AND NOT regexp_matches(source_text_lc, 'lymphatic\s+invasion\s*:?\s*(not identified|negative|absent|no)')
        ) AS separate_lymphatic_present,
        NULLIF(regexp_extract(lymphatic_norm, '(<\s*[0-9]+(\.[0-9]+)?\s*per\s*2\s*mm2?)', 1), '') AS lymphatic_quantifier
    FROM path_syn_lvi_flags
),
path_syn_lvi_emit AS (
    SELECT 'lymphatic_microscopic' AS invasion_type,
           'mig179_lvi_structured_or_text' AS pattern_name,
           lymphatic_quantifier AS pattern_qualifier,
           *
    FROM path_syn_lvi_classified
    WHERE lymphatic_structured_present
       OR combined_lymphovascular_present
       OR lymphangitic_present
       OR separate_lymphatic_present
    UNION ALL
    SELECT 'vascular_microscopic' AS invasion_type,
           'mig179_combined_lymphovascular' AS pattern_name,
           NULL::VARCHAR AS pattern_qualifier,
           *
    FROM path_syn_lvi_classified
    WHERE combined_lymphovascular_present
),
supplemental_findings AS (
    SELECT
        invasion_type,
        'present' AS finding_status,
        'synoptic_path' AS source_modality,
        'structured_mig179' AS source_kind,
        'main.path_synoptics' AS source_table,
        'rid=' || CAST(research_id AS VARCHAR) || '|surg=' || COALESCE(CAST(finding_date AS VARCHAR), 'NA') || '|tumor=' || CAST(tumor_index AS VARCHAR) || '|pattern=' || pattern_name || '|type=' || invasion_type AS source_row_id,
        research_id,
        finding_date,
        0.90::DOUBLE AS confidence,
        md5(COALESCE(source_text, '')) AS evidence_span_hash,
        TRIM(BOTH '|' FROM 'pattern=' || pattern_name || '|lymphatic_raw=' || COALESCE(lymphatic_raw, '') || '|angio_raw=' || COALESCE(angio_raw, '') || '|angio_qty=' || COALESCE(angio_qty_raw, '') || '|quantifier=' || COALESCE(pattern_qualifier, '')) AS evidence_qualifier,
        'mig_179_events_rebuild_lvi_extraction_20260429' AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM path_syn_lvi_emit
),
linked_supplemental AS (
    SELECT
        f.*,
        (SELECT MIN(oe.surgery_episode_id)
           FROM main.canonical_operative_events_v1 oe
          WHERE TRY_CAST(oe.research_id AS BIGINT) = f.research_id
            AND ABS(DATE_DIFF('day', TRY_CAST(oe.surgery_date_native AS DATE), f.finding_date)) <= 90) AS linked_surgery_episode_id,
        (SELECT MIN(pm.path_surgery_id)
           FROM main.canonical_path_malignant_events_v1 pm
          WHERE TRY_CAST(pm.research_id AS BIGINT) = f.research_id
            AND TRY_CAST(pm.surgery_date AS DATE) = f.finding_date) AS linked_path_malignant_event_id,
        COUNT(*) OVER (PARTITION BY f.research_id, f.finding_date) AS n_candidate_episodes_window
    FROM supplemental_findings f
),
supplemental_events AS (
    SELECT
        md5(CAST(research_id AS VARCHAR) || '|' || source_modality || '|' || source_kind || '|' || source_table || '|' || source_row_id || '|' || invasion_type) AS invasion_event_id,
        research_id,
        invasion_type,
        finding_status,
        source_modality,
        source_kind,
        source_table,
        source_row_id,
        finding_date,
        linked_surgery_episode_id,
        linked_path_malignant_event_id,
        CASE
            WHEN linked_surgery_episode_id IS NULL THEN 'unlinked'
            WHEN n_candidate_episodes_window > 1 THEN 'temporal_90d_ambiguous'
            ELSE 'temporal_90d'
        END AS linkage_method,
        CAST(n_candidate_episodes_window AS INTEGER) AS n_candidate_episodes,
        (n_candidate_episodes_window > 1) AS linkage_ambiguous_multi_finding,
        confidence,
        evidence_span_hash,
        evidence_qualifier,
        extraction_run_id,
        '179'::VARCHAR AS build_script,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
    FROM linked_supplemental
    WHERE research_id IS NOT NULL
      AND invasion_type IS NOT NULL
)
SELECT * FROM existing_events
UNION ALL
SELECT * FROM supplemental_events;

SELECT
    source_kind,
    invasion_type,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_patients
FROM main.__mig179_canonical_invasion_events_v1_stage
WHERE source_kind = 'structured_mig179'
GROUP BY source_kind, invasion_type
ORDER BY invasion_type;

CREATE OR REPLACE TABLE main.canonical_invasion_events_v1 AS
SELECT * FROM main.__mig179_canonical_invasion_events_v1_stage;

DROP TABLE main.__mig179_canonical_invasion_events_v1_stage;

COMMENT ON TABLE main.canonical_invasion_events_v1 IS '[domain=invasion_findings; grain=per_invasion_mention] — source: script 363 + mig_179 (2026-04-29). Cross-modal canonical invasion findings with supplemental path_synoptics LVI re-extract for combined lymph-vascular/angiolymphatic, separate lymphatic, lymphangitic, and quantitative LVI patterns.';

-- -------------------------------------------------------------------------
-- C/E. Rebuild patient rollup from refreshed events
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.canonical_invasion_patient_rollup_v1 AS
SELECT
    research_id,
    BOOL_OR(invasion_type='gross_ete' AND finding_status='present') AS any_gross_ete_anywhere,
    BOOL_OR(invasion_type='gross_ete' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_gross_ete_in_op_or_path,
    BOOL_OR(invasion_type='gross_ete' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_gross_ete_in_imaging,
    BOOL_OR(invasion_type='microscopic_ete' AND finding_status='present') AS any_microscopic_ete_anywhere,
    BOOL_OR(invasion_type='microscopic_ete' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_microscopic_ete_in_op_or_path,
    BOOL_OR(invasion_type='microscopic_ete' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_microscopic_ete_in_imaging,
    BOOL_OR(invasion_type='ete_present_not_further_specified' AND finding_status='present') AS any_ete_present_not_further_specified_anywhere,
    BOOL_OR(invasion_type='ete_present_not_further_specified' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_ete_present_not_further_specified_in_op_or_path,
    BOOL_OR(invasion_type='ete_present_not_further_specified' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_ete_present_not_further_specified_in_imaging,
    BOOL_OR(invasion_type='vascular_microscopic' AND finding_status='present') AS any_vascular_microscopic_anywhere,
    BOOL_OR(invasion_type='vascular_microscopic' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_vascular_microscopic_in_op_or_path,
    BOOL_OR(invasion_type='vascular_microscopic' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_vascular_microscopic_in_imaging,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='present') AS any_lymphatic_microscopic_anywhere,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_lymphatic_microscopic_in_op_or_path,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_lymphatic_microscopic_in_imaging,
    BOOL_OR(invasion_type='capsular' AND finding_status='present') AS any_capsular_anywhere,
    BOOL_OR(invasion_type='capsular' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_capsular_in_op_or_path,
    BOOL_OR(invasion_type='capsular' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_capsular_in_imaging,
    BOOL_OR(invasion_type='perineural' AND finding_status='present') AS any_perineural_anywhere,
    BOOL_OR(invasion_type='perineural' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_perineural_in_op_or_path,
    BOOL_OR(invasion_type='perineural' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_perineural_in_imaging,
    BOOL_OR(invasion_type='soft_tissue' AND finding_status='present') AS any_soft_tissue_anywhere,
    BOOL_OR(invasion_type='soft_tissue' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_soft_tissue_in_op_or_path,
    BOOL_OR(invasion_type='soft_tissue' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_soft_tissue_in_imaging,
    BOOL_OR(invasion_type='airway' AND finding_status='present') AS any_airway_anywhere,
    BOOL_OR(invasion_type='airway' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_airway_in_op_or_path,
    BOOL_OR(invasion_type='airway' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_airway_in_imaging,
    BOOL_OR(invasion_type='tracheal' AND finding_status='present') AS any_tracheal_anywhere,
    BOOL_OR(invasion_type='tracheal' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_tracheal_in_op_or_path,
    BOOL_OR(invasion_type='tracheal' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_tracheal_in_imaging,
    BOOL_OR(invasion_type='esophageal' AND finding_status='present') AS any_esophageal_anywhere,
    BOOL_OR(invasion_type='esophageal' AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_esophageal_in_op_or_path,
    BOOL_OR(invasion_type='esophageal' AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_esophageal_in_imaging,
    BOOL_OR(invasion_type IN ('gross_ete','microscopic_ete','ete_present_not_further_specified','soft_tissue') AND finding_status='present') AS any_ete_anywhere,
    BOOL_OR(invasion_type IN ('gross_ete','microscopic_ete','ete_present_not_further_specified','soft_tissue') AND finding_status='present' AND source_modality IN ('op_note','synoptic_path','narrative_path','frozen_section')) AS any_ete_in_op_or_path,
    BOOL_OR(invasion_type IN ('gross_ete','microscopic_ete','ete_present_not_further_specified','soft_tissue') AND finding_status='present' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_ete_in_imaging,
    '179'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM main.canonical_invasion_events_v1
GROUP BY research_id;

COMMENT ON TABLE main.canonical_invasion_patient_rollup_v1 IS '[domain=invasion_findings; grain=per_patient] — source: script 363 + mig_179 (2026-04-29). Patient-level invasion finding rollup rebuilt from refreshed canonical_invasion_events_v1.';

-- -------------------------------------------------------------------------
-- D. Registry resync for refreshed event and rollup tables
-- -------------------------------------------------------------------------
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name IN ('canonical_invasion_events_v1', 'canonical_invasion_patient_rollup_v1')
  AND schema_name = 'main';

INSERT INTO manuscript_workspace.detail_table_registry_v1 BY NAME
SELECT
    'canonical_invasion_events_v1' AS detail_table_name,
    'main' AS schema_name,
    'research_id' AS join_key,
    'per_invasion_mention' AS grain,
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1) AS total_rows,
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_invasion_events_v1) AS total_patients,
    'invasion_findings' AS domain,
    NULL::VARCHAR AS feeds_master_columns,
    '[domain=invasion_findings; grain=per_invasion_mention] — source: script 363 + mig_179 (2026-04-29). LVI re-extract from path_synoptics added for combined lymph-vascular/angiolymphatic and lymphatic patterns.' AS description,
    'v1_0_mig179' AS canonical_version,
    NULL::VARCHAR AS feeds_master_columns_secondary,
    NULL::VARCHAR AS feeds_master_columns_array,
    FALSE AS needs_manual_review;

INSERT INTO manuscript_workspace.detail_table_registry_v1 BY NAME
SELECT
    'canonical_invasion_patient_rollup_v1' AS detail_table_name,
    'main' AS schema_name,
    'research_id' AS join_key,
    'per_patient' AS grain,
    (SELECT COUNT(*) FROM main.canonical_invasion_patient_rollup_v1) AS total_rows,
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_invasion_patient_rollup_v1) AS total_patients,
    'invasion_findings' AS domain,
    NULL::VARCHAR AS feeds_master_columns,
    '[domain=invasion_findings; grain=per_patient] — source: script 363 + mig_179 (2026-04-29). Rebuilt from refreshed canonical_invasion_events_v1.' AS description,
    'v1_0_mig179' AS canonical_version,
    NULL::VARCHAR AS feeds_master_columns_secondary,
    NULL::VARCHAR AS feeds_master_columns_array,
    FALSE AS needs_manual_review;

-- -------------------------------------------------------------------------
-- Post-state verification gates
-- -------------------------------------------------------------------------
SELECT
    'POST_CPM_ROW_INVARIANT' AS check_name,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_distinct_research_id,
    CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_patient_master;

SELECT
    invasion_type,
    finding_status,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_patients
FROM main.canonical_invasion_events_v1
WHERE invasion_type IN ('vascular_microscopic', 'lymphatic_microscopic', 'capsular', 'perineural')
GROUP BY invasion_type, finding_status
ORDER BY invasion_type, finding_status;

SELECT
    'POST_MIN_VASCULAR_PRESENT_GATE' AS check_name,
    COUNT(*) AS present_rows,
    COUNT(DISTINCT research_id) AS present_patients,
    CASE WHEN COUNT(*) >= 2883 AND COUNT(DISTINCT research_id) >= 1109 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_invasion_events_v1
WHERE invasion_type = 'vascular_microscopic'
  AND finding_status = 'present';

SELECT
    'POST_LYMPHATIC_GROWTH_GATE' AS check_name,
    COUNT(*) AS present_rows,
    COUNT(DISTINCT research_id) AS present_patients,
    CASE WHEN COUNT(*) > 1233 AND COUNT(DISTINCT research_id) > 780 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_invasion_events_v1
WHERE invasion_type = 'lymphatic_microscopic'
  AND finding_status = 'present';

SELECT
    'POST_ROLLUP_ROW_INVARIANT' AS check_name,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT research_id) AS n_distinct_research_id,
    CASE WHEN COUNT(*) = 10871 AND COUNT(DISTINCT research_id) = 10871 THEN 'PASS' ELSE 'FAIL' END AS status
FROM main.canonical_invasion_patient_rollup_v1;

SELECT
    'POST_ROLLUP_REDERIVED_FROM_EVENTS' AS check_name,
    SUM(CASE WHEN r.any_lymphatic_microscopic_anywhere IS DISTINCT FROM e.any_lymphatic_microscopic_anywhere THEN 1 ELSE 0 END) AS lymphatic_rollup_mismatches,
    SUM(CASE WHEN r.any_vascular_microscopic_anywhere IS DISTINCT FROM e.any_vascular_microscopic_anywhere THEN 1 ELSE 0 END) AS vascular_rollup_mismatches,
    CASE
        WHEN SUM(CASE WHEN r.any_lymphatic_microscopic_anywhere IS DISTINCT FROM e.any_lymphatic_microscopic_anywhere THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN r.any_vascular_microscopic_anywhere IS DISTINCT FROM e.any_vascular_microscopic_anywhere THEN 1 ELSE 0 END) = 0
        THEN 'PASS' ELSE 'FAIL'
    END AS status
FROM main.canonical_invasion_patient_rollup_v1 r
JOIN (
    SELECT
        research_id,
        BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present') AS any_lymphatic_microscopic_anywhere,
        BOOL_OR(invasion_type = 'vascular_microscopic' AND finding_status = 'present') AS any_vascular_microscopic_anywhere
    FROM main.canonical_invasion_events_v1
    GROUP BY research_id
) e USING (research_id);
