-- tumor_episode_master_v2
CREATE OR REPLACE TABLE tumor_episode_master_v2 AS
WITH ps AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(surg_date AS DATE)) AS surgery_episode_id,
        thyroid_procedure AS procedure_raw,
        -- tumor 1 fields from synoptics (column names reflect actual path_synoptics schema)
        tumor_1_histologic_type AS ps_histology_1,
        tumor_1_variant AS ps_variant_1,
        tumor_1_size_greatest_dimension_cm AS ps_size_cm_1,
        tumor_1_extrathyroidal_extension AS ps_ete_1,
        tumor_1_margin_status AS ps_margins_1,
        tumor_1_angioinvasion AS ps_vasc_inv_1,
        tumor_1_lymphatic_invasion AS ps_lymph_inv_1,
        tumor_1_perineural_invasion AS ps_perineural_1,
        tumor_1_capsular_invasion AS ps_capsular_inv_1,
        -- AJCC staging columns from path_synoptics
        tumor_1_pt AS ps_t_stage,
        tumor_1_pn AS ps_n_stage,
        tumor_1_pm AS ps_m_stage,
        NULL::VARCHAR AS ps_overall_stage,
        -- node counts
        tumor_1_ln_examined AS ps_nodes_total,
        tumor_1_ln_involved AS ps_nodes_positive,
        tumor_1_extranodal_extension AS ps_extranodal_ext,
        -- laterality
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%right%' THEN 'right'
            WHEN LOWER(thyroid_procedure) LIKE '%left%' THEN 'left'
            WHEN LOWER(thyroid_procedure) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(thyroid_procedure) LIKE '%total%' THEN 'bilateral'
            ELSE NULL
        END AS ps_laterality,
        -- multifocality
        tumor_1_multiple_tumor AS ps_tumor_count,
        CASE WHEN LOWER(COALESCE(tumor_1_multiple_tumor,'')) LIKE '%yes%'
                  OR LOWER(COALESCE(tumor_1_multiple_tumor,'')) LIKE '%multi%'
             THEN TRUE ELSE FALSE END AS ps_multifocal,
        -- consult / diagnosis summary
        path_diagnosis_summary AS ps_consult_diagnosis
    FROM path_synoptics
    WHERE research_id IS NOT NULL
),
tp AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        histology_1_type AS tp_histology,
        tumor_1_histology_variant AS tp_variant,
        histology_1_t_stage_ajcc8 AS tp_t_stage,
        histology_1_n_stage_ajcc8 AS tp_n_stage,
        histology_1_m_stage_ajcc8 AS tp_m_stage,
        NULL::VARCHAR AS tp_ete,
        NULL::VARCHAR AS tp_gross_ete,
        histology_1_largest_tumor_cm AS tp_size_cm,
        NULL::DATE AS tp_surgery_date
    FROM tumor_pathology
    WHERE research_id IS NOT NULL
)
SELECT
    ps.research_id,
    ps.surgery_episode_id,
    1 AS tumor_ordinal,
    ps.surgery_date,
    CASE
        WHEN ps.surgery_date IS NOT NULL THEN 'exact_source_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE
        WHEN ps.surgery_date IS NOT NULL THEN 100
        ELSE 0
    END AS date_confidence,
    -- histology with precedence: synoptic > tumor_pathology
    COALESCE(ps.ps_histology_1, tp.tp_histology) AS primary_histology,
    COALESCE(ps.ps_variant_1, tp.tp_variant) AS histology_variant,
    CASE
        WHEN ps.ps_histology_1 IS NOT NULL THEN 'path_synoptics'
        WHEN tp.tp_histology IS NOT NULL THEN 'tumor_pathology'
        ELSE NULL
    END AS histology_source,
    -- staging with precedence
    COALESCE(ps.ps_t_stage, tp.tp_t_stage) AS t_stage,
    COALESCE(ps.ps_n_stage, tp.tp_n_stage) AS n_stage,
    COALESCE(ps.ps_m_stage, tp.tp_m_stage) AS m_stage,
    ps.ps_overall_stage AS overall_stage,
    -- size
    COALESCE(TRY_CAST(ps.ps_size_cm_1 AS DOUBLE), TRY_CAST(tp.tp_size_cm AS DOUBLE)) AS tumor_size_cm,
    -- invasion/margin details
    COALESCE(ps.ps_ete_1, tp.tp_ete) AS extrathyroidal_extension,
    tp.tp_gross_ete AS gross_ete,
    ps.ps_vasc_inv_1 AS vascular_invasion,
    ps.ps_lymph_inv_1 AS lymphatic_invasion,
    ps.ps_perineural_1 AS perineural_invasion,
    ps.ps_capsular_inv_1 AS capsular_invasion,
    ps.ps_margins_1 AS margin_status,
    -- nodal disease
    TRY_CAST(ps.ps_nodes_positive AS INTEGER) AS nodal_disease_positive_count,
    TRY_CAST(ps.ps_nodes_total AS INTEGER) AS nodal_disease_total_count,
    ps.ps_extranodal_ext AS extranodal_extension,
    -- laterality / multifocality
    ps.ps_laterality AS laterality,
    TRY_CAST(ps.ps_tumor_count AS INTEGER) AS number_of_tumors,
    ps.ps_multifocal AS multifocality_flag,
    -- consult precedence
    ps.ps_consult_diagnosis AS consult_diagnosis,
    CASE WHEN ps.ps_consult_diagnosis IS NOT NULL THEN TRUE ELSE FALSE END AS consult_precedence_flag,
    -- discordance flags
    CASE WHEN ps.ps_histology_1 IS NOT NULL AND tp.tp_histology IS NOT NULL
         AND LOWER(TRIM(ps.ps_histology_1)) != LOWER(TRIM(tp.tp_histology))
         THEN TRUE ELSE FALSE END AS histology_discordance_flag,
    CASE WHEN ps.ps_t_stage IS NOT NULL AND tp.tp_t_stage IS NOT NULL
         AND UPPER(TRIM(ps.ps_t_stage)) != UPPER(TRIM(tp.tp_t_stage))
         THEN TRUE ELSE FALSE END AS t_stage_discordance_flag,
    -- confidence rank (1=synoptic, 2=tumor_path, 3=note-derived)
    CASE
        WHEN ps.ps_histology_1 IS NOT NULL THEN 1
        WHEN tp.tp_histology IS NOT NULL THEN 2
        ELSE 3
    END AS confidence_rank,
    -- provenance
    'path_synoptics+tumor_pathology' AS source_tables,
    ps.procedure_raw
FROM ps
LEFT JOIN tp
    ON ps.research_id = tp.research_id
    AND (ps.surgery_date = tp.tp_surgery_date
         OR tp.tp_surgery_date IS NULL
         OR ps.surgery_date IS NULL);

-- molecular_test_episode_v2
CREATE OR REPLACE TABLE molecular_test_episode_v2 AS
WITH mt AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY COALESCE(
                               TRY_CAST(date AS DATE),
                               DATE '2099-01-01')) AS molecular_episode_id,
        thyroseq_afirma AS platform_raw,
        CASE
            WHEN LOWER(thyroseq_afirma) LIKE '%thyroseq%' THEN 'ThyroSeq'
            WHEN LOWER(thyroseq_afirma) LIKE '%afirma%' THEN 'Afirma'
            ELSE 'Other'
        END AS platform,
        result,
        mutation,
        COALESCE(detailed_findings, '') AS detailed_findings_raw,
        TRY_CAST(date AS DATE) AS test_date_native,
        CASE
            WHEN TRY_CAST(date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(date AS DATE) IS NOT NULL THEN 100
            ELSE 0
        END AS date_confidence,
        -- result classification
        CASE
            WHEN LOWER(result) IN ('positive', 'detected', 'abnormal') THEN 'positive'
            WHEN LOWER(result) IN ('negative', 'not detected', 'normal', 'benign') THEN 'negative'
            WHEN LOWER(result) LIKE '%suspicious%' THEN 'suspicious'
            WHEN LOWER(result) LIKE '%indeterminate%' THEN 'indeterminate'
            WHEN LOWER(result) LIKE '%insufficient%' OR LOWER(result) LIKE '%inadequate%'
                 OR LOWER(result) LIKE '%non-diagnostic%' THEN 'non_diagnostic'
            WHEN LOWER(result) LIKE '%cancel%' THEN 'cancelled'
            ELSE 'other'
        END AS overall_result_class,
        -- mutation flags via regex on mutation+detailed_findings
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%braf%v600%' THEN TRUE
             WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%braf%' THEN TRUE
             ELSE FALSE END AS braf_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%v600e%' THEN 'V600E'
             WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%v600%' THEN 'V600'
             ELSE NULL END AS braf_variant,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(nras|hras|kras|ras)' THEN TRUE ELSE FALSE END AS ras_flag,
        CASE
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%nras%' THEN 'NRAS'
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%hras%' THEN 'HRAS'
            WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
                 LIKE '%kras%' THEN 'KRAS'
            ELSE NULL
        END AS ras_subtype,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ 'ret' THEN TRUE ELSE FALSE END AS ret_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ 'ret[/\s-]*(ptc|fusion)' THEN TRUE ELSE FALSE END AS ret_fusion_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%tert%' THEN TRUE ELSE FALSE END AS tert_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%ntrk%' THEN TRUE ELSE FALSE END AS ntrk_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%eif1ax%' THEN TRUE ELSE FALSE END AS eif1ax_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%tp53%' THEN TRUE ELSE FALSE END AS tp53_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%pax8%pparg%' THEN TRUE ELSE FALSE END AS pax8_pparg_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(copy.?number|cna|amplif|delet)' THEN TRUE ELSE FALSE END AS cna_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%fusion%' THEN TRUE ELSE FALSE END AS fusion_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(loss.?of.?heterozygos|loh)' THEN TRUE ELSE FALSE END AS loh_flag,
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%alk%' THEN TRUE ELSE FALSE END AS alk_flag,
        -- high-risk marker composite
        CASE WHEN LOWER(COALESCE(mutation,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(braf.*v600|tert|tp53|alk.*fusion|ret.*fusion|ntrk.*fusion)'
             THEN TRUE ELSE FALSE END AS high_risk_marker_flag,
        -- inadequate / cancelled
        CASE WHEN LOWER(COALESCE(result,'') || ' ' || COALESCE(detailed_findings,''))
             ~ '(insufficient|inadequate|low.?cellularity|non.?diagnostic|qns)'
             THEN TRUE ELSE FALSE END AS inadequate_flag,
        CASE WHEN LOWER(COALESCE(result,'') || ' ' || COALESCE(detailed_findings,''))
             LIKE '%cancel%' THEN TRUE ELSE FALSE END AS cancelled_flag,
        'molecular_testing' AS source_table
    FROM molecular_testing
    WHERE research_id IS NOT NULL
)
SELECT
    mt.*,
    TRY_CAST(mt.test_date_native AS VARCHAR) AS resolved_test_date,
    NULL::VARCHAR AS linked_fna_episode_id,
    NULL::VARCHAR AS linked_nodule_id,
    NULL::VARCHAR AS linked_surgery_episode_id,
    NULL::VARCHAR AS specimen_site_raw,
    NULL::VARCHAR AS specimen_site_normalized,
    NULL::INTEGER AS bethesda_category,
    NULL::VARCHAR AS platform_version,
    NULL::VARCHAR AS risk_language_raw,
    NULL::DOUBLE  AS molecular_confidence,
    'pending' AS adjudication_status
FROM mt;

-- rai_treatment_episode_v2
CREATE OR REPLACE TABLE rai_treatment_episode_v2 AS
WITH rai_notes AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        entity_value_raw,
        entity_value_norm,
        entity_date,
        note_date,
        present_or_negated,
        confidence,
        note_row_id,
        note_type
    FROM note_entities_medications
    WHERE LOWER(entity_value_norm) LIKE 'rai%'
       OR LOWER(entity_value_norm) LIKE 'i-131%'
       OR LOWER(entity_value_norm) LIKE 'i131%'
       OR LOWER(entity_value_norm) LIKE '%radioactive%iodine%'
       OR LOWER(entity_value_raw) LIKE '%radioactive%iodine%'
       OR LOWER(entity_value_raw) LIKE '%rai%'
       OR LOWER(entity_value_raw) LIKE '%i-131%'
       OR LOWER(entity_value_raw) LIKE '%131-i%'
       OR LOWER(entity_value_raw) LIKE '%131i%'
),
rai_parsed AS (
    SELECT
        research_id,
        ROW_NUMBER() OVER (PARTITION BY research_id
                           ORDER BY COALESCE(
                               TRY_CAST(entity_date AS DATE),
                               TRY_CAST(note_date AS DATE),
                               DATE '2099-01-01')) AS rai_episode_id,
        entity_value_raw AS rai_mention_raw,
        entity_value_norm AS rai_term_normalized,
        TRY_CAST(entity_date AS DATE) AS rai_date_native,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        COALESCE(TRY_CAST(entity_date AS DATE),
                 TRY_CAST(note_date AS DATE)) AS resolved_rai_date,
        CASE
            WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 'inferred_day_level_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(entity_date AS DATE) IS NOT NULL THEN 100
            WHEN TRY_CAST(note_date AS DATE) IS NOT NULL THEN 70
            ELSE 0
        END AS date_confidence,
        -- dose extraction from raw text
        TRY_CAST(
            regexp_extract(entity_value_raw, '(\d+\.?\d*)\s*(?:mCi|GBq|millicuries)', 1)
            AS DOUBLE
        ) AS dose_mci,
        entity_value_raw AS dose_text_raw,
        -- assertion status
        CASE
            WHEN present_or_negated = 'negated' THEN 'negated'
            WHEN LOWER(entity_value_raw) ~ '(recommend|consider|plan|discuss|would benefit)'
                 THEN 'planned'
            WHEN LOWER(entity_value_raw) ~ '(received|treated|administered|given|underwent|completed)'
                 THEN 'definite_received'
            WHEN LOWER(entity_value_raw) ~ '(schedul|upcoming|will receive|to receive)'
                 THEN 'planned'
            WHEN LOWER(entity_value_raw) ~ '(history|previous|prior|past)'
                 THEN 'historical'
            WHEN present_or_negated = 'present' AND
                 TRY_CAST(regexp_extract(entity_value_raw, '(\d+\.?\d*)\s*(?:mCi|GBq)', 1) AS DOUBLE) IS NOT NULL
                 THEN 'likely_received'
            ELSE 'ambiguous'
        END AS rai_assertion_status,
        -- treatment intent
        CASE
            WHEN LOWER(entity_value_raw) ~ '(remnant.?ablat|ablat)' THEN 'remnant_ablation'
            WHEN LOWER(entity_value_raw) ~ '(adjuvant|additional)' THEN 'adjuvant'
            WHEN LOWER(entity_value_raw) ~ '(metasta|distant|persistent.?disease)' THEN 'metastatic_disease'
            WHEN LOWER(entity_value_raw) ~ '(recur)' THEN 'recurrence'
            ELSE 'unknown'
        END AS rai_intent,
        -- completion status
        CASE
            WHEN present_or_negated = 'negated' THEN 'not_received'
            WHEN LOWER(entity_value_raw) ~ '(received|treated|administered|given|underwent|completed)'
                 THEN 'completed'
            WHEN LOWER(entity_value_raw) ~ '(recommend|consider|plan|discuss|schedul)'
                 THEN 'recommended'
            ELSE 'uncertain'
        END AS completion_status,
        confidence AS rai_confidence,
        note_row_id AS source_note_id,
        note_type AS source_note_type,
        'note_entities_medications' AS source_table,
        'pending' AS adjudication_status
    FROM rai_notes
)
SELECT
    rp.*,
    NULL::VARCHAR AS linked_surgery_episode_id,
    NULL::VARCHAR AS linked_pathology_episode_id,
    NULL::VARCHAR AS linked_recurrence_episode_id,
    FALSE AS pre_scan_flag,
    FALSE AS post_therapy_scan_flag,
    NULL::VARCHAR AS scan_findings_raw,
    FALSE AS iodine_avidity_flag,
    NULL::DOUBLE AS stimulated_tg,
    NULL::DOUBLE AS stimulated_tsh
FROM rai_parsed rp;

-- imaging_nodule_long_v2
CREATE OR REPLACE TABLE imaging_nodule_long_v2 AS
WITH us_unpivot AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        'US' AS modality,
        TRY_CAST(us_date AS DATE) AS exam_date_native,
        CASE
            WHEN TRY_CAST(us_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        CASE
            WHEN TRY_CAST(us_date AS DATE) IS NOT NULL THEN 100
            ELSE 0
        END AS date_confidence,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(us_date AS DATE)) AS imaging_exam_id,
        1 AS nodule_index_within_exam,
        NULL::VARCHAR AS composition,
        NULL::VARCHAR AS echogenicity,
        NULL::VARCHAR AS shape,
        NULL::VARCHAR AS margins,
        NULL::VARCHAR AS calcifications,
        NULL::INTEGER AS tirads_score,
        NULL::VARCHAR AS tirads_category,
        TRY_CAST(dominant_nodule_size_on_us AS DOUBLE) AS size_cm_max,
        NULL::DOUBLE AS size_cm_x,
        NULL::DOUBLE AS size_cm_y,
        NULL::DOUBLE AS size_cm_z,
        dominant_nodule_location AS laterality,
        dominant_nodule_location AS location_detail,
        'serial_imaging_us' AS report_source_table,
        COALESCE(us_findings_impression, us_impression) AS exam_impression_raw,
        FALSE AS suspicious_node_flag,
        NULL::VARCHAR AS suspicious_node_details,
        FALSE AS growth_flag,
        TRUE AS dominant_nodule_flag
    FROM serial_imaging_us
    WHERE research_id IS NOT NULL
      AND us_date IS NOT NULL
),
all_nodules AS (
    SELECT * FROM us_unpivot
)
SELECT
    an.*,
    CAST(an.research_id AS VARCHAR) || '-' || an.modality || '-' ||
        CAST(an.imaging_exam_id AS VARCHAR) || '-' ||
        CAST(an.nodule_index_within_exam AS VARCHAR) AS nodule_id,
    an.exam_date_native AS resolved_exam_date,
    NULL::INTEGER AS nodule_count_in_exam,
    NULL::DOUBLE AS imaging_confidence,
    NULL::VARCHAR AS linked_fna_episode_id,
    NULL::VARCHAR AS linked_molecular_episode_id,
    NULL::VARCHAR AS linked_pathology_tumor_id
FROM all_nodules an;

-- imaging_exam_summary_v2
CREATE OR REPLACE TABLE imaging_exam_summary_v2 AS
SELECT
    research_id,
    modality,
    imaging_exam_id,
    exam_date_native,
    resolved_exam_date,
    date_status,
    date_confidence,
    report_source_table,
    COUNT(*) AS nodule_count,
    MAX(size_cm_max) AS max_nodule_size_cm,
    MAX(tirads_score) AS max_tirads_score,
    BOOL_OR(suspicious_node_flag) AS any_suspicious_node,
    BOOL_OR(growth_flag) AS any_growth_noted
FROM imaging_nodule_long_v2
GROUP BY research_id, modality, imaging_exam_id, exam_date_native,
         resolved_exam_date, date_status, date_confidence, report_source_table;

-- operative_episode_detail_v2
CREATE OR REPLACE TABLE operative_episode_detail_v2 AS
WITH ops AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date_native,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY TRY_CAST(surg_date AS DATE)) AS surgery_episode_id,
        CASE
            WHEN TRY_CAST(surg_date AS DATE) IS NOT NULL THEN 'exact_source_date'
            ELSE 'unresolved_date'
        END AS date_status,
        preop_diagnosis_operative_sheet_not_true_preop_dx AS procedure_raw,
        side_of_largest_tumor_or_goiter AS laterality_raw,
        CASE
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%right%' THEN 'right'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%left%' THEN 'left'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%bilateral%' THEN 'bilateral'
            WHEN LOWER(COALESCE(side_of_largest_tumor_or_goiter,'')) LIKE '%isthmus%' THEN 'isthmus'
            ELSE NULL
        END AS laterality,
        TRY_CAST(ebl AS DOUBLE) AS ebl_ml,
        skin_skin_time_min AS skin_to_skin_time
    FROM operative_details
    WHERE research_id IS NOT NULL
),
ps_proc AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS ps_surgery_date,
        thyroid_procedure AS ps_procedure,
        -- procedure normalization
        CASE
            WHEN LOWER(thyroid_procedure) LIKE '%total thyroidectomy%' THEN 'total_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%near-total%' THEN 'total_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%completion%' THEN 'completion_thyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%lobectomy%' THEN 'hemithyroidectomy'
            WHEN LOWER(thyroid_procedure) LIKE '%hemithyroidectomy%' THEN 'hemithyroidectomy'
            ELSE 'other'
        END AS procedure_normalized,
        -- neck dissection flags from procedure text
        CASE WHEN LOWER(thyroid_procedure) ~ '(central.?neck|level.?vi|cnd)' THEN TRUE ELSE FALSE END AS cnd_from_ps,
        CASE WHEN LOWER(thyroid_procedure) ~ '(lateral.?neck|lnd|mrnd|modified.?radical)' THEN TRUE ELSE FALSE END AS lnd_from_ps
    FROM path_synoptics
    WHERE research_id IS NOT NULL
)
SELECT
    ops.research_id,
    ops.surgery_episode_id,
    ops.surgery_date_native,
    TRY_CAST(ops.surgery_date_native AS VARCHAR) AS resolved_surgery_date,
    ops.date_status,
    ops.procedure_raw,
    COALESCE(ps.procedure_normalized, 'unknown') AS procedure_normalized,
    ops.laterality,
    COALESCE(ps.cnd_from_ps, FALSE) AS central_neck_dissection_flag,
    COALESCE(ps.lnd_from_ps, FALSE) AS lateral_neck_dissection_flag,
    FALSE AS rln_monitoring_flag,
    NULL::VARCHAR AS rln_finding_raw,
    FALSE AS parathyroid_autograft_flag,
    NULL::INTEGER AS parathyroid_autograft_count,
    NULL::VARCHAR AS parathyroid_autograft_site,
    FALSE AS parathyroid_resection_flag,
    FALSE AS gross_ete_flag,
    FALSE AS local_invasion_flag,
    FALSE AS tracheal_involvement_flag,
    FALSE AS esophageal_involvement_flag,
    FALSE AS strap_muscle_involvement_flag,
    FALSE AS reoperative_field_flag,
    ops.ebl_ml,
    FALSE AS drain_flag,
    NULL::VARCHAR AS operative_findings_raw,
    'operative_details+path_synoptics' AS source_tables,
    NULL::DOUBLE AS op_confidence
FROM ops
LEFT JOIN ps_proc ps
    ON ops.research_id = ps.research_id
    AND (ops.surgery_date_native = ps.ps_surgery_date
         OR ps.ps_surgery_date IS NULL
         OR ops.surgery_date_native IS NULL);

-- fna_episode_master_v2
CREATE OR REPLACE TABLE fna_episode_master_v2 AS
SELECT
    CAST(research_id AS INTEGER) AS research_id,
    ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                       ORDER BY COALESCE(
                           TRY_CAST(fna_date_parsed AS DATE),
                           TRY_CAST(date AS DATE),
                           DATE '2099-01-01')) AS fna_episode_id,
    TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) AS fna_date_native,
    TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) AS resolved_fna_date,
    CASE
        WHEN TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) IS NOT NULL
             THEN 'exact_source_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE
        WHEN TRY_CAST(COALESCE(fna_date_parsed, date) AS DATE) IS NOT NULL THEN 100
        ELSE 0
    END AS date_confidence,
    bethesda AS bethesda_raw,
    TRY_CAST(bethesda AS INTEGER) AS bethesda_category,
    path AS pathology_diagnosis,
    path_extended AS pathology_extended,
    COALESCE(preop_specimen_received_fna_location, specimen) AS specimen_site_raw,
    CASE
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%right%' THEN 'right'
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%left%' THEN 'left'
        WHEN LOWER(COALESCE(preop_specimen_received_fna_location, specimen,'')) LIKE '%isthmus%' THEN 'isthmus'
        ELSE NULL
    END AS laterality,
    NULL::VARCHAR AS linked_molecular_episode_id,
    NULL::VARCHAR AS linked_imaging_nodule_id,
    NULL::VARCHAR AS linked_surgery_episode_id,
    'fna_history' AS source_table,
    NULL::DOUBLE AS fna_confidence
FROM fna_history
WHERE research_id IS NOT NULL;

-- event_date_audit_v2
CREATE OR REPLACE TABLE event_date_audit_v2 AS
SELECT 'tumor' AS domain, research_id,
       CAST(surgery_date AS VARCHAR) AS native_date,
       CAST(surgery_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       histology_source AS anchor_source,
       source_tables AS source_table
FROM tumor_episode_master_v2

UNION ALL

SELECT 'molecular' AS domain, research_id,
       CAST(test_date_native AS VARCHAR) AS native_date,
       resolved_test_date AS resolved_date,
       date_status, date_confidence,
       platform AS anchor_source,
       source_table
FROM molecular_test_episode_v2

UNION ALL

SELECT 'rai' AS domain, research_id,
       CAST(rai_date_native AS VARCHAR) AS native_date,
       CAST(resolved_rai_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       source_note_type AS anchor_source,
       source_table
FROM rai_treatment_episode_v2

UNION ALL

SELECT 'imaging' AS domain, research_id,
       CAST(exam_date_native AS VARCHAR) AS native_date,
       CAST(resolved_exam_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       modality AS anchor_source,
       report_source_table AS source_table
FROM imaging_nodule_long_v2

UNION ALL

SELECT 'operative' AS domain, research_id,
       CAST(surgery_date_native AS VARCHAR) AS native_date,
       resolved_surgery_date AS resolved_date,
       date_status, CASE WHEN date_status = 'exact_source_date' THEN 100 ELSE 0 END AS date_confidence,
       procedure_normalized AS anchor_source,
       source_tables AS source_table
FROM operative_episode_detail_v2

UNION ALL

SELECT 'fna' AS domain, research_id,
       CAST(fna_date_native AS VARCHAR) AS native_date,
       CAST(resolved_fna_date AS VARCHAR) AS resolved_date,
       date_status, date_confidence,
       specimen_site_raw AS anchor_source,
       source_table
FROM fna_episode_master_v2;

-- patient_cross_domain_timeline_v2
CREATE OR REPLACE TABLE patient_cross_domain_timeline_v2 AS
SELECT research_id, 'surgery' AS event_type, 'tumor' AS domain,
       surgery_date AS event_date, surgery_episode_id AS episode_id,
       primary_histology AS event_detail
FROM tumor_episode_master_v2

UNION ALL

SELECT research_id, 'molecular_test' AS event_type, 'molecular' AS domain,
       test_date_native AS event_date, molecular_episode_id AS episode_id,
       platform || ': ' || overall_result_class AS event_detail
FROM molecular_test_episode_v2

UNION ALL

SELECT research_id, 'rai_treatment' AS event_type, 'rai' AS domain,
       resolved_rai_date AS event_date, rai_episode_id AS episode_id,
       rai_assertion_status || ' ' || COALESCE(CAST(dose_mci AS VARCHAR),'') || ' mCi' AS event_detail
FROM rai_treatment_episode_v2

UNION ALL

SELECT research_id, 'imaging' AS event_type, 'imaging' AS domain,
       exam_date_native AS event_date, imaging_exam_id AS episode_id,
       modality || ' nodule' AS event_detail
FROM imaging_nodule_long_v2

UNION ALL

SELECT research_id, 'surgery' AS event_type, 'operative' AS domain,
       surgery_date_native AS event_date, surgery_episode_id AS episode_id,
       procedure_normalized AS event_detail
FROM operative_episode_detail_v2

UNION ALL

SELECT research_id, 'fna' AS event_type, 'fna' AS domain,
       fna_date_native AS event_date, fna_episode_id AS episode_id,
       'Bethesda ' || COALESCE(CAST(bethesda_category AS VARCHAR), '?') AS event_detail
FROM fna_episode_master_v2

ORDER BY research_id, event_date NULLS LAST, event_type;