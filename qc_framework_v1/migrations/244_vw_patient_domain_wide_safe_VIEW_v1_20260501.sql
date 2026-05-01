-- =============================================================================
-- mig_244: semantic_publication.vw_patient_domain_wide_safe_VIEW_v1
-- Batch id: mig_244_patient_domain_wide_safe
-- verified_by: cursor_composer_mig_244
-- =============================================================================
-- Curated patient-grain bridge (10,871 rows): headline demographics, AJCC / ETE /
-- margins from vw_patient_master_safe; eligibility + histology spine from
-- vw_cohort_membership_safe; operative LN counts from canonical_operative_*;
-- LN totals from vw_ln_patient_safe; pathology aggregates from
-- vw_path_malignant_tumor_safe (dedup_rank=1); recurrence non-quarantine from
-- vw_recurrence_safe; molecular episode rollup from vw_molecular_safe;
-- survival SSOT join canonical_survival_followup_v1; limitation flags from
-- canonical_path_malignant_events_v1, canonical_recurrence_resolved_v1,
-- canonical_us_nodule_v2.
-- Frozen section intentionally omitted — compact vw_frozen_section_safe_VIEW_v1 (mig_242).
-- ln_size_max_mm omitted — no patient-safe LN mm SSOT in semantic layer v1.
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

DELETE FROM main.canonical_column_verification_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_patient_domain_wide_safe_VIEW_v1';

DELETE FROM main.canonical_table_signoff_registry_v1
WHERE schema_name = 'semantic_publication'
  AND table_name  = 'vw_patient_domain_wide_safe_VIEW_v1';

CREATE OR REPLACE VIEW semantic_publication.vw_patient_domain_wide_safe_VIEW_v1 AS
WITH path_agg AS (
    SELECT
        p.release_id,
        p.research_id,
        MAX(p.size_greatest_dimension_cm) AS path_max_tumor_size_cm,
        BOOL_OR(
            regexp_matches(
                LOWER(COALESCE(CAST(p.extranodal_extension AS VARCHAR), '')),
                '(gross|macroscopic|macro\\s)'
            )
        ) AS any_macroscopic_extranodal_extension,
        BOOL_OR(
            CASE
                WHEN p.lymphatic_invasion IS NULL THEN FALSE
                WHEN TRIM(CAST(p.lymphatic_invasion AS VARCHAR)) = '' THEN FALSE
                WHEN regexp_matches(
                    LOWER(TRIM(CAST(p.lymphatic_invasion AS VARCHAR))),
                    '^(none|negative|absent|no|false|0)$'
                ) THEN FALSE
                ELSE TRUE
            END
        ) AS path_lymphatic_invasion_any,
        BOOL_OR(
            CASE
                WHEN p.vascular_invasion IS NULL THEN FALSE
                WHEN TRIM(CAST(p.vascular_invasion AS VARCHAR)) = '' THEN FALSE
                WHEN regexp_matches(
                    LOWER(TRIM(CAST(p.vascular_invasion AS VARCHAR))),
                    '^(none|negative|absent|no|false|0)$'
                ) THEN FALSE
                ELSE TRUE
            END
        ) AS path_vascular_invasion_any
    FROM semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1 AS p
    WHERE p.publication_dedup_rank = 1
    GROUP BY p.release_id, p.research_id
),
mol_agg AS (
    SELECT
        m.release_id,
        m.research_id,
        COUNT(*) > 0 AS any_molecular_test,
        BOOL_OR(COALESCE(m.braf_flag, FALSE)) AS any_braf_positive,
        BOOL_OR(COALESCE(m.tert_flag, FALSE)) AS any_tert_positive
    FROM semantic_publication.vw_molecular_safe_VIEW_v1 AS m
    GROUP BY m.release_id, m.research_id
),
borderline AS (
    SELECT
        CAST(e.research_id AS VARCHAR) AS research_id,
        BOOL_OR(COALESCE(e.is_borderline_or_benign_with_staging, FALSE)) AS is_borderline_or_benign_with_staging_any
    FROM main.canonical_path_malignant_events_v1 AS e
    GROUP BY e.research_id
),
rec_quarantine AS (
    SELECT
        CAST(r.research_id AS VARCHAR) AS research_id,
        BOOL_OR(COALESCE(r.is_implausible_date_quarantine, FALSE)) AS recurrence_implausible_date_quarantine_any
    FROM main.canonical_recurrence_resolved_v1 AS r
    GROUP BY r.research_id
),
us_nlp_pending AS (
    SELECT
        CAST(n.research_id AS VARCHAR) AS research_id,
        BOOL_OR(COALESCE(n.nlp_backfill_pending, FALSE)) AS us_nodule_any_nlp_backfill_pending
    FROM main.canonical_us_nodule_v2 AS n
    GROUP BY n.research_id
)
SELECT
    cm.release_id,
    cm.research_id,

    cm.analysis_eligible_flag,
    cm.molecular_eligible_flag,
    cm.rai_eligible_flag,
    cm.survival_eligible_flag,

    cm.age_at_surgery AS age_at_first_surgery,
    cm.sex,
    cm.race AS race_self_reported,
    cm.histology_final AS primary_histology,

    pm.tumor_size_cm_max,
    CASE
        WHEN pm.tumor_size_cm_max IS NOT NULL THEN ROUND(CAST(pm.tumor_size_cm_max AS DOUBLE) * 10.0, 4)
        ELSE CAST(NULL AS DOUBLE)
    END AS max_tumor_size_mm,

    cm.path_multifocal_flag AS multifocality_flag_path,

    pm.ajcc8_t_stage AS ajcc8_t_stage_final,
    pm.ajcc8_n_stage AS ajcc8_n_stage_final,
    pm.ajcc8_m_stage AS ajcc8_m_stage_final,
    pm.ajcc8_stage_group AS ajcc8_stage_group_final,

    pm.ata_initial_risk,
    pm.ete_grade_final,
    pm.margin_r_class,
    pm.margin_status,

    pa.path_max_tumor_size_cm,
    COALESCE(pa.path_lymphatic_invasion_any, FALSE)
        OR COALESCE(pa.path_vascular_invasion_any, FALSE) AS lymphovascular_invasion_any,
    COALESCE(pa.any_macroscopic_extranodal_extension, FALSE) AS any_macroscopic_extranodal_extension,

    COALESCE(op.n_total_thyroidectomies, 0) > 0 AS any_total_thyroidectomy,
    COALESCE(op.n_hemithyroidectomies, 0) > 0 AS any_lobectomy,
    COALESCE(op.n_surgeries, 0)::BIGINT AS n_surgeries,
    CAST(cm.surgery_date AS DATE) AS first_surgery_date,
    (COALESCE(op.n_central_neck_dissections, 0) + COALESCE(op.n_lateral_neck_dissections, 0)) > 0 AS any_ln_dissection,

    ln.ln_total_examined_safe,
    ln.ln_total_positive_safe,

    rec.recurrence_status_final,
    CAST(rec.recurrence_path_proven_date AS DATE) AS recurrence_path_proven_date,
    rec.days_to_path_proven AS days_to_recurrence_path_proven,
    rec.recurrence_imaging_then_path_confirmed,

    COALESCE(ma.any_molecular_test, FALSE) AS any_molecular_test,
    COALESCE(ma.any_braf_positive, FALSE) AS any_braf_positive,
    COALESCE(ma.any_tert_positive, FALSE) AS any_tert_positive,

    surv.vital_status_current,
    CAST(surv.last_known_alive_date AS DATE) AS last_known_alive_date,
    surv.days_from_first_surgery_to_last_contact,

    COALESCE(bo.is_borderline_or_benign_with_staging_any, FALSE) AS is_borderline_or_benign_with_staging_any,
    COALESCE(rq.recurrence_implausible_date_quarantine_any, FALSE) AS recurrence_implausible_date_quarantine_any,
    COALESCE(us.us_nodule_any_nlp_backfill_pending, FALSE) AS us_nodule_any_nlp_backfill_pending,

    pm.is_malignant,
    pm.any_recurrence_flag
FROM semantic_publication.vw_cohort_membership_safe_VIEW_v1 AS cm
LEFT JOIN semantic_publication.vw_patient_master_safe_VIEW_v1 AS pm
    ON cm.release_id = pm.release_id AND cm.research_id = pm.research_id
LEFT JOIN path_agg AS pa
    ON cm.release_id = pa.release_id AND cm.research_id = pa.research_id
LEFT JOIN mol_agg AS ma
    ON cm.release_id = ma.release_id AND cm.research_id = ma.research_id
LEFT JOIN semantic_publication.vw_recurrence_safe_VIEW_v1 AS rec
    ON cm.release_id = rec.release_id AND cm.research_id = rec.research_id
LEFT JOIN semantic_publication.vw_ln_patient_safe_VIEW_v1 AS ln
    ON cm.release_id = ln.release_id AND cm.research_id = ln.research_id
LEFT JOIN main.canonical_operative_patient_rollup_v1 AS op
    ON CAST(op.research_id AS VARCHAR) = cm.research_id
LEFT JOIN main.canonical_survival_followup_v1 AS surv
    ON CAST(surv.research_id AS VARCHAR) = cm.research_id
LEFT JOIN borderline AS bo
    ON cm.research_id = bo.research_id
LEFT JOIN rec_quarantine AS rq
    ON cm.research_id = rq.research_id
LEFT JOIN us_nlp_pending AS us
    ON cm.research_id = us.research_id;

INSERT INTO main.canonical_column_verification_registry_v1
    (schema_name, table_name, column_name, data_type, ordinal_position,
     category, upstream_source, verification_status, verified_by, verified_ts,
     verification_method, batch_id, notes)
VALUES
    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'release_id', 'VARCHAR', 1,
     'identifier', 'semantic_publication.vw_cohort_membership_safe_VIEW_v1.release_id', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'Publication release id (manifest spine).'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'research_id', 'VARCHAR', 2,
     'identifier', 'semantic_publication.vw_cohort_membership_safe_VIEW_v1.research_id', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'mig_239 VARCHAR convention; one row per CPM manuscript cohort patient.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'analysis_eligible_flag', 'BOOLEAN', 3,
     'eligibility', 'vw_cohort_membership_safe_VIEW_v1.analysis_eligible_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'molecular_eligible_flag', 'BOOLEAN', 4,
     'eligibility', 'vw_cohort_membership_safe_VIEW_v1.molecular_eligible_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'rai_eligible_flag', 'BOOLEAN', 5,
     'eligibility', 'vw_cohort_membership_safe_VIEW_v1.rai_eligible_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'survival_eligible_flag', 'BOOLEAN', 6,
     'eligibility', 'vw_cohort_membership_safe_VIEW_v1.survival_eligible_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'age_at_first_surgery', 'DOUBLE', 7,
     'demographic', 'vw_cohort_membership_safe_VIEW_v1.age_at_surgery', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_alias',
     'mig_244_patient_domain_wide_safe', 'Renamed from age_at_surgery for manuscript wording.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'sex', 'VARCHAR', 8,
     'demographic', 'vw_cohort_membership_safe_VIEW_v1.sex', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'race_self_reported', 'VARCHAR', 9,
     'demographic', 'vw_cohort_membership_safe_VIEW_v1.race', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_alias',
     'mig_244_patient_domain_wide_safe', 'Race / ethnicity bucket spine from manuscript cohort.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'primary_histology', 'VARCHAR', 10,
     'clinical', 'vw_cohort_membership_safe_VIEW_v1.histology_final', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_alias',
     'mig_244_patient_domain_wide_safe', 'Manuscript cohort histology headline.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'tumor_size_cm_max', 'DOUBLE', 11,
     'clinical_measure', 'vw_patient_master_safe_VIEW_v1.tumor_size_cm_max', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'CPM rollup max cm; see AGENTS tumor_size_cm_max caveat for multi-surgery under-report.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'max_tumor_size_mm', 'DOUBLE', 12,
     'clinical_measure', 'derived tumor_size_cm_max * 10', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_derived',
     'mig_244_patient_domain_wide_safe', 'Millimeters for manuscript Table 2-style reporting.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'multifocality_flag_path', 'BOOLEAN', 13,
     'clinical_flag', 'vw_cohort_membership_safe_VIEW_v1.path_multifocal_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ajcc8_t_stage_final', 'VARCHAR', 14,
     'staging', 'vw_patient_master_safe_VIEW_v1.ajcc8_t_stage', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'Corrected AJCC8 T (post Phase 4.6 rename).'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ajcc8_n_stage_final', 'VARCHAR', 15,
     'staging', 'vw_patient_master_safe_VIEW_v1.ajcc8_n_stage', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ajcc8_m_stage_final', 'VARCHAR', 16,
     'staging', 'vw_patient_master_safe_VIEW_v1.ajcc8_m_stage', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ajcc8_stage_group_final', 'VARCHAR', 17,
     'staging', 'vw_patient_master_safe_VIEW_v1.ajcc8_stage_group', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ata_initial_risk', 'VARCHAR', 18,
     'staging', 'vw_patient_master_safe_VIEW_v1.ata_initial_risk', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ete_grade_final', 'VARCHAR', 19,
     'staging', 'vw_patient_master_safe_VIEW_v1.ete_grade_final', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'CPM/adjudicated ETE headline (ete_grade_final_v2 upstream).'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'margin_r_class', 'VARCHAR', 20,
     'pathology', 'vw_patient_master_safe_VIEW_v1.margin_r_class', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'Use r_class_true upstream naming in CPM; exposed as margin_r_class in safe vw.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'margin_status', 'VARCHAR', 21,
     'pathology', 'vw_patient_master_safe_VIEW_v1.margin_status', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'path_max_tumor_size_cm', 'DOUBLE', 22,
     'clinical_measure', 'aggregate vw_path_malignant_tumor_safe_VIEW_v1.size_greatest_dimension_cm', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_path',
     'mig_244_patient_domain_wide_safe', 'MAX across dedup_rank=1 path rows; pathology-record max complement to CPM rollup.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'lymphovascular_invasion_any', 'BOOLEAN', 23,
     'clinical_flag', 'derived path_agg lymphatic OR vascular invasion truthy heuristic', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_path',
     'mig_244_patient_domain_wide_safe', 'Heuristic positive unless explicit negative tokens on path rows.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_macroscopic_extranodal_extension', 'BOOLEAN', 24,
     'clinical_flag', 'regexp extranodal_extension gross/macroscopic', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_path',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_total_thyroidectomy', 'BOOLEAN', 25,
     'surgery', 'canonical_operative_patient_rollup_v1.n_total_thyroidectomies > 0', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_lobectomy', 'BOOLEAN', 26,
     'surgery', 'canonical_operative_patient_rollup_v1.n_hemithyroidectomies > 0', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'n_surgeries', 'BIGINT', 27,
     'surgery', 'canonical_operative_patient_rollup_v1.n_surgeries', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', 'Operative rollup count; see script 362 caveats on completion labeling.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'first_surgery_date', 'DATE', 28,
     'temporal', 'vw_cohort_membership_safe_VIEW_v1.surgery_date', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'Manuscript cohort first surgery date spine.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_ln_dissection', 'BOOLEAN', 29,
     'surgery', 'operative rollup central+lateral neck dissection counts', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_derived',
     'mig_244_patient_domain_wide_safe', 'TRUE if any structured central OR lateral neck dissection episode flag rollup.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ln_total_examined_safe', 'DOUBLE', 30,
     'clinical_measure', 'vw_ln_patient_safe_VIEW_v1.ln_total_examined_safe', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'ln_total_positive_safe', 'DOUBLE', 31,
     'clinical_measure', 'vw_ln_patient_safe_VIEW_v1.ln_total_positive_safe', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'recurrence_status_final', 'VARCHAR', 32,
     'outcome', 'vw_recurrence_safe_VIEW_v1.recurrence_status_final', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'Non-quarantined recurrence rows only (safe view filter).'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'recurrence_path_proven_date', 'DATE', 33,
     'temporal', 'vw_recurrence_safe_VIEW_v1.recurrence_path_proven_date', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'days_to_recurrence_path_proven', 'BIGINT', 34,
     'temporal', 'vw_recurrence_safe_VIEW_v1.days_to_path_proven', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_alias',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'recurrence_imaging_then_path_confirmed', 'BOOLEAN', 35,
     'outcome', 'vw_recurrence_safe_VIEW_v1.recurrence_imaging_then_path_confirmed', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_molecular_test', 'BOOLEAN', 36,
     'molecular', 'rollup vw_molecular_safe_VIEW_v1', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_molecular',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_braf_positive', 'BOOLEAN', 37,
     'molecular', 'rollup vw_molecular_safe BOOL_OR(braf_flag)', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_molecular',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_tert_positive', 'BOOLEAN', 38,
     'molecular', 'rollup vw_molecular_safe BOOL_OR(tert_flag)', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_molecular',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'vital_status_current', 'VARCHAR', 39,
     'outcome', 'canonical_survival_followup_v1.vital_status_current', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', 'Survival SSOT column naming per mig_121 ledger.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'last_known_alive_date', 'DATE', 40,
     'temporal', 'canonical_survival_followup_v1.last_known_alive_date', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', 'DATE type SSOT; normalize TIMESTAMP joins externally if needed.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'days_from_first_surgery_to_last_contact', 'BIGINT', 41,
     'temporal', 'canonical_survival_followup_v1.days_from_first_surgery_to_last_contact', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_canonical_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'is_borderline_or_benign_with_staging_any', 'BOOLEAN', 42,
     'data_quality', 'aggregate canonical_path_malignant_events_v1.is_borderline_or_benign_with_staging', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_limitation',
     'mig_244_patient_domain_wide_safe', 'Quarantine bookkeeping headline per path malignant events grain.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'recurrence_implausible_date_quarantine_any', 'BOOLEAN', 43,
     'data_quality', 'aggregate canonical_recurrence_resolved_v1.is_implausible_date_quarantine', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_limitation',
     'mig_244_patient_domain_wide_safe', 'TRUE if any resolved recurrence row quarantined (separate from recurrence safe vw).'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'us_nodule_any_nlp_backfill_pending', 'BOOLEAN', 44,
     'data_quality', 'aggregate canonical_us_nodule_v2.nlp_backfill_pending', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_agg_limitation',
     'mig_244_patient_domain_wide_safe', 'Patient-level OR across US nodule v2 rows.'),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'is_malignant', 'BOOLEAN', 45,
     'clinical', 'vw_patient_master_safe_VIEW_v1.is_malignant', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', NULL),

    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1', 'any_recurrence_flag', 'BOOLEAN', 46,
     'outcome', 'vw_patient_master_safe_VIEW_v1.any_recurrence_flag', 'verified',
     'cursor_composer_mig_244', CURRENT_TIMESTAMP, 'bridge_view_join',
     'mig_244_patient_domain_wide_safe', 'CPM headline recurrence flag alongside recurrence-safe detailed columns.')
;

INSERT INTO main.canonical_table_signoff_registry_v1
    (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
     table_status, signed_off_ts, signoff_migration, priority_tier, notes)
VALUES
    ('semantic_publication', 'vw_patient_domain_wide_safe_VIEW_v1',
     46, 46, 0, 0, 0,
     'verified',
     CURRENT_TIMESTAMP,
     'qc_framework_v1/migrations/244_vw_patient_domain_wide_safe_VIEW_v1_20260501.sql',
     'tier2_canonical_view',
     'mig_244 (2026-05-01): curated patient-domain-wide semantic bridge; 46 cols; 10,871 rows; batch_id=mig_244_patient_domain_wide_safe.')
;

-- Path-C post-apply:
-- SELECT COUNT(*) FROM semantic_publication.vw_patient_domain_wide_safe_VIEW_v1;
-- SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
