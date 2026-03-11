-- scripts/21_survival_analysis_v3.sql
-- Survival Analysis V3 views for publication-ready analytics
-- uses inferred_event_date from timeline rescue for accuracy

USE thyroid_research_2026;

CREATE OR REPLACE VIEW time_to_rai_v3_mv AS
WITH patient_level AS (
    SELECT
        research_id,
        MIN(first_surgery_date) AS first_surgery_date,
        MAX(first_rai_date) AS timeline_first_rai_date,
        MAX(overall_stage_ajcc8) AS overall_stage_ajcc8,
        MAX(histology_1_type) AS histology_1_type,
        MAX(date_rescue_rate_pct) AS date_rescue_rate_pct
    FROM enriched_patient_timeline_v3_mv
    GROUP BY research_id
),
rai_first AS (
    SELECT
        research_id,
        MIN(TRY_CAST(resolved_rai_date AS DATE)) AS first_rai_date
    FROM rai_treatment_episode_v2
    WHERE resolved_rai_date IS NOT NULL
    GROUP BY research_id
)
SELECT
    p.research_id,
    p.first_surgery_date,
    COALESCE(r.first_rai_date, p.timeline_first_rai_date) AS first_rai_date,
    CASE
        WHEN p.first_surgery_date IS NOT NULL
         AND COALESCE(r.first_rai_date, p.timeline_first_rai_date) IS NOT NULL
        THEN DATEDIFF('day', p.first_surgery_date, COALESCE(r.first_rai_date, p.timeline_first_rai_date))
    END AS time_to_rai_days,
    CASE
        WHEN UPPER(COALESCE(p.overall_stage_ajcc8, '')) IN ('III', 'IVA', 'IVB', 'IVC', 'IV') THEN 'III/IV'
        WHEN UPPER(COALESCE(p.overall_stage_ajcc8, '')) IN ('I', 'IA', 'IB', 'II', 'IIA', 'IIB') THEN 'I/II'
        ELSE 'Unknown'
    END AS ajcc_stage_grouped,
    p.overall_stage_ajcc8,
    p.histology_1_type,
    p.date_rescue_rate_pct,
    CASE
        WHEN COALESCE(p.date_rescue_rate_pct, 0) >= 80 THEN 'high'
        WHEN COALESCE(p.date_rescue_rate_pct, 0) >= 50 THEN 'medium'
        ELSE 'low'
    END AS date_rescue_confidence
FROM patient_level p
LEFT JOIN rai_first r
    ON CAST(p.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR);

CREATE OR REPLACE VIEW recurrence_free_survival_v3_mv AS
WITH surgery_dates AS (
    SELECT
        research_id,
        MIN(first_surgery_date) AS first_surgery_date,
        MAX(overall_stage_ajcc8) AS overall_stage_ajcc8,
        MAX(histology_1_type) AS histology_1_type,
        MAX(date_rescue_rate_pct) AS date_rescue_rate_pct
    FROM enriched_patient_timeline_v3_mv
    GROUP BY research_id
),
recurrence_rrf AS (
    SELECT
        research_id,
        MIN(TRY_CAST(first_recurrence_date AS DATE)) AS first_recurrence_date,
        MAX(CASE
            WHEN LOWER(CAST(recurrence_flag AS VARCHAR)) IN ('1', 'true', 'yes') THEN 1
            ELSE 0
        END) AS recurrence_flag
    FROM recurrence_risk_features_mv
    GROUP BY research_id
),
recurrence_nlp AS (
    SELECT
        research_id,
        MIN(TRY_CAST(event_date AS DATE)) AS first_recurrence_date_nlp
    FROM extracted_clinical_events_v3
    WHERE event_subtype = 'recurrence'
      AND event_date IS NOT NULL
    GROUP BY research_id
),
last_contact AS (
    SELECT
        research_id,
        MAX(TRY_CAST(inferred_event_date AS DATE)) AS last_contact_date
    FROM enriched_patient_timeline_v3_mv
    GROUP BY research_id
)
SELECT
    s.research_id,
    s.first_surgery_date,
    COALESCE(
        CASE WHEN r.recurrence_flag = 1 THEN r.first_recurrence_date END,
        n.first_recurrence_date_nlp
    ) AS first_recurrence_date,
    COALESCE(
        CASE WHEN r.recurrence_flag = 1 THEN r.first_recurrence_date END,
        n.first_recurrence_date_nlp,
        l.last_contact_date,
        CURRENT_DATE
    ) AS censor_date,
    CASE
        WHEN COALESCE(
            CASE WHEN r.recurrence_flag = 1 THEN r.first_recurrence_date END,
            n.first_recurrence_date_nlp
        ) IS NULL THEN 1
        ELSE 0
    END AS censoring_flag,
    CASE
        WHEN s.first_surgery_date IS NOT NULL
        THEN DATEDIFF(
            'day',
            s.first_surgery_date,
            COALESCE(
                CASE WHEN r.recurrence_flag = 1 THEN r.first_recurrence_date END,
                n.first_recurrence_date_nlp,
                l.last_contact_date,
                CURRENT_DATE
            )
        )
    END AS time_to_recurrence_days,
    CASE
        WHEN UPPER(COALESCE(s.overall_stage_ajcc8, '')) IN ('III', 'IVA', 'IVB', 'IVC', 'IV') THEN 'III/IV'
        WHEN UPPER(COALESCE(s.overall_stage_ajcc8, '')) IN ('I', 'IA', 'IB', 'II', 'IIA', 'IIB') THEN 'I/II'
        ELSE 'Unknown'
    END AS ajcc_stage_grouped,
    s.overall_stage_ajcc8,
    s.histology_1_type,
    s.date_rescue_rate_pct,
    CASE
        WHEN COALESCE(s.date_rescue_rate_pct, 0) >= 80 THEN 'high'
        WHEN COALESCE(s.date_rescue_rate_pct, 0) >= 50 THEN 'medium'
        ELSE 'low'
    END AS date_rescue_confidence
FROM surgery_dates s
LEFT JOIN recurrence_rrf r
    ON CAST(s.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
LEFT JOIN recurrence_nlp n
    ON CAST(s.research_id AS VARCHAR) = CAST(n.research_id AS VARCHAR)
LEFT JOIN last_contact l
    ON CAST(s.research_id AS VARCHAR) = CAST(l.research_id AS VARCHAR);

CREATE OR REPLACE VIEW genotype_stratified_outcomes_v3_mv AS
WITH genetic_patients AS (
    SELECT DISTINCT research_id
    FROM genetic_testing
),
molecular_flags AS (
    SELECT
        COALESCE(CAST(r.research_id AS VARCHAR), CAST(tp.research_id AS VARCHAR)) AS research_id,
        CASE
            WHEN LOWER(CAST(r.braf_positive AS VARCHAR)) = 'true'
              OR LOWER(CAST(tp.braf_mutation_mentioned AS VARCHAR)) = 'true'
            THEN TRUE ELSE FALSE
        END AS braf_flag,
        CASE
            WHEN LOWER(CAST(r.ras_positive AS VARCHAR)) = 'true'
              OR LOWER(CAST(tp.ras_mutation_mentioned AS VARCHAR)) = 'true'
            THEN TRUE ELSE FALSE
        END AS ras_flag
    FROM recurrence_risk_features_mv r
    FULL OUTER JOIN tumor_pathology tp
        ON CAST(r.research_id AS VARCHAR) = CAST(tp.research_id AS VARCHAR)
)
SELECT
    rfs.research_id,
    tr.time_to_rai_days,
    rfs.time_to_recurrence_days,
    rfs.censoring_flag,
    rfs.ajcc_stage_grouped,
    CASE
        WHEN COALESCE(mf.braf_flag, FALSE) OR COALESCE(mf.ras_flag, FALSE)
            THEN 'BRAF/RAS+'
        ELSE 'wild-type'
    END AS braf_ras_status,
    rfs.date_rescue_confidence,
    rfs.histology_1_type,
    CASE WHEN gp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_genetic_testing
FROM recurrence_free_survival_v3_mv rfs
LEFT JOIN time_to_rai_v3_mv tr
    ON CAST(rfs.research_id AS VARCHAR) = CAST(tr.research_id AS VARCHAR)
LEFT JOIN molecular_flags mf
    ON CAST(rfs.research_id AS VARCHAR) = mf.research_id
LEFT JOIN genetic_patients gp
    ON CAST(rfs.research_id AS VARCHAR) = CAST(gp.research_id AS VARCHAR);
