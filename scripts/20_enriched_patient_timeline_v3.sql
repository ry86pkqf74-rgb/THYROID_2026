-- scripts/20_enriched_patient_timeline_v3.sql
-- Enriched patient timeline view joining timeline_rescue_v3_mv with
-- patient header and major episode tables.
--
-- COLUMNS USED (verified against AGENTS.md schema):
--   timeline_rescue_v3_mv  : entity_type, research_id, entity_date,
--                            evidence_span, note_date, inferred_event_date,
--                            date_status, date_is_source_native_flag,
--                            date_is_inferred_flag, date_requires_manual_review_flag
--   master_cohort          : sex, age_at_surgery (patient-level header)
--   path_synoptics         : surg_date, histology_1_type, overall_stage_ajcc8
--   tumor_episode_master_v2: tumor rows keyed by research_id
--   molecular_test_episode_v2: platform, result_category, inferred_event_date
--   rai_treatment_episode_v2 : rai_date, rai_dose_mci
--   imaging_nodule_long_v2   : nodule_size_mm, composition
--   operative_episode_detail_v2: operative_approach, rln_monitoring_flag
--
-- Deployment: run this SQL directly in MotherDuck SQL editor or via
--   duckdb "md:thyroid_research_2026" < scripts/20_enriched_patient_timeline_v3.sql
-- ---------------------------------------------------------------------------
USE thyroid_research_2026;

CREATE OR REPLACE VIEW enriched_patient_timeline_v3_mv AS
WITH

-- ── Patient spine ──────────────────────────────────────────────────────────
-- genetic_year is coarsened to YYYY-01-01 when used
patient_spine AS (
    SELECT
        pl.research_id,
        pl.sex,
        pl.age_at_surgery,
        pl.histology_1_type,
        pl.overall_stage_ajcc8,
        MIN(TRY_CAST(ps.surg_date AS DATE)) AS first_surgery_date
    FROM patient_level_summary_mv pl
    LEFT JOIN path_synoptics ps ON pl.research_id = ps.research_id
    GROUP BY
        pl.research_id, pl.sex, pl.age_at_surgery,
        pl.histology_1_type, pl.overall_stage_ajcc8
),

-- ── First RAI per patient (for time-to-RAI) ───────────────────────────────
first_rai AS (
    SELECT
        research_id,
        MIN(TRY_CAST(resolved_rai_date AS DATE)) AS first_rai_date,
        MAX(dose_mci)                             AS max_rai_dose
    FROM rai_treatment_episode_v2
    WHERE resolved_rai_date IS NOT NULL
    GROUP BY research_id
),

-- ── Per-patient date rescue rate ──────────────────────────────────────────
rescue_rate AS (
    SELECT
        research_id,
        COUNT(*)                                            AS total_entity_rows,
        SUM(CASE WHEN date_status != 'unresolved_date'
                 THEN 1 ELSE 0 END)                        AS rescued_rows,
        ROUND(
            100.0 * SUM(CASE WHEN date_status != 'unresolved_date'
                             THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 1)                      AS date_rescue_rate_pct
    FROM timeline_rescue_v3_mv
    GROUP BY research_id
)

-- ── Main select ───────────────────────────────────────────────────────────
SELECT
    -- provenance columns first
    t.research_id,
    t.entity_type,
    t.entity_date,
    t.note_date,
    t.inferred_event_date,
    t.date_status,
    t.date_is_source_native_flag,
    t.date_is_inferred_flag,
    t.date_requires_manual_review_flag,
    t.evidence_span,

    -- patient header
    sp.sex,
    sp.age_at_surgery,
    sp.histology_1_type,
    sp.overall_stage_ajcc8,
    sp.first_surgery_date,

    -- time-to-RAI (days from first surgery to first RAI)
    fr.first_rai_date,
    CASE
        WHEN sp.first_surgery_date IS NOT NULL AND fr.first_rai_date IS NOT NULL
        THEN DATEDIFF('day', sp.first_surgery_date, fr.first_rai_date)
    END AS time_to_rai_days,
    fr.max_rai_dose,

    -- per-patient date rescue
    rr.total_entity_rows,
    rr.rescued_rows,
    rr.date_rescue_rate_pct

FROM timeline_rescue_v3_mv t
LEFT JOIN patient_spine sp ON t.research_id = sp.research_id
LEFT JOIN first_rai    fr ON t.research_id = fr.research_id
LEFT JOIN rescue_rate  rr ON t.research_id = rr.research_id
;
