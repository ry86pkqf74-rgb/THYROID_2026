-- MotherDuck lymph node completeness audit (THYROID_2026)
-- Database: thyroid_research_2026 (run with USE thyroid_research_2026; if required)
--
-- Lineage (proposal2_ete_staging):
--   Publication README cites risk_enriched_mv as primary analytic cohort.
--   risk_enriched_mv = recurrence_risk_features_mv (+ survival join); LN counts on that
--   view come from tumor_pathology (histology_1_ln_examined / histology_1_ln_positive).
--   advanced_features_v3 / path_synoptics provide specimen-level LN columns
--   (tumor_1_ln_examined / tumor_1_ln_involved) joined at patient grain in views.
--
-- Null semantics (this audit):
--   NULL after cleaning = no numeric LN statement in structured synoptic/pathology field
--     (not proof that no nodes were examined — may be synoptic omit, parse failure, or 'x' only).
--   0 = explicit zero in structured field after cleaning (treated as explicit no-positive or
--     zero yield when paired with examined=0).
--   Raw 'x' in path_synoptcis often marks "present but unquantified" for other domains; for LN
--   counts, repository ETL typically strips 'x' before TRY_CAST, yielding NULL if no digits.

-- ---------------------------------------------------------------------------
-- A) Specimen-level spine: path_synoptics (one row per synoptic / specimen report)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _ln_audit_specimen AS
SELECT
    ps.research_id,
    ps.surg_date,
    YEAR(TRY_CAST(ps.surg_date AS DATE)) AS surgery_year,
    LOWER(TRIM(COALESCE(ps.thyroid_procedure, ''))) AS thyroid_procedure_raw,
    TRIM(COALESCE(ps.tumor_1_histologic_type, '')) AS tumor_1_histologic_type,
    TRIM(COALESCE(ps.specimen_type, '')) AS specimen_type,
    ps.tumor_1_ln_examined AS ln_examined_raw,
    ps.tumor_1_ln_involved AS ln_positive_raw,
    TRY_CAST(
        REPLACE(REPLACE(TRIM(CAST(ps.tumor_1_ln_examined AS VARCHAR)), ';', ''), 'x', '')
        AS DOUBLE
    ) AS ln_examined_clean,
    TRY_CAST(
        REPLACE(REPLACE(TRIM(CAST(ps.tumor_1_ln_involved AS VARCHAR)), ';', ''), 'x', '')
        AS DOUBLE
    ) AS ln_positive_clean,
    CASE
        WHEN central_compartment_dissection IS NOT NULL THEN 1
        WHEN LOWER(COALESCE(tumor_1_level_examined, '')) LIKE '%6%' THEN 1
        WHEN LOWER(COALESCE(other_ln_dissection, '')) LIKE '%central%'
             OR LOWER(COALESCE(other_ln_dissection, '')) LIKE '%level 6%' THEN 1
        WHEN LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%perithyroidal%'
             OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%pretracheal%'
             OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%paratracheal%'
             OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%delphian%'
             OR LOWER(COALESCE(tumor_1_ln_location, '')) LIKE '%prelaryngeal%' THEN 1
        ELSE 0
    END AS central_lnd_composite_flag,
    tp.histology_1_n_stage_ajcc8 AS n_stage_tp,
    TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS tp_ln_examined,
    TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS tp_ln_positive,
    TRY_CAST(tp.histology_1_ln_ratio AS DOUBLE) AS tp_ln_ratio
FROM path_synoptics ps
LEFT JOIN tumor_pathology tp
    ON CAST(ps.research_id AS BIGINT) = CAST(tp.research_id AS BIGINT)
WHERE ps.research_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- B) Patient-level analytic LN (recurrence_risk_features_mv / risk_enriched_mv)
-- ---------------------------------------------------------------------------
-- (Used in Python as separate query; kept here for documentation.)
-- SELECT research_id, ln_examined, ln_positive, ln_ratio, pn_stage
-- FROM recurrence_risk_features_mv;

-- ---------------------------------------------------------------------------
-- C) Executable audit
-- ---------------------------------------------------------------------------
-- Run:  studies/proposal2_ete_staging/run_motherduck_ln_completeness_audit.py
--        (creates temp _ln_specimen, writes CSVs + MOTHERDUCK_LYMPH_NODE_COMPLETENESS_AUDIT.md)
--
-- Duplicate-surgery conflicts use:
--   COUNT(*) > 1 per (research_id, surg_date)
--   AND COUNT(DISTINCT concat(examined,positive)) > 1
