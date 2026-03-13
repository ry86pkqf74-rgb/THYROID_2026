#!/usr/bin/env python3
"""
29_validation_engine.py -- MotherDuck-native validation engine

Uses MotherDuck SQL to confirm, validate, reconcile, and quantify
extraction/linkage quality across all clinical domains.

Tables created (all prefixed val_):
  1. val_histology_confirmation      -- cross-source histology concordance
  2. val_molecular_confirmation      -- molecular test cross-validation
  3. val_rai_confirmation            -- RAI episode confirmation
  4. val_chronology_anomalies        -- temporal ordering violations
  5. val_missing_derivable           -- NULL fields derivable from other sources
  6. val_unlinked_linkable           -- orphaned events with linkage evidence
  7. val_completeness_scorecard      -- per-domain, per-field fill rates
  8. val_review_queue_combined       -- priority-ranked combined review export

Run after scripts 22-25 (canonical episodes + QA).
Supports --md flag for MotherDuck deployment.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    tables = [
        "path_synoptics", "tumor_pathology", "operative_details",
        "molecular_testing", "fna_history", "fna_cytology",
        "ultrasound_reports", "us_nodules_tirads", "ct_imaging",
        "mri_imaging", "nuclear_med",
        "note_entities_staging", "note_entities_genetics",
        "note_entities_medications", "note_entities_procedures",
        "note_entities_complications", "note_entities_problem_list",
        "clinical_notes_long",
    ]
    for tbl in tables:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists() and not table_available(con, tbl):
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. ADJUDICATION CONFIRMATION — Histology
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_HISTOLOGY_CONFIRMATION_SQL = """
CREATE OR REPLACE TABLE val_histology_confirmation AS
WITH ps AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date,
        tumor_1_histologic_type AS ps_histology,
        tumor_1_variant AS ps_variant,
        NULL AS ps_t_stage,
        NULL AS ps_n_stage,
        TRY_CAST(REPLACE(CAST(tumor_1_size_greatest_dimension_cm AS VARCHAR), ';', '') AS DOUBLE) AS ps_size_cm
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
        TRY_CAST(tumor_1_size_cm AS DOUBLE) AS tp_size_cm
    FROM tumor_pathology
    WHERE research_id IS NOT NULL
),
canon AS (
    SELECT
        research_id, surgery_episode_id, surgery_date,
        primary_histology, histology_variant, histology_source,
        t_stage, n_stage, tumor_size_cm,
        histology_discordance_flag, t_stage_discordance_flag,
        confidence_rank
    FROM tumor_episode_master_v2
),
nlp_staging AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS nlp_staging_mention_ct,
        COUNT(DISTINCT entity_value_norm) AS nlp_distinct_stages
    FROM note_entities_staging
    WHERE present_or_negated = 'present'
    GROUP BY CAST(research_id AS INTEGER)
)
SELECT
    canon.research_id,
    canon.surgery_episode_id,
    canon.surgery_date,
    ps.ps_histology,
    tp.tp_histology,
    canon.primary_histology AS canonical_histology,
    canon.histology_source,
    ps.ps_t_stage,
    tp.tp_t_stage,
    canon.t_stage AS canonical_t_stage,
    ps.ps_size_cm,
    tp.tp_size_cm,
    canon.tumor_size_cm AS canonical_size_cm,
    COALESCE(nlp.nlp_staging_mention_ct, 0) AS nlp_staging_mentions,
    canon.histology_discordance_flag,
    canon.t_stage_discordance_flag,
    canon.confidence_rank,
    CASE
        WHEN ps.ps_histology IS NOT NULL AND tp.tp_histology IS NOT NULL
             AND LOWER(TRIM(ps.ps_histology)) = LOWER(TRIM(tp.tp_histology))
             THEN 'confirmed_concordant'
        WHEN canon.histology_discordance_flag THEN 'discordant_needs_review'
        WHEN ps.ps_histology IS NOT NULL AND tp.tp_histology IS NULL THEN 'single_source_ps'
        WHEN ps.ps_histology IS NULL AND tp.tp_histology IS NOT NULL THEN 'single_source_tp'
        WHEN ps.ps_histology IS NULL AND tp.tp_histology IS NULL THEN 'no_structured_source'
        ELSE 'partial_match'
    END AS confirmation_status,
    CASE
        WHEN canon.histology_discordance_flag OR canon.t_stage_discordance_flag THEN TRUE
        WHEN ps.ps_histology IS NULL AND tp.tp_histology IS NULL THEN TRUE
        ELSE FALSE
    END AS needs_review
FROM canon
LEFT JOIN ps ON canon.research_id = ps.research_id
    AND (canon.surgery_date = ps.surg_date OR ps.surg_date IS NULL OR canon.surgery_date IS NULL)
LEFT JOIN tp ON canon.research_id = tp.research_id
LEFT JOIN nlp_staging nlp ON canon.research_id = nlp.research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. ADJUDICATION CONFIRMATION — Molecular
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_MOLECULAR_CONFIRMATION_SQL = """
CREATE OR REPLACE TABLE val_molecular_confirmation AS
WITH canon AS (
    SELECT
        research_id, molecular_episode_id, platform, platform_raw,
        test_date_native, overall_result_class, date_status,
        braf_flag, braf_variant, ras_flag, ras_subtype,
        ret_flag, tert_flag, ntrk_flag, tp53_flag,
        high_risk_marker_flag, inadequate_flag, cancelled_flag,
        linked_fna_episode_id, linked_surgery_episode_id,
        adjudication_status
    FROM molecular_test_episode_v2
),
raw_mol AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        ROW_NUMBER() OVER (PARTITION BY CAST(research_id AS INTEGER)
                           ORDER BY COALESCE(TRY_CAST(date AS DATE), DATE '2099-01-01')) AS raw_ordinal,
        thyroseq_afirma AS raw_platform,
        result AS raw_result,
        mutation AS raw_mutation,
        TRY_CAST(date AS DATE) AS raw_date
    FROM molecular_testing
    WHERE research_id IS NOT NULL
),
nlp_genetics AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS nlp_mutation_mentions,
        STRING_AGG(DISTINCT entity_value_norm, '; ' ORDER BY entity_value_norm) AS nlp_mutations_found
    FROM note_entities_genetics
    WHERE present_or_negated = 'present' AND entity_value_norm IS NOT NULL
    GROUP BY CAST(research_id AS INTEGER)
)
SELECT
    canon.research_id,
    canon.molecular_episode_id,
    canon.platform AS canonical_platform,
    rm.raw_platform,
    canon.test_date_native AS canonical_date,
    rm.raw_date,
    canon.overall_result_class AS canonical_result,
    rm.raw_result,
    rm.raw_mutation,
    canon.braf_flag, canon.ras_flag, canon.ret_flag, canon.tert_flag,
    canon.high_risk_marker_flag,
    canon.inadequate_flag, canon.cancelled_flag,
    COALESCE(nlp.nlp_mutation_mentions, 0) AS nlp_mutation_mentions,
    nlp.nlp_mutations_found,
    canon.linked_fna_episode_id,
    canon.linked_surgery_episode_id,
    canon.date_status,
    CASE
        WHEN rm.raw_result IS NOT NULL
             AND LOWER(TRIM(COALESCE(rm.raw_result,''))) != ''
             AND canon.overall_result_class != 'other'
             THEN 'raw_and_canonical_present'
        WHEN rm.raw_result IS NULL AND canon.overall_result_class IS NOT NULL
             THEN 'canonical_only'
        WHEN rm.raw_result IS NOT NULL AND canon.overall_result_class IS NULL
             THEN 'raw_only'
        ELSE 'both_missing'
    END AS confirmation_status,
    CASE
        WHEN canon.braf_flag AND nlp.nlp_mutations_found LIKE '%braf%' THEN TRUE
        WHEN canon.ras_flag AND nlp.nlp_mutations_found LIKE '%ras%' THEN TRUE
        WHEN canon.ret_flag AND nlp.nlp_mutations_found LIKE '%ret%' THEN TRUE
        WHEN NOT canon.braf_flag AND NOT canon.ras_flag AND NOT canon.ret_flag
             AND nlp.nlp_mutation_mentions > 0 THEN FALSE
        ELSE NULL
    END AS nlp_corroborates_canonical,
    CASE
        WHEN canon.inadequate_flag OR canon.cancelled_flag THEN FALSE
        WHEN canon.high_risk_marker_flag AND nlp.nlp_mutation_mentions = 0 THEN TRUE
        WHEN canon.overall_result_class = 'other' THEN TRUE
        ELSE FALSE
    END AS needs_review
FROM canon
LEFT JOIN raw_mol rm ON canon.research_id = rm.research_id
    AND canon.molecular_episode_id = rm.raw_ordinal
LEFT JOIN nlp_genetics nlp ON canon.research_id = nlp.research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. ADJUDICATION CONFIRMATION — RAI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_RAI_CONFIRMATION_SQL = """
CREATE OR REPLACE TABLE val_rai_confirmation AS
WITH canon AS (
    SELECT
        research_id, rai_episode_id,
        resolved_rai_date, rai_assertion_status, rai_intent,
        completion_status, dose_mci, date_status, date_confidence,
        linked_surgery_episode_id, source_note_type, rai_confidence,
        rai_mention_raw
    FROM rai_treatment_episode_v2
),
rai_mention_counts AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS total_rai_mentions,
        COUNT(DISTINCT note_row_id) AS distinct_notes_with_rai,
        COUNT(*) FILTER (WHERE present_or_negated = 'present') AS affirmed_mentions,
        COUNT(*) FILTER (WHERE present_or_negated = 'negated') AS negated_mentions
    FROM note_entities_medications
    WHERE LOWER(entity_value_norm) LIKE 'rai%'
       OR LOWER(entity_value_norm) LIKE 'i-131%'
       OR LOWER(entity_value_norm) LIKE 'i131%'
       OR LOWER(entity_value_raw) LIKE '%radioactive%iodine%'
       OR LOWER(entity_value_raw) LIKE '%rai %'
       OR LOWER(entity_value_raw) LIKE '%i-131%'
    GROUP BY CAST(research_id AS INTEGER)
),
surgery_context AS (
    SELECT
        research_id,
        MIN(surgery_date_native) AS first_surgery_date,
        COUNT(*) AS surgery_count
    FROM operative_episode_detail_v2
    WHERE surgery_date_native IS NOT NULL
    GROUP BY research_id
)
SELECT
    canon.research_id,
    canon.rai_episode_id,
    canon.resolved_rai_date,
    canon.rai_assertion_status,
    canon.rai_intent,
    canon.completion_status,
    canon.dose_mci,
    canon.date_status,
    canon.linked_surgery_episode_id,
    COALESCE(mc.total_rai_mentions, 0) AS total_rai_note_mentions,
    COALESCE(mc.affirmed_mentions, 0) AS affirmed_rai_mentions,
    COALESCE(mc.negated_mentions, 0) AS negated_rai_mentions,
    sc.first_surgery_date,
    CASE WHEN canon.resolved_rai_date IS NOT NULL AND sc.first_surgery_date IS NOT NULL
         THEN DATEDIFF('day', sc.first_surgery_date, canon.resolved_rai_date)
         ELSE NULL
    END AS days_after_first_surgery,
    CASE
        WHEN canon.rai_assertion_status = 'definite_received'
             AND mc.affirmed_mentions >= 2 THEN 'confirmed_multi_mention'
        WHEN canon.rai_assertion_status = 'definite_received'
             AND mc.affirmed_mentions = 1 THEN 'single_mention_received'
        WHEN canon.rai_assertion_status = 'likely_received'
             AND canon.dose_mci IS NOT NULL THEN 'likely_with_dose'
        WHEN canon.rai_assertion_status = 'planned' THEN 'planned_not_confirmed'
        WHEN canon.rai_assertion_status = 'negated' THEN 'explicitly_negated'
        WHEN canon.rai_assertion_status = 'ambiguous' THEN 'ambiguous_needs_review'
        ELSE 'uncertain'
    END AS confirmation_status,
    CASE
        WHEN canon.rai_assertion_status = 'ambiguous' THEN TRUE
        WHEN canon.rai_assertion_status IN ('definite_received', 'likely_received')
             AND canon.dose_mci IS NULL THEN TRUE
        WHEN canon.resolved_rai_date IS NOT NULL
             AND sc.first_surgery_date IS NOT NULL
             AND canon.resolved_rai_date < sc.first_surgery_date
             AND canon.rai_assertion_status NOT IN ('historical', 'negated') THEN TRUE
        ELSE FALSE
    END AS needs_review
FROM canon
LEFT JOIN rai_mention_counts mc ON canon.research_id = mc.research_id
LEFT JOIN surgery_context sc ON canon.research_id = sc.research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. CHRONOLOGY ANOMALIES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_CHRONOLOGY_ANOMALIES_SQL = """
CREATE OR REPLACE TABLE val_chronology_anomalies AS

-- A. RAI before first surgery (non-historical, non-negated)
SELECT
    r.research_id,
    'rai_before_surgery' AS anomaly_type,
    'error' AS severity,
    'rai' AS event_a_domain,
    CAST(r.resolved_rai_date AS VARCHAR) AS event_a_date,
    'surgery' AS event_b_domain,
    CAST(o.surgery_date_native AS VARCHAR) AS event_b_date,
    DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) AS day_gap,
    'RAI (' || r.rai_assertion_status || ') precedes surgery by '
        || CAST(ABS(DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date)) AS VARCHAR) || ' days' AS detail
FROM rai_treatment_episode_v2 r
JOIN operative_episode_detail_v2 o ON r.research_id = o.research_id
WHERE r.resolved_rai_date IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND r.resolved_rai_date < o.surgery_date_native
  AND r.rai_assertion_status NOT IN ('historical', 'negated', 'planned')
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.research_id, r.rai_episode_id
                           ORDER BY o.surgery_date_native) = 1

UNION ALL

-- B. Very late RAI (> 365 days post-surgery)
SELECT
    r.research_id,
    'late_rai' AS anomaly_type,
    'warning' AS severity,
    'rai' AS event_a_domain,
    CAST(r.resolved_rai_date AS VARCHAR),
    'surgery' AS event_b_domain,
    CAST(o.surgery_date_native AS VARCHAR),
    DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date),
    'RAI ' || CAST(DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) AS VARCHAR)
        || ' days after surgery'
FROM rai_treatment_episode_v2 r
JOIN operative_episode_detail_v2 o ON r.research_id = o.research_id
WHERE r.resolved_rai_date IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) > 365
  AND r.rai_assertion_status NOT IN ('negated', 'historical')
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.research_id, r.rai_episode_id
                           ORDER BY o.surgery_date_native) = 1

UNION ALL

-- C. Multiple surgeries < 30 days apart (excluding completions)
SELECT
    o1.research_id,
    'rapid_repeat_surgery' AS anomaly_type,
    'error' AS severity,
    'surgery' AS event_a_domain,
    CAST(o1.surgery_date_native AS VARCHAR),
    'surgery' AS event_b_domain,
    CAST(o2.surgery_date_native AS VARCHAR),
    DATEDIFF('day', o1.surgery_date_native, o2.surgery_date_native),
    'Two surgeries only ' ||
        CAST(DATEDIFF('day', o1.surgery_date_native, o2.surgery_date_native) AS VARCHAR)
        || ' days apart'
FROM operative_episode_detail_v2 o1
JOIN operative_episode_detail_v2 o2
    ON o1.research_id = o2.research_id
    AND o1.surgery_episode_id < o2.surgery_episode_id
WHERE o1.surgery_date_native IS NOT NULL
  AND o2.surgery_date_native IS NOT NULL
  AND DATEDIFF('day', o1.surgery_date_native, o2.surgery_date_native) BETWEEN 1 AND 29
  AND o2.procedure_normalized != 'completion_thyroidectomy'

UNION ALL

-- D. Molecular test after surgery (potential post-op surveillance vs data error)
SELECT
    m.research_id,
    'molecular_post_surgery' AS anomaly_type,
    'info' AS severity,
    'molecular' AS event_a_domain,
    CAST(m.test_date_native AS VARCHAR),
    'surgery' AS event_b_domain,
    CAST(o.surgery_date_native AS VARCHAR),
    DATEDIFF('day', o.surgery_date_native, m.test_date_native),
    m.platform || ' test ' ||
        CAST(DATEDIFF('day', o.surgery_date_native, m.test_date_native) AS VARCHAR)
        || ' days post-surgery'
FROM molecular_test_episode_v2 m
JOIN operative_episode_detail_v2 o ON m.research_id = o.research_id
WHERE m.test_date_native IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND m.test_date_native > o.surgery_date_native
  AND DATEDIFF('day', o.surgery_date_native, m.test_date_native) > 30
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.research_id, m.molecular_episode_id
                           ORDER BY o.surgery_date_native) = 1

UNION ALL

-- E. FNA after surgery on same laterality (potential recurrence workup vs error)
SELECT
    f.research_id,
    'fna_after_surgery_same_side' AS anomaly_type,
    'warning' AS severity,
    'fna' AS event_a_domain,
    CAST(f.fna_date_native AS VARCHAR),
    'surgery' AS event_b_domain,
    CAST(o.surgery_date_native AS VARCHAR),
    DATEDIFF('day', o.surgery_date_native, f.fna_date_native),
    'FNA on ' || COALESCE(f.laterality, '?') || ' side '
        || CAST(DATEDIFF('day', o.surgery_date_native, f.fna_date_native) AS VARCHAR)
        || ' days post-surgery'
FROM fna_episode_master_v2 f
JOIN operative_episode_detail_v2 o ON f.research_id = o.research_id
WHERE f.fna_date_native IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND f.fna_date_native > o.surgery_date_native
  AND COALESCE(f.laterality, '') = COALESCE(o.laterality, '')
  AND f.laterality IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.research_id, f.fna_episode_id
                           ORDER BY o.surgery_date_native) = 1

UNION ALL

-- F. Future dates (any domain)
SELECT
    research_id,
    'future_date' AS anomaly_type,
    'error' AS severity,
    domain AS event_a_domain,
    resolved_date AS event_a_date,
    NULL AS event_b_domain,
    NULL AS event_b_date,
    NULL AS day_gap,
    domain || ' event has future date: ' || resolved_date AS detail
FROM event_date_audit_v2
WHERE TRY_CAST(resolved_date AS DATE) > CURRENT_DATE
  AND resolved_date IS NOT NULL

UNION ALL

-- G. Implausibly old dates (before 1990)
SELECT
    research_id,
    'implausible_old_date' AS anomaly_type,
    'error' AS severity,
    domain AS event_a_domain,
    resolved_date AS event_a_date,
    NULL, NULL, NULL,
    domain || ' event date before 1990: ' || resolved_date
FROM event_date_audit_v2
WHERE TRY_CAST(resolved_date AS DATE) < DATE '1990-01-01'
  AND resolved_date IS NOT NULL
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. MISSING-BUT-DERIVABLE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_MISSING_DERIVABLE_SQL = """
CREATE OR REPLACE TABLE val_missing_derivable AS

-- A. Histology missing in canonical but present in tumor_pathology
SELECT
    'tumor' AS domain,
    t.research_id,
    CAST(t.surgery_episode_id AS VARCHAR) AS episode_id,
    'primary_histology' AS field_name,
    tp.tp_histology AS derivable_value,
    'tumor_pathology' AS derivable_from,
    70 AS derivation_confidence
FROM tumor_episode_master_v2 t
JOIN (SELECT CAST(research_id AS INTEGER) AS research_id,
             histology_1_type AS tp_histology
      FROM tumor_pathology WHERE research_id IS NOT NULL AND histology_1_type IS NOT NULL) tp
    ON t.research_id = tp.research_id
WHERE t.primary_histology IS NULL AND tp.tp_histology IS NOT NULL

UNION ALL

-- B. T-stage missing in canonical but present in tumor_pathology
SELECT 'tumor', t.research_id, CAST(t.surgery_episode_id AS VARCHAR),
       't_stage', tp.tp_t_stage, 'tumor_pathology', 70
FROM tumor_episode_master_v2 t
JOIN (SELECT CAST(research_id AS INTEGER) AS research_id,
             histology_1_t_stage_ajcc8 AS tp_t_stage
      FROM tumor_pathology WHERE research_id IS NOT NULL AND histology_1_t_stage_ajcc8 IS NOT NULL) tp
    ON t.research_id = tp.research_id
WHERE t.t_stage IS NULL AND tp.tp_t_stage IS NOT NULL

UNION ALL

-- C. Laterality missing in tumor but derivable from operative
SELECT 'tumor', t.research_id, CAST(t.surgery_episode_id AS VARCHAR),
       'laterality', o.laterality, 'operative_episode_detail_v2', 80
FROM tumor_episode_master_v2 t
JOIN operative_episode_detail_v2 o
    ON t.research_id = o.research_id
    AND (t.surgery_date = o.surgery_date_native OR t.surgery_date IS NULL OR o.surgery_date_native IS NULL)
WHERE t.laterality IS NULL AND o.laterality IS NOT NULL

UNION ALL

-- D. Molecular test: bethesda derivable from linked FNA
SELECT 'molecular', m.research_id, CAST(m.molecular_episode_id AS VARCHAR),
       'bethesda_category', CAST(f.bethesda_category AS VARCHAR), 'fna_episode_master_v2', 90
FROM molecular_test_episode_v2 m
JOIN fna_episode_master_v2 f
    ON m.research_id = f.research_id
    AND CAST(f.fna_episode_id AS VARCHAR) = m.linked_fna_episode_id
WHERE m.bethesda_category IS NULL AND f.bethesda_category IS NOT NULL

UNION ALL

-- E. Molecular test: specimen_site derivable from linked FNA
SELECT 'molecular', m.research_id, CAST(m.molecular_episode_id AS VARCHAR),
       'specimen_site_raw', f.specimen_site_raw, 'fna_episode_master_v2', 85
FROM molecular_test_episode_v2 m
JOIN fna_episode_master_v2 f
    ON m.research_id = f.research_id
    AND CAST(f.fna_episode_id AS VARCHAR) = m.linked_fna_episode_id
WHERE m.specimen_site_raw IS NULL AND f.specimen_site_raw IS NOT NULL

UNION ALL

-- F. Operative: procedure derivable from path_synoptics procedure text
SELECT 'operative', o.research_id, CAST(o.surgery_episode_id AS VARCHAR),
       'procedure_normalized', ps.proc_norm, 'path_synoptics', 85
FROM operative_episode_detail_v2 o
JOIN (SELECT CAST(research_id AS INTEGER) AS research_id,
             TRY_CAST(surg_date AS DATE) AS ps_date,
             CASE
                 WHEN LOWER(thyroid_procedure) LIKE '%total thyroidectomy%' THEN 'total_thyroidectomy'
                 WHEN LOWER(thyroid_procedure) LIKE '%completion%' THEN 'completion_thyroidectomy'
                 WHEN LOWER(thyroid_procedure) LIKE '%lobectomy%' THEN 'hemithyroidectomy'
                 WHEN LOWER(thyroid_procedure) LIKE '%hemithyroidectomy%' THEN 'hemithyroidectomy'
                 ELSE NULL
             END AS proc_norm
      FROM path_synoptics WHERE research_id IS NOT NULL) ps
    ON o.research_id = ps.research_id
    AND (o.surgery_date_native = ps.ps_date OR ps.ps_date IS NULL OR o.surgery_date_native IS NULL)
WHERE o.procedure_normalized = 'unknown' AND ps.proc_norm IS NOT NULL

UNION ALL

-- G. Imaging: TI-RADS missing but available in extracted_tirads_validated_v1
SELECT 'imaging', i.research_id, i.nodule_id,
       'tirads_score', CAST(tv.tirads_best_score AS VARCHAR),
       'extracted_tirads_validated_v1', 90
FROM imaging_nodule_long_v2 i
LEFT JOIN (SELECT CAST(research_id AS INTEGER) AS research_id,
                  tirads_best_score
           FROM extracted_tirads_validated_v1 WHERE research_id IS NOT NULL) tv
    ON i.research_id = tv.research_id
WHERE i.tirads_score IS NULL AND tv.tirads_best_score IS NOT NULL AND i.modality = 'US'

UNION ALL

-- H. RAI dose missing but extractable from note text patterns
SELECT 'rai', r.research_id, CAST(r.rai_episode_id AS VARCHAR),
       'dose_mci', NULL, 'note_entities_medications', 40
FROM rai_treatment_episode_v2 r
WHERE r.dose_mci IS NULL
  AND r.rai_assertion_status IN ('definite_received', 'likely_received')
  AND r.completion_status = 'completed'
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. UNLINKED-BUT-LIKELY-LINKABLE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_UNLINKED_LINKABLE_SQL = """
CREATE OR REPLACE TABLE val_unlinked_linkable AS

-- A. Molecular tests with no FNA link but an FNA exists within 90 days
SELECT
    'molecular' AS source_domain,
    m.research_id,
    CAST(m.molecular_episode_id AS VARCHAR) AS source_episode_id,
    'fna' AS target_domain,
    CAST(f.fna_episode_id AS VARCHAR) AS candidate_target_id,
    m.test_date_native AS source_date,
    f.fna_date_native AS target_date,
    ABS(DATEDIFF('day', m.test_date_native, f.fna_date_native)) AS temporal_gap_days,
    NULL::BOOLEAN AS laterality_match,
    CASE
        WHEN ABS(DATEDIFF('day', m.test_date_native, f.fna_date_native)) <= 14 THEN 'high_confidence'
        WHEN ABS(DATEDIFF('day', m.test_date_native, f.fna_date_native)) <= 90 THEN 'plausible'
        ELSE 'weak'
    END AS suggested_confidence,
    m.platform || ' test near FNA (Bethesda ' ||
        COALESCE(CAST(f.bethesda_category AS VARCHAR), '?') || ')' AS detail
FROM molecular_test_episode_v2 m
JOIN fna_episode_master_v2 f ON m.research_id = f.research_id
WHERE m.linked_fna_episode_id IS NULL
  AND m.test_date_native IS NOT NULL
  AND f.fna_date_native IS NOT NULL
  AND ABS(DATEDIFF('day', m.test_date_native, f.fna_date_native)) <= 90
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.research_id, m.molecular_episode_id
                           ORDER BY ABS(DATEDIFF('day', m.test_date_native, f.fna_date_native))) = 1

UNION ALL

-- B. RAI with no surgery link but a surgery exists within 365 days before
SELECT
    'rai', r.research_id,
    CAST(r.rai_episode_id AS VARCHAR),
    'surgery', CAST(o.surgery_episode_id AS VARCHAR),
    r.resolved_rai_date, o.surgery_date_native,
    DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date),
    NULL,
    CASE
        WHEN DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) BETWEEN 14 AND 180
             THEN 'high_confidence'
        WHEN DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) BETWEEN 1 AND 365
             THEN 'plausible'
        ELSE 'weak'
    END,
    'RAI ' || CAST(DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) AS VARCHAR)
        || ' days post-surgery'
FROM rai_treatment_episode_v2 r
JOIN operative_episode_detail_v2 o ON r.research_id = o.research_id
WHERE r.linked_surgery_episode_id IS NULL
  AND r.resolved_rai_date IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date) BETWEEN 1 AND 365
  AND r.rai_assertion_status NOT IN ('negated', 'historical')
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.research_id, r.rai_episode_id
                           ORDER BY ABS(DATEDIFF('day', o.surgery_date_native, r.resolved_rai_date))) = 1

UNION ALL

-- C. Imaging nodules with no FNA link but FNA exists within 30 days
SELECT
    'imaging', i.research_id,
    i.nodule_id,
    'fna', CAST(f.fna_episode_id AS VARCHAR),
    i.exam_date_native, f.fna_date_native,
    ABS(DATEDIFF('day', i.exam_date_native, f.fna_date_native)),
    CASE
        WHEN CAST(i.laterality AS VARCHAR) = CAST(f.laterality AS VARCHAR) THEN TRUE
        WHEN i.laterality IS NULL OR f.laterality IS NULL THEN NULL
        ELSE FALSE
    END,
    CASE
        WHEN ABS(DATEDIFF('day', i.exam_date_native, f.fna_date_native)) <= 7
             AND COALESCE(CAST(i.laterality AS VARCHAR) = CAST(f.laterality AS VARCHAR), TRUE) THEN 'high_confidence'
        WHEN ABS(DATEDIFF('day', i.exam_date_native, f.fna_date_native)) <= 30
             THEN 'plausible'
        ELSE 'weak'
    END,
    i.modality || ' nodule near FNA (' || COALESCE(CAST(i.laterality AS VARCHAR), '?') || ' vs '
        || COALESCE(CAST(f.laterality AS VARCHAR), '?') || ')'
FROM imaging_nodule_long_v2 i
JOIN fna_episode_master_v2 f ON i.research_id = f.research_id
WHERE i.linked_fna_episode_id IS NULL
  AND i.exam_date_native IS NOT NULL
  AND f.fna_date_native IS NOT NULL
  AND ABS(DATEDIFF('day', i.exam_date_native, f.fna_date_native)) <= 30
QUALIFY ROW_NUMBER() OVER (PARTITION BY i.research_id, i.nodule_id
                           ORDER BY ABS(DATEDIFF('day', i.exam_date_native, f.fna_date_native))) = 1

UNION ALL

-- D. FNA with no molecular link but molecular test exists for same patient
SELECT
    'fna', f.research_id,
    CAST(f.fna_episode_id AS VARCHAR),
    'molecular', CAST(m.molecular_episode_id AS VARCHAR),
    f.fna_date_native, m.test_date_native,
    CASE WHEN f.fna_date_native IS NOT NULL AND m.test_date_native IS NOT NULL
         THEN ABS(DATEDIFF('day', f.fna_date_native, m.test_date_native))
         ELSE NULL END,
    NULL,
    CASE
        WHEN f.fna_date_native IS NOT NULL AND m.test_date_native IS NOT NULL
             AND ABS(DATEDIFF('day', f.fna_date_native, m.test_date_native)) <= 14
             THEN 'high_confidence'
        WHEN f.fna_date_native IS NOT NULL AND m.test_date_native IS NOT NULL
             AND ABS(DATEDIFF('day', f.fna_date_native, m.test_date_native)) <= 90
             THEN 'plausible'
        WHEN f.fna_date_native IS NULL OR m.test_date_native IS NULL THEN 'weak'
        ELSE 'weak'
    END,
    'FNA (Bethesda ' || COALESCE(CAST(f.bethesda_category AS VARCHAR), '?')
        || ') potentially linked to ' || m.platform
FROM fna_episode_master_v2 f
JOIN molecular_test_episode_v2 m ON f.research_id = m.research_id
WHERE f.linked_molecular_episode_id IS NULL
  AND (f.fna_date_native IS NULL OR m.test_date_native IS NULL
       OR ABS(DATEDIFF('day', f.fna_date_native, m.test_date_native)) <= 180)
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.research_id, f.fna_episode_id
                           ORDER BY COALESCE(
                               ABS(DATEDIFF('day', f.fna_date_native, m.test_date_native)),
                               999)) = 1
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. COMPLETENESS SCORECARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_COMPLETENESS_SCORECARD_SQL = """
CREATE OR REPLACE TABLE val_completeness_scorecard AS

SELECT 'tumor' AS domain, 'primary_histology' AS field_name,
       COUNT(*) AS total, COUNT(primary_histology) AS filled,
       COUNT(*) - COUNT(primary_histology) AS missing,
       ROUND(100.0 * COUNT(primary_histology) / NULLIF(COUNT(*), 0), 1) AS fill_pct
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 't_stage', COUNT(*), COUNT(t_stage),
       COUNT(*) - COUNT(t_stage),
       ROUND(100.0 * COUNT(t_stage) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 'n_stage', COUNT(*), COUNT(n_stage),
       COUNT(*) - COUNT(n_stage),
       ROUND(100.0 * COUNT(n_stage) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 'tumor_size_cm', COUNT(*), COUNT(tumor_size_cm),
       COUNT(*) - COUNT(tumor_size_cm),
       ROUND(100.0 * COUNT(tumor_size_cm) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 'laterality', COUNT(*), COUNT(laterality),
       COUNT(*) - COUNT(laterality),
       ROUND(100.0 * COUNT(laterality) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 'extrathyroidal_extension', COUNT(*), COUNT(extrathyroidal_extension),
       COUNT(*) - COUNT(extrathyroidal_extension),
       ROUND(100.0 * COUNT(extrathyroidal_extension) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2
UNION ALL
SELECT 'tumor', 'vascular_invasion', COUNT(*), COUNT(vascular_invasion),
       COUNT(*) - COUNT(vascular_invasion),
       ROUND(100.0 * COUNT(vascular_invasion) / NULLIF(COUNT(*), 0), 1)
FROM tumor_episode_master_v2

UNION ALL

SELECT 'molecular', 'platform', COUNT(*), COUNT(platform),
       COUNT(*) - COUNT(platform),
       ROUND(100.0 * COUNT(platform) / NULLIF(COUNT(*), 0), 1)
FROM molecular_test_episode_v2
UNION ALL
SELECT 'molecular', 'test_date_native', COUNT(*), COUNT(test_date_native),
       COUNT(*) - COUNT(test_date_native),
       ROUND(100.0 * COUNT(test_date_native) / NULLIF(COUNT(*), 0), 1)
FROM molecular_test_episode_v2
UNION ALL
SELECT 'molecular', 'overall_result_class', COUNT(*),
       COUNT(*) FILTER (WHERE overall_result_class NOT IN ('other','')),
       COUNT(*) FILTER (WHERE overall_result_class IN ('other','') OR overall_result_class IS NULL),
       ROUND(100.0 * COUNT(*) FILTER (WHERE overall_result_class NOT IN ('other',''))
             / NULLIF(COUNT(*), 0), 1)
FROM molecular_test_episode_v2
UNION ALL
SELECT 'molecular', 'linked_fna_episode_id', COUNT(*), COUNT(linked_fna_episode_id),
       COUNT(*) - COUNT(linked_fna_episode_id),
       ROUND(100.0 * COUNT(linked_fna_episode_id) / NULLIF(COUNT(*), 0), 1)
FROM molecular_test_episode_v2

UNION ALL

SELECT 'rai', 'resolved_rai_date', COUNT(*), COUNT(resolved_rai_date),
       COUNT(*) - COUNT(resolved_rai_date),
       ROUND(100.0 * COUNT(resolved_rai_date) / NULLIF(COUNT(*), 0), 1)
FROM rai_treatment_episode_v2
UNION ALL
SELECT 'rai', 'dose_mci', COUNT(*), COUNT(dose_mci),
       COUNT(*) - COUNT(dose_mci),
       ROUND(100.0 * COUNT(dose_mci) / NULLIF(COUNT(*), 0), 1)
FROM rai_treatment_episode_v2
UNION ALL
SELECT 'rai', 'rai_intent', COUNT(*),
       COUNT(*) FILTER (WHERE rai_intent != 'unknown'),
       COUNT(*) FILTER (WHERE rai_intent = 'unknown' OR rai_intent IS NULL),
       ROUND(100.0 * COUNT(*) FILTER (WHERE rai_intent != 'unknown')
             / NULLIF(COUNT(*), 0), 1)
FROM rai_treatment_episode_v2
UNION ALL
SELECT 'rai', 'linked_surgery_episode_id', COUNT(*), COUNT(linked_surgery_episode_id),
       COUNT(*) - COUNT(linked_surgery_episode_id),
       ROUND(100.0 * COUNT(linked_surgery_episode_id) / NULLIF(COUNT(*), 0), 1)
FROM rai_treatment_episode_v2

UNION ALL

SELECT 'imaging', 'size_cm_max', COUNT(*), COUNT(size_cm_max),
       COUNT(*) - COUNT(size_cm_max),
       ROUND(100.0 * COUNT(size_cm_max) / NULLIF(COUNT(*), 0), 1)
FROM imaging_nodule_long_v2
UNION ALL
SELECT 'imaging', 'tirads_score', COUNT(*), COUNT(tirads_score),
       COUNT(*) - COUNT(tirads_score),
       ROUND(100.0 * COUNT(tirads_score) / NULLIF(COUNT(*), 0), 1)
FROM imaging_nodule_long_v2
UNION ALL
SELECT 'imaging', 'composition', COUNT(*), COUNT(composition),
       COUNT(*) - COUNT(composition),
       ROUND(100.0 * COUNT(composition) / NULLIF(COUNT(*), 0), 1)
FROM imaging_nodule_long_v2
UNION ALL
SELECT 'imaging', 'linked_fna_episode_id', COUNT(*), COUNT(linked_fna_episode_id),
       COUNT(*) - COUNT(linked_fna_episode_id),
       ROUND(100.0 * COUNT(linked_fna_episode_id) / NULLIF(COUNT(*), 0), 1)
FROM imaging_nodule_long_v2

UNION ALL

SELECT 'operative', 'procedure_normalized', COUNT(*),
       COUNT(*) FILTER (WHERE procedure_normalized NOT IN ('unknown','other','')),
       COUNT(*) FILTER (WHERE procedure_normalized IN ('unknown','other','') OR procedure_normalized IS NULL),
       ROUND(100.0 * COUNT(*) FILTER (WHERE procedure_normalized NOT IN ('unknown','other',''))
             / NULLIF(COUNT(*), 0), 1)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'operative', 'laterality', COUNT(*), COUNT(laterality),
       COUNT(*) - COUNT(laterality),
       ROUND(100.0 * COUNT(laterality) / NULLIF(COUNT(*), 0), 1)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'operative', 'surgery_date_native', COUNT(*), COUNT(surgery_date_native),
       COUNT(*) - COUNT(surgery_date_native),
       ROUND(100.0 * COUNT(surgery_date_native) / NULLIF(COUNT(*), 0), 1)
FROM operative_episode_detail_v2

UNION ALL

SELECT 'fna', 'bethesda_category', COUNT(*), COUNT(bethesda_category),
       COUNT(*) - COUNT(bethesda_category),
       ROUND(100.0 * COUNT(bethesda_category) / NULLIF(COUNT(*), 0), 1)
FROM fna_episode_master_v2
UNION ALL
SELECT 'fna', 'laterality', COUNT(*), COUNT(laterality),
       COUNT(*) - COUNT(laterality),
       ROUND(100.0 * COUNT(laterality) / NULLIF(COUNT(*), 0), 1)
FROM fna_episode_master_v2
UNION ALL
SELECT 'fna', 'linked_molecular_episode_id', COUNT(*), COUNT(linked_molecular_episode_id),
       COUNT(*) - COUNT(linked_molecular_episode_id),
       ROUND(100.0 * COUNT(linked_molecular_episode_id) / NULLIF(COUNT(*), 0), 1)
FROM fna_episode_master_v2
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. COMBINED MANUAL REVIEW QUEUE (export-ready)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VAL_REVIEW_QUEUE_COMBINED_SQL = """
CREATE OR REPLACE TABLE val_review_queue_combined AS

SELECT
    research_id,
    'histology' AS domain,
    confirmation_status AS issue_type,
    CASE WHEN histology_discordance_flag THEN 'error' ELSE 'warning' END AS severity,
    1 AS review_priority,
    'Histology: ' || COALESCE(ps_histology, '?') || ' (PS) vs ' ||
        COALESCE(canonical_histology, '?') || ' (canonical)' AS description,
    'confidence_rank=' || CAST(confidence_rank AS VARCHAR) AS detail
FROM val_histology_confirmation
WHERE needs_review

UNION ALL

SELECT
    research_id, 'molecular', confirmation_status,
    CASE WHEN high_risk_marker_flag AND nlp_mutation_mentions = 0 THEN 'error'
         ELSE 'warning' END,
    CASE WHEN high_risk_marker_flag THEN 1 ELSE 2 END,
    'Molecular: ' || COALESCE(canonical_platform, '?') || ' ' ||
        COALESCE(canonical_result, '?'),
    'nlp_mentions=' || CAST(nlp_mutation_mentions AS VARCHAR)
FROM val_molecular_confirmation
WHERE needs_review

UNION ALL

SELECT
    research_id, 'rai', confirmation_status,
    CASE
        WHEN confirmation_status = 'ambiguous_needs_review' THEN 'error'
        WHEN days_after_first_surgery IS NOT NULL AND days_after_first_surgery < 0 THEN 'error'
        ELSE 'warning'
    END,
    CASE WHEN confirmation_status = 'ambiguous_needs_review' THEN 1 ELSE 2 END,
    'RAI: ' || rai_assertion_status || ' / ' || completion_status ||
        COALESCE(' / dose=' || CAST(dose_mci AS VARCHAR), ''),
    'mentions=' || CAST(total_rai_note_mentions AS VARCHAR) ||
        ', days_post_surg=' || COALESCE(CAST(days_after_first_surgery AS VARCHAR), '?')
FROM val_rai_confirmation
WHERE needs_review

UNION ALL

SELECT
    research_id, event_a_domain || '->' || COALESCE(event_b_domain, '?'),
    anomaly_type, severity,
    CASE severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
    detail,
    'gap=' || COALESCE(CAST(day_gap AS VARCHAR), '?') || ' days'
FROM val_chronology_anomalies

UNION ALL

SELECT
    research_id, source_domain || '->' || target_domain,
    'unlinked_linkable', 'info',
    CASE suggested_confidence
        WHEN 'high_confidence' THEN 2
        WHEN 'plausible' THEN 3
        ELSE 4
    END,
    detail,
    'gap=' || COALESCE(CAST(temporal_gap_days AS VARCHAR), '?')
        || ', confidence=' || suggested_confidence
FROM val_unlinked_linkable

ORDER BY review_priority, severity, research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Orchestration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

VAL_RLN_INTRINSIC_SQL = """
CREATE OR REPLACE TABLE val_rln_intrinsic_eval AS
WITH refined AS (
    SELECT
        research_id,
        rln_injury_tier,
        rln_injury_is_confirmed,
        classification,
        rln_injury_evidence_strength
    FROM extracted_rln_injury_refined_v2
),
original AS (
    SELECT research_id, source_tier, worst_status
    FROM vw_patient_postop_rln_injury_detail
)
SELECT
    'rln_intrinsic_eval'       AS check_id,
    'info'                     AS severity,
    o.research_id,
    'RLN tier comparison: original=' || o.source_tier
        || ' refined=' || COALESCE(r.classification, 'excluded')
        || ' confirmed=' || COALESCE(CAST(r.rln_injury_is_confirmed AS VARCHAR), 'false')
                                AS description,
    o.worst_status || ' | evidence=' || COALESCE(r.rln_injury_evidence_strength, 'none')
                                AS detail
FROM original o
LEFT JOIN refined r ON o.research_id = r.research_id
"""


VAL_PHASE6_STAGING_REFINEMENT_SQL = """
CREATE OR REPLACE TABLE val_phase6_staging_refinement AS
WITH
margin_stats AS (
    SELECT
        'margin_status' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN margin_r_classification = 'R0' THEN 1 END) AS r0,
        COUNT(CASE WHEN margin_r_classification = 'R0_close' THEN 1 END) AS r0_close,
        COUNT(CASE WHEN margin_r_classification = 'R1' THEN 1 END) AS r1,
        COUNT(CASE WHEN margin_r_classification = 'R2' THEN 1 END) AS r2,
        COUNT(CASE WHEN margin_r_classification = 'Rx' THEN 1 END) AS rx,
        COUNT(closest_margin_mm) AS n_with_distance,
        AVG(confidence) AS avg_confidence,
        'path_synoptic' AS primary_source
    FROM extracted_margins_refined_v1
),
vasc_stats AS (
    SELECT
        'vascular_invasion_who2022' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN vascular_positive THEN 1 END) AS positive,
        COUNT(CASE WHEN vascular_who_2022_grade = 'focal (<4 vessels)' THEN 1 END) AS who_focal,
        COUNT(CASE WHEN vascular_who_2022_grade = 'extensive (>=4 vessels)' THEN 1 END) AS who_extensive,
        0 AS extra1, 0 AS extra2,
        COUNT(vessel_count) AS n_with_quantify,
        AVG(confidence) AS avg_confidence,
        'path_synoptic' AS primary_source
    FROM extracted_invasion_profile_v1
),
lvi_stats AS (
    SELECT
        'lvi' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN lvi_positive THEN 1 END) AS positive,
        COUNT(CASE WHEN lvi_refined = 'focal' THEN 1 END) AS focal,
        COUNT(CASE WHEN lvi_refined = 'extensive' THEN 1 END) AS extensive,
        0 AS extra1, 0 AS extra2, 0 AS n_extra,
        AVG(confidence) AS avg_confidence,
        'path_synoptic' AS primary_source
    FROM extracted_invasion_profile_v1
),
pni_stats AS (
    SELECT
        'pni' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN pni_positive THEN 1 END) AS positive,
        COUNT(CASE WHEN pni_refined = 'focal' THEN 1 END) AS focal,
        0 AS extra1, 0 AS extra2, 0 AS extra3, 0 AS n_extra,
        AVG(confidence) AS avg_confidence,
        'path_synoptic' AS primary_source
    FROM extracted_invasion_profile_v1
),
ln_stats AS (
    SELECT
        'ln_yield' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN ln_positive_flag THEN 1 END) AS ln_positive,
        COUNT(CASE WHEN central_dissected THEN 1 END) AS central,
        COUNT(CASE WHEN lateral_dissected THEN 1 END) AS lateral,
        0 AS extra1, 0 AS extra2,
        COUNT(ln_ratio) AS n_with_ratio,
        AVG(confidence) AS avg_confidence,
        'path_synoptic' AS primary_source
    FROM extracted_ln_yield_v1
),
ene_stats AS (
    SELECT
        'ene_deepened' AS variable,
        COUNT(*) AS total_patients,
        COUNT(CASE WHEN ene_positive THEN 1 END) AS ene_positive,
        COUNT(CASE WHEN concordance_status = 'concordant_positive' THEN 1 END) AS concordant,
        COUNT(CASE WHEN concordance_status LIKE 'discordant%%' THEN 1 END) AS discordant,
        COUNT(CASE WHEN source_chain = 'path_synoptic+op_note' THEN 1 END) AS dual_source,
        0 AS extra1, 0 AS n_extra,
        AVG(confidence) AS avg_confidence,
        'path_synoptic+op_note' AS primary_source
    FROM extracted_ene_refined_v2
)
SELECT variable, total_patients, positive, focal, extensive, extra1, extra2,
       n_extra, avg_confidence, primary_source,
       CURRENT_TIMESTAMP AS validated_at
FROM (
    SELECT variable, total_patients, r1 AS positive, r0_close AS focal,
           r2 AS extensive, rx AS extra1, 0 AS extra2,
           n_with_distance AS n_extra, avg_confidence, primary_source
    FROM margin_stats
    UNION ALL SELECT * FROM vasc_stats
    UNION ALL SELECT * FROM lvi_stats
    UNION ALL SELECT * FROM pni_stats
    UNION ALL SELECT * FROM ln_stats
    UNION ALL SELECT * FROM ene_stats
)
ORDER BY total_patients DESC;
"""

VAL_PROVENANCE_TRACEABILITY_SQL = """
CREATE OR REPLACE TABLE val_provenance_traceability AS
-- Check 1: direct_source_link completeness (provenance_enriched_events_v1)
SELECT
    'direct_source_link_missing' AS check_id,
    'error'                      AS severity,
    CAST(research_id AS INT)     AS research_id,
    event_subtype                AS description,
    'provenance_enriched_events_v1' AS source_table,
    CONCAT(
        'event_type=', COALESCE(event_type, 'NULL'),
        ' date_status=', COALESCE(date_status_final, 'NULL')
    )                            AS detail,
    CURRENT_TIMESTAMP            AS validated_at
FROM provenance_enriched_events_v1
WHERE (direct_source_link IS NULL OR TRIM(direct_source_link) = '')

UNION ALL

-- Check 2: zero-tolerance for NOTE_DATE_FALLBACK on lab events
SELECT
    'lab_note_date_fallback'     AS check_id,
    'error'                      AS severity,
    CAST(research_id AS INT)     AS research_id,
    event_subtype                AS description,
    'provenance_enriched_events_v1' AS source_table,
    CONCAT(
        'event_date=', COALESCE(CAST(event_date AS VARCHAR), 'NULL'),
        ' source=', COALESCE(source_column, 'NULL')
    )                            AS detail,
    CURRENT_TIMESTAMP            AS validated_at
FROM provenance_enriched_events_v1
WHERE event_type = 'lab'
  AND date_status_final = 'NOTE_DATE_FALLBACK'

UNION ALL

-- Check 3: lab events with no date at all
SELECT
    'lab_no_date'                AS check_id,
    'warning'                    AS severity,
    CAST(research_id AS INT)     AS research_id,
    event_subtype                AS description,
    'provenance_enriched_events_v1' AS source_table,
    CONCAT('source=', COALESCE(source_column, 'NULL')) AS detail,
    CURRENT_TIMESTAMP            AS validated_at
FROM provenance_enriched_events_v1
WHERE event_type = 'lab'
  AND date_status_final = 'NO_DATE'

UNION ALL

-- Check 4: date traceability gaps in lineage audit
SELECT
    'lineage_date_untraced'      AS check_id,
    'warning'                    AS severity,
    CAST(research_id AS INT)     AS research_id,
    'lineage gap'                AS description,
    'lineage_audit_v1'           AS source_table,
    CONCAT(
        'traceability=', COALESCE(date_traceability_status, 'NULL'),
        ' surgery_date=', COALESCE(raw_surgery_date, 'NULL')
    )                            AS detail,
    CURRENT_TIMESTAMP            AS validated_at
FROM lineage_audit_v1
WHERE date_traceability_status = 'untraced'
"""


# NOTE: ALL_VALIDATION_SQL is assembled after all SQL variable definitions below.
# See the end of this module for the list definition.


VAL_PHASE10_STAGING_RECOVERY_SQL = """
CREATE OR REPLACE TABLE val_phase10_staging_recovery AS
WITH margin_stats AS (
    SELECT
        'margin_r0_recovery' AS variable,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN margin_status_recovered = 'negative' THEN 1 END) AS r0_recovered,
        COUNT(CASE WHEN margin_status_recovered = 'positive' THEN 1 END) AS r1_recovered,
        COUNT(CASE WHEN margin_status_recovered = 'not_applicable' THEN 1 END) AS benign_classified,
        STRING_AGG(DISTINCT source_type, ', ') AS sources_used
    FROM extracted_margin_r0_recovery_v1
),
invasion_stats AS (
    SELECT
        'invasion_grading' AS variable,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN grade_recovered = 'focal' THEN 1 END) AS focal_graded,
        COUNT(CASE WHEN grade_recovered = 'extensive' THEN 1 END) AS extensive_graded,
        0 AS benign_classified,
        STRING_AGG(DISTINCT source_type, ', ') AS sources_used
    FROM extracted_invasion_grading_recovery_v1
),
lateral_stats AS (
    SELECT
        'lateral_neck' AS variable,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT research_id) AS unique_patients,
        0 AS r0_or_focal,
        0 AS r1_or_extensive,
        0 AS benign_classified,
        STRING_AGG(DISTINCT source_type, ', ') AS sources_used
    FROM extracted_lateral_neck_v1
),
multi_tumor_stats AS (
    SELECT
        'multi_tumor_agg' AS variable,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN worst_angioinvasion IS NOT NULL THEN 1 END) AS has_angio,
        COUNT(CASE WHEN worst_margin IS NOT NULL THEN 1 END) AS has_margin,
        COUNT(CASE WHEN worst_ete IS NOT NULL THEN 1 END) AS has_ete,
        'path_synoptics_multi_tumor' AS sources_used
    FROM extracted_multi_tumor_aggregate_v1
),
mice_stats AS (
    SELECT
        'mice_imputation' AS variable,
        COALESCE(SUM(n_patients), 0) AS total_rows,
        COALESCE(MAX(n_patients), 0) AS unique_patients,
        COALESCE(MAX(m_imputations), 0) AS m_imputations,
        0 AS placeholder_1,
        0 AS placeholder_2,
        COALESCE(MAX(imputation_method), 'not_run') AS sources_used
    FROM extracted_mice_summary_v1
)
SELECT 'margin' AS domain, variable, total_rows, unique_patients, r0_recovered AS metric_1, r1_recovered AS metric_2, benign_classified AS metric_3, sources_used FROM margin_stats
UNION ALL SELECT 'invasion', variable, total_rows, unique_patients, focal_graded, extensive_graded, benign_classified, sources_used FROM invasion_stats
UNION ALL SELECT 'lateral_neck', variable, total_rows, unique_patients, r0_or_focal, r1_or_extensive, benign_classified, sources_used FROM lateral_stats
UNION ALL SELECT 'multi_tumor', variable, total_rows, unique_patients, has_angio, has_margin, has_ete, sources_used FROM multi_tumor_stats
UNION ALL SELECT 'mice', variable, total_rows, unique_patients, m_imputations, placeholder_1, placeholder_2, sources_used FROM mice_stats;
"""

VAL_PHASE9_TARGETED_REFINEMENT_SQL = """
CREATE OR REPLACE TABLE val_phase9_targeted_refinement AS
WITH lab_stats AS (
    SELECT
        'postop_labs' AS variable,
        COUNT(*) AS total_values,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN lab_type = 'pth' THEN 1 END) AS pth_count,
        COUNT(CASE WHEN lab_type = 'total_calcium' THEN 1 END) AS calcium_count,
        STRING_AGG(DISTINCT extraction_method, ', ') AS methods
    FROM extracted_postop_labs_expanded_v1
),
rai_stats AS (
    SELECT
        'rai_dose' AS variable,
        COUNT(*) AS total_values,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN linkage_status = 'structured' THEN 1 END) AS structured_count,
        COUNT(CASE WHEN linkage_status LIKE 'nlp%' THEN 1 END) AS nlp_count,
        STRING_AGG(DISTINCT source_table, ', ') AS methods
    FROM extracted_rai_dose_refined_v1
),
ete_stats AS (
    SELECT
        'ete_grading' AS variable,
        COUNT(*) AS total_values,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN ete_grade_v9 = 'microscopic' THEN 1 END) AS microscopic_count,
        COUNT(CASE WHEN ete_grade_v9 = 'gross' THEN 1 END) AS gross_count,
        STRING_AGG(DISTINCT ete_rule_applied, ', ') FILTER (WHERE ete_rule_applied IS NOT NULL) AS methods
    FROM extracted_ete_ene_tert_refined_v1
    WHERE ete_grade_v9 IS NOT NULL
),
tert_stats AS (
    SELECT
        'tert_subtyping' AS variable,
        COUNT(*) AS total_values,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN tert_variant_v9 = 'C228T' THEN 1 END) AS c228t_count,
        COUNT(CASE WHEN tert_variant_v9 = 'C250T' THEN 1 END) AS c250t_count,
        STRING_AGG(DISTINCT tert_variant_v9, ', ') FILTER (WHERE tert_variant_v9 IS NOT NULL) AS methods
    FROM extracted_ete_ene_tert_refined_v1
    WHERE tert_positive_v9 IS TRUE
),
ene_stats AS (
    SELECT
        'ene_grading' AS variable,
        COUNT(*) AS total_values,
        COUNT(DISTINCT research_id) AS unique_patients,
        COUNT(CASE WHEN ene_grade_v9 IN ('focal','extensive') THEN 1 END) AS graded_count,
        COUNT(CASE WHEN ene_grade_v9 = 'present_ungraded' THEN 1 END) AS ungraded_count,
        STRING_AGG(DISTINCT ene_grade_v9, ', ') FILTER (WHERE ene_grade_v9 IS NOT NULL) AS methods
    FROM extracted_ete_ene_tert_refined_v1
    WHERE ene_grade_v9 IS NOT NULL
)
SELECT variable, total_values, unique_patients, pth_count AS metric_a, calcium_count AS metric_b, methods
FROM lab_stats
UNION ALL
SELECT variable, total_values, unique_patients, structured_count, nlp_count, methods
FROM rai_stats
UNION ALL
SELECT variable, total_values, unique_patients, microscopic_count, gross_count, methods
FROM ete_stats
UNION ALL
SELECT variable, total_values, unique_patients, c228t_count, c250t_count, methods
FROM tert_stats
UNION ALL
SELECT variable, total_values, unique_patients, graded_count, ungraded_count, methods
FROM ene_stats
ORDER BY variable;
"""

VAL_PHASE5_REFINEMENT_SQL = """
CREATE OR REPLACE TABLE val_phase5_refinement AS
WITH ete_stats AS (
    SELECT
        'ete_subgrade' AS variable,
        COUNT(*) AS total_input,
        COUNT(CASE WHEN refined_ete_grade <> 'present_ungraded' THEN 1 END) AS refined,
        COUNT(CASE WHEN refined_ete_grade = 'present_ungraded' THEN 1 END) AS still_ungraded
    FROM extracted_ete_subgraded_v1
),
tert_stats AS (
    SELECT
        'tert' AS variable,
        COUNT(*) AS total_input,
        COUNT(CASE WHEN tert_positive_refined THEN 1 END) AS refined,
        COUNT(CASE WHEN tert_tested AND NOT tert_positive_refined THEN 1 END) AS still_ungraded
    FROM extracted_molecular_refined_v1
),
lab_stats AS (
    SELECT
        'postop_labs' AS variable,
        COUNT(*) AS total_input,
        COUNT(CASE WHEN lab_type = 'pth' THEN 1 END) AS refined,
        COUNT(CASE WHEN lab_type LIKE '%calcium%' THEN 1 END) AS still_ungraded
    FROM extracted_postop_labs_v1
),
rai_stats AS (
    SELECT
        'rai_validation' AS variable,
        COUNT(*) AS total_input,
        COUNT(CASE WHEN rai_validation_tier LIKE 'confirmed%' THEN 1 END) AS refined,
        COUNT(CASE WHEN rai_validation_tier LIKE 'unconfirmed%' THEN 1 END) AS still_ungraded
    FROM extracted_rai_validated_v1
),
ene_stats AS (
    SELECT
        'ene' AS variable,
        COUNT(*) AS total_input,
        COUNT(CASE WHEN ene_positive THEN 1 END) AS refined,
        COUNT(CASE WHEN ene_status_refined = 'present_ungraded' THEN 1 END) AS still_ungraded
    FROM extracted_ene_refined_v1
)
SELECT * FROM ete_stats
UNION ALL SELECT * FROM tert_stats
UNION ALL SELECT * FROM lab_stats
UNION ALL SELECT * FROM rai_stats
UNION ALL SELECT * FROM ene_stats;
"""

# Phase 2 QA: complication refinement validation
VAL_COMPLICATION_REFINEMENT_SQL = """
CREATE OR REPLACE TABLE val_complication_refinement AS
WITH raw_counts AS (
    SELECT entity_value_norm AS entity_name,
        COUNT(DISTINCT CAST(research_id AS INT)) AS raw_patients,
        SUM(CASE WHEN present_or_negated = 'present' THEN 1 ELSE 0 END) AS raw_present_mentions
    FROM note_entities_complications
    GROUP BY entity_value_norm
),
refined_counts AS (
    SELECT entity_name,
        COUNT(DISTINCT research_id) AS refined_patients,
        SUM(CASE WHEN entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_patients,
        SUM(CASE WHEN entity_tier = 1 THEN 1 ELSE 0 END) AS tier1_patients,
        SUM(CASE WHEN entity_tier = 2 THEN 1 ELSE 0 END) AS tier2_patients,
        SUM(CASE WHEN entity_tier = 3 THEN 1 ELSE 0 END) AS tier3_patients
    FROM extracted_complications_refined_v5
    GROUP BY entity_name
)
SELECT
    r.entity_name,
    r.raw_patients,
    r.raw_present_mentions,
    COALESCE(f.refined_patients, 0) AS refined_patients,
    COALESCE(f.confirmed_patients, 0) AS confirmed_patients,
    r.raw_patients - COALESCE(f.refined_patients, 0) AS excluded_patients,
    ROUND(100.0 * (r.raw_patients - COALESCE(f.refined_patients, 0)) / NULLIF(r.raw_patients, 0), 1) AS pct_excluded,
    ROUND(100.0 * COALESCE(f.confirmed_patients, 0) / NULLIF(r.raw_patients, 0), 1) AS pct_confirmed_of_raw,
    COALESCE(f.tier1_patients, 0) AS tier1_confirmed,
    COALESCE(f.tier2_patients, 0) AS tier2_probable,
    COALESCE(f.tier3_patients, 0) AS tier3_uncertain,
    CASE
        WHEN r.raw_patients > 0 AND COALESCE(f.refined_patients, 0) = 0 THEN 'no_refined_data'
        WHEN r.raw_patients > 0 AND ROUND(100.0 * COALESCE(f.confirmed_patients, 0) / r.raw_patients, 1) < 5.0 THEN 'high_fp_rate'
        WHEN COALESCE(f.confirmed_patients, 0) BETWEEN 1 AND 10 THEN 'low_volume_review'
        ELSE 'acceptable'
    END AS validation_status,
    CURRENT_TIMESTAMP AS validated_at
FROM raw_counts r
LEFT JOIN refined_counts f ON r.entity_name = f.entity_name
ORDER BY r.raw_patients DESC
"""

# Phase 4 QA: source-specific staging refinement validation
VAL_SOURCE_SPECIFIC_REFINEMENT_SQL = """
CREATE OR REPLACE TABLE val_source_specific_refinement AS
WITH

-- ETE concordance across sources
ete_stats AS (
    SELECT
        'ete' AS entity_name,
        COUNT(*) AS patients_with_data,
        SUM(CASE WHEN ete_path_confirmed THEN 1 ELSE 0 END) AS path_confirmed,
        SUM(CASE WHEN ete_op_note_observed THEN 1 ELSE 0 END) AS op_note_observed,
        SUM(CASE WHEN ete_overall_confirmed THEN 1 ELSE 0 END) AS overall_confirmed,
        SUM(CASE WHEN ete_grade = 'gross' THEN 1 ELSE 0 END) AS grade_gross,
        SUM(CASE WHEN ete_grade = 'microscopic' THEN 1 ELSE 0 END) AS grade_microscopic,
        SUM(CASE WHEN ete_grade = 'present_ungraded' THEN 1 ELSE 0 END) AS grade_ungraded,
        SUM(CASE WHEN ete_concordance_status = 'concordant' THEN 1 ELSE 0 END) AS concordant,
        SUM(CASE WHEN ete_concordance_status = 'discordant' THEN 1 ELSE 0 END) AS discordant,
        'path_report' AS primary_source,
        ROUND(100.0 * SUM(CASE WHEN ete_path_confirmed THEN 1 ELSE 0 END) /
              NULLIF(COUNT(*), 0), 1) AS source_fill_pct
    FROM patient_refined_staging_flags_v3
    WHERE ete_path_confirmed OR ete_op_note_observed
),

-- Invasion stats
invasion_stats AS (
    SELECT 'vascular_invasion' AS entity_name,
           COUNT(*) AS patients_with_data,
           SUM(CASE WHEN vascular_invasion_refined = 'extensive' THEN 1 ELSE 0 END) AS grade_extensive,
           SUM(CASE WHEN vascular_invasion_refined = 'focal' THEN 1 ELSE 0 END) AS grade_focal,
           SUM(CASE WHEN vascular_invasion_refined = 'present_ungraded' THEN 1 ELSE 0 END) AS grade_ungraded,
           0 AS concordant, 0 AS discordant,
           'path_report' AS primary_source,
           ROUND(100.0 * COUNT(*) / 10871.0, 1) AS source_fill_pct
    FROM patient_refined_staging_flags_v3
    WHERE vascular_invasion_refined IS NOT NULL AND vascular_invasion_refined != 'indeterminate'
    UNION ALL
    SELECT 'lvi', COUNT(*), 0, 0,
           SUM(CASE WHEN lvi_refined = 'extensive' THEN 1 ELSE 0 END),
           0, 0, 'path_report',
           ROUND(100.0 * COUNT(*) / 10871.0, 1)
    FROM patient_refined_staging_flags_v3
    WHERE lvi_refined IN ('present','extensive','focal')
    UNION ALL
    SELECT 'perineural_invasion', COUNT(*), 0, 0, 0, 0, 0, 'path_report',
           ROUND(100.0 * COUNT(*) / 10871.0, 1)
    FROM patient_refined_staging_flags_v3
    WHERE perineural_invasion_refined = 'present'
    UNION ALL
    SELECT 'margin_status_positive', COUNT(*), 0, 0, 0, 0, 0, 'path_report',
           ROUND(100.0 * COUNT(*) / 10871.0, 1)
    FROM patient_refined_staging_flags_v3
    WHERE margin_status_refined = 'positive'
),

combined AS (
    SELECT entity_name, patients_with_data, path_confirmed, op_note_observed,
           overall_confirmed, grade_gross, grade_microscopic, grade_ungraded,
           concordant, discordant, primary_source, source_fill_pct
    FROM ete_stats
    UNION ALL
    SELECT entity_name, patients_with_data, 0, 0,
           patients_with_data, grade_extensive, grade_focal, grade_ungraded,
           concordant, discordant, primary_source, source_fill_pct
    FROM invasion_stats
)

SELECT *, CURRENT_TIMESTAMP AS validated_at FROM combined
ORDER BY patients_with_data DESC
"""

# Phase 13 QA: final gaps closure validation
VAL_PHASE13_FINAL_GAPS_SQL = """
CREATE OR REPLACE TABLE val_phase13_final_gaps AS
SELECT 'vascular_graded' AS variable,
       (SELECT COUNT(*) FROM extracted_vascular_grading_v13
        WHERE vasc_grade_v13 IN ('focal','extensive')) AS refined_count,
       (SELECT COUNT(*) FROM extracted_vascular_grading_v13
        WHERE vasc_grade_v13 = 'present_ungraded') AS still_ungraded,
       (SELECT COUNT(*) FROM extracted_vascular_grading_v13) AS total_input,
       CURRENT_TIMESTAMP AS validated_at
UNION ALL
SELECT 'ihc_braf',
       (SELECT COUNT(*) FROM extracted_ihc_braf_v13
        WHERE ihc_braf_result = 'positive'),
       (SELECT COUNT(*) FROM extracted_ihc_braf_v13
        WHERE ihc_braf_result = 'negative'),
       (SELECT COUNT(*) FROM extracted_ihc_braf_v13),
       CURRENT_TIMESTAMP
UNION ALL
SELECT 'ras_resolved',
       (SELECT COUNT(*) FROM extracted_ras_resolved_v13),
       65 - (SELECT COUNT(*) FROM extracted_ras_resolved_v13),
       65,
       CURRENT_TIMESTAMP
UNION ALL
SELECT 'master_v12_total',
       (SELECT COUNT(*) FROM patient_refined_master_clinical_v12),
       0,
       (SELECT COUNT(*) FROM patient_refined_master_clinical_v12),
       CURRENT_TIMESTAMP
"""

VAL_LAB_CANONICAL_SQL = """
CREATE OR REPLACE TABLE val_lab_canonical_v1 AS
WITH plausibility AS (
    SELECT
        lab_name_standardized,
        analyte_group,
        COUNT(*) AS n_total,
        COUNT(*) FILTER (WHERE value_numeric IS NOT NULL) AS n_numeric,
        COUNT(*) FILTER (WHERE is_censored IS TRUE) AS n_censored,
        COUNT(*) FILTER (WHERE lab_date > CURRENT_DATE) AS n_future_dates,
        MIN(value_numeric) AS val_min,
        MAX(value_numeric) AS val_max,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'thyroglobulin'
            AND value_numeric IS NOT NULL AND (value_numeric < 0 OR value_numeric > 100000)) AS n_tg_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'pth'
            AND value_numeric IS NOT NULL AND (value_numeric < 0.5 OR value_numeric > 500)) AS n_pth_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'calcium_total'
            AND value_numeric IS NOT NULL AND (value_numeric < 4 OR value_numeric > 15)) AS n_ca_oob
    FROM longitudinal_lab_canonical_v1
    GROUP BY lab_name_standardized, analyte_group
),
tier_check AS (
    SELECT COUNT(*) AS n_invalid_tiers
    FROM longitudinal_lab_canonical_v1
    WHERE data_completeness_tier NOT IN (
        'current_structured', 'current_nlp_partial', 'future_institutional_required')
)
SELECT
    p.lab_name_standardized,
    p.n_total,
    p.n_numeric,
    p.n_censored,
    p.n_future_dates,
    (p.n_tg_oob + p.n_pth_oob + p.n_ca_oob) AS n_plausibility_violations,
    t.n_invalid_tiers,
    CASE
        WHEN p.n_future_dates > 0 THEN 'FAIL'
        WHEN (p.n_tg_oob + p.n_pth_oob + p.n_ca_oob) > 0 THEN 'WARN'
        WHEN t.n_invalid_tiers > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS validation_status,
    CURRENT_TIMESTAMP AS audited_at
FROM plausibility p
CROSS JOIN tier_check t
ORDER BY p.n_total DESC
"""

# All SQL variables are now defined above; assemble the registry.
ALL_VALIDATION_SQL: list[tuple[str, str, str]] = [
    ("val_histology_confirmation",  VAL_HISTOLOGY_CONFIRMATION_SQL,  "Adjudication: histology"),
    ("val_molecular_confirmation",  VAL_MOLECULAR_CONFIRMATION_SQL,  "Adjudication: molecular"),
    ("val_rai_confirmation",        VAL_RAI_CONFIRMATION_SQL,        "Adjudication: RAI"),
    ("val_chronology_anomalies",    VAL_CHRONOLOGY_ANOMALIES_SQL,    "Chronology anomalies"),
    ("val_missing_derivable",       VAL_MISSING_DERIVABLE_SQL,       "Missing-but-derivable"),
    ("val_unlinked_linkable",       VAL_UNLINKED_LINKABLE_SQL,       "Unlinked-but-linkable"),
    ("val_completeness_scorecard",  VAL_COMPLETENESS_SCORECARD_SQL,  "Completeness scorecard"),
    ("val_review_queue_combined",   VAL_REVIEW_QUEUE_COMBINED_SQL,   "Combined review queue"),
    ("val_rln_intrinsic_eval",      VAL_RLN_INTRINSIC_SQL,           "RLN intrinsic evaluation"),
    ("val_complication_refinement", VAL_COMPLICATION_REFINEMENT_SQL, "Phase 2 complication refinement audit"),
    ("val_source_specific_refinement", VAL_SOURCE_SPECIFIC_REFINEMENT_SQL, "Phase 4 source-specific variable refinement audit"),
    ("val_phase5_refinement",       VAL_PHASE5_REFINEMENT_SQL,       "Phase 5 top-5 variable refinement audit"),
    ("val_phase6_staging_refinement", VAL_PHASE6_STAGING_REFINEMENT_SQL, "Phase 6 source-linked staging refinement audit"),
    ("val_phase9_targeted_refinement", VAL_PHASE9_TARGETED_REFINEMENT_SQL, "Phase 9 targeted refinement audit"),
    ("val_phase10_staging_recovery",   VAL_PHASE10_STAGING_RECOVERY_SQL,  "Phase 10 source-linked recovery audit"),
    ("val_provenance_traceability",    VAL_PROVENANCE_TRACEABILITY_SQL,   "Phase 11 provenance + date-accuracy traceability"),
    ("val_phase13_final_gaps",         VAL_PHASE13_FINAL_GAPS_SQL,        "Phase 13 final gaps closure audit"),
    ("val_lab_canonical_v1",             VAL_LAB_CANONICAL_SQL,             "Canonical lab contract validation"),
]


def build_all(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Execute all validation table creation SQL. Returns {name: row_count}."""
    counts: dict[str, int] = {}
    for name, sql, desc in ALL_VALIDATION_SQL:
        section(desc)
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            counts[name] = cnt
            print(f"  Created {name:<45} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN: {name} skipped -- {e}")
            counts[name] = -1
    return counts


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    section("Validation Engine Summary")

    print("  === Confirmation Status Distribution ===\n")
    for tbl, status_col in [
        ("val_histology_confirmation", "confirmation_status"),
        ("val_molecular_confirmation", "confirmation_status"),
        ("val_rai_confirmation", "confirmation_status"),
    ]:
        if not table_available(con, tbl):
            continue
        print(f"  {tbl}:")
        rows = con.execute(
            f"SELECT {status_col}, COUNT(*) FROM {tbl} GROUP BY {status_col} "
            f"ORDER BY COUNT(*) DESC"
        ).fetchall()
        for r in rows:
            print(f"    {r[0]:<35} {r[1]:>6,}")
        print()

    print("  === Chronology Anomalies ===\n")
    if table_available(con, "val_chronology_anomalies"):
        rows = con.execute(
            "SELECT anomaly_type, severity, COUNT(*) FROM val_chronology_anomalies "
            "GROUP BY anomaly_type, severity ORDER BY severity, COUNT(*) DESC"
        ).fetchall()
        print(f"  {'Anomaly':<30} {'Severity':<10} {'Count':>8}")
        print(f"  {'-'*30} {'-'*10} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<30} {r[1]:<10} {r[2]:>8,}")
        print()

    print("  === Missing-but-Derivable ===\n")
    if table_available(con, "val_missing_derivable"):
        rows = con.execute(
            "SELECT domain, field_name, COUNT(*), ROUND(AVG(derivation_confidence),0) "
            "FROM val_missing_derivable GROUP BY domain, field_name "
            "ORDER BY COUNT(*) DESC"
        ).fetchall()
        print(f"  {'Domain':<12} {'Field':<30} {'Count':>8} {'Avg Conf':>8}")
        print(f"  {'-'*12} {'-'*30} {'-'*8} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<12} {r[1]:<30} {r[2]:>8,} {r[3]:>7.0f}%")
        print()

    print("  === Unlinked-but-Linkable ===\n")
    if table_available(con, "val_unlinked_linkable"):
        rows = con.execute(
            "SELECT source_domain, target_domain, suggested_confidence, COUNT(*) "
            "FROM val_unlinked_linkable GROUP BY source_domain, target_domain, suggested_confidence "
            "ORDER BY COUNT(*) DESC"
        ).fetchall()
        print(f"  {'Source':<12} {'Target':<12} {'Confidence':<18} {'Count':>8}")
        print(f"  {'-'*12} {'-'*12} {'-'*18} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<12} {r[1]:<12} {r[2]:<18} {r[3]:>8,}")
        print()

    print("  === Completeness Scorecard (lowest fill rates) ===\n")
    if table_available(con, "val_completeness_scorecard"):
        rows = con.execute(
            "SELECT domain, field_name, total, filled, fill_pct "
            "FROM val_completeness_scorecard ORDER BY fill_pct ASC LIMIT 15"
        ).fetchall()
        print(f"  {'Domain':<12} {'Field':<30} {'Total':>8} {'Filled':>8} {'Fill%':>7}")
        print(f"  {'-'*12} {'-'*30} {'-'*8} {'-'*8} {'-'*7}")
        for r in rows:
            print(f"  {r[0]:<12} {r[1]:<30} {r[2]:>8,} {r[3]:>8,} {r[4]:>6.1f}%")
        print()

    print("  === Review Queue ===\n")
    if table_available(con, "val_review_queue_combined"):
        cnt = con.execute("SELECT COUNT(*) FROM val_review_queue_combined").fetchone()[0]
        sev = con.execute(
            "SELECT severity, COUNT(*) FROM val_review_queue_combined "
            "GROUP BY severity ORDER BY CASE severity "
            "WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END"
        ).fetchall()
        print(f"  Total review items: {cnt:,}")
        for s in sev:
            print(f"    {s[0]:<10} {s[1]:>8,}")

    print("\n  === Provenance Traceability (val_provenance_traceability) ===\n")
    if table_available(con, "val_provenance_traceability"):
        rows = con.execute(
            "SELECT check_id, severity, COUNT(*) AS n "
            "FROM val_provenance_traceability "
            "GROUP BY check_id, severity "
            "ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, n DESC"
        ).fetchall()
        if rows:
            print(f"  {'Check ID':<40} {'Severity':<10} {'Count':>8}")
            print(f"  {'-'*40} {'-'*10} {'-'*8}")
            for r in rows:
                print(f"  {r[0]:<40} {r[1]:<10} {r[2]:>8,}")
            total_errors = sum(r[2] for r in rows if r[1] == "error")
            total_warn = sum(r[2] for r in rows if r[1] == "warning")
            print(f"\n  Summary: {total_errors:,} errors, {total_warn:,} warnings")
            if total_errors == 0:
                print("  STATUS: PASS -- all provenance checks green")
            else:
                print("  STATUS: FAIL -- provenance errors require remediation")
        else:
            print("  STATUS: PASS -- no provenance issues found")
    else:
        print("  INFO: val_provenance_traceability not yet built"
              " (run script 46 + script 29 --md)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Deploy to MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    section("29 -- Validation Engine")

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            print("  Falling back to local DuckDB")
            con = duckdb.connect(str(DB_PATH))
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Using local DuckDB: {DB_PATH}")

    register_parquets(con)

    required = [
        "tumor_episode_master_v2", "molecular_test_episode_v2",
        "rai_treatment_episode_v2", "imaging_nodule_long_v2",
        "operative_episode_detail_v2", "fna_episode_master_v2",
        "event_date_audit_v2",
    ]
    missing = [t for t in required if not table_available(con, t)]
    if missing:
        print(f"\n  ERROR: Missing required tables: {', '.join(missing)}")
        print("  Run scripts 22-25 first.")
        sys.exit(1)

    build_all(con)
    print_summary(con)

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
