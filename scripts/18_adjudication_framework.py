#!/usr/bin/env python3
"""
18_adjudication_framework.py — Phases B–H: Adjudication, Confidence Hardening,
and App-Ready Audit Outputs

Builds on:
  - Script 15 (Phase 1 enriched views)
  - Script 16 (reconciliation_v2)
  - Script 17 (semantic cleanup / date_status taxonomy)

Phases:
  B. Manual adjudication review queues (5 views)
  C. Molecular episode v3 with multi-dim confidence + analysis eligibility
  D. RAI episode v3 with assertion status + treatment classification
  E. Histology discordance adjudication + analysis cohort
  F. Patient reconciliation summary (fixed: uses path_synoptics as spine)
  G. Streamlit-ready support layer (5 app-facing views)
  H. QA metrics / deployment summary (computed at runtime)

Run after scripts 15, 16, 17.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"
SQL_OUT = ROOT / "scripts" / "18_adjudication_framework_views.sql"

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
        "path_synoptics", "tumor_pathology", "note_entities_staging",
        "note_entities_genetics", "note_entities_medications",
        "note_entities_procedures", "note_entities_complications",
        "note_entities_problem_list", "molecular_testing", "fna_history",
        "clinical_notes_long",
    ]
    for tbl in tables:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
        elif table_available(con, tbl):
            print(f"  Existing  {tbl:<45}")
        else:
            print(f"  SKIP      {tbl:<45}")

    stubs = {
        "tumor_pathology": """
            CREATE TABLE IF NOT EXISTS tumor_pathology AS
            SELECT NULL::VARCHAR AS research_id,
                   NULL::VARCHAR AS histology_1_type,
                   NULL::VARCHAR AS tumor_1_histology_variant,
                   NULL::VARCHAR AS histology_1_t_stage_ajcc8,
                   NULL::VARCHAR AS histology_1_n_stage_ajcc8,
                   NULL::VARCHAR AS histology_1_m_stage_ajcc8,
                   NULL::VARCHAR AS histology_1_overall_stage_ajcc8,
                   NULL::VARCHAR AS histology_1_largest_tumor_cm,
                   NULL::VARCHAR AS tumor_focality_overall,
                   NULL::VARCHAR AS braf_mutation_mentioned,
                   NULL::VARCHAR AS ras_mutation_mentioned,
                   NULL::VARCHAR AS ret_mutation_mentioned,
                   NULL::VARCHAR AS tert_mutation_mentioned
            WHERE FALSE
        """,
    }
    for tbl, stub_sql in stubs.items():
        if not table_available(con, tbl):
            con.execute(stub_sql)
            print(f"  STUB      {tbl:<45}")


def deploy_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    view_log: list[tuple[str, str]],
) -> bool:
    try:
        con.execute(sql)
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name:<55} {cnt:>8,} rows")
        view_log.append((name, sql))
        return True
    except Exception as e:
        print(f"  FAILED  {name}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE C — Molecular Episode V3
# ═══════════════════════════════════════════════════════════════════════════════

MOLECULAR_V3_SQL = """
CREATE OR REPLACE VIEW molecular_episode_v3 AS
WITH base AS (
    SELECT
        m.*,
        -- Date raw classification
        CASE
            WHEN m.specimen_date_raw IS NULL OR TRIM(CAST(m.specimen_date_raw AS VARCHAR)) = ''
                THEN 'missing'
            WHEN TRY_CAST(m.specimen_date_raw AS DATE) IS NOT NULL
                THEN 'exact_valid_date'
            WHEN regexp_matches(CAST(m.specimen_date_raw AS VARCHAR), '^\\d{4}$')
                THEN 'year_only'
            WHEN LOWER(CAST(m.specimen_date_raw AS VARCHAR)) IN ('x','none','maybe?','n/a','na','unknown')
                THEN 'placeholder'
            ELSE 'garbage_unparseable'
        END AS molecular_date_raw_class,
        CASE
            WHEN m.molecular_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS molecular_date_parse_success_flag,
        COALESCE(m.molecular_date_granularity, 'none') AS molecular_date_precision,

        -- Temporal linkage confidence (0-100)
        CASE
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'day'
                 AND m.linked_fna_date IS NOT NULL
                 AND ABS(m.days_molecular_to_fna) <= 30  THEN 95
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'day'
                 AND m.linked_fna_date IS NOT NULL
                 AND ABS(m.days_molecular_to_fna) <= 90  THEN 85
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'day'
                 AND m.linked_fna_date IS NOT NULL        THEN 70
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'day'
                 AND m.linked_surgery_date IS NOT NULL     THEN 60
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'year'
                 AND m.linked_fna_date IS NOT NULL
                 AND EXTRACT(YEAR FROM m.molecular_date) = EXTRACT(YEAR FROM m.linked_fna_date)
                THEN 50
            WHEN m.molecular_date IS NOT NULL AND m.molecular_date_granularity = 'year'
                THEN 30
            WHEN m.molecular_date IS NULL AND m.linked_fna_date IS NOT NULL
                THEN 20
            ELSE 0
        END AS temporal_linkage_confidence,

        -- Platform confidence (well-known assay = higher trust)
        CASE
            WHEN m.platform_normalized IN ('ThyroSeq', 'Afirma') THEN 90
            WHEN m.platform_raw IS NOT NULL AND TRIM(CAST(m.platform_raw AS VARCHAR)) != ''
                THEN 60
            ELSE 30
        END AS platform_confidence,

        -- Pathology concordance confidence
        CASE
            WHEN m.linked_surgery_date IS NOT NULL AND m.linked_histology IS NOT NULL
                THEN 80
            WHEN m.linked_surgery_date IS NOT NULL
                THEN 60
            WHEN m.linked_fna_date IS NOT NULL
                THEN 50
            ELSE 10
        END AS pathology_concordance_confidence

    FROM molecular_episode_v2 m
)
SELECT
    b.*,
    ROUND((b.temporal_linkage_confidence * 0.50
         + b.platform_confidence * 0.20
         + b.pathology_concordance_confidence * 0.30), 0)::INTEGER
        AS overall_linkage_confidence,
    CASE
        WHEN b.temporal_linkage_confidence >= 70
             AND b.platform_confidence >= 60
             AND b.pathology_concordance_confidence >= 50
            THEN TRUE
        WHEN b.temporal_linkage_confidence >= 85
             AND b.platform_confidence >= 30
            THEN TRUE
        ELSE FALSE
    END AS molecular_analysis_eligible_flag
FROM base b;
"""

MOLECULAR_ANALYSIS_COHORT_SQL = """
CREATE OR REPLACE VIEW molecular_analysis_cohort_v AS
SELECT *
FROM molecular_episode_v3
WHERE molecular_analysis_eligible_flag = TRUE;
"""

MOLECULAR_LINKAGE_FAILURE_SQL = """
CREATE OR REPLACE VIEW molecular_linkage_failure_summary_v AS
SELECT
    molecular_date_raw_class,
    molecular_date_precision,
    linkage_method,
    linkage_confidence AS v2_linkage_confidence,
    COUNT(*) AS row_count,
    COUNT(DISTINCT research_id) AS patient_count,
    ROUND(AVG(overall_linkage_confidence), 1) AS avg_overall_confidence,
    SUM(CASE WHEN molecular_analysis_eligible_flag THEN 1 ELSE 0 END) AS eligible_count,
    SUM(CASE WHEN NOT molecular_analysis_eligible_flag THEN 1 ELSE 0 END) AS ineligible_count
FROM molecular_episode_v3
WHERE molecular_analysis_eligible_flag = FALSE
GROUP BY 1, 2, 3, 4
ORDER BY row_count DESC;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE D — RAI Episode V3
# ═══════════════════════════════════════════════════════════════════════════════

RAI_V3_SQL = """
CREATE OR REPLACE VIEW rai_episode_v3 AS
WITH base AS (
    SELECT
        r.*,
        -- Assertion status (replaces simple rai_given_flag)
        CASE
            WHEN r.rai_given_flag = 'negated' THEN 'negated'
            WHEN r.rai_given_flag = 'planned' THEN 'planned'
            WHEN r.rai_given_flag = 'historical' THEN 'historical'
            WHEN r.rai_given_flag = 'definite' AND r.dose_mci IS NOT NULL
                THEN 'definite_received'
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365
                THEN 'definite_received'
            WHEN r.rai_given_flag = 'definite'
                THEN 'likely_received'
            ELSE 'ambiguous'
        END AS rai_assertion_status,

        -- Treatment certainty (0-100)
        CASE
            WHEN r.rai_given_flag = 'negated' THEN 0
            WHEN r.rai_given_flag = 'definite' AND r.dose_mci IS NOT NULL
                 AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365
                THEN 95
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365
                THEN 85
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                THEN 65
            WHEN r.rai_given_flag = 'definite'
                THEN 50
            WHEN r.rai_given_flag = 'planned' THEN 30
            WHEN r.rai_given_flag = 'historical' THEN 40
            ELSE 20
        END AS rai_treatment_certainty,

        -- Interval classification (replaces single 'invalid interval')
        CASE
            WHEN r.rai_date IS NULL THEN 'missing_rai_date'
            WHEN r.linked_surgery_date IS NULL THEN 'missing_surgery_anchor'
            WHEN r.days_surgery_to_rai < -30 THEN 'pre_index_surgery'
            WHEN r.days_surgery_to_rai > 730 THEN 'likely_recurrence_related'
            WHEN r.days_surgery_to_rai > 365 THEN 'too_remote_from_index_surgery'
            WHEN r.days_surgery_to_rai BETWEEN -30 AND 365 THEN 'plausible_index_treatment'
            ELSE 'plausible_index_treatment'
        END AS rai_interval_class

    FROM rai_episode_v2 r
)
SELECT
    b.*,
    CASE
        WHEN b.rai_assertion_status IN ('definite_received', 'likely_received')
             AND b.rai_interval_class = 'plausible_index_treatment'
            THEN TRUE
        WHEN b.rai_assertion_status = 'definite_received'
             AND b.rai_interval_class = 'too_remote_from_index_surgery'
            THEN TRUE
        ELSE FALSE
    END AS rai_eligible_for_analysis_flag
FROM base b;
"""

RAI_ANALYSIS_COHORT_SQL = """
CREATE OR REPLACE VIEW rai_analysis_cohort_v AS
SELECT *
FROM rai_episode_v3
WHERE rai_eligible_for_analysis_flag = TRUE;
"""

RAI_LINKAGE_FAILURE_SQL = """
CREATE OR REPLACE VIEW rai_linkage_failure_summary_v AS
SELECT
    rai_assertion_status,
    rai_interval_class,
    rai_given_flag AS v2_mention_type,
    COUNT(*) AS row_count,
    COUNT(DISTINCT CAST(research_id AS BIGINT)) AS patient_count,
    ROUND(AVG(rai_treatment_certainty), 1) AS avg_certainty,
    SUM(CASE WHEN rai_eligible_for_analysis_flag THEN 1 ELSE 0 END) AS eligible,
    SUM(CASE WHEN NOT rai_eligible_for_analysis_flag THEN 1 ELSE 0 END) AS ineligible
FROM rai_episode_v3
WHERE rai_eligible_for_analysis_flag = FALSE
GROUP BY 1, 2, 3
ORDER BY row_count DESC;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE E — Histology / Stage Discordance Adjudication
# ═══════════════════════════════════════════════════════════════════════════════

HISTOLOGY_ANALYSIS_COHORT_SQL = """
CREATE OR REPLACE VIEW histology_analysis_cohort_v AS
SELECT
    h.research_id,
    h.op_seq,
    h.pathology_date,

    -- Final analysis values with explicit source priority
    COALESCE(h.histology_normalized, 'unclassified') AS final_histology_for_analysis,
    h.variant_normalized AS final_variant_for_analysis,
    COALESCE(h.t_stage_reconciled, h.t_stage_source_note) AS final_t_stage_for_analysis,
    COALESCE(h.n_stage_reconciled, h.n_stage_source_note) AS final_n_stage_for_analysis,
    COALESCE(h.laterality_reconciled, 'unknown') AS final_laterality_for_analysis,
    h.tumor_behavior_category,

    -- Raw values preserved
    h.source_histology_raw_ps,
    h.source_histology_raw_tp,
    h.source_histology_raw,
    h.t_stage_source_path,
    h.t_stage_tp,
    h.t_stage_source_note,
    h.n_stage_source_path,
    h.n_stage_tp,
    h.n_stage_source_note,
    h.overall_stage_tp,
    h.laterality_path,
    h.multifocality_path,
    h.largest_focus_cm,
    h.tumor_1_extrathyroidal_extension,
    h.source_priority_used,

    -- Discordance typing (expanded)
    CASE
        WHEN h.histology_discordance_flag THEN 'histology_type_conflict'
        WHEN h.stage_discordance_flag AND h.n_stage_discordance_flag
            THEN 't_stage_conflict,n_stage_conflict'
        WHEN h.stage_discordance_flag THEN 't_stage_conflict'
        WHEN h.n_stage_discordance_flag THEN 'n_stage_conflict'
        WHEN h.laterality_reconciled IS NULL AND h.pathology_date IS NOT NULL
            THEN 'laterality_conflict'
        WHEN h.histology_normalized IS NULL AND h.source_histology_raw IS NOT NULL
            THEN 'note_only_histology'
        WHEN h.t_stage_reconciled IS NULL AND h.t_stage_source_note IS NOT NULL
            THEN 'missing_path_primary'
        ELSE NULL
    END AS discordance_type,

    -- Adjudication flags
    CASE
        WHEN h.histology_discordance_flag OR h.stage_discordance_flag
            THEN TRUE
        ELSE FALSE
    END AS adjudication_needed_flag,

    CASE
        WHEN h.unresolved_flag THEN FALSE
        WHEN h.histology_normalized IS NULL THEN FALSE
        WHEN h.histology_discordance_flag THEN FALSE
        ELSE TRUE
    END AS analysis_eligible_flag,

    h.histology_discordance_flag,
    h.stage_discordance_flag,
    h.n_stage_discordance_flag,
    h.unresolved_flag,
    h.reconciliation_status

FROM histology_reconciliation_v2 h;
"""

HISTOLOGY_DISCORDANCE_SUMMARY_SQL = """
CREATE OR REPLACE VIEW histology_discordance_summary_v AS
SELECT
    discordance_type,
    analysis_eligible_flag,
    adjudication_needed_flag,
    COUNT(*) AS row_count,
    COUNT(DISTINCT research_id) AS patient_count
FROM histology_analysis_cohort_v
WHERE discordance_type IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY row_count DESC;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE B — Manual Adjudication Review Queues
# ═══════════════════════════════════════════════════════════════════════════════

HISTOLOGY_REVIEW_QUEUE_SQL = """
CREATE OR REPLACE VIEW histology_manual_review_queue_v AS
SELECT
    ROW_NUMBER() OVER (ORDER BY
        CASE WHEN h.histology_discordance_flag THEN 0 ELSE 1 END,
        CASE WHEN h.stage_discordance_flag THEN 0 ELSE 1 END,
        h.research_id
    ) AS queue_row_id,
    CAST(h.research_id AS BIGINT) AS research_id,
    CASE
        WHEN h.histology_discordance_flag AND h.stage_discordance_flag THEN 100
        WHEN h.histology_discordance_flag THEN 90
        WHEN h.stage_discordance_flag THEN 80
        WHEN h.n_stage_discordance_flag THEN 50
        WHEN h.unresolved_flag THEN 40
        ELSE 10
    END AS priority_score,
    'histology' AS review_domain,
    h.discordance_type AS unresolved_reason,
    CASE
        WHEN h.histology_discordance_flag
            THEN 'PS=' || COALESCE(h.source_histology_raw_ps, 'NULL')
                 || ' vs TP=' || COALESCE(h.source_histology_raw_tp, 'NULL')
        WHEN h.stage_discordance_flag
            THEN 'Path T=' || COALESCE(h.t_stage_source_path, 'NULL')
                 || ' vs Note T=' || COALESCE(h.t_stage_source_note, 'NULL')
        ELSE 'staging incomplete'
    END AS conflict_summary,
    h.source_histology_raw_ps,
    h.source_histology_raw_tp,
    h.t_stage_source_path,
    h.t_stage_source_note,
    h.final_histology_for_analysis,
    h.final_t_stage_for_analysis,
    CASE
        WHEN h.histology_discordance_flag
            THEN 'Review pathology reports; determine correct histology type'
        WHEN h.stage_discordance_flag
            THEN 'Compare synoptic vs note staging; confirm AJCC8 stage'
        WHEN h.unresolved_flag
            THEN 'Obtain missing staging data from chart review'
        ELSE 'Verify reconciled values'
    END AS recommended_reviewer_action,
    'path_synoptics, tumor_pathology, note_entities_staging' AS supporting_source_objects,
    h.op_seq AS linked_episode_id
FROM histology_analysis_cohort_v h
WHERE h.adjudication_needed_flag = TRUE OR h.unresolved_flag = TRUE;
"""

MOLECULAR_REVIEW_QUEUE_SQL = """
CREATE OR REPLACE VIEW molecular_manual_review_queue_v AS
SELECT
    ROW_NUMBER() OVER (ORDER BY
        CASE WHEN m.high_risk_molecular_flag AND NOT m.molecular_analysis_eligible_flag THEN 0 ELSE 1 END,
        m.overall_linkage_confidence ASC,
        m.research_id
    ) AS queue_row_id,
    CAST(m.research_id AS BIGINT) AS research_id,
    CASE
        WHEN m.high_risk_molecular_flag AND NOT m.molecular_analysis_eligible_flag THEN 100
        WHEN m.high_risk_molecular_flag AND m.molecular_analysis_eligible_flag THEN 30
        WHEN NOT m.molecular_analysis_eligible_flag AND m.molecular_date_raw_class = 'exact_valid_date'
            THEN 70
        WHEN NOT m.molecular_analysis_eligible_flag THEN 50
        ELSE 10
    END AS priority_score,
    'molecular' AS review_domain,
    CASE
        WHEN m.molecular_date_raw_class = 'missing' THEN 'no_source_date'
        WHEN m.molecular_date_raw_class IN ('garbage_unparseable', 'placeholder')
            THEN 'unparseable_date'
        WHEN m.molecular_date_raw_class = 'year_only' THEN 'year_only_date'
        WHEN m.linkage_method = 'unlinked' THEN 'no_linkage_anchor'
        ELSE 'low_confidence_linkage'
    END AS unresolved_reason,
    'Date=' || COALESCE(m.specimen_date_raw, 'NULL')
        || ' Platform=' || COALESCE(m.platform_normalized, 'NULL')
        || ' Confidence=' || CAST(m.overall_linkage_confidence AS VARCHAR)
        AS conflict_summary,
    m.specimen_date_raw,
    m.platform_normalized,
    m.test_name_raw,
    m.result_category_normalized,
    m.result_summary_raw,
    CASE
        WHEN m.molecular_date_raw_class = 'missing'
            THEN 'Locate specimen date from lab report or FNA records'
        WHEN m.molecular_date_raw_class IN ('garbage_unparseable', 'placeholder')
            THEN 'Verify date in source record; correct if parseable'
        WHEN m.linkage_method = 'unlinked'
            THEN 'Link to FNA or surgical specimen via chart review'
        ELSE 'Verify temporal proximity to FNA/surgery'
    END AS recommended_reviewer_action,
    'molecular_testing, fna_history, path_synoptics' AS supporting_source_objects,
    m.molecular_episode_id AS linked_episode_id
FROM molecular_episode_v3 m
WHERE m.molecular_analysis_eligible_flag = FALSE
  AND m.molecular_date_raw_class != 'placeholder';
"""

RAI_REVIEW_QUEUE_SQL = """
CREATE OR REPLACE VIEW rai_manual_review_queue_v AS
SELECT
    ROW_NUMBER() OVER (ORDER BY
        CASE WHEN r.rai_assertion_status IN ('definite_received','likely_received')
                  AND NOT r.rai_eligible_for_analysis_flag THEN 0 ELSE 1 END,
        r.rai_treatment_certainty DESC,
        CAST(r.research_id AS BIGINT)
    ) AS queue_row_id,
    CAST(r.research_id AS BIGINT) AS research_id,
    CASE
        WHEN r.rai_assertion_status = 'definite_received'
             AND NOT r.rai_eligible_for_analysis_flag THEN 100
        WHEN r.rai_assertion_status = 'likely_received'
             AND NOT r.rai_eligible_for_analysis_flag THEN 90
        WHEN r.rai_assertion_status = 'ambiguous' THEN 60
        WHEN r.rai_assertion_status = 'planned' THEN 40
        WHEN r.rai_assertion_status = 'historical' THEN 30
        ELSE 10
    END AS priority_score,
    'rai' AS review_domain,
    r.rai_interval_class AS unresolved_reason,
    'Assertion=' || r.rai_assertion_status
        || ' Interval=' || r.rai_interval_class
        || ' Certainty=' || CAST(r.rai_treatment_certainty AS VARCHAR)
        AS conflict_summary,
    r.rai_term_normalized,
    r.rai_date,
    r.linked_surgery_date,
    r.days_surgery_to_rai,
    r.dose_mci,
    CAST(r.rai_mention_text AS VARCHAR) AS rai_mention_text_short,
    CASE
        WHEN r.rai_interval_class = 'missing_rai_date'
            THEN 'Locate RAI treatment date from nuclear medicine records'
        WHEN r.rai_interval_class = 'missing_surgery_anchor'
            THEN 'Link to thyroidectomy record'
        WHEN r.rai_interval_class = 'pre_index_surgery'
            THEN 'Verify if prior thyroid surgery exists or date is incorrect'
        WHEN r.rai_interval_class = 'likely_recurrence_related'
            THEN 'Determine if this is recurrence treatment vs index treatment'
        WHEN r.rai_interval_class = 'too_remote_from_index_surgery'
            THEN 'Verify treatment context — delayed vs recurrence'
        ELSE 'Review mention context'
    END AS recommended_reviewer_action,
    'note_entities_medications, note_entities_procedures, path_synoptics'
        AS supporting_source_objects,
    r.rai_episode_id AS linked_episode_id
FROM rai_episode_v3 r
WHERE r.rai_eligible_for_analysis_flag = FALSE
  AND r.rai_assertion_status != 'negated';
"""

TIMELINE_REVIEW_QUEUE_SQL = """
CREATE OR REPLACE VIEW timeline_manual_review_queue_v AS
SELECT
    ROW_NUMBER() OVER (ORDER BY
        CASE WHEN v.severity = 'error' THEN 0 ELSE 1 END,
        v.research_id
    ) AS queue_row_id,
    CAST(v.research_id AS BIGINT) AS research_id,
    CASE
        WHEN v.issue_code = 'future_date' THEN 100
        WHEN v.issue_code = 'implausible_historical_date' THEN 95
        WHEN v.issue_code = 'entity_vs_note_date_gap' THEN 70
        WHEN v.issue_code = 'no_date_recoverable' THEN 60
        ELSE 30
    END AS priority_score,
    'timeline' AS review_domain,
    v.issue_code AS unresolved_reason,
    v.issue_description AS conflict_summary,
    v.detected_value,
    v.domain AS source_domain,
    v.recommended_action AS recommended_reviewer_action,
    v.source_objects AS supporting_source_objects,
    NULL AS linked_episode_id
FROM validation_failures_v3 v
WHERE v.validation_domain = 'date_validation'
  AND v.severity IN ('error', 'warning');
"""

PATIENT_REVIEW_SUMMARY_SQL = """
CREATE OR REPLACE VIEW patient_manual_review_summary_v AS
WITH all_queues AS (
    SELECT research_id, priority_score, review_domain FROM histology_manual_review_queue_v
    UNION ALL
    SELECT research_id, priority_score, review_domain FROM molecular_manual_review_queue_v
    UNION ALL
    SELECT research_id, priority_score, review_domain FROM rai_manual_review_queue_v
    UNION ALL
    SELECT research_id, priority_score, review_domain FROM timeline_manual_review_queue_v
)
SELECT
    research_id,
    COUNT(*) AS total_review_items,
    MAX(priority_score) AS max_priority_score,
    SUM(CASE WHEN review_domain = 'histology' THEN 1 ELSE 0 END) AS histology_items,
    SUM(CASE WHEN review_domain = 'molecular' THEN 1 ELSE 0 END) AS molecular_items,
    SUM(CASE WHEN review_domain = 'rai' THEN 1 ELSE 0 END) AS rai_items,
    SUM(CASE WHEN review_domain = 'timeline' THEN 1 ELSE 0 END) AS timeline_items,
    STRING_AGG(DISTINCT review_domain, ', ' ORDER BY review_domain) AS affected_domains,
    CASE
        WHEN MAX(priority_score) >= 90 THEN 'critical'
        WHEN MAX(priority_score) >= 60 THEN 'high'
        WHEN MAX(priority_score) >= 30 THEN 'medium'
        ELSE 'low'
    END AS review_priority_tier
FROM all_queues
GROUP BY research_id;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE F — Patient Reconciliation Summary (fixed)
# ═══════════════════════════════════════════════════════════════════════════════

PATIENT_RECON_SUMMARY_SQL = """
CREATE OR REPLACE VIEW patient_reconciliation_summary_v AS
WITH patient_spine AS (
    SELECT DISTINCT CAST(research_id AS BIGINT) AS research_id
    FROM (
        SELECT research_id FROM path_synoptics
        UNION
        SELECT CAST(research_id AS BIGINT) FROM molecular_episode_v2
        UNION
        SELECT CAST(research_id AS BIGINT) FROM rai_episode_v2
    ) all_ids
),
hist_agg AS (
    SELECT
        research_id,
        histology_normalized,
        variant_normalized,
        t_stage_reconciled,
        reconciliation_status,
        histology_discordance_flag,
        stage_discordance_flag,
        unresolved_flag
    FROM histology_reconciliation_v2
    WHERE op_seq = 1
),
hist_issue_ct AS (
    SELECT
        research_id,
        COUNT(*) AS histology_issue_count
    FROM histology_analysis_cohort_v
    WHERE adjudication_needed_flag = TRUE OR unresolved_flag = TRUE
    GROUP BY research_id
),
mol_agg AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        COUNT(*) AS molecular_test_count,
        SUM(CASE WHEN high_risk_molecular_flag THEN 1 ELSE 0 END) AS high_risk_count,
        SUM(CASE WHEN molecular_analysis_eligible_flag THEN 1 ELSE 0 END) AS mol_eligible_count,
        SUM(CASE WHEN NOT molecular_analysis_eligible_flag THEN 1 ELSE 0 END) AS mol_issue_count
    FROM molecular_episode_v3
    GROUP BY CAST(research_id AS BIGINT)
),
rai_agg AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        COUNT(*) AS rai_episode_count,
        SUM(CASE WHEN rai_assertion_status IN ('definite_received','likely_received')
            THEN 1 ELSE 0 END) AS definite_likely_count,
        SUM(CASE WHEN rai_eligible_for_analysis_flag THEN 1 ELSE 0 END) AS rai_eligible_count,
        SUM(CASE WHEN NOT rai_eligible_for_analysis_flag
                      AND rai_assertion_status != 'negated'
            THEN 1 ELSE 0 END) AS rai_issue_count
    FROM rai_episode_v3
    GROUP BY CAST(research_id AS BIGINT)
),
timeline_issue_ct AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        COUNT(*) AS timeline_issue_count
    FROM validation_failures_v3
    WHERE validation_domain = 'date_validation' AND severity IN ('error', 'warning')
    GROUP BY CAST(research_id AS BIGINT)
),
val_agg AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        highest_severity,
        total_issues
    FROM patient_validation_rollup_v2_mv
),
review_agg AS (
    SELECT research_id, max_priority_score, review_priority_tier, total_review_items
    FROM patient_manual_review_summary_v
)
SELECT
    ps.research_id,

    -- Histology
    h.histology_normalized,
    h.variant_normalized,
    h.t_stage_reconciled,
    h.reconciliation_status AS histology_status,
    h.histology_discordance_flag,
    h.stage_discordance_flag,
    COALESCE(hic.histology_issue_count, 0) AS histology_issue_count,

    -- Molecular
    COALESCE(mol.molecular_test_count, 0) AS molecular_test_count,
    COALESCE(mol.high_risk_count, 0) > 0 AS has_high_risk_molecular,
    COALESCE(mol.mol_eligible_count, 0) AS molecular_eligible_count,
    COALESCE(mol.mol_issue_count, 0) AS molecular_issue_count,

    -- RAI
    COALESCE(rai.rai_episode_count, 0) AS rai_episode_count,
    COALESCE(rai.definite_likely_count, 0) > 0 AS has_definite_rai,
    COALESCE(rai.rai_eligible_count, 0) AS rai_eligible_count,
    COALESCE(rai.rai_issue_count, 0) AS rai_issue_count,

    -- Timeline
    COALESCE(tic.timeline_issue_count, 0) AS timeline_issue_count,

    -- Validation
    COALESCE(val.total_issues, 0) AS total_validation_issues,
    COALESCE(val.highest_severity, 'none') AS highest_severity,

    -- Manual review
    COALESCE(rev.max_priority_score, 0) AS manual_review_priority,
    COALESCE(rev.review_priority_tier, 'none') AS review_priority_tier,
    COALESCE(rev.total_review_items, 0) AS total_review_items,

    -- Analysis eligibility summary
    CASE
        WHEN h.unresolved_flag = TRUE THEN FALSE
        WHEN h.histology_normalized IS NULL THEN FALSE
        WHEN h.histology_discordance_flag = TRUE THEN FALSE
        ELSE TRUE
    END AS histology_analysis_eligible,
    CASE
        WHEN COALESCE(mol.mol_eligible_count, 0) > 0 THEN TRUE
        ELSE FALSE
    END AS has_eligible_molecular,
    CASE
        WHEN COALESCE(rai.rai_eligible_count, 0) > 0 THEN TRUE
        ELSE FALSE
    END AS has_eligible_rai

FROM patient_spine ps
LEFT JOIN hist_agg h ON ps.research_id = CAST(h.research_id AS BIGINT)
LEFT JOIN hist_issue_ct hic ON ps.research_id = CAST(hic.research_id AS BIGINT)
LEFT JOIN mol_agg mol ON ps.research_id = mol.research_id
LEFT JOIN rai_agg rai ON ps.research_id = rai.research_id
LEFT JOIN timeline_issue_ct tic ON ps.research_id = tic.research_id
LEFT JOIN val_agg val ON ps.research_id = val.research_id
LEFT JOIN review_agg rev ON ps.research_id = rev.research_id;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE G — Streamlit-Ready Support Layer
# ═══════════════════════════════════════════════════════════════════════════════

STREAMLIT_HEADER_SQL = """
CREATE OR REPLACE VIEW streamlit_patient_header_v AS
SELECT
    s.research_id,
    s.histology_normalized AS primary_histology,
    s.variant_normalized AS primary_variant,
    s.t_stage_reconciled AS primary_t_stage,
    s.histology_status,
    s.highest_severity AS overall_severity,
    s.review_priority_tier,
    s.total_review_items,
    s.histology_analysis_eligible,
    s.has_eligible_molecular,
    s.has_eligible_rai,
    s.molecular_test_count,
    s.rai_episode_count,
    s.total_validation_issues
FROM patient_reconciliation_summary_v s;
"""

STREAMLIT_TIMELINE_SQL = """
CREATE OR REPLACE VIEW streamlit_patient_timeline_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    episode_seq,
    event_date,
    event_type,
    event_detail,
    stage_info,
    status AS resolution_status,
    CASE
        WHEN event_type = 'surgery' THEN '#2196F3'
        WHEN event_type = 'molecular_test' THEN '#FF9800'
        WHEN event_type = 'rai_treatment' THEN '#F44336'
        ELSE '#9E9E9E'
    END AS display_color,
    CASE
        WHEN status = 'unresolved' THEN 'dashed'
        ELSE 'solid'
    END AS display_line_style
FROM patient_master_timeline_v2
ORDER BY research_id, event_date NULLS LAST;
"""

STREAMLIT_CONFLICTS_SQL = """
CREATE OR REPLACE VIEW streamlit_patient_conflicts_v AS

SELECT
    CAST(research_id AS BIGINT) AS research_id,
    'histology' AS conflict_domain,
    discordance_type AS conflict_type,
    CASE
        WHEN histology_discordance_flag
            THEN 'PS: ' || COALESCE(source_histology_raw_ps, 'NULL')
                 || ' | TP: ' || COALESCE(source_histology_raw_tp, 'NULL')
        WHEN stage_discordance_flag
            THEN 'Path: ' || COALESCE(t_stage_source_path, 'NULL')
                 || ' | Note: ' || COALESCE(t_stage_source_note, 'NULL')
        ELSE reconciliation_status
    END AS conflict_detail,
    CASE WHEN adjudication_needed_flag THEN 'needs_review' ELSE 'resolved' END AS status
FROM histology_analysis_cohort_v
WHERE discordance_type IS NOT NULL

UNION ALL

SELECT
    CAST(research_id AS BIGINT) AS research_id,
    'molecular' AS conflict_domain,
    'low_confidence_linkage' AS conflict_type,
    'Confidence=' || CAST(overall_linkage_confidence AS VARCHAR)
        || ' Method=' || linkage_method AS conflict_detail,
    CASE WHEN molecular_analysis_eligible_flag THEN 'eligible' ELSE 'needs_review' END AS status
FROM molecular_episode_v3
WHERE overall_linkage_confidence < 50

UNION ALL

SELECT
    CAST(research_id AS BIGINT) AS research_id,
    'rai' AS conflict_domain,
    rai_interval_class AS conflict_type,
    'Status=' || rai_assertion_status
        || ' Certainty=' || CAST(rai_treatment_certainty AS VARCHAR) AS conflict_detail,
    CASE WHEN rai_eligible_for_analysis_flag THEN 'eligible' ELSE 'needs_review' END AS status
FROM rai_episode_v3
WHERE rai_assertion_status != 'negated'
  AND rai_eligible_for_analysis_flag = FALSE;
"""

STREAMLIT_REVIEW_SQL = """
CREATE OR REPLACE VIEW streamlit_patient_manual_review_v AS
SELECT
    research_id, queue_row_id, priority_score, review_domain,
    unresolved_reason, conflict_summary, recommended_reviewer_action,
    linked_episode_id
FROM histology_manual_review_queue_v

UNION ALL

SELECT
    research_id, queue_row_id, priority_score, review_domain,
    unresolved_reason, conflict_summary, recommended_reviewer_action,
    linked_episode_id
FROM molecular_manual_review_queue_v

UNION ALL

SELECT
    research_id, queue_row_id, priority_score, review_domain,
    unresolved_reason, conflict_summary, recommended_reviewer_action,
    linked_episode_id
FROM rai_manual_review_queue_v

UNION ALL

SELECT
    research_id, queue_row_id, priority_score, review_domain,
    unresolved_reason, conflict_summary, recommended_reviewer_action,
    CAST(linked_episode_id AS BIGINT) AS linked_episode_id
FROM timeline_manual_review_queue_v;
"""

STREAMLIT_QC_SUMMARY_SQL = """
CREATE OR REPLACE VIEW streamlit_cohort_qc_summary_v AS
WITH total_patients AS (
    SELECT COUNT(DISTINCT research_id) AS n FROM patient_reconciliation_summary_v
)
SELECT
    -- Histology
    (SELECT COUNT(*) FROM histology_analysis_cohort_v
     WHERE discordance_type IS NOT NULL) AS histology_discordant,
    (SELECT COUNT(*) FROM histology_analysis_cohort_v
     WHERE analysis_eligible_flag = TRUE) AS histology_analysis_eligible,
    (SELECT COUNT(*) FROM histology_manual_review_queue_v) AS histology_review_needed,

    -- Molecular
    (SELECT COUNT(*) FROM molecular_episode_v3) AS molecular_total_rows,
    (SELECT COUNT(*) FROM molecular_analysis_cohort_v) AS molecular_analysis_eligible,
    (SELECT COUNT(*) FROM molecular_episode_v3
     WHERE unresolved_flag = TRUE) AS molecular_unresolved,
    (SELECT COUNT(*) FROM molecular_episode_v3
     WHERE overall_linkage_confidence < 50
       AND NOT unresolved_flag) AS molecular_low_confidence,

    -- RAI
    (SELECT COUNT(*) FROM rai_episode_v3) AS rai_total_captured,
    (SELECT COUNT(*) FROM rai_episode_v3
     WHERE rai_assertion_status IN ('definite_received','likely_received')) AS rai_definite_likely,
    (SELECT COUNT(*) FROM rai_analysis_cohort_v) AS rai_analyzable,
    (SELECT COUNT(*) FROM rai_episode_v3
     WHERE rai_eligible_for_analysis_flag = FALSE
       AND rai_assertion_status != 'negated') AS rai_unresolved,

    -- Timeline date status
    (SELECT COUNT(*) FROM (
        SELECT research_id FROM enriched_note_entities_genetics WHERE date_status = 'exact_source_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_staging WHERE date_status = 'exact_source_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_procedures WHERE date_status = 'exact_source_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_complications WHERE date_status = 'exact_source_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_medications WHERE date_status = 'exact_source_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_problem_list WHERE date_status = 'exact_source_date'
    ) x) AS timeline_exact_date,
    (SELECT COUNT(*) FROM (
        SELECT research_id FROM enriched_note_entities_genetics WHERE date_status = 'inferred_day_level_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_staging WHERE date_status = 'inferred_day_level_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_procedures WHERE date_status = 'inferred_day_level_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_complications WHERE date_status = 'inferred_day_level_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_medications WHERE date_status = 'inferred_day_level_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_problem_list WHERE date_status = 'inferred_day_level_date'
    ) x) AS timeline_inferred_day,
    (SELECT COUNT(*) FROM (
        SELECT research_id FROM enriched_note_entities_genetics WHERE date_status = 'coarse_anchor_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_staging WHERE date_status = 'coarse_anchor_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_procedures WHERE date_status = 'coarse_anchor_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_complications WHERE date_status = 'coarse_anchor_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_medications WHERE date_status = 'coarse_anchor_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_problem_list WHERE date_status = 'coarse_anchor_date'
    ) x) AS timeline_coarse_anchor,
    (SELECT COUNT(*) FROM (
        SELECT research_id FROM enriched_note_entities_genetics WHERE date_status = 'unresolved_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_staging WHERE date_status = 'unresolved_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_procedures WHERE date_status = 'unresolved_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_complications WHERE date_status = 'unresolved_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_medications WHERE date_status = 'unresolved_date'
        UNION ALL SELECT research_id FROM enriched_note_entities_problem_list WHERE date_status = 'unresolved_date'
    ) x) AS timeline_unresolved,

    -- Validation
    (SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'error') AS validation_errors,
    (SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'warning') AS validation_warnings,
    (SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'info') AS validation_info,
    (SELECT COUNT(DISTINCT research_id) FROM validation_failures_v3) AS validation_patients_affected,

    -- Manual review
    (SELECT COUNT(*) FROM patient_manual_review_summary_v) AS review_queue_patients,
    (SELECT COUNT(*) FROM patient_manual_review_summary_v
     WHERE review_priority_tier = 'critical') AS review_critical_patients,
    (SELECT COUNT(*) FROM patient_manual_review_summary_v
     WHERE review_priority_tier = 'high') AS review_high_patients,

    -- Total patients
    tp.n AS total_patients
FROM total_patients tp;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  PHASE H — QA Metrics (runtime)
# ═══════════════════════════════════════════════════════════════════════════════

def print_qa_metrics(con: duckdb.DuckDBPyConnection) -> None:
    """Phase H: Compute and print comprehensive QA metrics."""
    section("PHASE H — QA METRICS & DEPLOYMENT SUMMARY")

    try:
        qc = con.execute("SELECT * FROM streamlit_cohort_qc_summary_v").fetchone()
        cols = [desc[0] for desc in con.description]
        metrics = dict(zip(cols, qc))
    except Exception as e:
        print(f"  ERROR reading QC summary: {e}")
        return

    print("  ┌─────────────────────────────────────────────────────────────┐")
    print(f"  │  TOTAL PATIENTS:  {metrics['total_patients']:>8,}                              │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  HISTOLOGY                                                 │")
    print(f"  │    Discordant:              {metrics['histology_discordant']:>8,}                    │")
    print(f"  │    Analysis eligible:       {metrics['histology_analysis_eligible']:>8,}                    │")
    print(f"  │    Manual review needed:    {metrics['histology_review_needed']:>8,}                    │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  MOLECULAR                                                 │")
    print(f"  │    Total rows:              {metrics['molecular_total_rows']:>8,}                    │")
    print(f"  │    Analysis eligible:       {metrics['molecular_analysis_eligible']:>8,}                    │")
    print(f"  │    Unresolved:              {metrics['molecular_unresolved']:>8,}                    │")
    print(f"  │    Low confidence:          {metrics['molecular_low_confidence']:>8,}                    │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  RAI                                                       │")
    print(f"  │    Total captured:          {metrics['rai_total_captured']:>8,}                    │")
    print(f"  │    Definite/likely:         {metrics['rai_definite_likely']:>8,}                    │")
    print(f"  │    Analyzable:              {metrics['rai_analyzable']:>8,}                    │")
    print(f"  │    Unresolved:              {metrics['rai_unresolved']:>8,}                    │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  TIMELINE                                                  │")
    print(f"  │    Exact date:              {metrics['timeline_exact_date']:>8,}                    │")
    print(f"  │    Inferred day-level:      {metrics['timeline_inferred_day']:>8,}                    │")
    print(f"  │    Coarse anchor:           {metrics['timeline_coarse_anchor']:>8,}                    │")
    print(f"  │    Unresolved:              {metrics['timeline_unresolved']:>8,}                    │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  VALIDATION                                                │")
    print(f"  │    Errors:                  {metrics['validation_errors']:>8,}                    │")
    print(f"  │    Warnings:                {metrics['validation_warnings']:>8,}                    │")
    print(f"  │    Info:                    {metrics['validation_info']:>8,}                    │")
    print(f"  │    Patients affected:       {metrics['validation_patients_affected']:>8,}                    │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  MANUAL REVIEW                                             │")
    print(f"  │    Patients in queue:       {metrics['review_queue_patients']:>8,}                    │")
    print(f"  │    Critical priority:       {metrics['review_critical_patients']:>8,}                    │")
    print(f"  │    High priority:           {metrics['review_high_patients']:>8,}                    │")
    print("  └─────────────────────────────────────────────────────────────┘")

    section("REMAINING GAPS (require source-data improvement, not code)")
    gaps = [
        ("Molecular rows with no parseable date (placeholder/garbage)",
         "SELECT COUNT(*) FROM molecular_episode_v3 WHERE molecular_date_raw_class IN ('placeholder','garbage_unparseable','missing')"),
        ("RAI mentions with no date at all",
         "SELECT COUNT(*) FROM rai_episode_v3 WHERE rai_interval_class = 'missing_rai_date'"),
        ("Patients with no path_synoptics entry",
         """SELECT COUNT(DISTINCT CAST(research_id AS BIGINT))
            FROM molecular_testing
            WHERE CAST(research_id AS BIGINT) NOT IN (
                SELECT CAST(research_id AS BIGINT) FROM path_synoptics)"""),
        ("Histology patients with no staging from any source",
         "SELECT COUNT(*) FROM histology_analysis_cohort_v WHERE unresolved_flag = TRUE AND discordance_type IS NULL"),
    ]
    for label, sql in gaps:
        try:
            cnt = con.execute(sql).fetchone()[0]
            print(f"  {label:<65} {cnt:>6,}")
        except Exception as e:
            print(f"  {label:<65} ERROR")


def write_sql_file(view_log: list[tuple[str, str]]) -> None:
    with open(SQL_OUT, "w") as f:
        f.write("-- Adjudication Framework Views (Phases B-G)\n")
        f.write("-- Generated by 18_adjudication_framework.py\n")
        f.write("-- Depends on: scripts 15, 16, 17\n")
        f.write("--\n")
        f.write("-- Phase B: Manual review queues (histology, molecular, RAI, timeline)\n")
        f.write("-- Phase C: molecular_episode_v3, molecular_analysis_cohort_v\n")
        f.write("-- Phase D: rai_episode_v3, rai_analysis_cohort_v\n")
        f.write("-- Phase E: histology_analysis_cohort_v, histology_discordance_summary_v\n")
        f.write("-- Phase F: patient_reconciliation_summary_v (fixed)\n")
        f.write("-- Phase G: streamlit_* views\n\n")
        for name, sql in view_log:
            f.write(f"-- === {name} ===\n")
            f.write(sql.strip())
            f.write("\n\n")
    print(f"  SQL saved to: {SQL_OUT}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phases B-H: Adjudication Framework"
    )
    parser.add_argument("--md", action="store_true",
                        help="Use MotherDuck instead of local DuckDB")
    args = parser.parse_args()

    print("=" * 80)
    print("  ADJUDICATION FRAMEWORK — Phases B through H")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    section("REGISTERING DATA SOURCES")
    if args.md:
        for tbl in ["path_synoptics", "molecular_testing", "fna_history",
                     "note_entities_medications", "note_entities_procedures",
                     "note_entities_staging", "note_entities_genetics",
                     "note_entities_complications", "note_entities_problem_list",
                     "tumor_pathology"]:
            status = "OK" if table_available(con, tbl) else "MISS"
            print(f"  {status:<6} {tbl}")
    else:
        register_parquets(con)

    section("CHECKING PREREQUISITES (scripts 15-17)")
    for v in ["enriched_note_entities_genetics", "enriched_note_entities_medications",
              "histology_reconciliation_v2", "molecular_episode_v2", "rai_episode_v2",
              "validation_failures_v3", "patient_validation_rollup_v2_mv"]:
        status = "OK" if table_available(con, v) else "MISS"
        print(f"  {status:<6} {v}")

    view_log: list[tuple[str, str]] = []

    # Phase C first (molecular v3 needed by Phase B queues)
    section("PHASE C: MOLECULAR EPISODE V3")
    deploy_view(con, "molecular_episode_v3", MOLECULAR_V3_SQL, view_log)
    deploy_view(con, "molecular_analysis_cohort_v", MOLECULAR_ANALYSIS_COHORT_SQL, view_log)
    deploy_view(con, "molecular_linkage_failure_summary_v", MOLECULAR_LINKAGE_FAILURE_SQL, view_log)

    # Phase D (RAI v3 needed by Phase B queues)
    section("PHASE D: RAI EPISODE V3")
    deploy_view(con, "rai_episode_v3", RAI_V3_SQL, view_log)
    deploy_view(con, "rai_analysis_cohort_v", RAI_ANALYSIS_COHORT_SQL, view_log)
    deploy_view(con, "rai_linkage_failure_summary_v", RAI_LINKAGE_FAILURE_SQL, view_log)

    # Phase E (histology analysis cohort needed by Phase B queues)
    section("PHASE E: HISTOLOGY ANALYSIS COHORT")
    deploy_view(con, "histology_analysis_cohort_v", HISTOLOGY_ANALYSIS_COHORT_SQL, view_log)
    deploy_view(con, "histology_discordance_summary_v", HISTOLOGY_DISCORDANCE_SUMMARY_SQL, view_log)

    # Phase B (review queues depend on C, D, E)
    section("PHASE B: MANUAL ADJUDICATION REVIEW QUEUES")
    deploy_view(con, "histology_manual_review_queue_v", HISTOLOGY_REVIEW_QUEUE_SQL, view_log)
    deploy_view(con, "molecular_manual_review_queue_v", MOLECULAR_REVIEW_QUEUE_SQL, view_log)
    deploy_view(con, "rai_manual_review_queue_v", RAI_REVIEW_QUEUE_SQL, view_log)
    deploy_view(con, "timeline_manual_review_queue_v", TIMELINE_REVIEW_QUEUE_SQL, view_log)
    deploy_view(con, "patient_manual_review_summary_v", PATIENT_REVIEW_SUMMARY_SQL, view_log)

    # Phase F (patient summary depends on everything above)
    section("PHASE F: PATIENT RECONCILIATION SUMMARY (fixed)")
    deploy_view(con, "patient_reconciliation_summary_v", PATIENT_RECON_SUMMARY_SQL, view_log)

    # Phase G (streamlit views depend on everything above)
    section("PHASE G: STREAMLIT-READY SUPPORT LAYER")
    deploy_view(con, "streamlit_patient_header_v", STREAMLIT_HEADER_SQL, view_log)
    deploy_view(con, "streamlit_patient_timeline_v", STREAMLIT_TIMELINE_SQL, view_log)
    deploy_view(con, "streamlit_patient_conflicts_v", STREAMLIT_CONFLICTS_SQL, view_log)
    deploy_view(con, "streamlit_patient_manual_review_v", STREAMLIT_REVIEW_SQL, view_log)
    deploy_view(con, "streamlit_cohort_qc_summary_v", STREAMLIT_QC_SUMMARY_SQL, view_log)

    # Phase H (QA metrics)
    print_qa_metrics(con)

    # Write SQL
    section("WRITING SQL FILE")
    write_sql_file(view_log)

    # Files summary
    section("FILES CREATED / MODIFIED")
    print("  scripts/18_adjudication_framework.py         (this script)")
    print("  scripts/18_adjudication_framework_views.sql  (generated SQL)")

    section("SQL/VIEW OBJECTS ADDED")
    for name, _ in view_log:
        print(f"  {name}")

    section("DEPLOYMENT ORDER")
    print("  1. scripts/15_date_association_audit.py       (Phase 1 enriched views)")
    print("  2. scripts/16_reconciliation_v2.py            (reconciliation v2)")
    print("  3. scripts/17_semantic_cleanup_v3.py          (date_status taxonomy fix)")
    print("  4. scripts/18_adjudication_framework.py       (this script: phases B-H)")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Adjudication Framework complete (Phases B through H)")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
