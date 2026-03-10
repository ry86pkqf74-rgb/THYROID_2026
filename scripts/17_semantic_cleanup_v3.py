#!/usr/bin/env python3
"""
17_semantic_cleanup_v3.py — Phase A: Data Contract Cleanup / Semantic Consistency

Fixes the timeline contradiction where timeline_rescue_mv reports 0 unresolved
but validation_failures_v2 reports 8,555 error-severity no_date_recoverable issues.

Root cause: enriched_note_entities_medications and enriched_note_entities_problem_list
lack surgery/FNA fallback that the other 4 enriched views have. timeline_rescue_mv
rescues them, but validation_failures_v2 reads from pre-rescue enriched views.

Fix: Integrate surgery/FNA fallback into the enriched views with proper date_status
taxonomy, then refactor validation to use post-reconciliation semantics.

New/updated views:
  - enriched_note_entities_medications (replaced — adds surgery/FNA fallback + date_status)
  - enriched_note_entities_problem_list (replaced — adds surgery/FNA fallback + date_status)
  - enriched_note_entities_genetics (replaced — adds date_status columns)
  - enriched_note_entities_staging (replaced — adds date_status columns)
  - enriched_note_entities_procedures (replaced — adds date_status columns)
  - enriched_note_entities_complications (replaced — adds date_status columns)
  - timeline_rescue_v2_mv (only truly unresolvable rows)
  - timeline_unresolved_summary_v2_mv (summary of remaining gaps)
  - validation_failures_v3 (corrected: coarse anchor = warning, not error)
  - patient_validation_rollup_v2_mv (rollup using v3 validation)

Date status taxonomy (applied to all enriched views):
  - exact_source_date: entity_date parsed successfully
  - inferred_day_level_date: note_date used as proxy
  - coarse_anchor_date: surgery/FNA/molecular date from another table
  - unresolved_date: no date from any source

Modes:
  --local : Uses local DuckDB with parquets from processed/ (default)
  --md    : Uses MotherDuck (requires MOTHERDUCK_TOKEN)

Run after 15_date_association_audit.py and 16_reconciliation_v2.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"
SQL_OUT = ROOT / "scripts" / "17_semantic_cleanup_v3_views.sql"

sys.path.insert(0, str(ROOT))

REQUIRED_TABLES = [
    "path_synoptics",
    "note_entities_genetics",
    "note_entities_medications",
    "note_entities_procedures",
    "note_entities_complications",
    "note_entities_problem_list",
    "note_entities_staging",
    "molecular_testing",
    "fna_history",
]


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


def register_parquets(con: duckdb.DuckDBPyConnection) -> list[str]:
    registered: list[str] = []
    for tbl in REQUIRED_TABLES:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
            registered.append(tbl)
        else:
            if table_available(con, tbl):
                registered.append(tbl)
                print(f"  Existing  {tbl:<45}")
            else:
                print(f"  SKIP      {tbl:<45} (not available)")
    return registered


def deploy_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    view_log: list[tuple[str, str]],
) -> bool:
    try:
        con.execute(sql)
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  Created {name:<55} {cnt:>8,} rows")
        view_log.append((name, sql))
        return True
    except Exception as e:
        print(f"  FAILED  {name}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  Shared CTE: surgery and FNA anchors
# ═══════════════════════════════════════════════════════════════════════════════

PS_ANCHOR_CTE = """
ps_primary AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date_parsed,
        surg_date AS surg_date_raw,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
)"""

FNA_ANCHOR_CTE = """
fna_primary AS (
    SELECT
        research_id,
        fna_date_parsed,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
        fna_index,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST,
                     fna_index DESC
        ) AS fna_seq
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
)"""

MT_ANCHOR_CTE = """
mt_valid AS (
    SELECT
        research_id,
        CASE
            WHEN TRY_CAST("date" AS DATE) IS NOT NULL
                THEN TRY_CAST("date" AS DATE)
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\\d{4}$')
                THEN TRY_CAST(CAST("date" AS VARCHAR) || '-01-01' AS DATE)
            ELSE NULL
        END AS mt_date_parsed,
        CASE
            WHEN regexp_matches(CAST("date" AS VARCHAR), '^\\d{4}$')
                THEN 'year'
            ELSE 'day'
        END AS mt_date_granularity,
        "date" AS mt_date_raw,
        test_index,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY
                CASE WHEN TRY_CAST("date" AS DATE) IS NOT NULL THEN 0 ELSE 1 END,
                TRY_CAST("date" AS DATE) DESC NULLS LAST,
                test_index DESC
        ) AS mt_seq
    FROM molecular_testing
    WHERE "date" IS NOT NULL
      AND CAST("date" AS VARCHAR) NOT IN ('x', 'X', '', 'None', 'maybe?')
)"""

# Date status taxonomy columns — reusable SQL fragment
# Parameters: {inferred_event_date_expr}, {date_source_expr}, {date_granularity_expr},
#             {date_confidence_expr}, {anchor_type_expr}, {anchor_table_expr}
DATE_STATUS_COLUMNS = """
    CASE
        WHEN {date_source_expr} = 'entity_date' THEN 'exact_source_date'
        WHEN {date_source_expr} = 'note_date' THEN 'inferred_day_level_date'
        WHEN {date_source_expr} IN ('surg_date', 'fna_date_parsed',
                                     'molecular_testing_date')
            THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    {date_granularity_expr} AS date_resolution_level,
    CASE WHEN {date_source_expr} = 'entity_date' THEN TRUE ELSE FALSE
    END AS date_is_source_native_flag,
    CASE WHEN {date_source_expr} NOT IN ('entity_date', 'unrecoverable')
        THEN TRUE ELSE FALSE
    END AS date_is_inferred_flag,
    CASE
        WHEN {date_source_expr} = 'unrecoverable' THEN TRUE
        WHEN {date_confidence_expr} < 50 THEN TRUE
        WHEN {date_source_expr} IN ('surg_date', 'fna_date_parsed')
            THEN TRUE
        ELSE FALSE
    END AS date_requires_manual_review_flag"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Updated enriched views with date_status taxonomy
# ═══════════════════════════════════════════════════════════════════════════════

ENRICHED_GENETICS_SQL = f"""
CREATE OR REPLACE VIEW enriched_note_entities_genetics AS
WITH {PS_ANCHOR_CTE},
{FNA_ANCHOR_CTE},
{MT_ANCHOR_CTE}
SELECT
    e.*,
    COALESCE(
            TRY_CAST(e.entity_date AS DATE),
            TRY_CAST(e.note_date AS DATE),
            ps.surg_date_parsed,
            mt.mt_date_parsed,
            fna.fna_date
    ) AS inferred_event_date,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
            WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular_testing_date'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_date_parsed'
            ELSE 'unrecoverable'
    END AS date_source,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
            WHEN mt.mt_date_parsed IS NOT NULL THEN mt.mt_date_granularity
            WHEN fna.fna_date IS NOT NULL THEN 'day'
            ELSE NULL
    END AS date_granularity,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 100
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 70
            WHEN ps.surg_date_parsed IS NOT NULL THEN 60
            WHEN mt.mt_date_parsed IS NOT NULL
            THEN CASE WHEN mt.mt_date_granularity = 'year' THEN 50 ELSE 60 END
            WHEN fna.fna_date IS NOT NULL THEN 55
            ELSE 0
    END AS date_confidence,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'extracted'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'encounter'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'
            WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular'
            WHEN fna.fna_date IS NOT NULL THEN 'cytology'
            ELSE 'none'
    END AS date_anchor_type,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'note_entities_genetics'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'clinical_notes_long'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'
            WHEN mt.mt_date_parsed IS NOT NULL THEN 'molecular_testing'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_history'
            ELSE 'none'
    END AS date_anchor_table,
    -- v3: date_status taxonomy
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'exact_source_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'inferred_day_level_date'
        WHEN ps.surg_date_parsed IS NOT NULL OR mt.mt_date_parsed IS NOT NULL
             OR fna.fna_date IS NOT NULL
            THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_source_native_flag,
    CASE WHEN (e.entity_date IS NULL OR TRY_CAST(e.entity_date AS DATE) IS NULL)
              AND COALESCE(TRY_CAST(e.note_date AS DATE), ps.surg_date_parsed,
                           mt.mt_date_parsed, fna.fna_date) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_inferred_flag,
    CASE
        WHEN e.entity_date IS NULL AND e.note_date IS NULL
             AND ps.surg_date_parsed IS NULL AND mt.mt_date_parsed IS NULL
             AND fna.fna_date IS NULL THEN TRUE
        WHEN ps.surg_date_parsed IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
            THEN TRUE
        WHEN fna.fna_date IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
             AND ps.surg_date_parsed IS NULL
            THEN TRUE
        ELSE FALSE
    END AS date_requires_manual_review_flag
FROM note_entities_genetics e
LEFT JOIN ps_primary ps
    ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1
LEFT JOIN mt_valid mt
    ON CAST(e.research_id AS BIGINT) = mt.research_id AND mt.mt_seq = 1
LEFT JOIN fna_primary fna
    ON CAST(e.research_id AS BIGINT) = fna.research_id AND fna.fna_seq = 1;
"""


def _enriched_simple_view(entity_table: str) -> str:
    """Generate enriched view for staging/procedures/complications (surgery-only fallback)."""
    return f"""
CREATE OR REPLACE VIEW enriched_{entity_table} AS
WITH {PS_ANCHOR_CTE}
SELECT
    e.*,
    COALESCE(
            TRY_CAST(e.entity_date AS DATE),
            TRY_CAST(e.note_date AS DATE),
            ps.surg_date_parsed
    ) AS inferred_event_date,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
            ELSE 'unrecoverable'
    END AS date_source,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
            ELSE NULL
    END AS date_granularity,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 100
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 70
            WHEN ps.surg_date_parsed IS NOT NULL THEN 60
            ELSE 0
    END AS date_confidence,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'extracted'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'encounter'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'
            ELSE 'none'
    END AS date_anchor_type,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN '{entity_table}'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'clinical_notes_long'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'
            ELSE 'none'
    END AS date_anchor_table,
    -- v3: date_status taxonomy
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'exact_source_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'inferred_day_level_date'
        WHEN ps.surg_date_parsed IS NOT NULL THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_source_native_flag,
    CASE WHEN (e.entity_date IS NULL OR TRY_CAST(e.entity_date AS DATE) IS NULL)
              AND COALESCE(TRY_CAST(e.note_date AS DATE), ps.surg_date_parsed) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_inferred_flag,
    CASE
        WHEN e.entity_date IS NULL AND e.note_date IS NULL
             AND ps.surg_date_parsed IS NULL THEN TRUE
        WHEN ps.surg_date_parsed IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
            THEN TRUE
        ELSE FALSE
    END AS date_requires_manual_review_flag
FROM {entity_table} e
LEFT JOIN ps_primary ps
    ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1;
"""


def _enriched_with_fallback_view(entity_table: str) -> str:
    """Generate enriched view for medications/problem_list WITH surgery/FNA fallback.

    This is the key fix: previously these views had NO cross-table fallback,
    causing all rows without entity_date/note_date to be 'unrecoverable'.
    Now they get surgery/FNA anchors (marked as coarse_anchor_date).
    """
    return f"""
CREATE OR REPLACE VIEW enriched_{entity_table} AS
WITH {PS_ANCHOR_CTE},
{FNA_ANCHOR_CTE}
SELECT
    e.*,
    COALESCE(
            TRY_CAST(e.entity_date AS DATE),
            TRY_CAST(e.note_date AS DATE),
            ps.surg_date_parsed,
            fna.fna_date
    ) AS inferred_event_date,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_date_parsed'
            ELSE 'unrecoverable'
    END AS date_source,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'day'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
            WHEN fna.fna_date IS NOT NULL THEN 'day'
            ELSE NULL
    END AS date_granularity,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 100
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 70
            WHEN ps.surg_date_parsed IS NOT NULL THEN 40
            WHEN fna.fna_date IS NOT NULL THEN 35
            ELSE 0
    END AS date_confidence,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'extracted'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'encounter'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'
            WHEN fna.fna_date IS NOT NULL THEN 'cytology'
            ELSE 'none'
    END AS date_anchor_type,
    CASE
            WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN '{entity_table}'
            WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'clinical_notes_long'
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_history'
            ELSE 'none'
    END AS date_anchor_table,
    -- v3: date_status taxonomy
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'exact_source_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'inferred_day_level_date'
        WHEN ps.surg_date_parsed IS NOT NULL OR fna.fna_date IS NOT NULL
            THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_source_native_flag,
    CASE WHEN (e.entity_date IS NULL OR TRY_CAST(e.entity_date AS DATE) IS NULL)
              AND COALESCE(TRY_CAST(e.note_date AS DATE), ps.surg_date_parsed,
                           fna.fna_date) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_inferred_flag,
    CASE
        WHEN e.entity_date IS NULL AND e.note_date IS NULL
             AND ps.surg_date_parsed IS NULL AND fna.fna_date IS NULL THEN TRUE
        WHEN ps.surg_date_parsed IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
            THEN TRUE
        WHEN fna.fna_date IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
             AND ps.surg_date_parsed IS NULL
            THEN TRUE
        ELSE FALSE
    END AS date_requires_manual_review_flag
FROM {entity_table} e
LEFT JOIN ps_primary ps
    ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1
LEFT JOIN fna_primary fna
    ON CAST(e.research_id AS BIGINT) = fna.research_id AND fna.fna_seq = 1;
"""


ENRICHED_STAGING_SQL = _enriched_simple_view("note_entities_staging")
ENRICHED_PROCEDURES_SQL = _enriched_simple_view("note_entities_procedures")
ENRICHED_COMPLICATIONS_SQL = _enriched_simple_view("note_entities_complications")
ENRICHED_MEDICATIONS_SQL = _enriched_with_fallback_view("note_entities_medications")
ENRICHED_PROBLEM_LIST_SQL = _enriched_with_fallback_view("note_entities_problem_list")


# ═══════════════════════════════════════════════════════════════════════════════
#  Timeline rescue v2: only truly unresolvable rows
# ═══════════════════════════════════════════════════════════════════════════════

TIMELINE_RESCUE_V2_SQL = """
CREATE OR REPLACE VIEW timeline_rescue_v2_mv AS
SELECT
    CASE
        WHEN e.research_id IS NOT NULL THEN 'medications'
        ELSE NULL
    END AS source_table,
    e.research_id,
    e.entity_value_norm,
    e.entity_value_raw,
    e.entity_date AS original_entity_date,
    e.note_date AS original_note_date,
    e.inferred_event_date,
    e.date_source,
    e.date_granularity AS date_resolution_level,
    e.date_confidence,
    e.date_status,
    e.date_is_inferred_flag,
    e.date_requires_manual_review_flag,
    CASE
        WHEN e.date_status = 'unresolved_date' AND NOT EXISTS (
            SELECT 1 FROM path_synoptics p2
            WHERE p2.research_id = CAST(e.research_id AS BIGINT)
        ) THEN 'patient_missing_anchor_tables'
        WHEN e.date_status = 'unresolved_date' AND e.entity_date IS NOT NULL
             AND TRY_CAST(e.entity_date AS DATE) IS NULL
            THEN 'parse_failure'
        WHEN e.date_status = 'unresolved_date'
            THEN 'no_source_date'
        WHEN e.date_status = 'coarse_anchor_date'
            THEN 'rescued_coarse_anchor'
        WHEN e.date_status IN ('exact_source_date', 'inferred_day_level_date')
            THEN 'has_direct_date'
        ELSE 'unknown'
    END AS rescue_status
FROM enriched_note_entities_medications e
WHERE e.date_status IN ('unresolved_date', 'coarse_anchor_date')

UNION ALL

SELECT
    'problem_list' AS source_table,
    e.research_id,
    e.entity_value_norm,
    e.entity_value_raw,
    e.entity_date AS original_entity_date,
    e.note_date AS original_note_date,
    e.inferred_event_date,
    e.date_source,
    e.date_granularity AS date_resolution_level,
    e.date_confidence,
    e.date_status,
    e.date_is_inferred_flag,
    e.date_requires_manual_review_flag,
    CASE
        WHEN e.date_status = 'unresolved_date' AND NOT EXISTS (
            SELECT 1 FROM path_synoptics p2
            WHERE p2.research_id = CAST(e.research_id AS BIGINT)
        ) THEN 'patient_missing_anchor_tables'
        WHEN e.date_status = 'unresolved_date' AND e.entity_date IS NOT NULL
             AND TRY_CAST(e.entity_date AS DATE) IS NULL
            THEN 'parse_failure'
        WHEN e.date_status = 'unresolved_date'
            THEN 'no_source_date'
        WHEN e.date_status = 'coarse_anchor_date'
            THEN 'rescued_coarse_anchor'
        WHEN e.date_status IN ('exact_source_date', 'inferred_day_level_date')
            THEN 'has_direct_date'
        ELSE 'unknown'
    END AS rescue_status
FROM enriched_note_entities_problem_list e
WHERE e.date_status IN ('unresolved_date', 'coarse_anchor_date');
"""

TIMELINE_UNRESOLVED_SUMMARY_V2_SQL = """
CREATE OR REPLACE VIEW timeline_unresolved_summary_v2_mv AS
SELECT
    source_table,
    rescue_status,
    date_status,
    COUNT(*) AS row_count,
    COUNT(DISTINCT research_id) AS patient_count,
    ROUND(100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY source_table), 2) AS pct_of_table,
    ROUND(AVG(date_confidence), 1) AS avg_confidence
FROM timeline_rescue_v2_mv
GROUP BY 1, 2, 3
ORDER BY source_table, row_count DESC;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Validation failures v3: corrected semantics
# ═══════════════════════════════════════════════════════════════════════════════

VALIDATION_V3_SQL = """
CREATE OR REPLACE VIEW validation_failures_v3 AS

WITH date_issues AS (
    SELECT
        domain,
        research_id,
        'date_validation' AS validation_domain,
        CASE
            WHEN date_status = 'unresolved_date' THEN 'no_date_recoverable'
            WHEN inferred_event_date > CURRENT_DATE THEN 'future_date'
            WHEN inferred_event_date < DATE '1990-01-01' THEN 'implausible_historical_date'
            WHEN entity_date IS NOT NULL AND note_date IS NOT NULL
                 AND ABS(DATE_DIFF('day',
                         TRY_CAST(entity_date AS DATE),
                         TRY_CAST(note_date AS DATE))) > 365
                THEN 'entity_vs_note_date_gap'
            WHEN date_status = 'coarse_anchor_date' THEN 'coarse_anchor_only'
            WHEN date_confidence < 50 AND date_status != 'coarse_anchor_date'
                THEN 'low_confidence'
        END AS issue_code,
        CASE
            WHEN date_status = 'unresolved_date' THEN 'error'
            WHEN inferred_event_date > CURRENT_DATE THEN 'error'
            WHEN inferred_event_date < DATE '1990-01-01' THEN 'error'
            WHEN entity_date IS NOT NULL AND note_date IS NOT NULL
                 AND ABS(DATE_DIFF('day',
                         TRY_CAST(entity_date AS DATE),
                         TRY_CAST(note_date AS DATE))) > 365
                THEN 'warning'
            WHEN date_status = 'coarse_anchor_date' THEN 'info'
            WHEN date_confidence < 50 AND date_status != 'coarse_anchor_date'
                THEN 'info'
            ELSE NULL
        END AS severity,
        CAST(inferred_event_date AS VARCHAR) AS detected_value,
        domain AS source_objects,
        date_status,
        date_confidence
    FROM (
        SELECT 'genetics' AS domain, research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_genetics
        UNION ALL
        SELECT 'staging', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_staging
        UNION ALL
        SELECT 'procedures', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_procedures
        UNION ALL
        SELECT 'complications', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_complications
        UNION ALL
        SELECT 'medications', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_medications
        UNION ALL
        SELECT 'problem_list', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence, date_status
        FROM enriched_note_entities_problem_list
    ) all_enriched
    WHERE date_status = 'unresolved_date'
       OR inferred_event_date > CURRENT_DATE
       OR inferred_event_date < DATE '1990-01-01'
       OR (entity_date IS NOT NULL AND note_date IS NOT NULL
           AND ABS(DATE_DIFF('day',
                   TRY_CAST(entity_date AS DATE),
                   TRY_CAST(note_date AS DATE))) > 365)
       OR date_status = 'coarse_anchor_date'
       OR (date_confidence < 50 AND date_status != 'coarse_anchor_date')
),

molecular_issues AS (
    SELECT
        'molecular' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'molecular_linkage' AS validation_domain,
        'molecular_without_anchor' AS issue_code,
        'warning' AS severity,
        CAST(molecular_date AS VARCHAR) AS detected_value,
        'molecular_episode_v2' AS source_objects,
        NULL AS date_status,
        NULL::INTEGER AS date_confidence
    FROM molecular_episode_v2
    WHERE unresolved_flag = TRUE
),

rai_issues AS (
    SELECT
        'rai' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'rai_linkage' AS validation_domain,
        CASE
            WHEN rai_date IS NULL THEN 'rai_without_date'
            WHEN linked_surgery_date IS NULL THEN 'rai_without_surgery_anchor'
            WHEN days_surgery_to_rai < -30 THEN 'invalid_interval_surgery_to_rai'
            WHEN days_surgery_to_rai > 730 THEN 'invalid_interval_surgery_to_rai'
        END AS issue_code,
        CASE
            WHEN rai_date IS NULL THEN 'error'
            WHEN linked_surgery_date IS NULL THEN 'warning'
            ELSE 'warning'
        END AS severity,
        CAST(days_surgery_to_rai AS VARCHAR) AS detected_value,
        'rai_episode_v2' AS source_objects,
        NULL AS date_status,
        NULL::INTEGER AS date_confidence
    FROM rai_episode_v2
    WHERE unresolved_flag = TRUE
),

histology_issues AS (
    SELECT
        'histology' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'histology_reconciliation' AS validation_domain,
        'histology_conflict' AS issue_code,
        'warning' AS severity,
        source_histology_raw_ps || ' vs ' ||
            COALESCE(source_histology_raw_tp, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects,
        NULL AS date_status,
        NULL::INTEGER AS date_confidence
    FROM histology_reconciliation_v2
    WHERE histology_discordance_flag = TRUE
),

stage_issues AS (
    SELECT
        'staging' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'stage_reconciliation' AS validation_domain,
        'stage_conflict' AS issue_code,
        'warning' AS severity,
        COALESCE(t_stage_source_path, 'NULL') || ' vs ' ||
            COALESCE(t_stage_source_note, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects,
        NULL AS date_status,
        NULL::INTEGER AS date_confidence
    FROM histology_reconciliation_v2
    WHERE stage_discordance_flag = TRUE
),

n_stage_issues AS (
    SELECT
        'staging' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'stage_reconciliation' AS validation_domain,
        'n_stage_conflict' AS issue_code,
        'info' AS severity,
        COALESCE(n_stage_source_path, 'NULL') || ' vs ' ||
            COALESCE(n_stage_source_note, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects,
        NULL AS date_status,
        NULL::INTEGER AS date_confidence
    FROM histology_reconciliation_v2
    WHERE n_stage_discordance_flag = TRUE
)

SELECT
    ROW_NUMBER() OVER (ORDER BY severity DESC, domain, research_id) AS validation_id,
    CAST(research_id AS VARCHAR) AS research_id,
    domain,
    validation_domain,
    severity,
    issue_code,
    CASE
        WHEN issue_code = 'no_date_recoverable'
            THEN 'Entity has no recoverable date from any source (including cross-table fallback)'
        WHEN issue_code = 'future_date'
            THEN 'Inferred date is in the future'
        WHEN issue_code = 'implausible_historical_date'
            THEN 'Inferred date before 1990'
        WHEN issue_code = 'entity_vs_note_date_gap'
            THEN 'Entity date and note date differ by >1 year'
        WHEN issue_code = 'coarse_anchor_only'
            THEN 'Date assigned from cross-table anchor (surgery/FNA), not from entity source'
        WHEN issue_code = 'low_confidence'
            THEN 'Date confidence score below 50'
        WHEN issue_code = 'molecular_without_anchor'
            THEN 'Molecular test has no date or episode linkage'
        WHEN issue_code = 'rai_without_date'
            THEN 'RAI mention has no resolved date'
        WHEN issue_code = 'rai_without_surgery_anchor'
            THEN 'RAI mention has no linked surgery'
        WHEN issue_code = 'invalid_interval_surgery_to_rai'
            THEN 'Surgery-to-RAI interval implausible (<-30d or >730d)'
        WHEN issue_code = 'histology_conflict'
            THEN 'Histology type differs between path_synoptics and tumor_pathology'
        WHEN issue_code = 'stage_conflict'
            THEN 'T-stage differs between pathology report and clinical notes'
        WHEN issue_code = 'n_stage_conflict'
            THEN 'N-stage differs between pathology report and clinical notes'
        ELSE issue_code
    END AS issue_description,
    source_objects,
    NULL AS related_episode_ids,
    detected_value,
    date_status AS date_status_context,
    CASE
        WHEN issue_code IN ('future_date','implausible_historical_date')
            THEN 'date must be between 1990-01-01 and today'
        WHEN issue_code = 'entity_vs_note_date_gap'
            THEN 'entity_date and note_date should be within 365 days'
        WHEN issue_code = 'invalid_interval_surgery_to_rai'
            THEN 'surgery_to_rai interval should be -30..730 days'
        WHEN issue_code = 'coarse_anchor_only'
            THEN 'date is usable but low precision; consider for timeline display, not clinical analysis'
        ELSE NULL
    END AS expected_rule,
    CASE
        WHEN severity = 'error' THEN 'manual_review_required'
        WHEN severity = 'warning' THEN 'verify_and_resolve'
        WHEN severity = 'info' THEN 'monitor'
        ELSE NULL
    END AS recommended_action
FROM (
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM date_issues WHERE issue_code IS NOT NULL
    UNION ALL
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM molecular_issues
    UNION ALL
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM rai_issues WHERE issue_code IS NOT NULL
    UNION ALL
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM histology_issues
    UNION ALL
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM stage_issues
    UNION ALL
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects, date_status, date_confidence
    FROM n_stage_issues
) combined;
"""

PATIENT_ROLLUP_V2_SQL = """
CREATE OR REPLACE VIEW patient_validation_rollup_v2_mv AS
SELECT
    research_id,
    COUNT(*) AS total_issues,
    SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning_count,
    SUM(CASE WHEN severity = 'info' THEN 1 ELSE 0 END) AS info_count,
    CASE
        WHEN SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) > 0
            THEN 'error'
        WHEN SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) > 0
            THEN 'warning'
        ELSE 'info'
    END AS highest_severity,
    STRING_AGG(DISTINCT domain, ', ' ORDER BY domain) AS affected_domains,
    STRING_AGG(DISTINCT issue_code, ', ' ORDER BY issue_code) AS issue_codes
FROM validation_failures_v3
GROUP BY research_id;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Deployment
# ═══════════════════════════════════════════════════════════════════════════════

def profile_before(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Capture baseline counts from v2 views."""
    section("BASELINE COUNTS (BEFORE — from v2 views)")
    counts: dict[str, int] = {}
    queries = {
        "validation_v2_total": "SELECT COUNT(*) FROM validation_failures_v2",
        "validation_v2_errors": "SELECT COUNT(*) FROM validation_failures_v2 WHERE severity = 'error'",
        "validation_v2_no_date_errors": """
            SELECT COUNT(*) FROM validation_failures_v2
            WHERE issue_code = 'no_date_recoverable'
        """,
        "timeline_rescue_total": "SELECT COUNT(*) FROM timeline_rescue_mv",
        "timeline_rescue_rescued": """
            SELECT COUNT(*) FROM timeline_rescue_mv WHERE rescue_status = 'rescued'
        """,
        "meds_unrecoverable": """
            SELECT COUNT(*) FROM enriched_note_entities_medications
            WHERE date_source = 'unrecoverable'
        """,
        "problem_list_unrecoverable": """
            SELECT COUNT(*) FROM enriched_note_entities_problem_list
            WHERE date_source = 'unrecoverable'
        """,
    }
    for label, sql in queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            counts[label] = cnt
            print(f"  {label:<45} {cnt:>8,}")
        except Exception as e:
            counts[label] = -1
            print(f"  {label:<45} ERROR: {e}")
    return counts


def profile_after(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Capture post-upgrade counts from v3 views."""
    section("UPGRADED COUNTS (AFTER — from v3 views)")
    counts: dict[str, int] = {}
    queries = {
        "validation_v3_total": "SELECT COUNT(*) FROM validation_failures_v3",
        "validation_v3_errors": """
            SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'error'
        """,
        "validation_v3_warnings": """
            SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'warning'
        """,
        "validation_v3_info": """
            SELECT COUNT(*) FROM validation_failures_v3 WHERE severity = 'info'
        """,
        "validation_v3_no_date_errors": """
            SELECT COUNT(*) FROM validation_failures_v3
            WHERE issue_code = 'no_date_recoverable'
        """,
        "validation_v3_coarse_anchor_info": """
            SELECT COUNT(*) FROM validation_failures_v3
            WHERE issue_code = 'coarse_anchor_only'
        """,
        "meds_unrecoverable_v3": """
            SELECT COUNT(*) FROM enriched_note_entities_medications
            WHERE date_status = 'unresolved_date'
        """,
        "problem_list_unrecoverable_v3": """
            SELECT COUNT(*) FROM enriched_note_entities_problem_list
            WHERE date_status = 'unresolved_date'
        """,
        "meds_coarse_anchor_v3": """
            SELECT COUNT(*) FROM enriched_note_entities_medications
            WHERE date_status = 'coarse_anchor_date'
        """,
        "problem_list_coarse_anchor_v3": """
            SELECT COUNT(*) FROM enriched_note_entities_problem_list
            WHERE date_status = 'coarse_anchor_date'
        """,
        "timeline_v2_truly_unresolved": """
            SELECT COUNT(*) FROM timeline_rescue_v2_mv
            WHERE date_status = 'unresolved_date'
        """,
        "timeline_v2_coarse_rescued": """
            SELECT COUNT(*) FROM timeline_rescue_v2_mv
            WHERE rescue_status = 'rescued_coarse_anchor'
        """,
        "patients_with_issues_v3": """
            SELECT COUNT(*) FROM patient_validation_rollup_v2_mv
        """,
    }
    for label, sql in queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            counts[label] = cnt
            print(f"  {label:<45} {cnt:>8,}")
        except Exception as e:
            counts[label] = -1
            print(f"  {label:<45} ERROR: {e}")
    return counts


def write_sql_file(view_log: list[tuple[str, str]]) -> None:
    """Write generated SQL definitions to file."""
    with open(SQL_OUT, "w") as f:
        f.write("-- Semantic Cleanup V3 Views\n")
        f.write("-- Generated by 17_semantic_cleanup_v3.py\n")
        f.write("-- Fixes the timeline/validation contradiction from reconciliation_v2.\n")
        f.write("--\n")
        f.write("-- Date status taxonomy applied to all enriched views:\n")
        f.write("--   exact_source_date       entity_date parsed (confidence 100)\n")
        f.write("--   inferred_day_level_date  note_date used (confidence 70)\n")
        f.write("--   coarse_anchor_date       surgery/FNA/molecular anchor (confidence 35-60)\n")
        f.write("--   unresolved_date          no date from any source (confidence 0)\n")
        f.write("--\n")
        f.write("-- Replaces: enriched_note_entities_* (all 6), timeline_rescue_mv,\n")
        f.write("--           validation_failures_v2\n")
        f.write("-- Adds: validation_failures_v3, timeline_rescue_v2_mv,\n")
        f.write("--        timeline_unresolved_summary_v2_mv, patient_validation_rollup_v2_mv\n")
        f.write("--\n")
        f.write("-- Depends on: base tables (note_entities_*, path_synoptics, fna_history,\n")
        f.write("--              molecular_testing), reconciliation_v2 views\n\n")
        for name, sql in view_log:
            f.write(f"-- === {name} ===\n")
            f.write(sql.strip())
            f.write("\n\n")
    print(f"  SQL definitions saved to: {SQL_OUT}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase A: Semantic Cleanup / Timeline Contradiction Fix"
    )
    parser.add_argument(
        "--md", action="store_true",
        help="Use MotherDuck instead of local DuckDB",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("  PHASE A: SEMANTIC CLEANUP V3 — Timeline Contradiction Fix")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    # Register data sources
    section("REGISTERING DATA SOURCES")
    if args.md:
        for tbl in REQUIRED_TABLES:
            if table_available(con, tbl):
                print(f"  Available {tbl}")
            else:
                print(f"  MISSING   {tbl}")
    else:
        register_parquets(con)

    # Verify Phase 1 prerequisite views exist
    section("CHECKING PREREQUISITES")
    prereqs = [
        "enriched_note_entities_genetics",
        "enriched_note_entities_staging",
        "enriched_note_entities_medications",
        "enriched_note_entities_problem_list",
        "timeline_rescue_mv",
        "validation_failures_v2",
        "histology_reconciliation_v2",
        "molecular_episode_v2",
        "rai_episode_v2",
    ]
    for v in prereqs:
        status = "OK" if table_available(con, v) else "MISS"
        print(f"  {status:<6} {v}")

    # Baseline counts
    before = profile_before(con)

    # Deploy updated enriched views
    view_log: list[tuple[str, str]] = []

    section("STEP 1: UPDATING ENRICHED VIEWS (surgery/FNA fallback + date_status)")

    deploy_view(con, "enriched_note_entities_genetics",
                ENRICHED_GENETICS_SQL, view_log)
    deploy_view(con, "enriched_note_entities_staging",
                ENRICHED_STAGING_SQL, view_log)
    deploy_view(con, "enriched_note_entities_procedures",
                ENRICHED_PROCEDURES_SQL, view_log)
    deploy_view(con, "enriched_note_entities_complications",
                ENRICHED_COMPLICATIONS_SQL, view_log)
    deploy_view(con, "enriched_note_entities_medications",
                ENRICHED_MEDICATIONS_SQL, view_log)
    deploy_view(con, "enriched_note_entities_problem_list",
                ENRICHED_PROBLEM_LIST_SQL, view_log)

    section("STEP 2: TIMELINE RESCUE V2 (truly unresolvable only)")
    deploy_view(con, "timeline_rescue_v2_mv",
                TIMELINE_RESCUE_V2_SQL, view_log)
    deploy_view(con, "timeline_unresolved_summary_v2_mv",
                TIMELINE_UNRESOLVED_SUMMARY_V2_SQL, view_log)

    section("STEP 3: VALIDATION FAILURES V3 (corrected semantics)")
    deploy_view(con, "validation_failures_v3",
                VALIDATION_V3_SQL, view_log)
    deploy_view(con, "patient_validation_rollup_v2_mv",
                PATIENT_ROLLUP_V2_SQL, view_log)

    # Post-deployment counts
    after = profile_after(con)

    # Before/after comparison
    section("BEFORE / AFTER COMPARISON — CONTRADICTION RESOLUTION")
    print(f"  {'metric':<50} {'v2':>8} {'v3':>8} {'delta':>8}")
    print("  " + "-" * 76)

    comparisons = [
        ("Validation: total issues",
         before.get("validation_v2_total", -1),
         after.get("validation_v3_total", -1)),
        ("Validation: error severity",
         before.get("validation_v2_errors", -1),
         after.get("validation_v3_errors", -1)),
        ("Validation: no_date_recoverable errors",
         before.get("validation_v2_no_date_errors", -1),
         after.get("validation_v3_no_date_errors", -1)),
        ("Meds: unrecoverable/unresolved",
         before.get("meds_unrecoverable", -1),
         after.get("meds_unrecoverable_v3", -1)),
        ("Problem list: unrecoverable/unresolved",
         before.get("problem_list_unrecoverable", -1),
         after.get("problem_list_unrecoverable_v3", -1)),
    ]
    for label, b, a in comparisons:
        delta = a - b if b >= 0 and a >= 0 else "N/A"
        print(f"  {label:<50} {b:>8,} {a:>8,} {str(delta):>8}")

    print()
    print("  KEY RESULT: no_date_recoverable errors should drop from 8,555 to ~0")
    print("  Former 'errors' reclassified as 'info' (coarse_anchor_only)")

    # Severity breakdown
    section("VALIDATION V3 SEVERITY BREAKDOWN")
    try:
        rows = con.execute("""
            SELECT severity, domain, issue_code, COUNT(*) AS n,
                   COUNT(DISTINCT research_id) AS patients
            FROM validation_failures_v3
            GROUP BY 1, 2, 3
            ORDER BY
                CASE severity WHEN 'error' THEN 1
                              WHEN 'warning' THEN 2
                              ELSE 3 END,
                n DESC
        """).fetchall()
        print(f"  {'severity':<10} {'domain':<15} {'issue_code':<35} "
              f"{'rows':>7} {'patients':>9}")
        print("  " + "-" * 78)
        for r in rows:
            print(f"  {str(r[0]):<10} {str(r[1]):<15} {str(r[2]):<35} "
                  f"{r[3]:>7} {r[4]:>9}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Date status distribution
    section("DATE STATUS DISTRIBUTION ACROSS ALL ENRICHED VIEWS")
    try:
        rows = con.execute("""
            SELECT domain, date_status, COUNT(*) AS n
            FROM (
                SELECT 'genetics' AS domain, date_status FROM enriched_note_entities_genetics
                UNION ALL
                SELECT 'staging', date_status FROM enriched_note_entities_staging
                UNION ALL
                SELECT 'procedures', date_status FROM enriched_note_entities_procedures
                UNION ALL
                SELECT 'complications', date_status FROM enriched_note_entities_complications
                UNION ALL
                SELECT 'medications', date_status FROM enriched_note_entities_medications
                UNION ALL
                SELECT 'problem_list', date_status FROM enriched_note_entities_problem_list
            ) x
            GROUP BY 1, 2
            ORDER BY domain, date_status
        """).fetchall()
        print(f"  {'domain':<20} {'date_status':<30} {'count':>8}")
        print("  " + "-" * 60)
        for r in rows:
            print(f"  {r[0]:<20} {r[1]:<30} {r[2]:>8,}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Timeline rescue v2 summary
    section("TIMELINE RESCUE V2 SUMMARY")
    try:
        rows = con.execute("""
            SELECT source_table, rescue_status, date_status,
                   row_count, patient_count, pct_of_table, avg_confidence
            FROM timeline_unresolved_summary_v2_mv
            ORDER BY source_table, row_count DESC
        """).fetchall()
        print(f"  {'table':<15} {'rescue_status':<30} {'date_status':<22} "
              f"{'rows':>6} {'pts':>5} {'pct':>6} {'conf':>5}")
        print("  " + "-" * 91)
        for r in rows:
            print(f"  {r[0]:<15} {r[1]:<30} {r[2]:<22} "
                  f"{r[3]:>6} {r[4]:>5} {r[5]:>5.1f}% {r[6]:>5.1f}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Write SQL file
    section("WRITING SQL FILE")
    write_sql_file(view_log)

    # Verification: the contradiction test
    section("CONTRADICTION VERIFICATION")
    try:
        truly_unresolved = con.execute("""
            SELECT COUNT(*) FROM timeline_rescue_v2_mv
            WHERE date_status = 'unresolved_date'
        """).fetchone()[0]
        val_no_date_errors = con.execute("""
            SELECT COUNT(*) FROM validation_failures_v3
            WHERE issue_code = 'no_date_recoverable'
        """).fetchone()[0]
        print(f"  Timeline truly unresolved:      {truly_unresolved:>8,}")
        print(f"  Validation no_date errors:      {val_no_date_errors:>8,}")
        if truly_unresolved == val_no_date_errors:
            print("  PASS: Timeline and validation agree on unresolved count")
        else:
            print("  WARN: Counts differ — investigate")
    except Exception as e:
        print(f"  ERROR: {e}")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Phase A: Semantic Cleanup complete")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
