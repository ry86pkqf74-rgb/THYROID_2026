#!/usr/bin/env python3
"""
46_provenance_audit.py -- Full provenance + date-accuracy audit for THYROID_2026

Enforces 100% direct-source traceability and accurate event-date precedence
(lab collection date ALWAYS takes precedence over note_date).

Steps performed:
  1. Schema audit: check every MotherDuck table for 7 provenance columns
  2. Lab-date accuracy audit: measure note_date fallback rate per lab type
  3. Create provenance_enriched_events_v1 TABLE (strict date precedence)
  4. Create lineage_audit_v1 TABLE (raw -> note -> extracted -> final cohort)
  5. Insert QA issues into qa_issues for all NOTE_DATE_FALLBACK lab events
  6. Write docs/provenance_coverage_report.md
  7. Write docs/date_accuracy_verification_report_YYYYMMDD.md

Usage:
  .venv/bin/python scripts/46_provenance_audit.py --md           # MotherDuck
  .venv/bin/python scripts/46_provenance_audit.py --local        # local DuckDB
  .venv/bin/python scripts/46_provenance_audit.py --md --dry-run # audit only
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DOCS = ROOT / "docs"
DOCS.mkdir(exist_ok=True)

TODAY = datetime.now().strftime("%Y%m%d")

# ── provenance columns we check for on every table ──────────────────────────
PROVENANCE_COLUMNS = [
    "source_table",
    "source_column",
    "date_source",
    "date_confidence",
    "inferred_event_date",
    "extraction_method",
    "evidence_span",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Connection helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def connect_md() -> duckdb.DuckDBPyConnection:
    token = _get_token()
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def connect_local() -> duckdb.DuckDBPyConnection:
    local_path = os.getenv("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
    return duckdb.connect(local_path)


def tbl_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def safe_count(con: duckdb.DuckDBPyConnection, name: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        return 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 1: Schema + provenance column audit
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def audit_schema(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return per-table provenance column coverage."""
    print("  Querying information_schema.columns ...")
    rows = con.execute("""
        SELECT DISTINCT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, column_name
    """).fetchall()

    # Build {table: set(columns)}
    table_cols: dict[str, set[str]] = {}
    for tname, cname in rows:
        table_cols.setdefault(tname, set()).add(cname)

    results = []
    for tname, cols in sorted(table_cols.items()):
        present = {c for c in PROVENANCE_COLUMNS if c in cols}
        missing = [c for c in PROVENANCE_COLUMNS if c not in cols]
        pct = round(100.0 * len(present) / len(PROVENANCE_COLUMNS), 1)
        results.append({
            "table": tname,
            "cols_present": sorted(present),
            "cols_missing": missing,
            "coverage_pct": pct,
        })

    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 1: Lab-date accuracy audit
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LAB_DATE_AUDIT_SQL = """
WITH lab_events AS (
    SELECT
        e.research_id,
        e.event_date,
        e.event_subtype,
        e.source_column,
        t.specimen_collect_dt,
        CASE
            WHEN t.specimen_collect_dt IS NOT NULL
                 AND TRIM(t.specimen_collect_dt) <> ''
            THEN 'has_lab_date'
            WHEN e.event_date IS NOT NULL THEN 'note_date_only'
            ELSE 'no_date'
        END AS date_quality
    FROM extracted_clinical_events_v4 AS e
    LEFT JOIN thyroglobulin_labs AS t
        ON CAST(e.research_id AS INT) = CAST(t.research_id AS INT)
        AND e.event_subtype IN ('thyroglobulin', 'tsh', 'anti_thyroglobulin')
    WHERE e.event_type = 'lab'
)
SELECT
    event_subtype,
    COUNT(*) AS total,
    COUNT(CASE WHEN date_quality = 'has_lab_date' THEN 1 END) AS has_lab_date,
    COUNT(CASE WHEN date_quality = 'note_date_only' THEN 1 END) AS note_date_only,
    COUNT(CASE WHEN date_quality = 'no_date' THEN 1 END) AS no_date,
    ROUND(
        100.0 * COUNT(CASE WHEN date_quality = 'has_lab_date' THEN 1 END) / COUNT(*),
        2
    ) AS correct_pct,
    ROUND(
        100.0 * COUNT(CASE WHEN date_quality = 'note_date_only' THEN 1 END) / COUNT(*),
        2
    ) AS fallback_pct
FROM lab_events
GROUP BY event_subtype
ORDER BY total DESC
"""

LAB_DATE_AUDIT_ALL_SQL = """
WITH lab_events AS (
    SELECT
        e.research_id,
        e.event_date,
        e.event_subtype,
        e.source_column,
        t.specimen_collect_dt,
        CASE
            WHEN t.specimen_collect_dt IS NOT NULL
                 AND TRIM(t.specimen_collect_dt) <> ''
            THEN 'has_lab_date'
            WHEN e.event_date IS NOT NULL THEN 'note_date_only'
            ELSE 'no_date'
        END AS date_quality
    FROM extracted_clinical_events_v4 AS e
    LEFT JOIN thyroglobulin_labs AS t
        ON CAST(e.research_id AS INT) = CAST(t.research_id AS INT)
        AND e.event_subtype IN ('thyroglobulin', 'tsh', 'anti_thyroglobulin')
    WHERE e.event_type = 'lab'
)
SELECT
    'ALL_LABS' AS event_subtype,
    COUNT(*) AS total,
    COUNT(CASE WHEN date_quality = 'has_lab_date' THEN 1 END) AS has_lab_date,
    COUNT(CASE WHEN date_quality = 'note_date_only' THEN 1 END) AS note_date_only,
    COUNT(CASE WHEN date_quality = 'no_date' THEN 1 END) AS no_date,
    ROUND(
        100.0 * COUNT(CASE WHEN date_quality = 'has_lab_date' THEN 1 END) / COUNT(*),
        2
    ) AS correct_pct,
    ROUND(
        100.0 * COUNT(CASE WHEN date_quality = 'note_date_only' THEN 1 END) / COUNT(*),
        2
    ) AS fallback_pct
FROM lab_events
"""


def audit_lab_dates(con: duckdb.DuckDBPyConnection) -> tuple[list[dict], dict]:
    """Audit lab-date accuracy. Returns (per-subtype rows, overall row)."""
    print("  Running lab-date accuracy audit ...")
    if not tbl_exists(con, "extracted_clinical_events_v4"):
        print("  WARNING: extracted_clinical_events_v4 not found; skipping lab audit")
        return [], {}
    if not tbl_exists(con, "thyroglobulin_labs"):
        print("  WARNING: thyroglobulin_labs not found; skipping lab-date join")
        return [], {}

    rows = con.execute(LAB_DATE_AUDIT_SQL).fetchall()
    cols = ["event_subtype", "total", "has_lab_date", "note_date_only",
            "no_date", "correct_pct", "fallback_pct"]
    per_type = [dict(zip(cols, r)) for r in rows]

    overall_row = con.execute(LAB_DATE_AUDIT_ALL_SQL).fetchone()
    overall = dict(zip(cols, overall_row)) if overall_row else {}

    return per_type, overall


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 2: Create provenance_enriched_events_v1
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROVENANCE_ENRICHED_SQL = """
CREATE OR REPLACE TABLE provenance_enriched_events_v1 AS
SELECT
    e.research_id,
    e.event_type,
    e.event_subtype,
    e.event_value,
    e.event_unit,
    e.event_date,
    e.event_text,
    e.source_column,
    e.followup_date,
    e.days_since_nearest_surgery,
    e.nearest_surgery_number,
    e.confidence_score,
    -- Lab-specific: specimen_collect_dt from structured thyroglobulin_labs
    t.specimen_collect_dt,
    -- Strict date precedence: specimen_collect_dt > entity event_date > followup_date
    COALESCE(
        TRY_CAST(t.specimen_collect_dt AS DATE),
        TRY_CAST(e.event_date AS DATE),
        e.followup_date
    ) AS event_date_correct,
    -- Date quality classification
    CASE
        WHEN t.specimen_collect_dt IS NOT NULL
             AND TRIM(t.specimen_collect_dt) <> ''
        THEN 'LAB_DATE_USED'
        WHEN e.event_date IS NOT NULL
             AND e.event_date <> ''
             AND (e.followup_date IS NULL
                  OR TRY_CAST(e.event_date AS DATE) <> e.followup_date)
        THEN 'ENTITY_DATE_USED'
        WHEN e.event_date IS NOT NULL AND e.event_date <> ''
        THEN 'ENTITY_DATE_EQUALS_NOTE_DATE'
        WHEN e.followup_date IS NOT NULL
        THEN 'NOTE_DATE_FALLBACK'
        ELSE 'NO_DATE'
    END AS date_status_final,
    -- Direct source link: source_column|research_id|event_subtype|truncated_text
    CONCAT_WS('|',
        COALESCE(e.source_column, 'unknown_source'),
        COALESCE(CAST(e.research_id AS VARCHAR), ''),
        COALESCE(e.event_subtype, ''),
        COALESCE(LEFT(e.event_text, 80), '')
    ) AS direct_source_link,
    -- Audit timestamp
    CURRENT_TIMESTAMP AS provenance_created_at
FROM extracted_clinical_events_v4 AS e
LEFT JOIN thyroglobulin_labs AS t
    ON CAST(e.research_id AS INT) = CAST(t.research_id AS INT)
    AND e.event_type = 'lab'
    AND e.event_subtype IN ('thyroglobulin', 'tsh', 'anti_thyroglobulin')
"""

PROVENANCE_ENRICHED_FALLBACK_SQL = """
CREATE OR REPLACE TABLE provenance_enriched_events_v1 AS
SELECT
    e.research_id,
    e.event_type,
    e.event_subtype,
    e.event_value,
    e.event_unit,
    e.event_date,
    e.event_text,
    e.source_column,
    e.followup_date,
    e.days_since_nearest_surgery,
    e.nearest_surgery_number,
    e.confidence_score,
    NULL::VARCHAR AS specimen_collect_dt,
    COALESCE(
        TRY_CAST(e.event_date AS DATE),
        e.followup_date
    ) AS event_date_correct,
    CASE
        WHEN e.event_date IS NOT NULL AND e.event_date <> ''
        THEN 'ENTITY_DATE_USED'
        WHEN e.followup_date IS NOT NULL
        THEN 'NOTE_DATE_FALLBACK'
        ELSE 'NO_DATE'
    END AS date_status_final,
    CONCAT_WS('|',
        COALESCE(e.source_column, 'unknown_source'),
        COALESCE(CAST(e.research_id AS VARCHAR), ''),
        COALESCE(e.event_subtype, ''),
        COALESCE(LEFT(e.event_text, 80), '')
    ) AS direct_source_link,
    CURRENT_TIMESTAMP AS provenance_created_at
FROM extracted_clinical_events_v4 AS e
"""


def create_provenance_enriched(con: duckdb.DuckDBPyConnection) -> int:
    """Create provenance_enriched_events_v1. Returns row count."""
    print("  Creating provenance_enriched_events_v1 ...")
    if not tbl_exists(con, "extracted_clinical_events_v4"):
        print("  SKIP: extracted_clinical_events_v4 not available")
        return 0

    if tbl_exists(con, "thyroglobulin_labs"):
        con.execute(PROVENANCE_ENRICHED_SQL)
    else:
        print("  WARNING: thyroglobulin_labs missing; creating without specimen_collect_dt join")
        con.execute(PROVENANCE_ENRICHED_FALLBACK_SQL)

    n = safe_count(con, "provenance_enriched_events_v1")
    print(f"  => provenance_enriched_events_v1: {n:,} rows")
    return n


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 4b: Create lineage_audit_v1 (raw -> note -> extracted -> final)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LINEAGE_AUDIT_SQL = """
CREATE OR REPLACE TABLE lineage_audit_v1 AS
WITH
-- One row per patient from the raw pathology synoptic (ground truth)
raw_path AS (
    SELECT DISTINCT
        CAST(research_id AS INT) AS research_id,
        surg_date               AS raw_surgery_date,
        tumor_1_histologic_type AS raw_histology_type,
        tumor_1_size_greatest_dimension_cm AS raw_tumor_size_cm,
        tumor_1_extrathyroidal_extension   AS raw_ete,
        tumor_1_ln_involved                AS raw_ln_positive
    FROM path_synoptics
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(research_id AS INT)
        ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
    ) = 1
),
-- Note-level anchor (one note per patient, most recent)
note_anchor AS (
    SELECT DISTINCT
        CAST(research_id AS INT) AS research_id,
        note_type,
        note_date
    FROM clinical_notes_long
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(research_id AS INT)
        ORDER BY TRY_CAST(note_date AS DATE) DESC NULLS LAST
    ) = 1
),
-- Extracted entity sample: one staging entity per patient
extracted_staging AS (
    SELECT DISTINCT
        CAST(research_id AS INT) AS research_id,
        entity_value_norm,
        entity_date,
        note_date        AS ne_note_date,
        inferred_event_date,
        date_source,
        date_confidence,
        extraction_method,
        evidence_span
    FROM note_entities_staging
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(research_id AS INT)
        ORDER BY date_confidence DESC NULLS LAST
    ) = 1
),
-- Final analytic cohort (most refined master table)
final_cohort AS (
    SELECT DISTINCT
        CAST(research_id AS INT) AS research_id,
        COALESCE(ete_grade_v9, ete_grade_v5, ete_grade_v3) AS final_ete_grade,
        braf_positive_v7       AS final_braf_positive,
        tert_positive_v9       AS final_tert_positive,
        margin_status_refined  AS final_margin_status,
        vascular_who_2022_grade AS final_vascular_grade,
        ln_positive_v6         AS final_ln_positive,
        recurrence_confirmed   AS final_recurrence
    FROM patient_refined_master_clinical_v9
)
SELECT
    rp.research_id,
    -- Tier 1: raw structured source
    rp.raw_surgery_date,
    rp.raw_histology_type,
    rp.raw_tumor_size_cm,
    rp.raw_ete,
    rp.raw_ln_positive,
    -- Tier 2: note anchor
    na.note_type         AS anchor_note_type,
    na.note_date         AS anchor_note_date,
    -- Tier 3: extracted entity
    es.entity_value_norm AS extracted_staging_value,
    es.entity_date       AS extracted_entity_date,
    es.ne_note_date      AS extracted_note_date,
    es.inferred_event_date,
    es.date_source,
    es.date_confidence,
    es.extraction_method,
    es.evidence_span,
    -- Tier 4: final analytic cohort
    fc.final_ete_grade,
    fc.final_braf_positive,
    fc.final_tert_positive,
    fc.final_margin_status,
    fc.final_vascular_grade,
    fc.final_ln_positive,
    fc.final_recurrence,
    -- Derived: date traceability flag
    CASE
        WHEN es.entity_date IS NOT NULL       THEN 'entity_date_traced'
        WHEN es.inferred_event_date IS NOT NULL THEN 'inferred_date_traced'
        WHEN na.note_date IS NOT NULL          THEN 'note_date_only'
        WHEN rp.raw_surgery_date IS NOT NULL   THEN 'surgery_anchor_only'
        ELSE 'untraced'
    END AS date_traceability_status,
    CURRENT_TIMESTAMP AS audit_created_at
FROM raw_path AS rp
LEFT JOIN note_anchor      AS na ON rp.research_id = na.research_id
LEFT JOIN extracted_staging AS es ON rp.research_id = es.research_id
LEFT JOIN final_cohort      AS fc ON rp.research_id = fc.research_id
"""

LINEAGE_AUDIT_SIMPLE_SQL = """
CREATE OR REPLACE TABLE lineage_audit_v1 AS
SELECT
    CAST(ps.research_id AS INT) AS research_id,
    ps.surg_date   AS raw_surgery_date,
    ps.tumor_1_histologic_type AS raw_histology_type,
    ps.tumor_1_size_greatest_dimension_cm AS raw_tumor_size_cm,
    ps.tumor_1_extrathyroidal_extension   AS raw_ete,
    ps.tumor_1_ln_involved                AS raw_ln_positive,
    NULL::VARCHAR  AS anchor_note_type,
    NULL::VARCHAR  AS anchor_note_date,
    NULL::VARCHAR  AS extracted_staging_value,
    NULL::VARCHAR  AS extracted_entity_date,
    NULL::VARCHAR  AS extracted_note_date,
    NULL::DATE     AS inferred_event_date,
    NULL::VARCHAR  AS date_source,
    NULL::INT      AS date_confidence,
    NULL::VARCHAR  AS extraction_method,
    NULL::VARCHAR  AS evidence_span,
    NULL::VARCHAR  AS final_ete_grade,
    NULL::VARCHAR  AS final_braf_positive,
    NULL::VARCHAR  AS final_tert_positive,
    NULL::VARCHAR  AS final_margin_status,
    NULL::VARCHAR  AS final_vascular_grade,
    NULL::VARCHAR  AS final_ln_positive,
    NULL::BOOLEAN  AS final_recurrence,
    'untraced'     AS date_traceability_status,
    CURRENT_TIMESTAMP AS audit_created_at
FROM path_synoptics AS ps
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(ps.research_id AS INT)
    ORDER BY TRY_CAST(ps.surg_date AS DATE) ASC NULLS LAST
) = 1
"""


def create_lineage_audit(con: duckdb.DuckDBPyConnection) -> int:
    """Create lineage_audit_v1. Returns row count."""
    print("  Creating lineage_audit_v1 ...")

    required = ["path_synoptics", "clinical_notes_long",
                "note_entities_staging", "patient_refined_master_clinical_v9"]
    missing = [t for t in required if not tbl_exists(con, t)]

    if not tbl_exists(con, "path_synoptics"):
        print("  SKIP: path_synoptics not available")
        return 0

    if missing:
        print(f"  WARNING: missing {missing}; building simplified lineage_audit_v1")
        con.execute(LINEAGE_AUDIT_SIMPLE_SQL)
    else:
        try:
            con.execute(LINEAGE_AUDIT_SQL)
        except Exception as exc:
            print(f"  WARNING: full lineage SQL failed ({exc}); using simplified version")
            con.execute(LINEAGE_AUDIT_SIMPLE_SQL)

    n = safe_count(con, "lineage_audit_v1")
    print(f"  => lineage_audit_v1: {n:,} rows")
    return n


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 4c: Insert QA issues for NOTE_DATE_FALLBACK lab events
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def insert_qa_issues(con: duckdb.DuckDBPyConnection) -> int:
    """Insert provenance QA issues into qa_issues table. Returns rows inserted."""
    if not tbl_exists(con, "qa_issues"):
        print("  SKIP: qa_issues table not found")
        return 0
    if not tbl_exists(con, "provenance_enriched_events_v1"):
        print("  SKIP: provenance_enriched_events_v1 not ready")
        return 0

    # Remove any prior provenance audit entries to avoid duplicates
    try:
        con.execute("DELETE FROM qa_issues WHERE check_id LIKE 'provenance_%'")
    except Exception:
        pass

    insert_sql = """
    INSERT INTO qa_issues (check_id, severity, research_id, description, detail)
    SELECT
        'provenance_lab_note_date_fallback' AS check_id,
        'error'                             AS severity,
        CAST(research_id AS INT)            AS research_id,
        'Lab event using note_date instead of specimen_collect_dt' AS description,
        CONCAT(
            'event_subtype=', COALESCE(event_subtype, 'NULL'),
            ' event_date=', COALESCE(CAST(event_date AS VARCHAR), 'NULL'),
            ' source=', COALESCE(source_column, 'NULL')
        )                                   AS detail
    FROM provenance_enriched_events_v1
    WHERE event_type = 'lab'
      AND date_status_final = 'NOTE_DATE_FALLBACK'
    """
    try:
        con.execute(insert_sql)
        n = safe_count(con, "qa_issues") 
        # Just count the new ones
        n_new = con.execute(
            "SELECT COUNT(*) FROM qa_issues WHERE check_id = 'provenance_lab_note_date_fallback'"
        ).fetchone()[0]
        print(f"  Inserted {n_new:,} QA issues (provenance_lab_note_date_fallback)")
        return n_new
    except Exception as exc:
        print(f"  WARNING: QA issue insert failed: {exc}")
        return 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Report generation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def write_coverage_report(
    schema_audit: list[dict],
    lab_audit: list[dict],
    overall_lab: dict,
    prov_enriched_n: int,
    lineage_n: int,
) -> Path:
    """Write docs/provenance_coverage_report.md."""
    out = DOCS / "provenance_coverage_report.md"

    fully_covered = [r for r in schema_audit if r["coverage_pct"] == 100.0]
    partially_covered = [r for r in schema_audit if 0 < r["coverage_pct"] < 100.0]
    not_covered = [r for r in schema_audit if r["coverage_pct"] == 0.0]
    avg_pct = (
        sum(r["coverage_pct"] for r in schema_audit) / len(schema_audit)
        if schema_audit else 0.0
    )

    lines: list[str] = [
        "# Provenance Coverage Report",
        f"",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"",
        "## Summary",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total tables audited | {len(schema_audit)} |",
        f"| Avg provenance coverage | {avg_pct:.1f}% |",
        f"| Tables fully covered (100%) | {len(fully_covered)} |",
        f"| Tables partially covered | {len(partially_covered)} |",
        f"| Tables with no provenance | {len(not_covered)} |",
        f"| `provenance_enriched_events_v1` rows | {prov_enriched_n:,} |",
        f"| `lineage_audit_v1` rows | {lineage_n:,} |",
        f"",
        "## Provenance Columns Checked",
        f"",
    ]
    for c in PROVENANCE_COLUMNS:
        lines.append(f"- `{c}`")

    lines += [
        f"",
        "## Tables — Missing Provenance Columns",
        f"",
        f"| Table | Coverage % | Missing Columns |",
        f"|-------|-----------|----------------|",
    ]
    for r in sorted(schema_audit, key=lambda x: x["coverage_pct"]):
        if r["cols_missing"]:
            missing_str = ", ".join(f"`{c}`" for c in r["cols_missing"])
            lines.append(f"| {r['table']} | {r['coverage_pct']:.0f}% | {missing_str} |")

    lines += [
        f"",
        "## Core NLP Provenance Tables (note_entities_*)",
        f"",
        f"These 6 tables have the richest provenance via scripts 15/17/27:",
        f"- `note_entities_staging`, `note_entities_genetics`, `note_entities_procedures`",
        f"- `note_entities_complications`, `note_entities_medications`, `note_entities_problem_list`",
        f"",
        f"Provenance columns present: `inferred_event_date`, `date_source`, `date_granularity`, `date_confidence`",
        f"Enriched views add: `date_status`, `date_anchor_type`, `date_anchor_table`",
        f"",
        "## Lab Date Accuracy Audit",
        f"",
    ]

    if lab_audit:
        lines += [
            f"| Lab Type | Total | Has Lab Date | Note-Date Fallback | No Date | Correct % | Fallback % |",
            f"|----------|-------|-------------|-------------------|---------|-----------|------------|",
        ]
        for r in lab_audit:
            lines.append(
                f"| {r['event_subtype']} | {r['total']:,} | {r['has_lab_date']:,} | "
                f"{r['note_date_only']:,} | {r['no_date']:,} | "
                f"{r.get('correct_pct', 0):.1f}% | {r.get('fallback_pct', 0):.1f}% |"
            )
        if overall_lab:
            lines += [
                f"",
                f"**Overall:** {overall_lab.get('correct_pct', 0):.1f}% use lab-specific date; "
                f"{overall_lab.get('fallback_pct', 0):.1f}% fall back to note_date.",
            ]
    else:
        lines.append("_Lab date audit unavailable (extracted_clinical_events_v4 or thyroglobulin_labs missing)._")

    lines += [
        f"",
        "## Strict Date Precedence Rule",
        f"",
        "```sql",
        "-- Enforced in provenance_enriched_events_v1",
        "COALESCE(",
        "    TRY_CAST(specimen_collect_dt AS DATE),   -- 1. Lab collection date (confidence 1.0)",
        "    TRY_CAST(event_date AS DATE),             -- 2. Entity-extracted date (confidence 0.7)",
        "    followup_date                             -- 3. Note encounter date (fallback)",
        ") AS event_date_correct",
        "```",
        f"",
        "## Remediation Guidance",
        f"",
        "Tables missing `source_table` / `source_column` should have these added via ALTER TABLE",
        "in `scripts/27_date_provenance_formalization.sql`.",
        f"",
        "Tables missing `extraction_method` / `evidence_span` are non-NLP tables (structured Excel).",
        "For structured tables, use `source_table = 'table_name'` and `source_column = 'column_name'`",
        "as the provenance link in downstream analytic views.",
        f"",
        "---",
        f"",
        "_Generated by `scripts/46_provenance_audit.py`_",
    ]

    out.write_text("\n".join(lines))
    print(f"  => {out}")
    return out


def write_date_verification_report(
    lab_audit: list[dict],
    overall_lab: dict,
    qa_issues_inserted: int,
    con: duckdb.DuckDBPyConnection,
) -> Path:
    """Write docs/date_accuracy_verification_report_YYYYMMDD.md."""
    out = DOCS / f"date_accuracy_verification_report_{TODAY}.md"

    # Pull date_status distribution from provenance_enriched_events_v1
    status_rows: list[tuple] = []
    if tbl_exists(con, "provenance_enriched_events_v1"):
        try:
            status_rows = con.execute("""
                SELECT date_status_final, COUNT(*) AS n,
                       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
                FROM provenance_enriched_events_v1
                GROUP BY date_status_final
                ORDER BY n DESC
            """).fetchall()
        except Exception:
            pass

    # Pull date_traceability_status from lineage_audit_v1
    lineage_rows: list[tuple] = []
    if tbl_exists(con, "lineage_audit_v1"):
        try:
            lineage_rows = con.execute("""
                SELECT date_traceability_status, COUNT(*) AS n
                FROM lineage_audit_v1
                GROUP BY date_traceability_status
                ORDER BY n DESC
            """).fetchall()
        except Exception:
            pass

    lines: list[str] = [
        "# Date Accuracy Verification Report",
        f"",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"",
        "## Lab Date Accuracy — Summary",
        f"",
    ]

    if lab_audit:
        lines += [
            f"| Lab Type | Total Events | Correct Date % | Note-Date Fallback % |",
            f"|----------|-------------|----------------|---------------------|",
        ]
        for r in lab_audit:
            status = "OK" if r.get("correct_pct", 0) >= 80 else "NEEDS ATTENTION"
            lines.append(
                f"| {r['event_subtype']} | {r['total']:,} | "
                f"**{r.get('correct_pct', 0):.1f}%** | "
                f"{r.get('fallback_pct', 0):.1f}% {status} |"
            )
        if overall_lab:
            lines += [
                f"",
                f"**Overall correct date coverage: {overall_lab.get('correct_pct', 0):.1f}%**",
                f"**Overall note-date fallback rate: {overall_lab.get('fallback_pct', 0):.1f}%**",
            ]
    else:
        lines.append("_Lab date audit data unavailable._")

    if status_rows:
        lines += [
            f"",
            "## provenance_enriched_events_v1 — Date Status Distribution",
            f"",
            f"| date_status_final | Count | % |",
            f"|-------------------|-------|---|",
        ]
        for r in status_rows:
            lines.append(f"| {r[0]} | {r[1]:,} | {r[2]:.1f}% |")

    if lineage_rows:
        lines += [
            f"",
            "## lineage_audit_v1 — Date Traceability Distribution",
            f"",
            f"| date_traceability_status | Patients |",
            f"|--------------------------|---------|",
        ]
        for r in lineage_rows:
            lines.append(f"| {r[0]} | {r[1]:,} |")

    lines += [
        f"",
        "## QA Issues Inserted",
        f"",
        f"- `provenance_lab_note_date_fallback`: **{qa_issues_inserted:,}** error-severity issues inserted into `qa_issues`",
        f"",
        "## Remediation SQL",
        f"",
        "To fix remaining NOTE_DATE_FALLBACK lab events, link thyroglobulin_labs.specimen_collect_dt:",
        f"",
        "```sql",
        "-- Find patients with lab events needing date correction",
        "SELECT research_id, event_subtype, event_date, direct_source_link",
        "FROM provenance_enriched_events_v1",
        "WHERE event_type = 'lab'",
        "  AND date_status_final = 'NOTE_DATE_FALLBACK'",
        "ORDER BY research_id, event_subtype;",
        "",
        "-- Cross-check against thyroglobulin_labs for available dates",
        "SELECT t.research_id, t.specimen_collect_dt, t.result, e.event_date",
        "FROM thyroglobulin_labs t",
        "JOIN extracted_clinical_events_v4 e",
        "    ON CAST(t.research_id AS INT) = CAST(e.research_id AS INT)",
        "    AND e.event_type = 'lab'",
        "WHERE e.date_status_final = 'NOTE_DATE_FALLBACK';",
        "```",
        f"",
        "---",
        f"",
        "_Generated by `scripts/46_provenance_audit.py`_",
    ]

    out.write_text("\n".join(lines))
    print(f"  => {out}")
    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def section(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="46 -- Provenance audit + date-accuracy enforcement"
    )
    parser.add_argument("--md", action="store_true",
                        help="Use MotherDuck (default: local DuckDB)")
    parser.add_argument("--local", action="store_true",
                        help="Use local DuckDB explicitly")
    parser.add_argument("--dry-run", action="store_true",
                        help="Audit only; do not create tables or insert QA issues")
    args = parser.parse_args()

    section("46 -- Provenance Audit + Date-Accuracy Enforcement")

    # Connect
    if args.md and not args.local:
        try:
            con = connect_md()
            print("  Connected to MotherDuck (RW)")
        except Exception as exc:
            print(f"  MotherDuck unavailable: {exc}")
            print("  Falling back to local DuckDB")
            con = connect_local()
    else:
        con = connect_local()
        print(f"  Using local DuckDB")

    # ── Phase 1: Schema audit ───────────────────────────────────────────────
    section("Phase 1: Schema Provenance Audit")
    schema_audit = audit_schema(con)
    print(f"  Audited {len(schema_audit)} tables")
    covered_100 = sum(1 for r in schema_audit if r["coverage_pct"] == 100.0)
    covered_0 = sum(1 for r in schema_audit if r["coverage_pct"] == 0.0)
    avg_pct = sum(r["coverage_pct"] for r in schema_audit) / max(len(schema_audit), 1)
    print(f"  Average provenance coverage: {avg_pct:.1f}%")
    print(f"  Fully covered: {covered_100}  |  No coverage: {covered_0}")

    # ── Phase 1: Lab-date audit ─────────────────────────────────────────────
    section("Phase 1: Lab-Date Accuracy Audit")
    lab_audit, overall_lab = audit_lab_dates(con)
    if lab_audit:
        print(f"  {'Lab Type':<30} {'Total':>8} {'Correct%':>10} {'Fallback%':>10}")
        print(f"  {'-'*30} {'-'*8} {'-'*10} {'-'*10}")
        for r in lab_audit:
            print(f"  {r['event_subtype']:<30} {r['total']:>8,} "
                  f"{r.get('correct_pct', 0):>9.1f}% {r.get('fallback_pct', 0):>9.1f}%")
        if overall_lab:
            print(f"\n  Overall: {overall_lab.get('correct_pct', 0):.1f}% correct, "
                  f"{overall_lab.get('fallback_pct', 0):.1f}% note-date fallback")

    # ── Phase 2: Create provenance_enriched_events_v1 ──────────────────────
    prov_enriched_n = 0
    if not args.dry_run:
        section("Phase 2: Create provenance_enriched_events_v1")
        prov_enriched_n = create_provenance_enriched(con)
    else:
        print("\n  [DRY RUN] Skipping provenance_enriched_events_v1 creation")

    # ── Phase 4b: Create lineage_audit_v1 ──────────────────────────────────
    lineage_n = 0
    if not args.dry_run:
        section("Phase 4b: Create lineage_audit_v1")
        lineage_n = create_lineage_audit(con)
    else:
        print("  [DRY RUN] Skipping lineage_audit_v1 creation")

    # ── Phase 4c: Insert QA issues ──────────────────────────────────────────
    qa_issues_inserted = 0
    if not args.dry_run:
        section("Phase 4c: Insert QA Issues")
        qa_issues_inserted = insert_qa_issues(con)
    else:
        print("  [DRY RUN] Skipping QA issue insertion")

    # ── Reports ─────────────────────────────────────────────────────────────
    section("Writing Reports")
    write_coverage_report(
        schema_audit, lab_audit, overall_lab, prov_enriched_n, lineage_n
    )
    write_date_verification_report(lab_audit, overall_lab, qa_issues_inserted, con)

    con.close()
    section("Done")
    print(f"  provenance_coverage_report.md -> docs/")
    print(f"  date_accuracy_verification_report_{TODAY}.md -> docs/")
    if not args.dry_run:
        print(f"  Tables created: provenance_enriched_events_v1, lineage_audit_v1")
        print(f"  QA issues inserted: {qa_issues_inserted:,}")


if __name__ == "__main__":
    main()
