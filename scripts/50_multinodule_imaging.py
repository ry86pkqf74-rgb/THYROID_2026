#!/usr/bin/env python3
"""
50_multinodule_imaging.py -- Multi-nodule thyroid ultrasound support

Ingests the wide-format per-nodule data from raw_us_tirads_excel_v1 and
raw_us_tirads_scored_v1 into a proper long-format structure, enabling
nodule-level linkage to FNA and pathology.

Tables created:
  imaging_nodule_master_v1    -- one row per nodule per exam (long-format)
  imaging_exam_master_v1      -- one row per imaging exam
  imaging_patient_summary_v1  -- one row per patient

Also creates nodule identity tracking across serial exams when possible.

Run after script 47 (reads raw_us_tirads_excel_v1 from MotherDuck or
builds from raw Excel files when available locally).
Supports --md, --local, --dry-run flags.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"
RAW = ROOT / "raw"

sys.path.insert(0, str(ROOT))

TODAY = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


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
    return duckdb.connect(str(DB_PATH))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# ACR TI-RADS calculator helper (SQL expression)
# Reference: Tessler FN et al., JACR 2017;14:1317-1322
# ---------------------------------------------------------------------------
def acr_tirads_sql(prefix: str = "") -> str:
    """Return a SQL CASE expression computing ACR TI-RADS score from columns.
    prefix: e.g. '' for direct columns, 't.' for aliased table
    """
    p = prefix
    return f"""
        COALESCE(
            -- Composition: anechoic=0, spongiform=0, mixed/cystic=1, solid=2
            CASE LOWER(TRIM(CAST({p}composition AS VARCHAR)))
                WHEN 'anechoic' THEN 0 WHEN 'spongiform' THEN 0
                WHEN 'mixed' THEN 1 WHEN 'cystic' THEN 1
                WHEN 'solid' THEN 2 ELSE 0 END, 0)
        + COALESCE(
            -- Echogenicity: anechoic=0, hyperechoic=1, isoechoic=1,
            --               hypoechoic=2, very_hypoechoic=3
            CASE LOWER(TRIM(CAST({p}echogenicity AS VARCHAR)))
                WHEN 'anechoic' THEN 0 WHEN 'hyperechoic' THEN 1
                WHEN 'isoechoic' THEN 1 WHEN 'hypoechoic' THEN 2
                WHEN 'very_hypoechoic' THEN 3 ELSE 0 END, 0)
        + COALESCE(
            -- Shape: wider_than_tall=0, taller_than_wide=3
            CASE LOWER(TRIM(CAST({p}shape AS VARCHAR)))
                WHEN 'wider than tall' THEN 0 WHEN 'wider_than_tall' THEN 0
                WHEN 'taller than wide' THEN 3 WHEN 'taller_than_wide' THEN 3
                ELSE 0 END, 0)
        + COALESCE(
            -- Margins: smooth=0, ill_defined=0, lobulated/irregular=2,
            --          extra_thyroidal=3
            CASE LOWER(TRIM(CAST({p}margins AS VARCHAR)))
                WHEN 'smooth' THEN 0 WHEN 'ill-defined' THEN 0
                WHEN 'ill_defined' THEN 0
                WHEN 'lobulated' THEN 2 WHEN 'irregular' THEN 2
                WHEN 'microlobulated' THEN 2
                WHEN 'extra-thyroidal extension' THEN 3
                WHEN 'extra_thyroidal' THEN 3 ELSE 0 END, 0)
        + COALESCE(
            -- Echogenic foci: none=0, comet_tail=0, macrocalcification=1,
            --                 peripheral=2, punctate=3
            CASE LOWER(TRIM(CAST({p}calcifications AS VARCHAR)))
                WHEN 'none' THEN 0 WHEN 'comet tail' THEN 0
                WHEN 'comet_tail' THEN 0
                WHEN 'macrocalcifications' THEN 1 WHEN 'peripheral' THEN 2
                WHEN 'microcalcifications' THEN 3 WHEN 'punctate' THEN 3
                ELSE 0 END, 0)
    """


def tirads_category_sql(score_expr: str) -> str:
    """Return CASE expression mapping numeric ACR score to TR category."""
    return f"""
        CASE
            WHEN {score_expr} = 0 THEN 'TR1'
            WHEN {score_expr} <= 2 THEN 'TR2'
            WHEN {score_expr} = 3 THEN 'TR3'
            WHEN {score_expr} <= 6 THEN 'TR4'
            ELSE 'TR5'
        END
    """


# ---------------------------------------------------------------------------
# Build imaging_nodule_master_v1 via Python unpivoting
# (DuckDB does not have a native UNPIVOT for dynamic column groups)
# ---------------------------------------------------------------------------

def _get_raw_tirads_cols(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return all column names from raw_us_tirads_excel_v1."""
    try:
        return [r[0] for r in con.execute(
            "SELECT DISTINCT column_name FROM information_schema.columns "
            "WHERE table_name='raw_us_tirads_excel_v1' AND table_schema='main' "
            "ORDER BY column_name"
        ).fetchall()]
    except Exception:
        return []


def _is_normalized_long_format(col_lower: dict[str, str]) -> bool:
    """Detect Phase 12 normalized schema (already one-row-per-nodule)."""
    required = {"nodule_number", "composition_norm", "tirads_reported",
                "nodule_length_mm"}
    return required.issubset(col_lower.keys())


def _build_from_normalized_long(col_lower: dict[str, str]) -> str:
    """Build imaging_nodule_master_v1 from the Phase 12 normalized schema."""

    def qcol(name: str) -> str:
        return f'"{col_lower[name]}"' if name in col_lower else "NULL"

    id_col = qcol("research_id")
    date_col = qcol("us_date")
    nnum = qcol("nodule_number")
    ti_rep = qcol("tirads_reported")
    ti_rec = qcol("tirads_recalculated")
    comp = qcol("composition_norm")
    echo = qcol("echogenicity_norm")
    shape = qcol("shape_norm")
    marg = qcol("margin_norm")
    calc = qcol("calcification_norm")
    l_mm = qcol("nodule_length_mm")
    w_mm = qcol("nodule_width_mm")
    h_mm = qcol("nodule_height_mm")
    vol_str = qcol("nodule_volume_str")
    loc = qcol("nodule_location")

    return f"""
CREATE OR REPLACE TABLE imaging_nodule_master_v1 AS
WITH src AS (
    SELECT
        CAST({id_col} AS INTEGER)       AS research_id,
        TRY_CAST({date_col} AS DATE)    AS exam_date,
        CAST({nnum} AS INTEGER)         AS nodule_number,
        MD5(CONCAT(
            CAST(COALESCE({id_col}, 0) AS VARCHAR), '_',
            CAST(COALESCE({date_col}, '1900-01-01') AS VARCHAR)
        ))                              AS exam_id,
        MD5(CONCAT(
            CAST(COALESCE({id_col}, 0) AS VARCHAR), '_',
            CAST(COALESCE({date_col}, '1900-01-01') AS VARCHAR), '_',
            CAST({nnum} AS VARCHAR)
        ))                              AS nodule_id,
        TRY_CAST({ti_rep} AS INTEGER)   AS tirads_reported,
        TRY_CAST({ti_rec} AS INTEGER)   AS tirads_acr_recalculated,
        CAST({comp} AS VARCHAR)         AS composition,
        CAST({echo} AS VARCHAR)         AS echogenicity,
        CAST({shape} AS VARCHAR)        AS shape,
        CAST({marg} AS VARCHAR)         AS margins,
        CAST({calc} AS VARCHAR)         AS calcifications,
        TRY_CAST({l_mm} AS DOUBLE)      AS length_mm,
        TRY_CAST({w_mm} AS DOUBLE)      AS width_mm,
        TRY_CAST({h_mm} AS DOUBLE)      AS height_mm,
        TRY_CAST(REGEXP_EXTRACT(CAST({vol_str} AS VARCHAR),
                 '([0-9]+\\.?[0-9]*)', 1) AS DOUBLE) AS volume_ml,
        CAST({loc} AS VARCHAR)          AS location_raw,
        CASE
            WHEN LOWER(CAST({loc} AS VARCHAR)) LIKE '%left%'    THEN 'left'
            WHEN LOWER(CAST({loc} AS VARCHAR)) LIKE '%right%'   THEN 'right'
            WHEN LOWER(CAST({loc} AS VARCHAR)) LIKE '%isthmus%' THEN 'isthmus'
            ELSE NULL
        END                             AS laterality,
        'raw_us_tirads_excel_v1'        AS source_table
    FROM raw_us_tirads_excel_v1
    WHERE {id_col} IS NOT NULL
)
SELECT
    s.*,
    CASE
        WHEN tirads_reported IS NULL
             OR tirads_acr_recalculated IS NULL THEN NULL
        WHEN ABS(tirads_reported - tirads_acr_recalculated) <= 1 THEN TRUE
        ELSE FALSE
    END AS tirads_concordant_flag,
    CASE
        WHEN length_mm IS NOT NULL AND width_mm IS NOT NULL
             AND height_mm IS NOT NULL
             THEN GREATEST(length_mm, width_mm, height_mm) / 10.0
        WHEN length_mm IS NOT NULL
             THEN length_mm / 10.0
        ELSE NULL
    END AS max_dimension_cm,
    CASE COALESCE(tirads_reported, tirads_acr_recalculated)
        WHEN 1 THEN 'TR1' WHEN 2 THEN 'TR2' WHEN 3 THEN 'TR3'
        WHEN 4 THEN 'TR4' WHEN 5 THEN 'TR5' WHEN 6 THEN 'TR5'
        ELSE NULL
    END AS tirads_category,
    COALESCE(tirads_reported, tirads_acr_recalculated) >= 4
        AS suspicious_flag
FROM src s
"""


def build_nodule_long_sql(con: duckdb.DuckDBPyConnection) -> str:
    """
    Dynamically detect Nodule N column groups in raw_us_tirads_excel_v1
    and build an unpivot UNION ALL SQL.

    Supports two schemas:
      1. Phase 12 normalized long-format (MotherDuck): composition_norm,
         tirads_reported, nodule_length_mm, nodule_number, etc.
      2. Raw wide-format from COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx:
         TI_RADS, Composition, Nodule 2 TI_RADS, etc.
    """
    all_cols = _get_raw_tirads_cols(con)
    if not all_cols:
        return ""

    col_lower = {c.lower(): c for c in all_cols}

    # Phase 12 normalized schema — already one-row-per-nodule
    if _is_normalized_long_format(col_lower):
        return _build_from_normalized_long(col_lower)

    # Legacy wide-format unpivot path
    def col(name: str, default: str = "NULL") -> str:
        """Return original col name if it exists, else NULL."""
        lname = name.lower()
        return f'"{col_lower[lname]}"' if lname in col_lower else default

    def nodule_col(n: int, field: str, default: str = "NULL") -> str:
        """Return column reference for nodule N feature."""
        if n == 1:
            return col(field, default)
        candidates = [
            f"nodule {n} {field}",
            f"nodule_{n}_{field}",
            f"nodule{n} {field}",
        ]
        for c in candidates:
            lc = c.lower()
            if lc in col_lower:
                return f'"{col_lower[lc]}"'
        return default

    max_nodule = 1
    for n in range(2, 15):
        for field in ["ti_rads", "composition", "length"]:
            lname = f"nodule {n} {field}".lower()
            if lname in col_lower:
                max_nodule = n
                break

    id_cols = col("research_id", "NULL::INTEGER")
    date_col = col("exam_date", col("us_date", col("date", "NULL::DATE")))

    parts: list[str] = []
    for n in range(1, max_nodule + 1):
        ti = nodule_col(n, "ti_rads")
        comp = nodule_col(n, "composition")
        echo = nodule_col(n, "echogenicity")
        shape = nodule_col(n, "shape")
        marg = nodule_col(n, "margins")
        calc = nodule_col(n, "calcifications")
        length_ = nodule_col(n, "length")
        width_ = nodule_col(n, "width")
        height_ = nodule_col(n, "height")
        vol_ = nodule_col(n, "volume")
        loc_ = nodule_col(n, "location")

        has_any = any(x != "NULL" for x in [ti, comp, echo, shape, marg, calc])
        if not has_any and n > 1:
            continue

        part = f"""
    SELECT
        CAST({id_cols} AS INTEGER)  AS research_id,
        TRY_CAST({date_col} AS DATE) AS exam_date,
        {n}                          AS nodule_number,
        MD5(CONCAT(
            CAST(COALESCE({id_cols}, 0) AS VARCHAR), '_',
            CAST(COALESCE({date_col}, '1900-01-01') AS VARCHAR)
        ))                           AS exam_id,
        MD5(CONCAT(
            CAST(COALESCE({id_cols}, 0) AS VARCHAR), '_',
            CAST(COALESCE({date_col}, '1900-01-01') AS VARCHAR), '_',
            '{n}'
        ))                           AS nodule_id,
        TRY_CAST({ti} AS INTEGER)    AS tirads_reported,
        (
            COALESCE(CASE LOWER(TRIM(CAST({comp} AS VARCHAR)))
                WHEN 'anechoic' THEN 0 WHEN 'spongiform' THEN 0
                WHEN 'mixed' THEN 1 WHEN 'cystic' THEN 1
                WHEN 'solid' THEN 2 ELSE 0 END, 0)
            + COALESCE(CASE LOWER(TRIM(CAST({echo} AS VARCHAR)))
                WHEN 'anechoic' THEN 0 WHEN 'hyperechoic' THEN 1
                WHEN 'isoechoic' THEN 1 WHEN 'hypoechoic' THEN 2
                WHEN 'very_hypoechoic' THEN 3 ELSE 0 END, 0)
            + COALESCE(CASE LOWER(TRIM(CAST({shape} AS VARCHAR)))
                WHEN 'wider than tall' THEN 0 WHEN 'wider_than_tall' THEN 0
                WHEN 'taller than wide' THEN 3 WHEN 'taller_than_wide' THEN 3
                ELSE 0 END, 0)
            + COALESCE(CASE LOWER(TRIM(CAST({marg} AS VARCHAR)))
                WHEN 'smooth' THEN 0 WHEN 'ill-defined' THEN 0
                WHEN 'ill_defined' THEN 0 WHEN 'lobulated' THEN 2
                WHEN 'irregular' THEN 2 WHEN 'microlobulated' THEN 2
                WHEN 'extra-thyroidal extension' THEN 3
                WHEN 'extra_thyroidal' THEN 3 ELSE 0 END, 0)
            + COALESCE(CASE LOWER(TRIM(CAST({calc} AS VARCHAR)))
                WHEN 'none' THEN 0 WHEN 'comet tail' THEN 0
                WHEN 'comet_tail' THEN 0 WHEN 'macrocalcifications' THEN 1
                WHEN 'peripheral' THEN 2 WHEN 'microcalcifications' THEN 3
                WHEN 'punctate' THEN 3 ELSE 0 END, 0)
        )                            AS tirads_acr_recalculated,
        CAST({comp} AS VARCHAR)      AS composition,
        CAST({echo} AS VARCHAR)      AS echogenicity,
        CAST({shape} AS VARCHAR)     AS shape,
        CAST({marg} AS VARCHAR)      AS margins,
        CAST({calc} AS VARCHAR)      AS calcifications,
        TRY_CAST({length_} AS DOUBLE) AS length_mm,
        TRY_CAST({width_} AS DOUBLE)  AS width_mm,
        TRY_CAST({height_} AS DOUBLE) AS height_mm,
        TRY_CAST({vol_} AS DOUBLE)    AS volume_ml,
        CAST({loc_} AS VARCHAR)       AS location_raw,
        CASE
            WHEN LOWER(CAST({loc_} AS VARCHAR)) LIKE '%left%'  THEN 'left'
            WHEN LOWER(CAST({loc_} AS VARCHAR)) LIKE '%right%' THEN 'right'
            WHEN LOWER(CAST({loc_} AS VARCHAR)) LIKE '%isthmus%' THEN 'isthmus'
            ELSE NULL
        END                          AS laterality,
        'raw_us_tirads_excel_v1'     AS source_table
    FROM raw_us_tirads_excel_v1
    WHERE {id_cols} IS NOT NULL
      AND ({ti} IS NOT NULL
           OR {comp} IS NOT NULL
           OR {length_} IS NOT NULL)
"""
        parts.append(part)

    if not parts:
        return ""

    union_sql = "\nUNION ALL\n".join(parts)
    return f"""
CREATE OR REPLACE TABLE imaging_nodule_master_v1 AS
WITH raw_unpivoted AS (
{union_sql}
),
with_concordance AS (
    SELECT *,
        CASE
            WHEN tirads_reported IS NULL
                 OR tirads_acr_recalculated IS NULL THEN NULL
            WHEN ABS(tirads_reported - tirads_acr_recalculated) <= 1 THEN TRUE
            ELSE FALSE
        END AS tirads_concordant_flag,
        CASE
            WHEN length_mm IS NOT NULL AND width_mm IS NOT NULL
                 AND height_mm IS NOT NULL
                 THEN GREATEST(length_mm, width_mm, height_mm) / 10.0
            WHEN length_mm IS NOT NULL
                 THEN length_mm / 10.0
            ELSE NULL
        END AS max_dimension_cm,
        CASE COALESCE(tirads_reported, tirads_acr_recalculated)
            WHEN 1 THEN 'TR1' WHEN 2 THEN 'TR2' WHEN 3 THEN 'TR3'
            WHEN 4 THEN 'TR4' WHEN 5 THEN 'TR5' WHEN 6 THEN 'TR5'
            ELSE NULL
        END AS tirads_category,
        COALESCE(tirads_reported, tirads_acr_recalculated) >= 4
            AS suspicious_flag
    FROM raw_unpivoted
)
SELECT * FROM with_concordance
"""


# ---------------------------------------------------------------------------
# imaging_exam_master_v1 -- one row per exam
# ---------------------------------------------------------------------------
IMAGING_EXAM_MASTER_SQL = """
CREATE OR REPLACE TABLE imaging_exam_master_v1 AS
SELECT
    research_id,
    exam_date,
    exam_id,
    COUNT(*)                        AS n_nodules,
    MAX(COALESCE(tirads_reported, tirads_acr_recalculated))
                                    AS max_tirads,
    CASE WHEN MAX(COALESCE(tirads_reported, tirads_acr_recalculated)) >= 4
         THEN TRUE ELSE FALSE END    AS has_suspicious_nodule,
    -- Bilateral: both left and right nodules present
    BOOL_OR(laterality = 'left')
        AND BOOL_OR(laterality = 'right') AS bilateral_flag,
    -- Largest nodule
    MAX(max_dimension_cm)           AS largest_nodule_cm,
    -- Dominant nodule id (highest TIRADS, then largest)
    FIRST(nodule_id ORDER BY
          COALESCE(tirads_reported, tirads_acr_recalculated) DESC NULLS LAST,
          max_dimension_cm DESC NULLS LAST)
                                    AS dominant_nodule_id,
    'raw_us_tirads_excel_v1'        AS source
FROM imaging_nodule_master_v1
GROUP BY research_id, exam_date, exam_id
"""

# ---------------------------------------------------------------------------
# imaging_patient_summary_v1 -- one row per patient
# ---------------------------------------------------------------------------
IMAGING_PATIENT_SUMMARY_SQL = """
CREATE OR REPLACE TABLE imaging_patient_summary_v1 AS
WITH multi_exam AS (
    SELECT
        research_id,
        COUNT(DISTINCT exam_id)             AS n_exams,
        SUM(n_nodules)                      AS n_total_nodules,
        MAX(max_tirads)                     AS max_tirads_ever,
        BOOL_OR(bilateral_flag)             AS bilateral_disease_flag,
        MAX(largest_nodule_cm)              AS dominant_nodule_size_cm,
        -- Multi-focal: >1 nodule ever seen
        BOOL_OR(n_nodules > 1)              AS multifocal_flag,
        -- Any biopsied candidate: TR4 or TR5 seen
        BOOL_OR(has_suspicious_nodule)      AS has_suspicious_candidate,
        MIN(exam_date)                      AS first_exam_date,
        MAX(exam_date)                      AS last_exam_date
    FROM imaging_exam_master_v1
    GROUP BY research_id
)
SELECT
    m.*,
    CASE m.max_tirads_ever
        WHEN 1 THEN 'TR1' WHEN 2 THEN 'TR2' WHEN 3 THEN 'TR3'
        WHEN 4 THEN 'TR4' WHEN 5 THEN 'TR5' WHEN 6 THEN 'TR5'
        ELSE NULL
    END AS worst_tirads_category,
    -- Longitudinal size change flag: only calculable if >1 exam
    (n_exams > 1) AS longitudinal_assessment_available,
    CURRENT_TIMESTAMP AS created_at
FROM multi_exam m
"""


# ---------------------------------------------------------------------------
# Fallback: also ingest raw_us_tirads_scored_v1 (14-sheet scored workbook)
# ---------------------------------------------------------------------------
SCORED_SUPPLEMENT_SQL = """
-- Supplement imaging_patient_summary_v1 with patients from scored workbook
-- who are NOT already in imaging_nodule_master_v1
CREATE OR REPLACE TABLE imaging_patient_summary_v1 AS
SELECT * FROM imaging_patient_summary_v1

UNION ALL

SELECT
    research_id,
    1                               AS n_exams,
    1                               AS n_total_nodules,
    tirads_best_score               AS max_tirads_ever,
    FALSE                           AS bilateral_disease_flag,
    NULL::DOUBLE                    AS dominant_nodule_size_cm,
    FALSE                           AS multifocal_flag,
    (tirads_best_score >= 4)        AS has_suspicious_candidate,
    NULL::DATE                      AS first_exam_date,
    NULL::DATE                      AS last_exam_date,
    CASE tirads_best_score
        WHEN 1 THEN 'TR1' WHEN 2 THEN 'TR2' WHEN 3 THEN 'TR3'
        WHEN 4 THEN 'TR4' WHEN 5 THEN 'TR5' WHEN 6 THEN 'TR5'
        ELSE NULL
    END AS worst_tirads_category,
    FALSE                           AS longitudinal_assessment_available,
    CURRENT_TIMESTAMP               AS created_at
FROM extracted_tirads_validated_v1
WHERE research_id NOT IN (SELECT DISTINCT research_id FROM imaging_patient_summary_v1)
"""


def build_multinodule_tables(con: duckdb.DuckDBPyConnection,
                             dry_run: bool = False) -> None:
    section("Building multi-nodule imaging tables")

    has_excel = table_available(con, "raw_us_tirads_excel_v1")
    has_scored = table_available(con, "extracted_tirads_validated_v1")
    has_us_tirads = table_available(con, "us_nodules_tirads")

    print(f"  raw_us_tirads_excel_v1:       {'present' if has_excel else 'missing'}")
    print(f"  extracted_tirads_validated_v1: {'present' if has_scored else 'missing'}")
    print(f"  us_nodules_tirads:             {'present' if has_us_tirads else 'missing'}")

    if dry_run:
        print("  [DRY-RUN] Would create imaging_nodule_master_v1, "
              "imaging_exam_master_v1, imaging_patient_summary_v1")
        return

    if has_excel:
        print("\n  Building imaging_nodule_master_v1 from raw Excel source...")
        nodule_sql = build_nodule_long_sql(con)
        if nodule_sql:
            con.execute(nodule_sql)
            r = con.execute(
                "SELECT COUNT(*), COUNT(DISTINCT research_id) "
                "FROM imaging_nodule_master_v1"
            ).fetchone()
            print(f"    imaging_nodule_master_v1: {r[0]:,} nodule rows, "
                  f"{r[1]:,} patients")

            print("  Building imaging_exam_master_v1...")
            con.execute(IMAGING_EXAM_MASTER_SQL)
            r = con.execute(
                "SELECT COUNT(*), COUNT(DISTINCT research_id) "
                "FROM imaging_exam_master_v1"
            ).fetchone()
            print(f"    imaging_exam_master_v1: {r[0]:,} exams, "
                  f"{r[1]:,} patients")

            print("  Building imaging_patient_summary_v1...")
            con.execute(IMAGING_PATIENT_SUMMARY_SQL)
            r = con.execute(
                "SELECT COUNT(*), SUM(n_total_nodules), AVG(n_total_nodules) "
                "FROM imaging_patient_summary_v1"
            ).fetchone()
            print(f"    imaging_patient_summary_v1: {r[0]:,} patients, "
                  f"{int(r[1]) if r[1] else 0:,} total nodules, "
                  f"{(float(r[2]) if r[2] is not None else 0.0):.1f} avg nodules/patient")

            # Supplement with scored workbook patients if available
            if has_scored:
                print("  Supplementing with scored workbook patients...")
                con.execute(SCORED_SUPPLEMENT_SQL)
                r = con.execute(
                    "SELECT COUNT(*) FROM imaging_patient_summary_v1"
                ).fetchone()
                print(f"    After supplement: {r[0]:,} patients")
        else:
            print("  [WARN] Could not detect nodule column groups in "
                  "raw_us_tirads_excel_v1 -- building from scored workbook only")
            has_excel = False

    if not has_excel and has_scored:
        # Build patient summary directly from validated TIRADS
        print("\n  Building imaging_patient_summary_v1 from validated TIRADS...")
        con.execute("""
CREATE OR REPLACE TABLE imaging_patient_summary_v1 AS
SELECT
    research_id,
    1                               AS n_exams,
    1                               AS n_total_nodules,
    tirads_best_score               AS max_tirads_ever,
    FALSE                           AS bilateral_disease_flag,
    nodule_size_max_mm / 10.0       AS dominant_nodule_size_cm,
    (n_nodule_records > 1)          AS multifocal_flag,
    (tirads_best_score >= 4)        AS has_suspicious_candidate,
    NULL::DATE                      AS first_exam_date,
    NULL::DATE                      AS last_exam_date,
    tirads_best_category            AS worst_tirads_category,
    FALSE                           AS longitudinal_assessment_available,
    CURRENT_TIMESTAMP               AS created_at
FROM extracted_tirads_validated_v1
""")
        r = con.execute("SELECT COUNT(*) FROM imaging_patient_summary_v1").fetchone()
        print(f"    imaging_patient_summary_v1 (TIRADS-only): {r[0]:,} patients")

        # Create minimal nodule master stub
        if not table_available(con, "imaging_nodule_master_v1"):
            con.execute("""
CREATE OR REPLACE TABLE imaging_nodule_master_v1 AS
SELECT
    research_id,
    NULL::DATE      AS exam_date,
    MD5(CAST(research_id AS VARCHAR)) AS exam_id,
    1               AS nodule_number,
    MD5(CONCAT(CAST(research_id AS VARCHAR), '_1')) AS nodule_id,
    tirads_best_score               AS tirads_reported,
    tirads_best_score               AS tirads_acr_recalculated,
    NULL::VARCHAR   AS composition,
    NULL::VARCHAR   AS echogenicity,
    NULL::VARCHAR   AS shape,
    NULL::VARCHAR   AS margins,
    NULL::VARCHAR   AS calcifications,
    nodule_size_max_mm              AS length_mm,
    NULL::DOUBLE    AS width_mm,
    NULL::DOUBLE    AS height_mm,
    NULL::DOUBLE    AS volume_ml,
    NULL::VARCHAR   AS location_raw,
    NULL::VARCHAR   AS laterality,
    TRUE            AS tirads_concordant_flag,
    nodule_size_max_mm / 10.0       AS max_dimension_cm,
    tirads_best_category            AS tirads_category,
    (tirads_best_score >= 4)        AS suspicious_flag,
    'extracted_tirads_validated_v1' AS source_table
FROM extracted_tirads_validated_v1
""")

    if not has_excel and not has_scored:
        print("  [WARN] No imaging source tables found. Creating empty stubs.")
        con.execute("""
CREATE OR REPLACE TABLE imaging_nodule_master_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::DATE AS exam_date,
       NULL::VARCHAR AS exam_id, NULL::INTEGER AS nodule_number,
       NULL::VARCHAR AS nodule_id, NULL::INTEGER AS tirads_reported,
       NULL::INTEGER AS tirads_acr_recalculated,
       NULL::VARCHAR AS composition, NULL::VARCHAR AS echogenicity,
       NULL::VARCHAR AS shape, NULL::VARCHAR AS margins,
       NULL::VARCHAR AS calcifications,
       NULL::DOUBLE AS length_mm, NULL::DOUBLE AS width_mm,
       NULL::DOUBLE AS height_mm, NULL::DOUBLE AS volume_ml,
       NULL::VARCHAR AS location_raw, NULL::VARCHAR AS laterality,
       NULL::BOOLEAN AS tirads_concordant_flag, NULL::DOUBLE AS max_dimension_cm,
       NULL::VARCHAR AS tirads_category, NULL::BOOLEAN AS suspicious_flag,
       NULL::VARCHAR AS source_table
WHERE 1=0
""")
        con.execute("""
CREATE OR REPLACE TABLE imaging_exam_master_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::DATE AS exam_date,
       NULL::VARCHAR AS exam_id, NULL::INTEGER AS n_nodules,
       NULL::INTEGER AS max_tirads, NULL::BOOLEAN AS has_suspicious_nodule,
       NULL::BOOLEAN AS bilateral_flag, NULL::DOUBLE AS largest_nodule_cm,
       NULL::VARCHAR AS dominant_nodule_id, NULL::VARCHAR AS source
WHERE 1=0
""")
        con.execute("""
CREATE OR REPLACE TABLE imaging_patient_summary_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::INTEGER AS n_exams,
       NULL::INTEGER AS n_total_nodules, NULL::INTEGER AS max_tirads_ever,
       NULL::BOOLEAN AS bilateral_disease_flag,
       NULL::DOUBLE AS dominant_nodule_size_cm,
       NULL::BOOLEAN AS multifocal_flag,
       NULL::BOOLEAN AS has_suspicious_candidate,
       NULL::DATE AS first_exam_date, NULL::DATE AS last_exam_date,
       NULL::VARCHAR AS worst_tirads_category,
       NULL::BOOLEAN AS longitudinal_assessment_available,
       NULL::TIMESTAMP AS created_at
WHERE 1=0
""")

    # Print summary statistics
    if table_available(con, "imaging_patient_summary_v1"):
        summary = con.execute("""
            SELECT
                COUNT(*) AS n_patients,
                SUM(CASE WHEN max_tirads_ever >= 4 THEN 1 ELSE 0 END) AS n_tr4_or_5,
                SUM(CASE WHEN bilateral_disease_flag THEN 1 ELSE 0 END) AS n_bilateral,
                SUM(CASE WHEN multifocal_flag THEN 1 ELSE 0 END) AS n_multifocal,
                ROUND(AVG(n_total_nodules), 2) AS avg_nodules
            FROM imaging_patient_summary_v1
        """).fetchdf()
        print("\n  Summary:")
        print(summary.to_string(index=False))

    print("\n  [DONE] Multi-nodule imaging tables created")


def main() -> None:
    p = argparse.ArgumentParser(description="50_multinodule_imaging.py")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--dry-run", action="store_true", help="Audit only, no writes")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        build_multinodule_tables(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 50_multinodule_imaging.py finished")


if __name__ == "__main__":
    main()
