#!/usr/bin/env python3
"""
11.5_cross_file_validation.py — Cross-File Logical Consistency QA

Runs between scripts 11 and 12 while MotherDuck trial compute is active.
Complements single-file QA (script 11) with cross-table consistency checks.

Phases:
  1. Connect + MotherDuck optimization review (varchar check, excel ext)
  2. Execute 3 cross-file consistency checks → materialized tables
  3. Insert flagged issues into qa_issues
  4. Update qa_summary_report.md, export Parquet
  5. Summary table + "Ready for script 12" message

Schema adaptations from ideal (see user spec):
  - research_id used consistently (not patient_id)
  - us_nodules_tirads lacks side/nodule_id columns → laterality check uses
    operative_details vs path_synoptics procedure laterality instead
  - fna_history: bethesda (not fna_bethesda), fna_date_parsed (not fna_date)
  - patient_level_summary_mv: age_at_surgery/sex (not age/gender);
    race sourced from path_synoptics via LEFT JOIN

Usage:
  python scripts/11.5_cross_file_validation.py
  python scripts/11.5_cross_file_validation.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("xfile_validation")

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports"
QA_DIR = ROOT / "studies" / "qa_crosscheck"
MD_DATABASE = "thyroid_research_2026"

PHASE_TIMES: dict[str, float] = {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MotherDuck Optimization Notes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Future ingestion improvement — replace read_xlsx (all_varchar=True):
#   INSTALL excel; LOAD excel;
#   CREATE TABLE tbl AS SELECT * FROM read_excel('file.xlsx', sheet='Sheet1');
#   Preserves native types and avoids downstream TRY_CAST overhead.
#
# Compute Tier Switching (MotherDuck Business/Enterprise):
#   SET motherduck_default_server_instance_type = 'jumbo';
#   -- or 'mega' for heaviest workloads
#   -- Check current tier:
#   SELECT current_setting('motherduck_default_server_instance_type');
#
# Read-replica awareness:
#   For dashboard queries on read-replica, append ?access_mode=read_only
#   to the connection string. Writes go to the primary instance only.
#
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Check A — Nodule/Operative Laterality Consistency
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# us_nodules_tirads is wide-format (us_1_date..us_14_date, nodule_1..n14)
# and lacks a single 'side' column. Laterality is therefore checked between
# operative_details.side_of_largest_tumor_or_goiter and path_synoptics
# thyroid_procedure (from which laterality is inferred).

LATERALITY_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE qa_laterality_mismatches AS
    WITH operative_side AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            TRY_CAST(surg_date AS DATE) AS surgery_date,
            LOWER(TRIM(CAST(side_of_largest_tumor_or_goiter AS VARCHAR)))
                AS operative_side
        FROM operative_details
        WHERE side_of_largest_tumor_or_goiter IS NOT NULL
          AND TRIM(CAST(side_of_largest_tumor_or_goiter AS VARCHAR)) != ''
    ),
    path_side_derived AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            TRY_CAST(surg_date AS DATE) AS surgery_date,
            CAST(thyroid_procedure AS VARCHAR) AS path_procedure,
            CASE
                WHEN LOWER(CAST(thyroid_procedure AS VARCHAR))
                     LIKE '%total thyroidectomy%'
                     OR LOWER(CAST(thyroid_procedure AS VARCHAR))
                     LIKE '%bilateral%'
                THEN 'bilateral'
                WHEN LOWER(CAST(thyroid_procedure AS VARCHAR))
                     LIKE '%right%lobect%'
                     AND LOWER(CAST(thyroid_procedure AS VARCHAR))
                     NOT LIKE '%left%'
                THEN 'right'
                WHEN LOWER(CAST(thyroid_procedure AS VARCHAR))
                     LIKE '%left%lobect%'
                     AND LOWER(CAST(thyroid_procedure AS VARCHAR))
                     NOT LIKE '%right%'
                THEN 'left'
                WHEN LOWER(CAST(thyroid_procedure AS VARCHAR))
                     LIKE '%isthmusect%'
                THEN 'isthmus'
                ELSE NULL
            END AS path_side
        FROM path_synoptics
        WHERE thyroid_procedure IS NOT NULL
    )
    SELECT
        o.research_id,
        o.operative_side,
        ps.path_procedure,
        ps.path_side,
        o.surgery_date,
        mt.surgery_number,
        CASE
            WHEN ps.path_side IS NULL THEN 'INCOMPLETE'
            WHEN o.operative_side IN ('bilateral', 'both', 'b', 'total')
                 OR ps.path_side = 'bilateral'
            THEN 'MATCH'
            WHEN o.operative_side = ps.path_side
            THEN 'MATCH'
            ELSE 'LATERALITY_MISMATCH'
        END AS laterality_flag
    FROM operative_side o
    LEFT JOIN path_side_derived ps
        ON o.research_id = ps.research_id
        AND (o.surgery_date = ps.surgery_date
             OR ps.surgery_date IS NULL)
    LEFT JOIN master_timeline mt
        ON o.research_id = mt.research_id
        AND mt.surgery_date = o.surgery_date
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Check B — Report Matching (FNA ↔ Pathology + US ↔ Operative)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Column adaptations:
#   fna_bethesda   → bethesda
#   fna_date       → fna_date_parsed
#   path_diagnosis → path_diagnosis_summary
#   surgery_date   → surg_date  (in path_synoptics and operative_details)
#   u.size_mm      → dominant_nodule_size_on_us
#   o.size_mm      → maximum_diameter_of_largest_tumor_or_goiter_from_operative_sheet

REPORT_MATCHING_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE qa_report_matching AS
    SELECT
        COUNT(*) AS total_pairs,
        SUM(CASE WHEN f.bethesda IS NOT NULL
                  AND p.path_diagnosis_summary IS NOT NULL
             THEN 1 ELSE 0 END) AS matched,
        ROUND(100.0 * SUM(CASE
            WHEN f.bethesda IS NOT NULL
                 AND p.path_diagnosis_summary IS NOT NULL
            THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS match_pct,
        'fna_path' AS check_type
    FROM fna_history f
    JOIN path_synoptics p
        ON CAST(f.research_id AS INT) = CAST(p.research_id AS INT)
        AND ABS(DATEDIFF('day',
            TRY_CAST(f.fna_date_parsed AS DATE),
            TRY_CAST(p.surg_date AS DATE)
        )) <= 365

    UNION ALL

    SELECT
        COUNT(*) AS total_pairs,
        SUM(CASE
            WHEN TRY_CAST(u.dominant_nodule_size_on_us AS DOUBLE) IS NOT NULL
                 AND TRY_CAST(
                     o.maximum_diameter_of_largest_tumor_or_goiter_from_operative_sheet
                     AS DOUBLE) IS NOT NULL
            THEN 1 ELSE 0 END) AS matched,
        ROUND(100.0 * SUM(CASE
            WHEN TRY_CAST(u.dominant_nodule_size_on_us AS DOUBLE) IS NOT NULL
                 AND TRY_CAST(
                     o.maximum_diameter_of_largest_tumor_or_goiter_from_operative_sheet
                     AS DOUBLE) IS NOT NULL
            THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS match_pct,
        'us_operative' AS check_type
    FROM serial_imaging_us u
    JOIN operative_details o
        ON CAST(u.research_id AS INT) = CAST(o.research_id AS INT)
        AND ABS(DATEDIFF('day',
            TRY_CAST(u.us_date AS DATE),
            TRY_CAST(o.surg_date AS DATE)
        )) <= 180
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SQL: Check C — Missing Demographics Imputation Candidates
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# patient_level_summary_mv has age_at_surgery and sex but not race.
# Race is sourced from path_synoptics (which stores it from synoptic
# reports). Gender mapped to sex for consistency.

DEMOGRAPHICS_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE qa_missing_demographics AS
    SELECT
        CAST(p.research_id AS INT) AS research_id,
        p.age_at_surgery,
        p.sex,
        ps_race.race,
        CASE WHEN p.age_at_surgery IS NULL
             THEN 'MISSING_AGE' ELSE 'OK' END AS age_flag,
        CASE WHEN p.sex IS NULL
             THEN 'MISSING_SEX' ELSE 'OK' END AS sex_flag,
        CASE WHEN ps_race.race IS NULL
             THEN 'MISSING_RACE' ELSE 'OK' END AS race_flag,
        'patient_summary_plus_path_synoptics' AS source_priority
    FROM patient_level_summary_mv p
    LEFT JOIN (
        SELECT
            CAST(research_id AS INT) AS research_id,
            MAX(CAST(race AS VARCHAR)) AS race
        FROM path_synoptics
        WHERE race IS NOT NULL
          AND TRIM(CAST(race AS VARCHAR)) != ''
        GROUP BY CAST(research_id AS INT)
    ) ps_race ON CAST(p.research_id AS INT) = ps_race.research_id
    WHERE p.age_at_surgery IS NULL
       OR p.sex IS NULL
       OR ps_race.race IS NULL
""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helpers (matching script 11 patterns)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_connection():
    import duckdb
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN not set. Export it before running this script."
        )
    return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")


def _table_exists(con, name: str) -> bool:
    n = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{name}'"
    ).fetchone()[0]
    return n > 0


def _safe_count(con, name: str) -> int | None:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        return None


def _safe_distinct(con, name: str, col: str = "research_id") -> int | None:
    try:
        return con.execute(
            f"SELECT COUNT(DISTINCT CAST({col} AS VARCHAR)) FROM {name}"
        ).fetchone()[0]
    except Exception:
        return None


def _timed_execute(con, sql: str, label: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql)
    elapsed = time.perf_counter() - t0
    log.info(f"  {label}: {elapsed:.2f}s")
    return elapsed


def _fmt_size(n_bytes: int | None) -> str:
    if n_bytes is None:
        return "N/A"
    if n_bytes > 1_048_576:
        return f"{n_bytes / 1_048_576:.1f} MB"
    if n_bytes > 1024:
        return f"{n_bytes / 1024:.1f} KB"
    return f"{n_bytes} B"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 1: Connect + Optimization Review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase1_connect_optimize(con) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 1: Connection & Optimization Review")
    log.info("=" * 72)
    report: list[str] = []

    db_name = con.execute("SELECT current_database()").fetchone()[0]
    version = con.execute("SELECT version()").fetchone()[0]
    log.info(f"  Database: {db_name}")
    log.info(f"  DuckDB version: {version}")
    report.append(f"- Database: `{db_name}`")
    report.append(f"- DuckDB version: {version}")

    # Check raw tables for all_varchar ingestion status
    log.info("\n  Checking raw table types (all_varchar ingestion check):")
    raw_tables = [
        "raw_complications", "raw_molecular_testing", "raw_operative_details",
        "raw_fna_history", "raw_us_nodules_tirads", "raw_serial_imaging",
        "raw_path_synoptics", "raw_clinical_notes",
    ]
    varchar_count = 0
    checked = 0
    for tbl in raw_tables:
        if not _table_exists(con, tbl):
            continue
        checked += 1
        try:
            cols = con.execute(f"DESCRIBE {tbl}").fetchall()
            all_varchar = all(r[1] == "VARCHAR" for r in cols)
            if all_varchar:
                varchar_count += 1
                log.info(f"    {tbl}: all VARCHAR ← read_xlsx(all_varchar=True)")
        except Exception:
            pass

    if varchar_count > 0:
        log.info(f"\n  {varchar_count}/{checked} raw tables still use "
                 "all_varchar ingestion.")
        log.info("  Recommendation: future ingestion can use DuckDB excel "
                 "extension:")
        log.info("    INSTALL excel; LOAD excel;")
        log.info("    CREATE TABLE t AS SELECT * FROM "
                 "read_excel('file.xlsx');")
        report.append(f"- {varchar_count}/{checked} raw tables still "
                      "all_varchar — recommend `excel` extension")
    elif checked > 0:
        report.append("- Raw tables: no all_varchar issues found")
    else:
        report.append("- No raw tables found in database")

    # Excel extension availability check
    try:
        con.execute("INSTALL excel")
        con.execute("LOAD excel")
        log.info("  DuckDB excel extension: installed and loaded")
        report.append("- DuckDB excel extension: available")
    except Exception:
        log.info("  DuckDB excel extension: not available on this instance")
        report.append("- DuckDB excel extension: not available")

    # Compute tier advisory
    log.info("\n  Compute tier advisory:")
    log.info("    Business trial active — using trial compute for "
             "cross-file joins.")
    log.info("    For heavier workloads, switch via MotherDuck UI or:")
    log.info("      SET motherduck_default_server_instance_type = 'jumbo';")
    report.append("- Advisory: Business trial active — cross-file joins "
                  "enabled")

    PHASE_TIMES["phase1"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 2: Cross-File Consistency Checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CHECKS: list[tuple[str, str, str]] = [
    ("qa_laterality_mismatches",
     "Check A: Laterality Consistency (Op vs Path)",
     LATERALITY_SQL),
    ("qa_report_matching",
     "Check B: Report Matching (FNA-Path + US-Op)",
     REPORT_MATCHING_SQL),
    ("qa_missing_demographics",
     "Check C: Missing Demographics",
     DEMOGRAPHICS_SQL),
]


def phase2_cross_file_checks(
    con, dry_run: bool,
) -> tuple[list[str], list[dict]]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 2: Cross-File Consistency Checks")
    log.info("=" * 72)
    report: list[str] = []
    results: list[dict] = []

    if dry_run:
        for table_name, label, sql in CHECKS:
            log.info(f"\n  [DRY RUN] {label}")
            log.info(f"    Would create: {table_name}")
        report.append("- DRY RUN: skipped all cross-file checks")
        return report, results

    for table_name, label, sql in CHECKS:
        log.info(f"\n  {label}")
        try:
            elapsed = _timed_execute(con, sql, f"{table_name} creation")
            row_count = _safe_count(con, table_name) or 0
            patient_count = _safe_distinct(con, table_name) or 0

            info: dict = {
                "table": table_name,
                "label": label,
                "rows": row_count,
                "patients": patient_count,
                "elapsed": elapsed,
                "status": "OK",
            }

            if table_name == "qa_laterality_mismatches":
                try:
                    mismatch_n = con.execute(
                        "SELECT COUNT(*) FROM qa_laterality_mismatches "
                        "WHERE laterality_flag = 'LATERALITY_MISMATCH'"
                    ).fetchone()[0]
                    incomplete_n = con.execute(
                        "SELECT COUNT(*) FROM qa_laterality_mismatches "
                        "WHERE laterality_flag = 'INCOMPLETE'"
                    ).fetchone()[0]
                    match_n = row_count - mismatch_n - incomplete_n
                    log.info(f"    Match: {match_n}, Mismatch: {mismatch_n}, "
                             f"Incomplete: {incomplete_n}")
                    info["detail"] = (
                        f"match={match_n}, mismatch={mismatch_n}, "
                        f"incomplete={incomplete_n}"
                    )
                    info["mismatches"] = mismatch_n
                except Exception:
                    pass

            elif table_name == "qa_report_matching":
                try:
                    rows = con.execute(
                        "SELECT check_type, total_pairs, matched, match_pct "
                        "FROM qa_report_matching"
                    ).fetchall()
                    for ct, tp, m, pct in rows:
                        log.info(f"    {ct}: {m}/{tp} matched ({pct}%)")
                    info["detail"] = "; ".join(
                        f"{ct}={m}/{tp} ({pct}%)" for ct, tp, m, pct in rows
                    )
                except Exception:
                    pass

            elif table_name == "qa_missing_demographics":
                try:
                    for flag_col, flag_val in [
                        ("age_flag", "MISSING_AGE"),
                        ("sex_flag", "MISSING_SEX"),
                        ("race_flag", "MISSING_RACE"),
                    ]:
                        n = con.execute(
                            f"SELECT COUNT(*) FROM qa_missing_demographics "
                            f"WHERE {flag_col} = '{flag_val}'"
                        ).fetchone()[0]
                        log.info(f"    {flag_val}: {n:,}")
                except Exception:
                    pass

            log.info(f"    Total: {row_count:,} rows, "
                     f"{patient_count:,} patients")
            report.append(
                f"- **{table_name}**: {row_count:,} rows, "
                f"{patient_count:,} patients ({elapsed:.1f}s)"
            )
            results.append(info)

        except Exception as exc:
            log.error(f"    CHECK FAILED: {exc}")
            report.append(f"- **{table_name}**: FAILED — {exc}")
            results.append({
                "table": table_name,
                "label": label,
                "rows": 0,
                "patients": 0,
                "elapsed": 0,
                "status": f"ERROR: {exc}",
            })

    PHASE_TIMES["phase2"] = time.perf_counter() - t0
    return report, results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 3: Insert cross-file issues into qa_issues
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QA_INSERTS: list[tuple[str, str, str]] = [
    (
        "xfile_laterality",
        "warning",
        textwrap.dedent("""\
            INSERT INTO qa_issues
                (check_id, severity, research_id, description, detail)
            SELECT
                'xfile_laterality',
                'warning',
                research_id,
                'Cross-file laterality mismatch (operative vs pathology)',
                'op_side=' || COALESCE(operative_side, '?')
                    || ' path_side=' || COALESCE(path_side, '?')
                    || ' procedure='
                    || COALESCE(SUBSTRING(path_procedure, 1, 80), '?')
                    || ' surg_date='
                    || COALESCE(CAST(surgery_date AS VARCHAR), '?')
            FROM qa_laterality_mismatches
            WHERE laterality_flag = 'LATERALITY_MISMATCH'
        """),
    ),
    (
        "xfile_demographics",
        "info",
        textwrap.dedent("""\
            INSERT INTO qa_issues
                (check_id, severity, research_id, description, detail)
            SELECT
                'xfile_demographics',
                'info',
                research_id,
                'Missing demographic field(s)',
                CASE
                    WHEN age_flag = 'MISSING_AGE' AND sex_flag = 'MISSING_SEX'
                    THEN 'missing: age + sex'
                    WHEN age_flag = 'MISSING_AGE' THEN 'missing: age'
                    WHEN sex_flag = 'MISSING_SEX' THEN 'missing: sex'
                    ELSE 'missing: race only'
                END
                || CASE
                    WHEN race_flag = 'MISSING_RACE' THEN ' + race'
                    ELSE ''
                END
            FROM qa_missing_demographics
            WHERE age_flag != 'OK' OR sex_flag != 'OK'
        """),
    ),
]


def phase3_qa_inserts(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 3: Insert Cross-File Issues into qa_issues")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        log.info("  [DRY RUN] Would insert cross-file issues into qa_issues")
        report.append("- DRY RUN: skipped qa_issues inserts")
        return report

    if not _table_exists(con, "qa_issues"):
        log.warning("  qa_issues table not found — creating it")
        con.execute("""\
            CREATE TABLE qa_issues (
                check_id    VARCHAR,
                severity    VARCHAR,
                research_id INT,
                description VARCHAR,
                detail      VARCHAR,
                checked_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    # Idempotency: remove prior xfile_ rows before re-inserting
    try:
        prior = con.execute(
            "SELECT COUNT(*) FROM qa_issues WHERE check_id LIKE 'xfile_%'"
        ).fetchone()[0]
        if prior > 0:
            con.execute(
                "DELETE FROM qa_issues WHERE check_id LIKE 'xfile_%'"
            )
            log.info(f"  Removed {prior} prior xfile_ rows from qa_issues")
    except Exception:
        pass

    total_inserted = 0
    for check_id, severity, sql in QA_INSERTS:
        log.info(f"\n  Inserting: {check_id}")

        source_tbl = (
            "qa_laterality_mismatches" if "laterality" in check_id
            else "qa_missing_demographics"
        )
        if not _table_exists(con, source_tbl):
            log.warning(f"    Source table {source_tbl} not found — skipping")
            report.append(f"- `{check_id}`: skipped (source not found)")
            continue

        try:
            before = _safe_count(con, "qa_issues") or 0
            con.execute(sql)
            after = _safe_count(con, "qa_issues") or 0
            n = after - before
            total_inserted += n
            log.info(f"    Inserted {n} rows")
            report.append(f"- `{check_id}`: {n} issues inserted")
        except Exception as exc:
            log.error(f"    Insert FAILED: {exc}")
            report.append(f"- `{check_id}`: FAILED — {exc}")

    log.info(f"\n  Total cross-file issues inserted: {total_inserted}")
    report.append(f"- **Total cross-file issues: {total_inserted}**")

    PHASE_TIMES["phase3"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 4: Report + Parquet Exports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase4_report(con, results: list[dict]) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 4: Update QA Summary Report + Parquet Exports")
    log.info("=" * 72)
    report: list[str] = []

    QA_DIR.mkdir(parents=True, exist_ok=True)
    qa_report_path = QA_DIR / "qa_summary_report.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    section_lines = [
        "",
        "## Cross-File Validation Report (11.5 — March 10, 2026)",
        "",
        f"**Generated:** {timestamp}",
        "",
        "### Check Results",
        "",
        "| Check | Table | Rows | Patients | Time | Status |",
        "|-------|-------|------|----------|------|--------|",
    ]
    for r in results:
        section_lines.append(
            f"| {r['label']} | `{r['table']}` | {r['rows']:,} "
            f"| {r['patients']:,} | {r['elapsed']:.1f}s | {r['status']} |"
        )

    section_lines.extend(["", "### Detailed Findings", ""])

    if _table_exists(con, "qa_laterality_mismatches"):
        try:
            dist = con.execute(
                "SELECT laterality_flag, COUNT(*) AS n "
                "FROM qa_laterality_mismatches GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall()
            section_lines.append("**Check A — Laterality Consistency:**")
            for flag, n in dist:
                section_lines.append(f"- {flag}: {n:,}")
            section_lines.append("")
        except Exception:
            pass

    if _table_exists(con, "qa_report_matching"):
        try:
            rows = con.execute(
                "SELECT check_type, total_pairs, matched, match_pct "
                "FROM qa_report_matching"
            ).fetchall()
            section_lines.append("**Check B — Report Matching:**")
            for ct, tp, m, pct in rows:
                section_lines.append(f"- {ct}: {m}/{tp} matched ({pct}%)")
            section_lines.append("")
        except Exception:
            pass

    if _table_exists(con, "qa_missing_demographics"):
        try:
            section_lines.append("**Check C — Missing Demographics:**")
            for flag_col, label in [
                ("age_flag", "Missing Age"),
                ("sex_flag", "Missing Sex"),
                ("race_flag", "Missing Race"),
            ]:
                n = con.execute(
                    f"SELECT COUNT(*) FROM qa_missing_demographics "
                    f"WHERE {flag_col} != 'OK'"
                ).fetchone()[0]
                section_lines.append(f"- {label}: {n:,} patients")
            section_lines.append("")
        except Exception:
            pass

    section_lines.extend([
        "### Recommendations",
        "",
        "1. Review laterality mismatches — may indicate data entry errors "
        "or bilateral procedures coded as unilateral",
        "2. FNA-pathology linkage gaps may reflect cases where FNA was "
        "performed at outside institutions",
        "3. Missing demographics (especially race) should be back-filled "
        "from clinical notes or registry data where possible",
        "4. Consider re-running after any manual corrections to track "
        "improvement",
        "",
        f"*Generated by `11.5_cross_file_validation.py` at {timestamp}*",
        "",
    ])

    section_text = "\n".join(section_lines)
    if qa_report_path.exists():
        existing = qa_report_path.read_text()
        marker = "## Cross-File Validation Report (11.5"
        if marker in existing:
            idx = existing.index(marker)
            existing = existing[:idx].rstrip()
        qa_report_path.write_text(existing + "\n" + section_text)
    else:
        qa_report_path.write_text(
            "# QA Summary Report — THYROID_2026 Lakehouse\n" + section_text
        )

    log.info(f"  Report updated: {qa_report_path}")
    report.append(f"- Report → `{qa_report_path}`")

    # Parquet exports
    EXPORTS.mkdir(exist_ok=True)
    for tbl in [
        "qa_laterality_mismatches",
        "qa_report_matching",
        "qa_missing_demographics",
    ]:
        if not _table_exists(con, tbl):
            continue
        out_path = EXPORTS / f"{tbl}.parquet"
        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_parquet(str(out_path), index=False)
            size_mb = out_path.stat().st_size / 1_048_576
            log.info(f"  Exported {tbl} → {out_path.name} "
                     f"({len(df):,} rows, {size_mb:.2f} MB)")
            report.append(
                f"- `{tbl}` → `{out_path.name}` "
                f"({len(df):,} rows, {size_mb:.2f} MB)"
            )
        except Exception as exc:
            log.error(f"  Export failed for {tbl}: {exc}")
            report.append(f"- `{tbl}` export FAILED: {exc}")

    PHASE_TIMES["phase4"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 5: Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase5_summary(con, results: list[dict]) -> None:
    log.info("\n" + "=" * 72)
    log.info("  PHASE 5: Final Summary")
    log.info("=" * 72)

    log.info("\n  Cross-File Validation Results:")
    log.info("  " + "-" * 68)
    log.info(f"  {'Check':<48s} {'Rows':>8s} {'Patients':>10s} "
             f"{'Status':>8s}")
    log.info("  " + "-" * 68)
    for r in results:
        log.info(
            f"  {r['label']:<48s} {r['rows']:>8,} "
            f"{r['patients']:>10,} {r['status']:>8s}"
        )
    log.info("  " + "-" * 68)

    total_issues = _safe_count(con, "qa_issues") or 0
    xfile_issues = 0
    try:
        xfile_issues = con.execute(
            "SELECT COUNT(*) FROM qa_issues WHERE check_id LIKE 'xfile_%'"
        ).fetchone()[0]
    except Exception:
        pass

    log.info(f"\n  qa_issues total: {total_issues:,} "
             f"(cross-file: {xfile_issues:,})")

    total_time = sum(PHASE_TIMES.values())
    log.info(f"\n  Phase compute times:")
    for phase, elapsed in PHASE_TIMES.items():
        log.info(f"    {phase}: {elapsed:.1f}s")
    log.info(f"    Total: {total_time:.1f}s")

    log.info("\n  New tables created:")
    for tbl in [
        "qa_laterality_mismatches",
        "qa_report_matching",
        "qa_missing_demographics",
    ]:
        rows = _safe_count(con, tbl)
        patients = _safe_distinct(con, tbl)
        if rows is not None:
            log.info(f"    {tbl:<35s} {rows:>8,} rows  "
                     f"{patients or 0:>6,} patients")

    log.info("\n" + "=" * 72)
    log.info("  CROSS-FILE VALIDATION COMPLETE")
    log.info("  Ready for script 12 (dashboard upgrade)")
    log.info("=" * 72)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-File Logical Consistency QA — validates data "
        "consistency across multiple source files"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print SQL plans without executing CREATE/DROP statements",
    )
    args = parser.parse_args()

    log.info("=" * 72)
    log.info("  THYROID LAKEHOUSE — CROSS-FILE VALIDATION (11.5)")
    log.info("=" * 72)
    if args.dry_run:
        log.info("*** DRY RUN MODE — no tables will be created ***\n")

    try:
        con = _get_connection()
    except Exception as exc:
        log.error(f"Connection failed: {exc}")
        sys.exit(1)

    try:
        phase1_connect_optimize(con)
        _report, results = phase2_cross_file_checks(con, args.dry_run)
        phase3_qa_inserts(con, args.dry_run)
        phase4_report(con, results)
        phase5_summary(con, results)
    finally:
        con.close()

    log.info(f"\n  QA report: {QA_DIR / 'qa_summary_report.md'}")
    log.info(f"  Exports:   {EXPORTS}")
    log.info("")


if __name__ == "__main__":
    main()
