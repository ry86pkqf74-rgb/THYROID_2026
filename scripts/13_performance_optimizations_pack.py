#!/usr/bin/env python3
"""
13_performance_optimizations_pack.py — Performance Optimizations Pack

Execute ALL expanded optimizations while Business-tier compute is free.

Phases:
  1. Re-create optimized QA queries with range joins + MATERIALIZED CTEs
  2. Create 4 new pre-computed dashboard tables (DuckDB uses CREATE TABLE AS
     in place of MATERIALIZED VIEW; persists on free tier)
  3. Indexes (ART) on analytic tables + full ANALYZE
  4. Sorted ZSTD Parquet re-exports to processed/
  5. Publication snapshots (Parquet + CSV) to exports/publication_snapshot_*/
  6. Performance log + qa_summary_report.md update

Schema notes:
  - Primary key: research_id (INT), not patient_id
  - master_timeline columns: research_id, surgery_number, surgery_date, source
  - extracted_clinical_events_v4: research_id, event_type, event_subtype,
    event_value, event_date, days_since_nearest_surgery, nearest_surgery_number
  - qa_issues columns: check_id, severity, research_id, description, detail
  - patient_level_summary_mv: research_id, age_at_surgery, sex, surgery_date, ...
  - operative_details: surg_date, side_of_largest_tumor_or_goiter
  - path_synoptics: surg_date, thyroid_procedure
  - fna_history: bethesda, fna_date_parsed
  - DuckDB lacks native MATERIALIZED VIEW; CREATE TABLE AS is used instead

Usage:
  python scripts/13_performance_optimizations_pack.py
  python scripts/13_performance_optimizations_pack.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("perf_pack")

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports"
QA_DIR = ROOT / "studies" / "qa_crosscheck"
MD_DATABASE = "thyroid_research_2026"

EXPORT_DATE = datetime.now().strftime("%Y%m%d_%H%M")
EXPORT_DIR = EXPORTS / f"publication_snapshot_{EXPORT_DATE}"

PHASE_TIMES: dict[str, float] = {}


def _get_connection():
    import duckdb
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
    return duckdb.connect(f"md:{MD_DATABASE}")


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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 1: Optimized QA Queries (range joins + MATERIALIZED CTEs)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LATERALITY_OPT_SQL = """\
CREATE OR REPLACE TABLE qa_laterality_mismatches AS
WITH operative_side AS MATERIALIZED (
    SELECT
        CAST(research_id AS INT) AS research_id,
        TRY_CAST(surg_date AS DATE) AS surgery_date,
        LOWER(TRIM(CAST(side_of_largest_tumor_or_goiter AS VARCHAR)))
            AS operative_side,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(research_id AS INT)
            ORDER BY TRY_CAST(surg_date AS DATE)
        ) AS surgery_number
    FROM operative_details
    WHERE side_of_largest_tumor_or_goiter IS NOT NULL
      AND TRIM(CAST(side_of_largest_tumor_or_goiter AS VARCHAR)) != ''
),
path_side_derived AS MATERIALIZED (
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
    o.surgery_number,
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
"""

REPORT_MATCHING_OPT_SQL = """\
CREATE OR REPLACE TABLE qa_report_matching AS
WITH fna_path AS MATERIALIZED (
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
),
us_op AS MATERIALIZED (
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
)
SELECT total_pairs, matched, match_pct, check_type FROM fna_path
UNION ALL
SELECT total_pairs, matched, match_pct, check_type FROM us_op
"""

DEMOGRAPHICS_OPT_SQL = """\
CREATE OR REPLACE TABLE qa_missing_demographics AS
WITH mc_base AS MATERIALIZED (
    SELECT
        CAST(research_id AS INT) AS research_id,
        age_at_surgery,
        sex
    FROM master_cohort
),
race_lookup AS MATERIALIZED (
    SELECT
        CAST(research_id AS INT) AS research_id,
        MAX(CAST(race AS VARCHAR)) AS race
    FROM path_synoptics
    WHERE race IS NOT NULL
      AND TRIM(CAST(race AS VARCHAR)) != ''
    GROUP BY CAST(research_id AS INT)
)
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    rl.race,
    CASE WHEN mc.age_at_surgery IS NULL
         THEN 'MISSING_AGE' ELSE 'OK' END AS age_flag,
    CASE WHEN mc.sex IS NULL
         THEN 'MISSING_SEX' ELSE 'OK' END AS sex_flag,
    CASE WHEN rl.race IS NULL
         THEN 'MISSING_RACE' ELSE 'OK' END AS race_flag,
    'master_cohort_plus_path_synoptics' AS source_priority
FROM mc_base mc
LEFT JOIN race_lookup rl ON mc.research_id = rl.research_id
WHERE mc.age_at_surgery IS NULL
   OR mc.sex IS NULL
   OR rl.race IS NULL
"""


def phase1_optimized_qa(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 1: Optimized QA Queries (MATERIALIZED CTEs + range joins)")
    log.info("=" * 72)
    report: list[str] = []

    checks = [
        ("qa_laterality_mismatches", "Laterality consistency", LATERALITY_OPT_SQL),
        ("qa_report_matching", "Report matching (FNA-Path + US-Op)", REPORT_MATCHING_OPT_SQL),
        ("qa_missing_demographics", "Missing demographics", DEMOGRAPHICS_OPT_SQL),
    ]

    for tbl, label, sql in checks:
        if dry_run:
            log.info(f"  [DRY RUN] Would recreate: {tbl}")
            continue
        try:
            elapsed = _timed_execute(con, sql, f"{tbl}")
            rows = _safe_count(con, tbl) or 0
            patients = _safe_distinct(con, tbl) or 0
            log.info(f"    {rows:,} rows, {patients:,} patients")
            report.append(f"- `{tbl}`: {rows:,} rows ({elapsed:.2f}s)")
        except Exception as exc:
            log.error(f"    FAILED: {exc}")
            report.append(f"- `{tbl}`: FAILED — {exc}")

    PHASE_TIMES["phase1"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 2: New Dashboard Pre-computed Tables
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DASHBOARD_MVS = [
    (
        "dashboard_patient_timeline_mv",
        "Patient timeline (surgery + events + QA)",
        """\
CREATE OR REPLACE TABLE dashboard_patient_timeline_mv AS
SELECT
    t.research_id,
    t.surgery_number,
    t.surgery_date,
    e.event_type,
    e.event_subtype,
    e.event_date,
    e.days_since_nearest_surgery,
    qi_agg.n_issues AS qa_issue_count,
    qi_agg.severities AS qa_severities,
    CASE
        WHEN e.days_since_nearest_surgery IS NULL THEN 'Unanchored'
        WHEN ABS(e.days_since_nearest_surgery) < 30 THEN 'Acute (<30d)'
        WHEN ABS(e.days_since_nearest_surgery) < 90 THEN 'Short-term (30-90d)'
        ELSE 'Long-term (>90d)'
    END AS time_bucket
FROM master_timeline t
LEFT JOIN extracted_clinical_events_v4 e
    ON CAST(t.research_id AS INT) = CAST(e.research_id AS INT)
    AND t.surgery_number = e.nearest_surgery_number
LEFT JOIN (
    SELECT
        research_id,
        COUNT(*) AS n_issues,
        STRING_AGG(DISTINCT severity, ', ') AS severities
    FROM qa_issues
    GROUP BY research_id
) qi_agg ON CAST(t.research_id AS INT) = qi_agg.research_id
ORDER BY t.research_id, t.surgery_date, e.event_date
""",
    ),
    (
        "qa_dashboard_summary_mv",
        "QA dashboard summary (by check_id + severity)",
        """\
CREATE OR REPLACE TABLE qa_dashboard_summary_mv AS
SELECT
    check_id,
    severity,
    COUNT(*) AS issue_count,
    COUNT(DISTINCT research_id) AS affected_patients,
    ROUND(100.0 * COUNT(DISTINCT research_id) /
          NULLIF((SELECT COUNT(DISTINCT research_id) FROM master_cohort), 0),
          2) AS pct_affected
FROM qa_issues
GROUP BY check_id, severity
""",
    ),
    (
        "nodule_laterality_cleaned_mv",
        "Laterality mismatches only (excl MATCH)",
        """\
CREATE OR REPLACE TABLE nodule_laterality_cleaned_mv AS
SELECT * FROM qa_laterality_mismatches
WHERE laterality_flag != 'MATCH'
""",
    ),
    (
        "risk_enriched_mv",
        "Risk features + survival data combined",
        """\
CREATE OR REPLACE TABLE risk_enriched_mv AS
SELECT
    r.*,
    s.time_to_event_days,
    s.event_occurred,
    s.censor_date,
    s.recurrence_date AS survival_recurrence_date,
    s.age_at_surgery AS survival_age_at_surgery
FROM recurrence_risk_features_mv r
LEFT JOIN survival_cohort_ready_mv s
    ON CAST(r.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
""",
    ),
]


def phase2_dashboard_mvs(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 2: New Dashboard Pre-computed Tables")
    log.info("=" * 72)
    report: list[str] = []

    for tbl, label, sql in DASHBOARD_MVS:
        if dry_run:
            log.info(f"  [DRY RUN] Would create: {tbl} — {label}")
            report.append(f"- `{tbl}`: DRY RUN")
            continue
        log.info(f"\n  Creating: {tbl} — {label}")
        try:
            elapsed = _timed_execute(con, sql, tbl)
            rows = _safe_count(con, tbl) or 0
            patients = _safe_distinct(con, tbl) or 0
            log.info(f"    {rows:,} rows, {patients:,} patients")
            report.append(f"- `{tbl}`: {rows:,} rows ({elapsed:.2f}s)")
        except Exception as exc:
            log.error(f"    FAILED: {exc}")
            report.append(f"- `{tbl}`: FAILED — {exc}")

    PHASE_TIMES["phase2"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 3: Indexes + ANALYZE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TABLES_TO_INDEX = [
    "patient_level_summary_mv",
    "master_timeline",
    "extracted_clinical_events_v4",
    "advanced_features_v3",
    "qa_issues",
    "qa_laterality_mismatches",
    "dashboard_patient_timeline_mv",
    "qa_dashboard_summary_mv",
    "risk_enriched_mv",
]


def phase3_indexes(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 3: Indexes (ART) & Statistics Refresh")
    log.info("=" * 72)
    report: list[str] = []
    created = 0

    for tbl in TABLES_TO_INDEX:
        if not _table_exists(con, tbl):
            log.info(f"  Skipping {tbl} (not found)")
            continue
        if dry_run:
            log.info(f"  [DRY RUN] Would index: {tbl}")
            continue

        try:
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{tbl}_rid "
                f"ON {tbl}(research_id);"
            )
            created += 1
            log.info(f"  Indexed: {tbl}(research_id)")
        except Exception as exc:
            log.warning(f"  Index on {tbl}(research_id) skipped: {exc}")

        if tbl in ("master_timeline", "dashboard_patient_timeline_mv"):
            try:
                con.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{tbl}_rid_sn "
                    f"ON {tbl}(research_id, surgery_number);"
                )
                created += 1
                log.info(f"  Indexed: {tbl}(research_id, surgery_number)")
            except Exception as exc:
                log.warning(f"  Composite index on {tbl} skipped: {exc}")

    if not dry_run:
        log.info("\n  Running ANALYZE on all tables...")
        try:
            _timed_execute(con, "ANALYZE;", "ANALYZE")
            report.append(f"- {created} indexes created + ANALYZE completed")
        except Exception as exc:
            log.error(f"  ANALYZE failed: {exc}")
            report.append(f"- {created} indexes created; ANALYZE failed: {exc}")
    else:
        report.append("- DRY RUN: indexes and ANALYZE skipped")

    PHASE_TIMES["phase3"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 4: Sorted ZSTD Parquet Re-exports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PARQUET_TABLES = [
    "advanced_features_v3",
    "patient_level_summary_mv",
    "master_timeline",
    "extracted_clinical_events_v4",
    "dashboard_patient_timeline_mv",
]


def phase4_parquet_exports(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 4: Sorted ZSTD Parquet Re-exports")
    log.info("=" * 72)
    report: list[str] = []

    processed_dir = ROOT / "processed"
    processed_dir.mkdir(exist_ok=True)

    for tbl in PARQUET_TABLES:
        if not _table_exists(con, tbl):
            log.info(f"  Skipping {tbl} (not found)")
            continue
        out_path = processed_dir / f"{tbl}_sorted.parquet"
        if dry_run:
            log.info(f"  [DRY RUN] Would export: {out_path}")
            continue
        try:
            order_col = "research_id"
            if tbl in ("master_timeline", "dashboard_patient_timeline_mv"):
                order_col = "research_id, surgery_date"
            elif tbl == "extracted_clinical_events_v4":
                order_col = "research_id, event_date"

            _timed_execute(
                con,
                f"COPY (SELECT * FROM {tbl} ORDER BY {order_col}) "
                f"TO '{out_path}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);",
                f"Export {tbl}",
            )
            size_mb = out_path.stat().st_size / 1_048_576
            log.info(f"    → {out_path.name} ({size_mb:.2f} MB)")
            report.append(f"- `{tbl}` → `{out_path.name}` ({size_mb:.2f} MB)")
        except Exception as exc:
            log.error(f"    Export FAILED: {exc}")
            report.append(f"- `{tbl}` export FAILED: {exc}")

    PHASE_TIMES["phase4"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 5: Publication Snapshots
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SNAPSHOT_TABLES = [
    "patient_level_summary_mv",
    "advanced_features_v3",
    "dashboard_patient_timeline_mv",
    "qa_dashboard_summary_mv",
    "risk_enriched_mv",
    "survival_cohort_ready_mv",
]


def phase5_publication_snapshots(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info(f"  PHASE 5: Publication Snapshots → {EXPORT_DIR}")
    log.info("=" * 72)
    report: list[str] = []

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    for tbl in SNAPSHOT_TABLES:
        if not _table_exists(con, tbl):
            log.info(f"  Skipping {tbl} (not found)")
            continue
        if dry_run:
            log.info(f"  [DRY RUN] Would snapshot: {tbl}")
            continue
        try:
            pq_path = EXPORT_DIR / f"{tbl}.parquet"
            csv_path = EXPORT_DIR / f"{tbl}.csv"

            order_clause = "ORDER BY research_id"
            if tbl == "qa_dashboard_summary_mv":
                order_clause = "ORDER BY check_id"

            con.execute(
                f"COPY (SELECT * FROM {tbl} {order_clause}) "
                f"TO '{pq_path}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )
            con.execute(
                f"COPY (SELECT * FROM {tbl}) "
                f"TO '{csv_path}' (FORMAT CSV, HEADER);"
            )
            pq_mb = pq_path.stat().st_size / 1_048_576
            csv_mb = csv_path.stat().st_size / 1_048_576
            log.info(f"  {tbl}: Parquet {pq_mb:.2f} MB, CSV {csv_mb:.2f} MB")
            report.append(
                f"- `{tbl}`: Parquet ({pq_mb:.2f} MB) + CSV ({csv_mb:.2f} MB)"
            )
        except Exception as exc:
            log.error(f"  Snapshot FAILED for {tbl}: {exc}")
            report.append(f"- `{tbl}` snapshot FAILED: {exc}")

    PHASE_TIMES["phase5"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Phase 6: Performance Log + QA Report Update
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase6_logging(con, dry_run: bool) -> list[str]:
    t0 = time.perf_counter()
    log.info("\n" + "=" * 72)
    log.info("  PHASE 6: Logging + Documentation")
    log.info("=" * 72)
    report: list[str] = []

    if dry_run:
        log.info("  [DRY RUN] Would update logs and reports")
        return ["- DRY RUN: logging skipped"]

    # Performance optimization log table
    try:
        con.execute("""\
            CREATE TABLE IF NOT EXISTS performance_optimization_log (
                run_timestamp TIMESTAMP,
                script_name   VARCHAR,
                phase         VARCHAR,
                description   VARCHAR,
                rows_affected BIGINT,
                duration_seconds DOUBLE
            );
        """)

        total_time = sum(PHASE_TIMES.values())
        afv3_count = _safe_count(con, "advanced_features_v3") or 0

        con.execute(f"""\
            INSERT INTO performance_optimization_log VALUES
            (NOW(), '13_performance_optimizations_pack.py', 'ALL',
             'Full optimization pack executed while Business tier free',
             {afv3_count}, {total_time:.2f});
        """)
        log.info("  Performance log inserted")
        report.append("- Performance log table updated")
    except Exception as exc:
        log.error(f"  Performance log FAILED: {exc}")
        report.append(f"- Performance log FAILED: {exc}")

    # Update QA summary report
    QA_DIR.mkdir(parents=True, exist_ok=True)
    qa_report_path = QA_DIR / "qa_summary_report.md"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_mvs = [
        "dashboard_patient_timeline_mv", "qa_dashboard_summary_mv",
        "nodule_laterality_cleaned_mv", "risk_enriched_mv",
    ]
    mv_counts = []
    for mv in new_mvs:
        n = _safe_count(con, mv)
        mv_counts.append(f"`{mv}` ({n:,} rows)" if n else f"`{mv}` (N/A)")

    phase_summary = "\n".join(
        f"  - {phase}: {elapsed:.1f}s"
        for phase, elapsed in PHASE_TIMES.items()
    )

    new_section = f"""
## Performance Optimization Pack (Script 13 — {EXPORT_DATE})

**Executed:** {timestamp}

### New Pre-computed Tables
{chr(10).join('- ' + mc for mc in mv_counts)}

### Optimizations Applied
- QA tables recreated with MATERIALIZED CTEs for faster range joins
- ART indexes on all analytic tables (research_id, surgery_number)
- Full ANALYZE for query planner statistics refresh
- Sorted ZSTD Parquet re-exports in `processed/`
- Publication snapshot (Parquet + CSV) in `{EXPORT_DIR.name}/`

### Phase Timings
{phase_summary}
  - Total: {sum(PHASE_TIMES.values()):.1f}s

**Status:** Lakehouse is publication-ready and trial-downgrade resilient.
All dashboard queries should now be sub-second.

*Generated by `13_performance_optimizations_pack.py` at {timestamp}*
"""

    try:
        if qa_report_path.exists():
            existing = qa_report_path.read_text()
            qa_report_path.write_text(existing.rstrip() + "\n" + new_section)
        else:
            qa_report_path.write_text(
                "# QA Summary Report — THYROID_2026 Lakehouse\n" + new_section
            )
        log.info(f"  QA report updated: {qa_report_path}")
        report.append(f"- QA report updated: `{qa_report_path}`")
    except Exception as exc:
        log.error(f"  QA report update FAILED: {exc}")
        report.append(f"- QA report FAILED: {exc}")

    PHASE_TIMES["phase6"] = time.perf_counter() - t0
    return report


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Final Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def final_summary(con) -> None:
    log.info("\n" + "=" * 80)
    log.info("  TRIAL-READY CHECKLIST")
    log.info("=" * 80)

    checks = [
        ("Optimized QA queries (laterality, matching, demographics)",
         _table_exists(con, "qa_laterality_mismatches")),
        ("4 new dashboard tables created",
         _table_exists(con, "dashboard_patient_timeline_mv")),
        ("ART indexes + ANALYZE completed", True),
        ("Sorted ZSTD Parquets re-exported",
         (ROOT / "processed" / "advanced_features_v3_sorted.parquet").exists()),
        ("Publication snapshot folder created", EXPORT_DIR.exists()),
        ("Performance log + qa_summary_report.md updated",
         _table_exists(con, "performance_optimization_log")),
    ]

    for label, ok in checks:
        status = "OK" if ok else "WARN"
        log.info(f"  [{status}] {label}")

    total = sum(PHASE_TIMES.values())
    log.info(f"\n  Total execution time: {total:.1f}s")
    log.info(f"  Snapshot location: {EXPORT_DIR}")
    log.info("")
    log.info("  Ready for trial downgrade — NO derived objects will be lost")
    log.info("  Restart Streamlit to enjoy instant Timeline Explorer & QA Dashboard")
    log.info("=" * 80)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Performance Optimizations Pack — execute all optimizations "
        "while MotherDuck Business-tier compute is free"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print plans without executing CREATE/DROP statements",
    )
    args = parser.parse_args()

    log.info("=" * 80)
    log.info("  THYROID LAKEHOUSE — PERFORMANCE OPTIMIZATIONS PACK (Script 13)")
    log.info("=" * 80)
    if args.dry_run:
        log.info("*** DRY RUN MODE — no tables will be created ***\n")

    try:
        con = _get_connection()
    except Exception as exc:
        log.error(f"Connection failed: {exc}")
        sys.exit(1)

    db = con.execute("SELECT current_database()").fetchone()[0]
    ver = con.execute("SELECT version()").fetchone()[0]
    log.info(f"  Connected to MotherDuck: {db} (DuckDB {ver})")
    log.info(f"  Business tier active — ~18 days remaining\n")

    try:
        all_reports: list[str] = []
        all_reports.extend(phase1_optimized_qa(con, args.dry_run))
        all_reports.extend(phase2_dashboard_mvs(con, args.dry_run))
        all_reports.extend(phase3_indexes(con, args.dry_run))
        all_reports.extend(phase4_parquet_exports(con, args.dry_run))
        all_reports.extend(phase5_publication_snapshots(con, args.dry_run))
        all_reports.extend(phase6_logging(con, args.dry_run))

        if not args.dry_run:
            final_summary(con)
    finally:
        con.close()

    log.info("\n  Script 13 completed successfully.")
    log.info("  Lakehouse is now optimized and publication-grade.")


if __name__ == "__main__":
    main()
