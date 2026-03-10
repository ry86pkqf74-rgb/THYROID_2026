#!/usr/bin/env python3
"""
25_qa_validation_v2.py -- Comprehensive QA validation rules (v2)

Implements 10 categories of quality-assurance checks across all canonical
episode tables and linkage outputs.  Produces:

  qa_issues_v2            -- one row per detected issue
  qa_summary_by_domain_v2 -- aggregated counts per domain / severity
  qa_high_priority_review_v2 -- filtered to error-severity items
  qa_date_completeness_v2 -- date quality metrics per domain

QA categories:
   1. Histology reconciliation mismatch
   2. Molecular chronology mismatch
   3. RAI chronology mismatch
   4. Nodule-FNA mismatch
   5. Imaging-pathology mismatch
   6. Op-pathology mismatch
   7. Parathyroid consistency check
   8. Date completeness by domain
   9. Duplicate event detection
  10. Missing-but-derivable fields

Run after scripts 22-24.
Supports --md flag.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

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


# ---------------------------------------------------------------------------
# QA issue collection SQL
# ---------------------------------------------------------------------------

QA_ISSUES_V2_SQL = """
CREATE OR REPLACE TABLE qa_issues_v2 AS

-- 1. Histology reconciliation mismatch
SELECT
    'histology_mismatch' AS check_id,
    'error' AS severity,
    research_id,
    'Histology discordance between sources: ' || COALESCE(primary_histology, '?')
        || ' (rank ' || CAST(confidence_rank AS VARCHAR) || ')' AS description,
    'tumor_episode_master_v2 row ' || CAST(surgery_episode_id AS VARCHAR) AS detail,
    CURRENT_TIMESTAMP AS checked_at
FROM tumor_episode_master_v2
WHERE histology_discordance_flag

UNION ALL

-- 1b. T-stage discordance
SELECT
    'tstage_m326mismatch', 'warning', research_id,
    'T-stage discordance: ' || COALESCE(t_stage, '?'),
    'tumor_episode_master_v2 row ' || CAST(surgery_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM tumor_episode_master_v2
WHERE t_stage_discordance_flag

UNION ALL

-- 2. Molecular chronology mismatch (post-surgery test without explicit context)
SELECT
    'molecular_chronology', 'warning', m.research_id,
    'Molecular test after linked surgery: ' || m.platform || ' ' || m.overall_result_class,
    'molecular_test_episode_v2 ep ' || CAST(m.molecular_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM molecular_test_episode_v2 m
JOIN operative_episode_detail_v2 o
    ON m.research_id = o.research_id
    AND CAST(o.surgery_episode_id AS VARCHAR) = m.linked_surgery_episode_id
WHERE m.test_date_native IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND m.test_date_native > o.surgery_date_native

UNION ALL

-- 3. RAI chronology mismatch (RAI before surgery)
SELECT
    'rai_before_surgery', 'error', r.research_id,
    'RAI before linked surgery: ' || r.rai_assertion_status,
    'rai_treatment_episode_v2 ep ' || CAST(r.rai_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM rai_treatment_episode_v2 r
JOIN operative_episode_detail_v2 o
    ON r.research_id = o.research_id
    AND CAST(o.surgery_episode_id AS VARCHAR) = r.linked_surgery_episode_id
WHERE r.resolved_rai_date IS NOT NULL
  AND o.surgery_date_native IS NOT NULL
  AND r.resolved_rai_date < o.surgery_date_native
  AND r.rai_assertion_status NOT IN ('historical', 'negated')

UNION ALL

-- 3b. Implausible RAI dose
SELECT
    'rai_implausible_dose', 'error', research_id,
    'Implausible RAI dose: ' || CAST(dose_mci AS VARCHAR) || ' mCi',
    'rai_treatment_episode_v2 ep ' || CAST(rai_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM rai_treatment_episode_v2
WHERE dose_mci IS NOT NULL AND (dose_mci < 10 OR dose_mci > 300)

UNION ALL

-- 4. Imaging-FNA laterality mismatch
SELECT
    'nodule_fna_laterality', 'warning', l.research_id,
    'Imaging-FNA laterality mismatch',
    'nodule ' || l.nodule_id || ' -> fna ep ' || CAST(l.fna_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM imaging_fna_linkage_v2 l
WHERE l.laterality_match = FALSE

UNION ALL

-- 5. Imaging-pathology size mismatch
SELECT
    'imaging_path_size', 'warning', research_id,
    'Imaging-pathology size mismatch: ' || CAST(img_size AS VARCHAR)
        || ' cm (img) vs ' || CAST(path_size AS VARCHAR) || ' cm (path)',
    'nodule ' || nodule_id,
    CURRENT_TIMESTAMP
FROM imaging_pathology_concordance_review_v2
WHERE size_mismatch

UNION ALL

-- 5b. Imaging-pathology laterality mismatch
SELECT
    'imaging_path_laterality', 'error', research_id,
    'Imaging-pathology laterality: ' || COALESCE(img_lat, '?')
        || ' vs ' || COALESCE(path_lat, '?'),
    'nodule ' || nodule_id,
    CURRENT_TIMESTAMP
FROM imaging_pathology_concordance_review_v2
WHERE laterality_mismatch

UNION ALL

-- 6. Op-pathology mismatch
SELECT
    'op_path_mismatch', review_severity, research_id,
    review_reason,
    'operative ep ' || CAST(surgery_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM operative_pathology_reconciliation_review_v2
WHERE review_reason != 'concordant'

UNION ALL

-- 7. Parathyroid consistency (autograft without context)
SELECT
    'parathyroid_consistency', 'warning', o.research_id,
    'Parathyroid autograft noted in notes but not in operative record',
    'operative ep ' || CAST(o.surgery_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM operative_episode_detail_v2 o
WHERE o.parathyroid_autograft_flag = FALSE
  AND EXISTS (
      SELECT 1 FROM note_entities_procedures np
      WHERE CAST(np.research_id AS INTEGER) = o.research_id
        AND np.entity_value_norm = 'parathyroid_autotransplant'
        AND np.present_or_negated = 'present'
  )

UNION ALL

-- 9. Duplicate RAI episodes (same patient, same date)
SELECT
    'duplicate_rai', 'warning', r1.research_id,
    'Duplicate RAI episodes on same date',
    'rai eps ' || CAST(r1.rai_episode_id AS VARCHAR) || ' and '
        || CAST(r2.rai_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM rai_treatment_episode_v2 r1
JOIN rai_treatment_episode_v2 r2
    ON r1.research_id = r2.research_id
    AND r1.rai_episode_id < r2.rai_episode_id
    AND r1.resolved_rai_date = r2.resolved_rai_date
WHERE r1.resolved_rai_date IS NOT NULL

UNION ALL

-- 9b. Duplicate molecular episodes (same patient, same date, same platform)
SELECT
    'duplicate_molecular', 'warning', m1.research_id,
    'Duplicate molecular tests: ' || m1.platform || ' on same date',
    'mol eps ' || CAST(m1.molecular_episode_id AS VARCHAR) || ' and '
        || CAST(m2.molecular_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM molecular_test_episode_v2 m1
JOIN molecular_test_episode_v2 m2
    ON m1.research_id = m2.research_id
    AND m1.molecular_episode_id < m2.molecular_episode_id
    AND m1.test_date_native = m2.test_date_native
    AND m1.platform = m2.platform
WHERE m1.test_date_native IS NOT NULL

UNION ALL

-- 10. Missing-but-derivable: molecular test with mutation but no linked FNA
SELECT
    'missing_fna_link', 'info', research_id,
    'Molecular test with mutation but no linked FNA',
    'mol ep ' || CAST(molecular_episode_id AS VARCHAR) || ' platform=' || platform,
    CURRENT_TIMESTAMP
FROM molecular_test_episode_v2
WHERE (braf_flag OR ras_flag OR ret_flag OR tert_flag)
  AND linked_fna_episode_id IS NULL

UNION ALL

-- 10b. Surgery without pathology
SELECT
    'surgery_no_pathology', 'warning', o.research_id,
    'Surgery without linked pathology',
    'operative ep ' || CAST(o.surgery_episode_id AS VARCHAR),
    CURRENT_TIMESTAMP
FROM operative_episode_detail_v2 o
LEFT JOIN tumor_episode_master_v2 t
    ON o.research_id = t.research_id
    AND o.surgery_date_native = t.surgery_date
WHERE t.research_id IS NULL
  AND o.surgery_date_native IS NOT NULL
"""

# Date completeness by domain
QA_DATE_COMPLETENESS_SQL = """
CREATE OR REPLACE TABLE qa_date_completeness_v2 AS
SELECT
    domain,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE date_status = 'exact_source_date') AS exact_ct,
    COUNT(*) FILTER (WHERE date_status = 'inferred_day_level_date') AS inferred_ct,
    COUNT(*) FILTER (WHERE date_status = 'coarse_anchor_date') AS coarse_ct,
    COUNT(*) FILTER (WHERE date_status = 'unresolved_date') AS unresolved_ct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE date_status = 'exact_source_date')
          / NULLIF(COUNT(*), 0), 1) AS pct_exact,
    ROUND(100.0 * COUNT(*) FILTER (WHERE date_status = 'unresolved_date')
          / NULLIF(COUNT(*), 0), 1) AS pct_unresolved
FROM event_date_audit_v2
GROUP BY domain
ORDER BY pct_unresolved DESC
"""

QA_SUMMARY_SQL = """
CREATE OR REPLACE TABLE qa_summary_by_domain_v2 AS
SELECT
    check_id,
    severity,
    COUNT(*) AS issue_count,
    COUNT(DISTINCT research_id) AS patient_count
FROM qa_issues_v2
GROUP BY check_id, severity
ORDER BY
    CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
    issue_count DESC
"""

QA_HIGH_PRIORITY_SQL = """
CREATE OR REPLACE TABLE qa_high_priority_review_v2 AS
SELECT * FROM qa_issues_v2
WHERE severity = 'error'
ORDER BY check_id, research_id
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Deploy to MotherDuck")
    args = parser.parse_args()

    section("25 -- QA Validation v2")

    if args.md:
        try:
            from motherduck_client import MotherDuckClient, MotherDuckConfig
            cfg = MotherDuckConfig(database="thyroid_research_2026")
            client = MotherDuckClient(cfg)
            con = client.connect_rw()
            print("  Connected to MotherDuck (RW)")
        except Exception as e:
            print(f"  MotherDuck unavailable: {e}")
            con = duckdb.connect(str(DB_PATH))
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Using local DuckDB: {DB_PATH}")

    required = [
        "tumor_episode_master_v2", "molecular_test_episode_v2",
        "rai_treatment_episode_v2", "imaging_nodule_long_v2",
        "operative_episode_detail_v2", "fna_episode_master_v2",
        "event_date_audit_v2",
    ]
    for tbl in required:
        if not table_available(con, tbl):
            print(f"  ERROR: {tbl} not found. Run scripts 22-24 first.")
            sys.exit(1)

    # Register note_entities_procedures if needed
    pq = ROOT / "processed" / "note_entities_procedures.parquet"
    if pq.exists() and not table_available(con, "note_entities_procedures"):
        con.execute(
            f"CREATE TABLE note_entities_procedures AS "
            f"SELECT * FROM read_parquet('{pq}')"
        )

    for name, sql, desc in [
        ("qa_issues_v2", QA_ISSUES_V2_SQL, "QA issues"),
        ("qa_date_completeness_v2", QA_DATE_COMPLETENESS_SQL, "Date completeness"),
        ("qa_summary_by_domain_v2", QA_SUMMARY_SQL, "QA summary"),
        ("qa_high_priority_review_v2", QA_HIGH_PRIORITY_SQL, "High priority review"),
    ]:
        section(desc)
        try:
            con.execute(sql)
            cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  Created {name:<45} {cnt:>8,} rows")
        except Exception as e:
            print(f"  WARN: {name} -- {e}")

    section("QA Summary")
    try:
        rows = con.execute(
            "SELECT check_id, severity, issue_count, patient_count "
            "FROM qa_summary_by_domain_v2 ORDER BY severity, issue_count DESC"
        ).fetchall()
        print(f"  {'Check':<30} {'Severity':<10} {'Issues':>8} {'Patients':>8}")
        print(f"  {'-'*30} {'-'*10} {'-'*8} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<30} {r[1]:<10} {r[2]:>8,} {r[3]:>8,}")
    except Exception:
        pass

    section("Date Completeness")
    try:
        rows = con.execute(
            "SELECT * FROM qa_date_completeness_v2 ORDER BY domain"
        ).fetchall()
        print(f"  {'Domain':<15} {'Total':>8} {'Exact':>8} {'Infer':>8} "
              f"{'Coarse':>8} {'Unresol':>8} {'%Exact':>7} {'%Unres':>7}")
        for r in rows:
            print(f"  {r[0]:<15} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} "
                  f"{r[4]:>8,} {r[5]:>8,} {r[6]:>6.1f}% {r[7]:>6.1f}%")
    except Exception:
        pass

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
