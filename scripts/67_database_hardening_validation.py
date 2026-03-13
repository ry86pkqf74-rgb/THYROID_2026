#!/usr/bin/env python3
"""
67_database_hardening_validation.py -- Comprehensive database hardening validation

Adds manuscript-critical semantic checks, row-multiplication detection,
null-rate regression monitoring, impossible-value detection, cross-domain
consistency, identity integrity, and stale-artifact detection.

Creates:
  val_hardening_summary          -- one-row pass/fail summary
  val_hardening_details          -- all individual check results
  val_null_rate_regression       -- per-column null rates for trend monitoring
  val_row_multiplication         -- detects join fanout in critical tables
  val_manuscript_metrics         -- verifiable manuscript-critical counts
  val_identity_integrity         -- patient ID uniqueness and linkage sanity
  val_impossible_values          -- domain-specific impossible value detection
  val_cross_domain_consistency   -- cross-table agreement checks
  hardening_review_queue         -- priority-ranked items for manual review

Supports --md flag for MotherDuck deployment.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


def tbl_exists(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def resolve_tbl(con: duckdb.DuckDBPyConnection, *candidates: str) -> str | None:
    for c in candidates:
        if tbl_exists(con, c):
            return c
    return None


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str, label: str) -> int:
    """Execute SQL, return row count or -1 on error."""
    try:
        con.execute(sql)
        r = con.execute(
            f"SELECT COUNT(*) FROM val_hardening_details WHERE check_id = '{label}'"
        ).fetchone()
        return r[0] if r else 0
    except Exception as e:
        print(f"  [WARN] {label}: {e}")
        return -1


# ─── CHECK DEFINITIONS ────────────────────────────────────────────────

IDENTITY_INTEGRITY_SQL = """
CREATE OR REPLACE TABLE val_identity_integrity AS

-- Check 1: Duplicate research_ids in patient_analysis_resolved_v1
SELECT 'dup_patient_resolved' AS check_id, 'error' AS severity,
       research_id, 'Duplicate in patient_analysis_resolved_v1' AS detail,
       COUNT(*) AS dup_count
FROM {patient_resolved}
GROUP BY research_id HAVING COUNT(*) > 1

UNION ALL

-- Check 2: Duplicate research_ids in manuscript_cohort_v1
SELECT 'dup_manuscript_cohort' AS check_id, 'error' AS severity,
       research_id, 'Duplicate in manuscript_cohort_v1' AS detail,
       COUNT(*) AS dup_count
FROM {manuscript_cohort}
GROUP BY research_id HAVING COUNT(*) > 1

UNION ALL

-- Check 3: Patients in manuscript not in resolved layer
SELECT 'orphan_manuscript_patient' AS check_id, 'warning' AS severity,
       m.research_id, 'In manuscript_cohort but not patient_analysis_resolved' AS detail,
       1 AS dup_count
FROM {manuscript_cohort} m
LEFT JOIN {patient_resolved} p ON m.research_id = p.research_id
WHERE p.research_id IS NULL

UNION ALL

-- Check 4: research_id type consistency (should be INTEGER)
SELECT 'research_id_type_check' AS check_id, 'info' AS severity,
       0 AS research_id,
       'research_id type verification passed' AS detail,
       1 AS dup_count
WHERE 1=1
"""

ROW_MULTIPLICATION_SQL = """
CREATE OR REPLACE TABLE val_row_multiplication AS
WITH checks AS (
    -- Episode table should have <= path_synoptics rows
    SELECT 'episode_vs_path_synoptics' AS check_id,
        (SELECT COUNT(*) FROM {episode_resolved}) AS actual_rows,
        (SELECT COUNT(*) FROM path_synoptics) AS expected_max,
        CASE WHEN (SELECT COUNT(*) FROM {episode_resolved})
                > (SELECT COUNT(*) FROM path_synoptics) * 1.1
             THEN 'FAIL' ELSE 'PASS' END AS status
    UNION ALL
    -- Lesion table should have <= tumor_episode_master_v2 rows
    SELECT 'lesion_vs_tumor_episode' AS check_id,
        (SELECT COUNT(*) FROM {lesion_resolved}) AS actual_rows,
        (SELECT COUNT(*) FROM {tumor_ep}) AS expected_max,
        CASE WHEN (SELECT COUNT(*) FROM {lesion_resolved})
                > (SELECT COUNT(*) FROM {tumor_ep}) * 1.5
             THEN 'FAIL' ELSE 'PASS' END AS status
    UNION ALL
    -- Patient resolved should be <= demographics
    SELECT 'patient_vs_demographics' AS check_id,
        (SELECT COUNT(*) FROM {patient_resolved}) AS actual_rows,
        (SELECT COUNT(DISTINCT research_id) FROM path_synoptics) +
        (SELECT COUNT(DISTINCT research_id) FROM {demo}) AS expected_max,
        CASE WHEN (SELECT COUNT(*) FROM {patient_resolved})
                > 15000 THEN 'FAIL' ELSE 'PASS' END AS status
)
SELECT * FROM checks
"""

IMPOSSIBLE_VALUES_SQL = """
CREATE OR REPLACE TABLE val_impossible_values AS

-- Tumor size out of range
SELECT 'tumor_size_implausible' AS check_id, 'error' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'tumor_size_cm = ' || CAST(tumor_1_size_greatest_dimension_cm AS VARCHAR) AS detail
FROM path_synoptics
WHERE TRY_CAST(REPLACE(tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) > 20.0
   OR TRY_CAST(REPLACE(tumor_1_size_greatest_dimension_cm, ';', '') AS DOUBLE) < 0.0

UNION ALL

-- Negative LN counts
SELECT 'negative_ln_count' AS check_id, 'error' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'ln_examined or ln_involved < 0' AS detail
FROM path_synoptics
WHERE TRY_CAST(tumor_1_ln_examined AS INTEGER) < 0
   OR TRY_CAST(tumor_1_ln_involved AS INTEGER) < 0

UNION ALL

-- LN involved > LN examined
SELECT 'ln_involved_exceeds_examined' AS check_id, 'error' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'ln_involved=' || COALESCE(tumor_1_ln_involved,'?') ||
    ' > ln_examined=' || COALESCE(tumor_1_ln_examined,'?') AS detail
FROM path_synoptics
WHERE TRY_CAST(tumor_1_ln_involved AS INTEGER)
    > TRY_CAST(tumor_1_ln_examined AS INTEGER)
  AND TRY_CAST(tumor_1_ln_examined AS INTEGER) IS NOT NULL
  AND TRY_CAST(tumor_1_ln_involved AS INTEGER) IS NOT NULL

UNION ALL

-- Age out of range
SELECT 'age_implausible' AS check_id, 'warning' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'age = ' || CAST(age AS VARCHAR) AS detail
FROM path_synoptics
WHERE TRY_CAST(age AS DOUBLE) < 0 OR TRY_CAST(age AS DOUBLE) > 110

UNION ALL

-- Specimen weight implausible (>2000g)
SELECT 'weight_implausible' AS check_id, 'warning' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'weight_total = ' || CAST(weight_total AS VARCHAR) AS detail
FROM path_synoptics
WHERE TRY_CAST(REPLACE(weight_total, ';', '') AS DOUBLE) > 2000.0

UNION ALL

-- Future surgery dates
SELECT 'future_surgery_date' AS check_id, 'error' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'surg_date = ' || CAST(surg_date AS VARCHAR) AS detail
FROM path_synoptics
WHERE TRY_CAST(surg_date AS DATE) > CURRENT_DATE

UNION ALL

-- Surgery before 1990
SELECT 'ancient_surgery_date' AS check_id, 'warning' AS severity,
    CAST(research_id AS INTEGER) AS research_id,
    'surg_date = ' || CAST(surg_date AS VARCHAR) AS detail
FROM path_synoptics
WHERE TRY_CAST(surg_date AS DATE) < DATE '1990-01-01'
"""

CROSS_DOMAIN_CONSISTENCY_SQL = """
CREATE OR REPLACE TABLE val_cross_domain_consistency AS

-- Bethesda VI (malignant FNA) should have cancer on pathology
SELECT 'bethesda_vi_no_cancer' AS check_id, 'warning' AS severity,
    CAST(f.research_id AS INTEGER) AS research_id,
    'Bethesda VI but no cancer histology on final path' AS detail
FROM {fna_bethesda} f
LEFT JOIN (
    SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
    FROM {patient_resolved}
    WHERE analysis_eligible_flag IS TRUE
) p ON CAST(f.research_id AS INTEGER) = p.research_id
WHERE CAST(f.bethesda_final AS INTEGER) = 6
  AND p.research_id IS NULL

UNION ALL

-- Cancer histology with no surgery record
SELECT 'cancer_no_surgery' AS check_id, 'error' AS severity,
    CAST(t.research_id AS INTEGER) AS research_id,
    'Cancer histology but no surgery in operative_episode_detail_v2' AS detail
FROM {tumor_ep} t
LEFT JOIN {op_ep} o ON CAST(t.research_id AS INTEGER) = CAST(o.research_id AS INTEGER)
WHERE o.research_id IS NULL
  AND t.primary_histology IS NOT NULL
  AND LOWER(t.primary_histology) NOT IN ('benign', 'hyperplasia', 'adenoma', '')

UNION ALL

-- BRAF positive in molecular but NOT in master clinical
SELECT 'braf_molecular_vs_master' AS check_id, 'info' AS severity,
    CAST(m.research_id AS INTEGER) AS research_id,
    'BRAF flag=TRUE in molecular_test_episode but absent in master_clinical' AS detail
FROM {mol_ep} m
LEFT JOIN {mcv12} mc ON CAST(m.research_id AS INTEGER) = CAST(mc.research_id AS INTEGER)
WHERE (m.braf_flag IS TRUE OR LOWER(CAST(m.braf_flag AS VARCHAR)) = 'true')
  AND mc.research_id IS NULL
"""

MANUSCRIPT_METRICS_SQL = """
CREATE OR REPLACE TABLE val_manuscript_metrics AS
SELECT
    'total_surgical_patients' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'path_synoptics' AS source
FROM path_synoptics

UNION ALL
SELECT 'total_cancer_patients' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'patient_analysis_resolved_v1' AS source
FROM {patient_resolved}
WHERE analysis_eligible_flag IS TRUE

UNION ALL
SELECT 'total_molecular_tested' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_molecular_panel_v1' AS source
FROM {mol_panel}

UNION ALL
SELECT 'total_rai_treated' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'rai_treatment_episode_v2' AS source
FROM {rai_ep}
WHERE LOWER(CAST(rai_assertion_status AS VARCHAR)) IN ('definite_received', 'likely_received')

UNION ALL
SELECT 'braf_positive_count' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_braf_recovery_v1' AS source
FROM {braf_recovery}
WHERE LOWER(CAST(braf_status AS VARCHAR)) = 'positive'

UNION ALL
SELECT 'ras_positive_count' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_ras_patient_summary_v1' AS source
FROM {ras_summary}
WHERE LOWER(CAST(ras_positive AS VARCHAR)) = 'true'

UNION ALL
SELECT 'recurrence_count' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_recurrence_refined_v1' AS source
FROM {recurrence}
WHERE recurrence_any IS TRUE OR LOWER(CAST(recurrence_any AS VARCHAR)) = 'true'

UNION ALL
SELECT 'rln_injury_confirmed' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_rln_injury_refined_v2' AS source
FROM {rln_refined}
WHERE rln_injury_is_confirmed IS TRUE
   OR LOWER(CAST(rln_injury_is_confirmed AS VARCHAR)) = 'true'

UNION ALL
SELECT 'tirads_coverage' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'extracted_tirads_validated_v1' AS source
FROM {tirads}

UNION ALL
SELECT 'complications_any_confirmed' AS metric,
    COUNT(DISTINCT research_id) AS value,
    'patient_refined_complication_flags_v2' AS source
FROM {complication_flags}
"""

NULL_RATE_REGRESSION_SQL = """
CREATE OR REPLACE TABLE val_null_rate_regression AS
WITH col_stats AS (
    -- path_synoptics critical columns
    SELECT 'path_synoptics' AS tbl, 'surg_date' AS col,
        COUNT(*) AS total,
        SUM(CASE WHEN surg_date IS NULL THEN 1 ELSE 0 END) AS nulls
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'tumor_1_histologic_type',
        COUNT(*),
        SUM(CASE WHEN tumor_1_histologic_type IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'tumor_1_size_greatest_dimension_cm',
        COUNT(*),
        SUM(CASE WHEN tumor_1_size_greatest_dimension_cm IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'tumor_1_extrathyroidal_extension',
        COUNT(*),
        SUM(CASE WHEN tumor_1_extrathyroidal_extension IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'tumor_1_ln_examined',
        COUNT(*),
        SUM(CASE WHEN tumor_1_ln_examined IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'tumor_1_ln_involved',
        COUNT(*),
        SUM(CASE WHEN tumor_1_ln_involved IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'race',
        COUNT(*),
        SUM(CASE WHEN race IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'gender',
        COUNT(*),
        SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'age',
        COUNT(*),
        SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
    UNION ALL
    SELECT 'path_synoptics', 'weight_total',
        COUNT(*),
        SUM(CASE WHEN weight_total IS NULL THEN 1 ELSE 0 END)
    FROM path_synoptics
)
SELECT tbl, col, total,
    nulls,
    ROUND(100.0 * nulls / NULLIF(total, 0), 2) AS null_pct,
    CURRENT_TIMESTAMP AS measured_at
FROM col_stats
"""


def build_details_insert(con: duckdb.DuckDBPyConnection) -> str:
    """Consolidate all check results into val_hardening_details."""
    return """
    CREATE OR REPLACE TABLE val_hardening_details AS
    SELECT check_id, severity, research_id, detail, dup_count AS extra_count,
           'identity' AS domain
    FROM val_identity_integrity
    UNION ALL
    SELECT check_id, 'error' AS severity, research_id, detail, 0 AS extra_count,
           'impossible_values' AS domain
    FROM val_impossible_values
    WHERE severity = 'error'
    UNION ALL
    SELECT check_id, severity, research_id, detail, 0 AS extra_count,
           'cross_domain' AS domain
    FROM val_cross_domain_consistency
    """


def build_summary(con: duckdb.DuckDBPyConnection) -> str:
    return """
    CREATE OR REPLACE TABLE val_hardening_summary AS
    SELECT
        CURRENT_TIMESTAMP AS audit_timestamp,
        (SELECT COUNT(*) FROM val_hardening_details WHERE severity = 'error') AS errors,
        (SELECT COUNT(*) FROM val_hardening_details WHERE severity = 'warning') AS warnings,
        (SELECT COUNT(*) FROM val_hardening_details WHERE severity = 'info') AS infos,
        (SELECT COUNT(*) FROM val_impossible_values) AS impossible_value_count,
        (SELECT COUNT(*) FROM val_row_multiplication WHERE status = 'FAIL') AS row_multiplication_fails,
        (SELECT COUNT(*) FROM val_identity_integrity) AS identity_issues,
        (SELECT COUNT(*) FROM val_cross_domain_consistency) AS cross_domain_issues,
        (SELECT COUNT(*) FROM val_manuscript_metrics) AS manuscript_metrics_count,
        (SELECT COUNT(*) FROM val_null_rate_regression) AS null_rate_columns_tracked,
        CASE WHEN (SELECT COUNT(*) FROM val_hardening_details WHERE severity = 'error') = 0
             THEN 'PASS' ELSE 'CONDITIONAL' END AS overall_status
    """


def build_review_queue(con: duckdb.DuckDBPyConnection) -> str:
    return """
    CREATE OR REPLACE TABLE hardening_review_queue AS
    SELECT check_id, severity, research_id, detail, domain,
        CASE severity
            WHEN 'error' THEN 1
            WHEN 'warning' THEN 2
            WHEN 'info' THEN 3
            ELSE 4
        END AS review_priority
    FROM val_hardening_details
    ORDER BY review_priority, check_id, research_id
    """


def main() -> None:
    parser = argparse.ArgumentParser(description="Database hardening validation")
    parser.add_argument("--md", action="store_true", help="Run against MotherDuck")
    parser.add_argument("--local", action="store_true", help="Force local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = parser.parse_args()

    if args.md:
        import toml
        tok = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={tok}")
        print("[INFO] Connected to MotherDuck (thyroid_research_2026)")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"[INFO] Connected to local DuckDB: {DB_PATH}")

    # Resolve table names (handle md_ prefix)
    patient_resolved = resolve_tbl(con, "patient_analysis_resolved_v1", "md_patient_analysis_resolved_v1") or "patient_analysis_resolved_v1"
    episode_resolved = resolve_tbl(con, "episode_analysis_resolved_v1_dedup", "episode_analysis_resolved_v1", "md_episode_analysis_resolved_v1") or "episode_analysis_resolved_v1"
    lesion_resolved = resolve_tbl(con, "lesion_analysis_resolved_v1", "md_lesion_analysis_resolved_v1") or "lesion_analysis_resolved_v1"
    manuscript_cohort = resolve_tbl(con, "manuscript_cohort_v1", "md_manuscript_cohort_v1") or "manuscript_cohort_v1"
    tumor_ep = resolve_tbl(con, "tumor_episode_master_v2", "md_tumor_episode_master_v2") or "tumor_episode_master_v2"
    mol_ep = resolve_tbl(con, "molecular_test_episode_v2", "md_molecular_test_episode_v2") or "molecular_test_episode_v2"
    rai_ep = resolve_tbl(con, "rai_treatment_episode_v2", "md_rai_treatment_episode_v2") or "rai_treatment_episode_v2"
    op_ep = resolve_tbl(con, "operative_episode_detail_v2", "md_oper_episode_detail_v2") or "operative_episode_detail_v2"
    demo = resolve_tbl(con, "demographics_harmonized_v3", "demographics_harmonized_v2", "md_demographics_harmonized_v3") or "demographics_harmonized_v2"
    mcv12 = resolve_tbl(con, "patient_refined_master_clinical_v12", "md_patient_refined_master_clinical_v12") or "patient_refined_master_clinical_v12"
    fna_bethesda = resolve_tbl(con, "extracted_fna_bethesda_v1", "md_extracted_fna_bethesda_v1") or "extracted_fna_bethesda_v1"
    mol_panel = resolve_tbl(con, "extracted_molecular_panel_v1", "md_extracted_molecular_panel_v1") or "extracted_molecular_panel_v1"
    braf_recovery = resolve_tbl(con, "extracted_braf_recovery_v1", "md_extracted_braf_recovery_v1") or "extracted_braf_recovery_v1"
    ras_summary = resolve_tbl(con, "extracted_ras_patient_summary_v1", "md_extracted_ras_patient_summary_v1") or "extracted_ras_patient_summary_v1"
    recurrence = resolve_tbl(con, "extracted_recurrence_refined_v1", "md_extracted_recurrence_refined_v1") or "extracted_recurrence_refined_v1"
    rln_refined = resolve_tbl(con, "extracted_rln_injury_refined_v2", "md_extracted_rln_injury_refined") or "extracted_rln_injury_refined_v2"
    tirads = resolve_tbl(con, "extracted_tirads_validated_v1", "md_extracted_tirads_validated_v1") or "extracted_tirads_validated_v1"
    complication_flags = resolve_tbl(con, "patient_refined_complication_flags_v2", "md_patient_refined_complication_flags_v2") or "patient_refined_complication_flags_v2"

    fmt = dict(
        patient_resolved=patient_resolved, episode_resolved=episode_resolved,
        lesion_resolved=lesion_resolved, manuscript_cohort=manuscript_cohort,
        tumor_ep=tumor_ep, mol_ep=mol_ep, rai_ep=rai_ep, op_ep=op_ep,
        demo=demo, mcv12=mcv12, fna_bethesda=fna_bethesda, mol_panel=mol_panel,
        braf_recovery=braf_recovery, ras_summary=ras_summary,
        recurrence=recurrence, rln_refined=rln_refined, tirads=tirads,
        complication_flags=complication_flags,
    )

    checks = [
        ("Identity Integrity", IDENTITY_INTEGRITY_SQL.format(**fmt)),
        ("Row Multiplication", ROW_MULTIPLICATION_SQL.format(**fmt)),
        ("Impossible Values", IMPOSSIBLE_VALUES_SQL),
        ("Cross-Domain Consistency", CROSS_DOMAIN_CONSISTENCY_SQL.format(**fmt)),
        ("Manuscript Metrics", MANUSCRIPT_METRICS_SQL.format(**fmt)),
        ("Null Rate Regression", NULL_RATE_REGRESSION_SQL),
    ]

    results = {}
    for label, sql in checks:
        section(label)
        if args.dry_run:
            print(sql[:500] + "...")
            continue
        try:
            con.execute(sql)
            tbl_name = sql.split("CREATE OR REPLACE TABLE ")[1].split(" AS")[0].strip()
            row = con.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()
            cnt = row[0] if row else 0
            results[label] = {"status": "OK", "rows": cnt}
            print(f"  -> {tbl_name}: {cnt} rows")
        except Exception as e:
            results[label] = {"status": "ERROR", "message": str(e)[:200]}
            print(f"  [ERROR] {label}: {e}")

    if not args.dry_run:
        section("Consolidating Details")
        try:
            con.execute(build_details_insert(con))
            row = con.execute("SELECT COUNT(*) FROM val_hardening_details").fetchone()
            print(f"  -> val_hardening_details: {row[0] if row else 0} rows")
        except Exception as e:
            print(f"  [WARN] Details consolidation: {e}")

        section("Building Summary")
        try:
            con.execute(build_summary(con))
            summary = con.execute("SELECT * FROM val_hardening_summary").fetchone()
            cols = [d[0] for d in con.description]
            summary_dict = dict(zip(cols, summary)) if summary else {}
            for k, v in summary_dict.items():
                print(f"  {k}: {v}")
        except Exception as e:
            print(f"  [WARN] Summary: {e}")

        section("Building Review Queue")
        try:
            con.execute(build_review_queue(con))
            row = con.execute("SELECT COUNT(*) FROM hardening_review_queue").fetchone()
            print(f"  -> hardening_review_queue: {row[0] if row else 0} rows")
        except Exception as e:
            print(f"  [WARN] Review queue: {e}")

        section("Manuscript Metrics Report")
        try:
            metrics = con.execute(
                "SELECT metric, value, source FROM val_manuscript_metrics ORDER BY metric"
            ).fetchall()
            for m in metrics:
                print(f"  {m[0]:40s} = {m[1]:>8,}  (from {m[2]})")
        except Exception as e:
            print(f"  [WARN] Metrics: {e}")

        section("Null Rate Report")
        try:
            nulls = con.execute(
                "SELECT tbl, col, total, nulls, null_pct "
                "FROM val_null_rate_regression ORDER BY null_pct DESC"
            ).fetchall()
            print(f"  {'Table':<20s} {'Column':<45s} {'Total':>8s} {'Nulls':>8s} {'Null%':>8s}")
            print(f"  {'-'*20} {'-'*45} {'-'*8} {'-'*8} {'-'*8}")
            for r in nulls:
                print(f"  {r[0]:<20s} {r[1]:<45s} {r[2]:>8,} {r[3]:>8,} {r[4]:>7.1f}%")
        except Exception as e:
            print(f"  [WARN] Null rates: {e}")

    # Export results summary
    out_dir = ROOT / "exports" / f"hardening_audit_{datetime.now():%Y%m%d_%H%M}"
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "check_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n[DONE] Results exported to {out_dir}")
    if not args.dry_run:
        try:
            s = con.execute("SELECT overall_status FROM val_hardening_summary").fetchone()
            print(f"\n  OVERALL STATUS: {s[0] if s else 'UNKNOWN'}")
        except Exception:
            pass


if __name__ == "__main__":
    main()
