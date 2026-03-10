#!/usr/bin/env python3
"""
20_manuscript_exports.py — Phase K: Analysis-Ready Cohort Export Layer

Creates manuscript-ready views that prefer reviewer-adjudicated values
when available, falling back to algorithmic analysis-eligible values.

Views created:
  - manuscript_histology_cohort_v
  - manuscript_molecular_cohort_v
  - manuscript_rai_cohort_v
  - manuscript_patient_summary_v

Export bundle (with --export flag):
  - CSV per cohort
  - Parquet per cohort
  - manifest.json with provenance metadata

Depends on: scripts 15-19
Falls back to *_analysis_cohort_v views if post-review views don't exist.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
SQL_OUT = ROOT / "scripts" / "20_manuscript_export_views.sql"

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
#  MANUSCRIPT VIEWS
# ═══════════════════════════════════════════════════════════════════════════════

def _histology_sql(has_post_review: bool) -> str:
    source = "histology_post_review_v" if has_post_review else "histology_analysis_cohort_v"

    if has_post_review:
        return f"""
CREATE OR REPLACE VIEW manuscript_histology_cohort_v AS
SELECT
    research_id,
    op_seq,
    pathology_date,
    effective_histology AS histology_for_analysis,
    effective_t_stage AS t_stage_for_analysis,
    effective_n_stage AS n_stage_for_analysis,
    algorithmic_variant AS variant_for_analysis,
    effective_eligible AS analysis_inclusion_flag,
    CASE
        WHEN NOT effective_eligible THEN discordance_type
        ELSE NULL
    END AS exclusion_reason,
    value_source AS algorithmic_vs_reviewer_source,
    adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    -- Raw provenance
    source_histology_raw_ps,
    source_histology_raw_tp,
    t_stage_source_path,
    t_stage_source_note,
    reconciliation_status
FROM {source}
WHERE effective_eligible = TRUE;
"""
    else:
        return f"""
CREATE OR REPLACE VIEW manuscript_histology_cohort_v AS
SELECT
    research_id,
    op_seq,
    pathology_date,
    final_histology_for_analysis AS histology_for_analysis,
    final_t_stage_for_analysis AS t_stage_for_analysis,
    final_n_stage_for_analysis AS n_stage_for_analysis,
    final_variant_for_analysis AS variant_for_analysis,
    analysis_eligible_flag AS analysis_inclusion_flag,
    CASE
        WHEN NOT analysis_eligible_flag THEN discordance_type
        ELSE NULL
    END AS exclusion_reason,
    'algorithmic' AS algorithmic_vs_reviewer_source,
    FALSE AS adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    source_histology_raw_ps,
    source_histology_raw_tp,
    t_stage_source_path,
    t_stage_source_note,
    reconciliation_status
FROM {source}
WHERE analysis_eligible_flag = TRUE;
"""


def _molecular_sql(has_post_review: bool) -> str:
    source = "molecular_post_review_v" if has_post_review else "molecular_analysis_cohort_v"

    if has_post_review:
        return f"""
CREATE OR REPLACE VIEW manuscript_molecular_cohort_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    molecular_episode_id,
    specimen_date_raw,
    platform_normalized,
    test_name_raw,
    result_category_normalized,
    result_summary_raw,
    temporal_linkage_confidence,
    platform_confidence,
    pathology_concordance_confidence,
    overall_linkage_confidence,
    effective_eligible AS analysis_inclusion_flag,
    CASE
        WHEN NOT effective_eligible THEN 'ineligible_after_review'
        ELSE NULL
    END AS exclusion_reason,
    value_source AS algorithmic_vs_reviewer_source,
    adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    molecular_date_raw_class,
    linkage_method,
    high_risk_molecular_flag
FROM {source}
WHERE effective_eligible = TRUE;
"""
    else:
        return f"""
CREATE OR REPLACE VIEW manuscript_molecular_cohort_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    molecular_episode_id,
    specimen_date_raw,
    platform_normalized,
    test_name_raw,
    result_category_normalized,
    result_summary_raw,
    temporal_linkage_confidence,
    platform_confidence,
    pathology_concordance_confidence,
    overall_linkage_confidence,
    TRUE AS analysis_inclusion_flag,
    NULL AS exclusion_reason,
    'algorithmic' AS algorithmic_vs_reviewer_source,
    FALSE AS adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    molecular_date_raw_class,
    linkage_method,
    high_risk_molecular_flag
FROM {source};
"""


def _rai_sql(has_post_review: bool) -> str:
    source = "rai_post_review_v" if has_post_review else "rai_analysis_cohort_v"

    if has_post_review:
        return f"""
CREATE OR REPLACE VIEW manuscript_rai_cohort_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    rai_episode_id,
    rai_date,
    dose_mci,
    rai_term_normalized,
    rai_assertion_status,
    rai_treatment_certainty,
    rai_interval_class,
    effective_eligible AS analysis_inclusion_flag,
    CASE
        WHEN NOT effective_eligible THEN 'ineligible_after_review'
        ELSE NULL
    END AS exclusion_reason,
    value_source AS algorithmic_vs_reviewer_source,
    adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    linked_surgery_date,
    days_surgery_to_rai,
    post_thyroidectomy_flag
FROM {source}
WHERE effective_eligible = TRUE;
"""
    else:
        return f"""
CREATE OR REPLACE VIEW manuscript_rai_cohort_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    rai_episode_id,
    rai_date,
    dose_mci,
    rai_term_normalized,
    rai_assertion_status,
    rai_treatment_certainty,
    rai_interval_class,
    TRUE AS analysis_inclusion_flag,
    NULL AS exclusion_reason,
    'algorithmic' AS algorithmic_vs_reviewer_source,
    FALSE AS adjudication_applied_flag,
    CURRENT_DATE AS analysis_ready_date,
    linked_surgery_date,
    days_surgery_to_rai,
    post_thyroidectomy_flag
FROM {source};
"""


PATIENT_SUMMARY_SQL = """
CREATE OR REPLACE VIEW manuscript_patient_summary_v AS
WITH hist AS (
    SELECT research_id,
           COUNT(*) AS histology_records,
           MAX(adjudication_applied_flag::INT) > 0 AS histology_adjudicated
    FROM manuscript_histology_cohort_v
    GROUP BY research_id
),
mol AS (
    SELECT research_id,
           COUNT(*) AS molecular_records,
           MAX(adjudication_applied_flag::INT) > 0 AS molecular_adjudicated
    FROM manuscript_molecular_cohort_v
    GROUP BY research_id
),
rai AS (
    SELECT research_id,
           COUNT(*) AS rai_records,
           MAX(adjudication_applied_flag::INT) > 0 AS rai_adjudicated
    FROM manuscript_rai_cohort_v
    GROUP BY research_id
),
all_ids AS (
    SELECT research_id FROM hist
    UNION SELECT research_id FROM mol
    UNION SELECT research_id FROM rai
)
SELECT
    a.research_id,
    COALESCE(h.histology_records, 0) AS histology_records,
    COALESCE(m.molecular_records, 0) AS molecular_records,
    COALESCE(r.rai_records, 0) AS rai_records,
    COALESCE(h.histology_adjudicated, FALSE) AS histology_adjudicated,
    COALESCE(m.molecular_adjudicated, FALSE) AS molecular_adjudicated,
    COALESCE(r.rai_adjudicated, FALSE) AS rai_adjudicated,
    (COALESCE(h.histology_records, 0) > 0)::INT
      + (COALESCE(m.molecular_records, 0) > 0)::INT
      + (COALESCE(r.rai_records, 0) > 0)::INT AS domains_with_data,
    CURRENT_DATE AS analysis_ready_date
FROM all_ids a
LEFT JOIN hist h ON a.research_id = h.research_id
LEFT JOIN mol m ON a.research_id = m.research_id
LEFT JOIN rai r ON a.research_id = r.research_id;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  EXPORT BUNDLE
# ═══════════════════════════════════════════════════════════════════════════════

def export_bundle(con: duckdb.DuckDBPyConnection) -> None:
    section("GENERATING EXPORT BUNDLE")

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    export_dir = ROOT / "exports" / f"manuscript_cohort_{ts}"
    export_dir.mkdir(parents=True, exist_ok=True)

    cohorts = {
        "manuscript_histology_cohort": "manuscript_histology_cohort_v",
        "manuscript_molecular_cohort": "manuscript_molecular_cohort_v",
        "manuscript_rai_cohort": "manuscript_rai_cohort_v",
        "manuscript_patient_summary": "manuscript_patient_summary_v",
    }

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "repo_commit_sha": _get_git_sha(),
        "source_scripts": ["15", "16", "17", "18", "19", "20"],
        "export_directory": str(export_dir),
        "cohorts": {},
        "unresolved_burdens": {},
    }

    for prefix, view in cohorts.items():
        if not table_available(con, view):
            print(f"  SKIP {view} (not available)")
            continue

        df = con.execute(f"SELECT * FROM {view}").fetchdf()
        csv_path = export_dir / f"{prefix}.csv"
        pq_path = export_dir / f"{prefix}.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)

        adj_count = 0
        if "adjudication_applied_flag" in df.columns:
            adj_count = int(df["adjudication_applied_flag"].sum())

        manifest["cohorts"][prefix] = {
            "rows": len(df),
            "adjudicated": adj_count,
        }
        print(f"  {prefix:<45} {len(df):>8,} rows → CSV + Parquet")

    # Unresolved burden counts
    burden_queries = {
        "histology_needing_review": "SELECT COUNT(*) FROM histology_manual_review_queue_v",
        "molecular_low_confidence": "SELECT COUNT(*) FROM molecular_episode_v3 WHERE overall_linkage_confidence < 50 AND NOT unresolved_flag",
        "rai_ambiguous": "SELECT COUNT(*) FROM rai_episode_v3 WHERE rai_assertion_status = 'ambiguous'",
        "timeline_errors": "SELECT COUNT(*) FROM timeline_manual_review_queue_v WHERE priority_score >= 70",
    }
    for key, sql in burden_queries.items():
        try:
            manifest["unresolved_burdens"][key] = con.execute(sql).fetchone()[0]
        except Exception:
            manifest["unresolved_burdens"][key] = -1

    manifest_path = export_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"\n  Manifest → {manifest_path}")
    print(f"  Bundle   → {export_dir}")


def _get_git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


# ═══════════════════════════════════════════════════════════════════════════════
#  SQL FILE OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

def write_sql_file(view_log: list[tuple[str, str]]) -> None:
    with open(SQL_OUT, "w") as f:
        f.write("-- Manuscript Export Views\n")
        f.write("-- Generated by 20_manuscript_exports.py\n")
        f.write("-- Depends on: scripts 15-19\n\n")
        for name, sql in view_log:
            f.write(f"-- === {name} ===\n")
            f.write(sql.strip())
            f.write("\n\n")
    print(f"  SQL saved to: {SQL_OUT}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase K: Manuscript Export Layer"
    )
    parser.add_argument("--md", action="store_true",
                        help="Use MotherDuck instead of local DuckDB")
    parser.add_argument("--export", action="store_true",
                        help="Generate export bundle (CSV + Parquet + manifest)")
    args = parser.parse_args()

    print("=" * 80)
    print("  MANUSCRIPT EXPORT LAYER — Phase K")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    # Check prerequisites
    section("CHECKING PREREQUISITES")
    has_post_review = True
    for v in ["histology_post_review_v", "molecular_post_review_v", "rai_post_review_v"]:
        avail = table_available(con, v)
        status = "OK" if avail else "MISS"
        if not avail:
            has_post_review = False
        print(f"  {status:<6} {v}")

    if has_post_review:
        print("\n  Using POST-REVIEW views (reviewer decisions will be preferred)")
    else:
        print("\n  Post-review views missing; falling back to algorithmic cohorts")

    for v in ["histology_analysis_cohort_v", "molecular_analysis_cohort_v",
              "rai_analysis_cohort_v"]:
        status = "OK" if table_available(con, v) else "MISS"
        print(f"  {status:<6} {v}")

    view_log: list[tuple[str, str]] = []

    # Create manuscript views
    section("CREATING MANUSCRIPT VIEWS")
    deploy_view(con, "manuscript_histology_cohort_v",
                _histology_sql(has_post_review), view_log)
    deploy_view(con, "manuscript_molecular_cohort_v",
                _molecular_sql(has_post_review), view_log)
    deploy_view(con, "manuscript_rai_cohort_v",
                _rai_sql(has_post_review), view_log)
    deploy_view(con, "manuscript_patient_summary_v",
                PATIENT_SUMMARY_SQL, view_log)

    # Write SQL file
    section("WRITING SQL FILE")
    write_sql_file(view_log)

    # Export if requested
    if args.export:
        export_bundle(con)

    # Summary
    section("OBJECTS CREATED")
    for name, _ in view_log:
        print(f"  {name}")

    section("DEPLOYMENT ORDER")
    print("  1. scripts/15_date_association_audit.py")
    print("  2. scripts/16_reconciliation_v2.py")
    print("  3. scripts/17_semantic_cleanup_v3.py")
    print("  4. scripts/18_adjudication_framework.py")
    print("  5. scripts/19_reviewer_persistence.py")
    print("  6. scripts/20_manuscript_exports.py  <-- this script")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Manuscript Export Layer complete")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
