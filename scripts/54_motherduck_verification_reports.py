#!/usr/bin/env python3
"""
54_motherduck_verification_reports.py -- MotherDuck verification + QA reports

Runs comprehensive verification queries against MotherDuck (or local DuckDB)
for all tables created by the analysis-grade optimization pipeline (scripts 48-53).

Verification categories:
  1. Row count audit -- before/after for all new tables vs source tables
  2. Null/missingness -- per-column NULL rate for patient_analysis_resolved_v1
  3. Linkage quality -- score distributions and tier breakdowns (v3 linkage)
  4. Ambiguity counts -- multi-candidate linkage counts by pair type
  5. Score calculability -- % of patients with each scoring system calculable
  6. Complication phenotype -- raw vs confirmed counts per entity
  7. Recurrence/outcome -- structural vs biochemical event counts
  8. Date precedence -- specimen_collect_dt usage rate vs fallback
  9. Concordance checks -- resolved layer vs source tables
  10. Duplicate checks -- confirm 0 duplicates in patient-level tables
  11. Score distributions -- MACIS/AJCC8/ATA histograms

Outputs reports to: exports/verification_reports/
  - linkage_quality_report.md
  - scoring_coverage_report.md
  - complication_definition_report.md
  - resolved_layer_data_dictionary.csv
  - analysis_grade_cohort_verification_report.md

Supports --md, --local, --dry-run flags.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "exports" / "verification_reports"
EXPORTS.mkdir(parents=True, exist_ok=True)

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
    return duckdb.connect(str(ROOT / "thyroid_master.duckdb"))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def safe_query(con: duckdb.DuckDBPyConnection, sql: str,
               fallback=None) -> object:
    try:
        return con.execute(sql).fetchdf()
    except Exception as exc:
        print(f"  [WARN] Query failed: {exc}")
        return fallback


def write_md_report(path: Path, sections: list[tuple[str, str]]) -> None:
    with open(path, "w") as f:
        f.write(f"# {path.stem.replace('_', ' ').title()}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        for heading, body in sections:
            f.write(f"## {heading}\n\n")
            f.write(body)
            f.write("\n\n")
    print(f"  Report: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Row count audit
# ─────────────────────────────────────────────────────────────────────────────
NEW_TABLES = [
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1",
    "lesion_analysis_resolved_v1",
    "thyroid_scoring_systems_v1",
    "imaging_nodule_master_v1",
    "imaging_exam_master_v1",
    "imaging_patient_summary_v1",
    "imaging_fna_linkage_v3",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
    "pathology_rai_linkage_v3",
    "linkage_summary_v3",
    "linkage_ambiguity_review_v1",
    "complication_phenotype_v1",
    "complication_patient_summary_v1",
    "complication_discrepancy_report_v1",
    "longitudinal_lab_clean_v1",
    "longitudinal_lab_patient_summary_v1",
    "recurrence_event_clean_v1",
    "val_scoring_systems",
]

SOURCE_TABLES = [
    "tumor_episode_master_v2",
    "operative_episode_detail_v2",
    "fna_episode_master_v2",
    "molecular_test_episode_v2",
    "rai_treatment_episode_v2",
    "imaging_nodule_long_v2",
    "imaging_fna_linkage_v2",
    "fna_molecular_linkage_v2",
    "preop_surgery_linkage_v2",
    "surgery_pathology_linkage_v2",
    "pathology_rai_linkage_v2",
    "patient_refined_master_clinical_v12",
    "extracted_tirads_validated_v1",
]


def row_count_audit(con: duckdb.DuckDBPyConnection) -> str:
    lines = ["| Table | Rows | Status |", "|-------|------|--------|"]
    for tbl in NEW_TABLES + SOURCE_TABLES:
        try:
            r = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            status = "NEW" if tbl in NEW_TABLES else "SOURCE"
            lines.append(f"| `{tbl}` | {r:,} | {status} |")
        except Exception:
            lines.append(f"| `{tbl}` | - | NOT FOUND |")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Null/missingness for patient_analysis_resolved_v1
# ─────────────────────────────────────────────────────────────────────────────
def null_audit(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "patient_analysis_resolved_v1"):
        return "_patient_analysis_resolved_v1 not found_"
    try:
        cols = [r[0] for r in con.execute(
            "SELECT DISTINCT column_name FROM information_schema.columns "
            "WHERE table_name='patient_analysis_resolved_v1' AND table_schema='main' "
            "ORDER BY column_name"
        ).fetchall()]
        total = con.execute(
            "SELECT COUNT(*) FROM patient_analysis_resolved_v1"
        ).fetchone()[0]

        lines = ["| Column | NULL Count | NULL % |",
                 "|--------|-----------|--------|"]
        for col in cols:
            try:
                n = con.execute(
                    f"SELECT COUNT(*) FROM patient_analysis_resolved_v1 "
                    f"WHERE {col} IS NULL"
                ).fetchone()[0]
                pct = round(100 * n / max(total, 1), 1)
                lines.append(f"| `{col}` | {n:,} | {pct}% |")
            except Exception:
                lines.append(f"| `{col}` | - | - |")
        return "\n".join(lines)
    except Exception as e:
        return f"_Error: {e}_"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Linkage quality
# ─────────────────────────────────────────────────────────────────────────────
def linkage_quality(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "linkage_summary_v3"):
        return "_linkage_summary_v3 not found_"

    df = safe_query(con, "SELECT * FROM linkage_summary_v3")
    if df is None:
        return "_Query failed_"

    lines = [df.to_markdown(index=False) if hasattr(df, "to_markdown")
             else df.to_string(index=False)]

    # Score distribution per linkage type
    for tbl in ["imaging_fna_linkage_v3", "fna_molecular_linkage_v3",
                "preop_surgery_linkage_v3"]:
        if table_available(con, tbl):
            df2 = safe_query(con, f"""
                SELECT
                    FLOOR(linkage_score * 10) / 10 AS score_bucket,
                    COUNT(*) AS n
                FROM {tbl}
                GROUP BY 1 ORDER BY 1
            """)
            if df2 is not None:
                lines.append(f"\n**Score distribution: `{tbl}`**\n")
                lines.append(df2.to_string(index=False))

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Ambiguity counts
# ─────────────────────────────────────────────────────────────────────────────
def ambiguity_audit(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "linkage_ambiguity_review_v1"):
        return "_linkage_ambiguity_review_v1 not found_"

    df = safe_query(con, """
        SELECT linkage_type,
               COUNT(DISTINCT research_id) AS n_patients,
               COUNT(*) AS n_ambiguous_links,
               ROUND(AVG(n_candidates), 1) AS avg_candidates,
               MAX(n_candidates) AS max_candidates
        FROM linkage_ambiguity_review_v1
        GROUP BY linkage_type ORDER BY n_ambiguous_links DESC
    """)
    return df.to_string(index=False) if df is not None else "_Query failed_"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Score calculability
# ─────────────────────────────────────────────────────────────────────────────
def score_coverage(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "thyroid_scoring_systems_v1"):
        return "_thyroid_scoring_systems_v1 not found_"

    df = safe_query(con, """
        SELECT
            COUNT(*) AS total,
            ROUND(100.0*SUM(CASE WHEN ajcc8_calculable_flag THEN 1 ELSE 0 END)/COUNT(*),1)
                AS ajcc8_pct,
            ROUND(100.0*SUM(CASE WHEN ata_calculable_flag THEN 1 ELSE 0 END)/COUNT(*),1)
                AS ata_pct,
            ROUND(100.0*SUM(CASE WHEN macis_calculable_flag THEN 1 ELSE 0 END)/COUNT(*),1)
                AS macis_pct,
            ROUND(100.0*SUM(CASE WHEN ages_calculable_flag THEN 1 ELSE 0 END)/COUNT(*),1)
                AS ages_pct,
            ROUND(100.0*SUM(CASE WHEN ames_calculable_flag THEN 1 ELSE 0 END)/COUNT(*),1)
                AS ames_pct
        FROM thyroid_scoring_systems_v1
    """)

    stage_dist = safe_query(con, """
        SELECT ajcc8_stage_group AS stage, COUNT(*) AS n,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) AS pct
        FROM thyroid_scoring_systems_v1
        WHERE ajcc8_stage_group IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)

    ata_dist = safe_query(con, """
        SELECT ata_risk_category AS ata_risk, COUNT(*) AS n,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) AS pct
        FROM thyroid_scoring_systems_v1
        WHERE ata_risk_category IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)

    macis_dist = safe_query(con, """
        SELECT macis_risk_group, COUNT(*) AS n,
               ROUND(MIN(macis_score),2) AS min_score,
               ROUND(AVG(macis_score),2) AS mean_score,
               ROUND(MAX(macis_score),2) AS max_score
        FROM thyroid_scoring_systems_v1
        WHERE macis_score IS NOT NULL
        GROUP BY 1 ORDER BY mean_score
    """)

    lines = ["**Calculability rates:**\n"]
    if df is not None:
        lines.append(df.to_string(index=False))
    if stage_dist is not None:
        lines.append("\n**AJCC8 Stage Distribution:**\n")
        lines.append(stage_dist.to_string(index=False))
    if ata_dist is not None:
        lines.append("\n**ATA Risk Distribution:**\n")
        lines.append(ata_dist.to_string(index=False))
    if macis_dist is not None:
        lines.append("\n**MACIS Risk Group Distribution:**\n")
        lines.append(macis_dist.to_string(index=False))

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Complication phenotype
# ─────────────────────────────────────────────────────────────────────────────
def complication_report(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "complication_discrepancy_report_v1"):
        return "_complication_discrepancy_report_v1 not found_"

    df = safe_query(con, "SELECT * FROM complication_discrepancy_report_v1")
    return df.to_string(index=False) if df is not None else "_Query failed_"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Recurrence / outcome
# ─────────────────────────────────────────────────────────────────────────────
def recurrence_audit(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "recurrence_event_clean_v1"):
        return "_recurrence_event_clean_v1 not found_"

    df = safe_query(con, """
        SELECT
            recurrence_type,
            recurrence_definition,
            COUNT(DISTINCT research_id) AS n_patients,
            COUNT(*) AS n_events
        FROM recurrence_event_clean_v1
        GROUP BY 1, 2 ORDER BY n_patients DESC
    """)
    return df.to_string(index=False) if df is not None else "_Query failed_"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Date precedence
# ─────────────────────────────────────────────────────────────────────────────
def date_precedence_audit(con: duckdb.DuckDBPyConnection) -> str:
    if not table_available(con, "longitudinal_lab_clean_v1"):
        return "_longitudinal_lab_clean_v1 not found_"

    df = safe_query(con, """
        SELECT
            lab_type,
            source_table,
            date_source,
            COUNT(*) AS n,
            ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY lab_type), 1) AS pct
        FROM longitudinal_lab_clean_v1
        GROUP BY 1, 2, 3 ORDER BY lab_type, n DESC
    """)
    return df.to_string(index=False) if df is not None else "_Query failed_"


# ─────────────────────────────────────────────────────────────────────────────
# 9. Concordance check
# ─────────────────────────────────────────────────────────────────────────────
def concordance_check(con: duckdb.DuckDBPyConnection) -> str:
    checks = []

    # histology_final vs tumor_episode_master_v2
    if table_available(con, "patient_analysis_resolved_v1") and \
       table_available(con, "tumor_episode_master_v2"):
        df = safe_query(con, """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN LOWER(COALESCE(r.histology_final,''))
                              = LOWER(COALESCE(t.primary_histology,''))
                         THEN 1 ELSE 0 END) AS concordant,
                SUM(CASE WHEN r.histology_final IS NULL THEN 1 ELSE 0 END) AS null_in_resolved
            FROM patient_analysis_resolved_v1 r
            LEFT JOIN (
                SELECT research_id, primary_histology
                FROM tumor_episode_master_v2
                QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id
                    ORDER BY tumor_ordinal, surgery_date) = 1
            ) t USING (research_id)
        """)
        if df is not None:
            checks.append(f"**histology_final concordance with tumor_episode_master_v2:**\n"
                          f"{df.to_string(index=False)}")

    # braf_positive_final concordance
    if table_available(con, "patient_analysis_resolved_v1") and \
       table_available(con, "thyroid_scoring_systems_v1"):
        df = safe_query(con, """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN LOWER(CAST(r.braf_positive_final AS VARCHAR))
                              = LOWER(CAST(s.braf_positive AS VARCHAR))
                         THEN 1 ELSE 0 END) AS concordant
            FROM patient_analysis_resolved_v1 r
            JOIN thyroid_scoring_systems_v1 s USING (research_id)
        """)
        if df is not None:
            checks.append(f"**braf_positive_final concordance with scoring table:**\n"
                          f"{df.to_string(index=False)}")

    return "\n\n".join(checks) if checks else "_No concordance checks run_"


# ─────────────────────────────────────────────────────────────────────────────
# 10. Duplicate check
# ─────────────────────────────────────────────────────────────────────────────
def duplicate_check(con: duckdb.DuckDBPyConnection) -> str:
    results = []

    patient_tables = {
        "patient_analysis_resolved_v1": "research_id",
        "thyroid_scoring_systems_v1": "research_id",
        "complication_patient_summary_v1": "research_id",
        "longitudinal_lab_patient_summary_v1": "research_id",
        "imaging_patient_summary_v1": "research_id",
    }

    for tbl, pk in patient_tables.items():
        if table_available(con, tbl):
            try:
                total = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                dupes = con.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT {pk} FROM {tbl} "
                    f"GROUP BY {pk} HAVING COUNT(*) > 1)"
                ).fetchone()[0]
                status = "PASS" if dupes == 0 else f"FAIL ({dupes} duplicates)"
                results.append(f"| `{tbl}` | {total:,} | {status} |")
            except Exception as e:
                results.append(f"| `{tbl}` | - | ERROR: {e} |")
        else:
            results.append(f"| `{tbl}` | - | NOT FOUND |")

    return "| Table | Rows | Duplicate Check |\n|-------|------|------------------|\n" + \
           "\n".join(results)


# ─────────────────────────────────────────────────────────────────────────────
# Generate resolved layer data dictionary
# ─────────────────────────────────────────────────────────────────────────────
def generate_data_dictionary(con: duckdb.DuckDBPyConnection) -> None:
    """Write CSV data dictionary for patient_analysis_resolved_v1."""
    if not table_available(con, "patient_analysis_resolved_v1"):
        print("  [SKIP] data dictionary -- patient_analysis_resolved_v1 not found")
        return

    try:
        cols = con.execute("""
            SELECT DISTINCT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'patient_analysis_resolved_v1'
            AND table_schema = 'main'
            ORDER BY column_name
        """).fetchall()

        total = con.execute(
            "SELECT COUNT(*) FROM patient_analysis_resolved_v1"
        ).fetchone()[0]

        outpath = EXPORTS / "resolved_layer_data_dictionary.csv"
        with open(outpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["column_name", "data_type", "null_count",
                             "null_pct", "domain", "notes"])
            for col_name, dtype in cols:
                try:
                    n_null = con.execute(
                        f"SELECT COUNT(*) FROM patient_analysis_resolved_v1 "
                        f"WHERE {col_name} IS NULL"
                    ).fetchone()[0]
                    null_pct = round(100 * n_null / max(total, 1), 1)
                except Exception:
                    n_null = -1
                    null_pct = -1

                # Infer domain from column name prefix
                domain = "metadata"
                for prefix in ["demo", "path", "ete", "vascular", "margin", "ln",
                                "mol", "braf", "ras", "tert", "fna", "imaging",
                                "surg", "rai", "ajcc8", "ata", "macis", "ages",
                                "ames", "hypocalcemia", "rln", "hematoma", "seroma",
                                "chyle", "wound", "tg", "anti_tg", "tsh", "pth",
                                "calcium", "recurrence", "provenance", "lab",
                                "scoring", "analysis"]:
                    if col_name.startswith(prefix):
                        domain = prefix
                        break

                writer.writerow([col_name, dtype, n_null, null_pct, domain, ""])
        print(f"  Data dictionary: {outpath}")
    except Exception as e:
        print(f"  [WARN] Data dictionary failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main report assembly
# ─────────────────────────────────────────────────────────────────────────────

def run_all_verifications(con: duckdb.DuckDBPyConnection,
                          dry_run: bool = False) -> None:
    if dry_run:
        print("  [DRY-RUN] Would run all verification queries and write reports")
        return

    section("1. Row count audit")
    row_counts = row_count_audit(con)
    print(row_counts[:2000])

    section("2. Duplicate check")
    dup_result = duplicate_check(con)
    print(dup_result)

    section("3. Score calculability")
    score_result = score_coverage(con)
    print(score_result[:3000])

    section("4. Linkage quality")
    link_result = linkage_quality(con)
    print(link_result[:3000])

    section("5. Ambiguity audit")
    amb_result = ambiguity_audit(con)
    print(amb_result)

    section("6. Complication phenotype")
    comp_result = complication_report(con)
    print(comp_result)

    section("7. Recurrence events")
    rec_result = recurrence_audit(con)
    print(rec_result)

    section("8. Date precedence")
    date_result = date_precedence_audit(con)
    print(date_result)

    section("9. Concordance")
    conc_result = concordance_check(con)
    print(conc_result[:2000])

    # Generate data dictionary
    section("Generating data dictionary CSV")
    generate_data_dictionary(con)

    # Null audit (write to file only -- too wide to print)
    section("Writing null audit report")
    null_result = null_audit(con)

    # Write reports
    write_md_report(EXPORTS / "linkage_quality_report.md", [
        ("Row Count Audit", row_counts),
        ("Linkage Quality (v3)", link_result),
        ("Ambiguity Counts", amb_result),
    ])

    write_md_report(EXPORTS / "scoring_coverage_report.md", [
        ("Score Calculability", score_result),
        ("Concordance Checks", conc_result),
        ("Duplicate Checks", dup_result),
    ])

    write_md_report(EXPORTS / "complication_definition_report.md", [
        ("Complication Phenotype", comp_result),
        ("Date Precedence Audit", date_result),
    ])

    write_md_report(EXPORTS / "analysis_grade_cohort_verification_report.md", [
        ("Executive Summary", f"Verification run: {datetime.now().isoformat()}\n"
                              f"Environment: {'MotherDuck' if 'md:' in str(con) else 'local'}"),
        ("Row Counts", row_counts),
        ("Duplicate Checks", dup_result),
        ("Null/Missingness", null_result),
        ("Score Calculability", score_result),
        ("Linkage Quality", link_result),
        ("Complication Phenotype", comp_result),
        ("Recurrence Events", rec_result),
        ("Date Precedence", date_result),
        ("Concordance", conc_result),
    ])

    print(f"\n  Reports written to: {EXPORTS}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="54_motherduck_verification_reports.py"
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--dry-run", action="store_true", help="Print plan, no reports")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        run_all_verifications(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 54_motherduck_verification_reports.py finished")


if __name__ == "__main__":
    main()
