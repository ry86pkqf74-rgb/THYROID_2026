#!/usr/bin/env python3
"""
56_pre_manuscript_audit.py -- Clinical face-validity audit for manuscript readiness

Evaluates whether the analysis-grade resolved layer outputs are clinically
believable and publication-ready. Goes beyond structural validation (script 55)
to check distributions, rates, calculability, and provisional flags.

Checks:
  1.  Histology distribution by year and overall
  2.  AJCC8 stage distribution by age group (<55 vs >=55)
  3.  ATA risk vs gross ETE / LN burden
  4.  RAI usage rates by ATA risk
  5.  Complication rates by procedure type
  6.  Recurrence rates by follow-up length
  7.  Score calculability rates (AJCC8, ATA, MACIS, AGES, AMES)
  8.  Core variable missingness
  9.  Provisional definition counts
  10. Ambiguous linkage burden
  11. Imaging data limitations
  12. Molecular field sanity (RAS flag bug neutralization)

Output:
  exports/verification_reports/pre_manuscript_audit_report.md

Supports --md, --local, --dry-run.
"""
from __future__ import annotations

import argparse
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
    raise RuntimeError("MOTHERDUCK_TOKEN not set.")


def connect_md() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        f"md:thyroid_research_2026?motherduck_token={_get_token()}"
    )


def connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(ROOT / "thyroid_master.duckdb"))


def tbl_ok(con, tbl):
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def q(con, sql, label=""):
    try:
        return con.execute(sql).fetchdf()
    except Exception as exc:
        print(f"  [WARN] {label or 'query'}: {exc}")
        return None


def fmt(df):
    if df is None:
        return "_Query returned no data._"
    if hasattr(df, "to_markdown"):
        return df.to_markdown(index=False)
    return df.to_string(index=False)


# ─────────────────────────────────────────────────────────────────────────────
# Audit checks
# ─────────────────────────────────────────────────────────────────────────────

def check_histology_distribution(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            COALESCE(histology_final, 'NULL/missing') AS histology,
            COUNT(*) AS n,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM {tbl}
        WHERE analysis_eligible_flag
        GROUP BY 1 ORDER BY n DESC
    """, "histology")
    by_year = q(con, f"""
        SELECT
            EXTRACT(YEAR FROM first_surgery_date) AS year,
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE histology_final IS NOT NULL) AS with_histology
        FROM {tbl}
        WHERE first_surgery_date IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """, "histology_by_year")
    parts = ["**Overall histology (analysis-eligible):**\n", fmt(df)]
    if by_year is not None:
        parts += ["\n\n**Cases by surgery year:**\n", fmt(by_year)]
    return "\n".join(parts)


def check_ajcc8_distribution(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            CASE WHEN age_at_surgery < 55 THEN '<55' ELSE '>=55' END AS age_group,
            ajcc8_stage_group AS stage,
            COUNT(*) AS n
        FROM {tbl}
        WHERE scoring_ajcc8_flag AND ajcc8_stage_group IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2
    """, "ajcc8")
    return fmt(df)


def check_ata_risk(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            ata_risk_category,
            COUNT(*) AS n,
            SUM(CASE WHEN ete_grade_final IN ('gross','yes, extensive') THEN 1 ELSE 0 END) AS n_gross_ete,
            ROUND(AVG(CASE WHEN ln_positive_final IS NOT NULL
                           THEN TRY_CAST(ln_positive_final AS DOUBLE) END), 1) AS avg_ln_positive
        FROM {tbl}
        WHERE scoring_ata_flag AND ata_risk_category IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
    """, "ata_risk")
    return fmt(df)


def check_rai_by_ata(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            COALESCE(ata_risk_category, 'unknown') AS ata_risk,
            COUNT(*) AS total,
            SUM(CASE WHEN rai_received_flag THEN 1 ELSE 0 END) AS n_rai,
            ROUND(100.0 * SUM(CASE WHEN rai_received_flag THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 1) AS rai_pct
        FROM {tbl}
        WHERE analysis_eligible_flag
        GROUP BY 1 ORDER BY total DESC
    """, "rai_by_ata")
    return fmt(df)


def check_complications_by_procedure(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            surg_procedure_type,
            COUNT(*) AS n,
            SUM(CASE WHEN any_confirmed_complication THEN 1 ELSE 0 END) AS n_complications,
            ROUND(100.0 * SUM(CASE WHEN any_confirmed_complication THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 1) AS complication_pct,
            SUM(CASE WHEN rln_permanent_flag THEN 1 ELSE 0 END) AS n_perm_rln,
            SUM(CASE WHEN hypocalcemia_status NOT IN ('unknown','absent') THEN 1 ELSE 0 END)
                AS n_hypocalcemia
        FROM {tbl}
        WHERE surg_procedure_type IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
    """, "complications_by_proc")
    return fmt(df)


def check_recurrence_by_followup(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            CASE
                WHEN tg_n_measurements >= 5 THEN '5+ Tg labs'
                WHEN tg_n_measurements >= 2 THEN '2-4 Tg labs'
                WHEN tg_n_measurements >= 1 THEN '1 Tg lab'
                ELSE 'No Tg labs'
            END AS followup_category,
            COUNT(*) AS n,
            SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) AS n_recurrence,
            ROUND(100.0 * SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 1) AS recurrence_pct
        FROM {tbl}
        WHERE analysis_eligible_flag
        GROUP BY 1 ORDER BY n DESC
    """, "recurrence_followup")
    return fmt(df)


def check_score_calculability(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            COUNT(*) AS total_eligible,
            ROUND(100.0 * SUM(CASE WHEN scoring_ajcc8_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS ajcc8_pct,
            ROUND(100.0 * SUM(CASE WHEN scoring_ata_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS ata_pct,
            ROUND(100.0 * SUM(CASE WHEN scoring_macis_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS macis_pct,
            ROUND(100.0 * SUM(CASE WHEN ata_response_category IS NOT NULL THEN 1 ELSE 0 END)
                  / COUNT(*), 1) AS ata_response_pct,
            ROUND(100.0 * SUM(CASE WHEN molecular_eligible_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS mol_tested_pct,
            ROUND(100.0 * SUM(CASE WHEN rai_eligible_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS rai_eligible_pct
        FROM {tbl}
        WHERE analysis_eligible_flag
    """, "calculability")
    return fmt(df)


def check_core_missingness(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    key_cols = [
        "age_at_surgery", "sex", "race", "histology_final", "first_surgery_date",
        "path_tumor_size_cm", "ete_grade_final", "vascular_invasion_final",
        "margin_status_final", "ln_positive_final", "braf_positive_final",
        "ras_positive_final", "tert_positive_final", "fna_bethesda_final",
        "imaging_tirads_best", "surg_procedure_type", "ajcc8_stage_group",
        "ata_risk_category", "ata_response_category", "tg_nadir",
        "lab_completeness_score",
    ]
    rows = []
    total = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    for col in key_cols:
        try:
            n_null = con.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL"
            ).fetchone()[0]
            rows.append(f"| `{col}` | {n_null:,} | {round(100*n_null/max(total,1),1)}% |")
        except Exception:
            rows.append(f"| `{col}` | - | column missing |")
    header = "| Column | NULL Count | NULL % |\n|--------|-----------|--------|\n"
    return header + "\n".join(rows)


def check_provisional_flags(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    parts = []
    df = q(con, f"""
        SELECT
            'ata_response_category' AS field,
            COUNT(*) FILTER (WHERE ata_response_category IS NOT NULL) AS n_assigned,
            COUNT(*) FILTER (WHERE analysis_eligible_flag) AS n_eligible
        FROM {tbl}
    """, "provisional_ata_response")
    parts.append("**ATA response-to-therapy (PROVISIONAL):**\n" + fmt(df))

    df2 = q(con, f"""
        SELECT
            COUNT(*) FILTER (WHERE biochemical_recurrence_flag) AS n_biochemical_recurrence,
            COUNT(*) FILTER (WHERE structural_recurrence_flag) AS n_structural_recurrence,
            COUNT(*) FILTER (WHERE any_recurrence_flag) AS n_any_recurrence
        FROM {tbl}
    """, "provisional_recurrence")
    parts.append("\n\n**Recurrence classification (PROVISIONAL biochemical):**\n" + fmt(df2))

    df3 = q(con, f"""
        SELECT
            COUNT(*) FILTER (WHERE rln_permanent_flag) AS n_perm_rln,
            COUNT(*) FILTER (WHERE rln_transient_flag) AS n_transient_rln,
            COUNT(*) FILTER (WHERE hypocalcemia_status NOT IN ('unknown','absent'))
                AS n_hypocalcemia_any,
            COUNT(*) FILTER (WHERE hypoparathyroidism_status NOT IN ('unknown','absent'))
                AS n_hypopara_any
        FROM {tbl}
    """, "provisional_complications")
    parts.append("\n\n**Complication flags (permanent classification PROVISIONAL):**\n" + fmt(df3))
    return "\n".join(parts)


def check_imaging_limitations(con) -> str:
    parts = []
    tbl_r = "patient_analysis_resolved_v1"
    if tbl_ok(con, tbl_r):
        df = q(con, f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE imaging_tirads_best IS NOT NULL) AS with_tirads,
                COUNT(*) FILTER (WHERE imaging_nodule_size_cm IS NOT NULL) AS with_nodule_size,
                COUNT(*) FILTER (WHERE imaging_n_nodule_records > 0) AS with_any_imaging
            FROM {tbl_r}
        """, "imaging_resolved")
        parts.append("**Imaging coverage in resolved layer:**\n" + fmt(df))

    if tbl_ok(con, "imaging_nodule_long_v2"):
        df2 = q(con, """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE tirads_score IS NOT NULL) AS with_tirads,
                COUNT(*) FILTER (WHERE max_dimension_cm IS NOT NULL
                                 OR length_mm IS NOT NULL) AS with_size
            FROM imaging_nodule_long_v2
        """, "imaging_v2")
        parts.append("\n\n**imaging_nodule_long_v2 (known empty sizes):**\n" + fmt(df2))

    for t in ["raw_us_tirads_excel_v1", "extracted_tirads_validated_v1"]:
        if tbl_ok(con, t):
            r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {t}").fetchone()
            parts.append(f"\n`{t}`: {r[0]:,} rows, {r[1]:,} patients")

    return "\n".join(parts) if parts else "_No imaging tables found_"


def check_molecular_sanity(con) -> str:
    tbl = "patient_analysis_resolved_v1"
    if not tbl_ok(con, tbl):
        return f"_{tbl} not found_"
    df = q(con, f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN braf_positive_final THEN 1 ELSE 0 END) AS braf_pos,
            SUM(CASE WHEN ras_positive_final THEN 1 ELSE 0 END) AS ras_pos,
            SUM(CASE WHEN tert_positive_final THEN 1 ELSE 0 END) AS tert_pos,
            SUM(CASE WHEN molecular_eligible_flag THEN 1 ELSE 0 END) AS mol_tested,
            ROUND(100.0 * SUM(CASE WHEN braf_positive_final THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN molecular_eligible_flag THEN 1 ELSE 0 END), 0), 1)
                AS braf_pct_of_tested
        FROM {tbl}
    """, "molecular_sanity")
    parts = ["**Molecular positivity in resolved layer:**\n", fmt(df)]

    if tbl_ok(con, "molecular_test_episode_v2"):
        df2 = q(con, """
            SELECT
                SUM(CASE WHEN LOWER(CAST(ras_flag AS VARCHAR))='true' THEN 1 ELSE 0 END) AS ras_flag_true,
                SUM(CASE WHEN ras_subtype IS NOT NULL THEN 1 ELSE 0 END) AS ras_subtype_present,
                COUNT(DISTINCT research_id) FILTER (
                    WHERE LOWER(CAST(ras_flag AS VARCHAR))='false' AND ras_subtype IS NOT NULL
                ) AS ras_flag_bug_patients
            FROM molecular_test_episode_v2
        """, "ras_bug")
        parts.append("\n\n**RAS flag bug check (ras_flag=false but ras_subtype present):**\n" + fmt(df2))

    return "\n".join(parts)


def check_ambiguous_linkage(con) -> str:
    if tbl_ok(con, "linkage_ambiguity_review_v1"):
        df = q(con, """
            SELECT linkage_type,
                   COUNT(DISTINCT research_id) AS n_patients,
                   COUNT(*) AS n_links
            FROM linkage_ambiguity_review_v1
            GROUP BY 1 ORDER BY n_links DESC
        """, "ambiguity")
        return fmt(df)
    return ("_linkage_ambiguity_review_v1 not found on MotherDuck_ — "
            "run scripts/49_enhanced_linkage_v3.py --md first")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_audit(con, dry_run=False):
    if dry_run:
        print("  [DRY-RUN] Would run 12 clinical face-validity checks")
        return

    checks = [
        ("1. Histology Distribution", check_histology_distribution),
        ("2. AJCC8 Stage by Age Group", check_ajcc8_distribution),
        ("3. ATA Risk vs ETE / LN Burden", check_ata_risk),
        ("4. RAI Usage by ATA Risk", check_rai_by_ata),
        ("5. Complication Rates by Procedure", check_complications_by_procedure),
        ("6. Recurrence by Follow-up Availability", check_recurrence_by_followup),
        ("7. Score Calculability Rates", check_score_calculability),
        ("8. Core Variable Missingness", check_core_missingness),
        ("9. Provisional Definition Counts", check_provisional_flags),
        ("10. Ambiguous Linkage Burden", check_ambiguous_linkage),
        ("11. Imaging Data Limitations", check_imaging_limitations),
        ("12. Molecular Field Sanity", check_molecular_sanity),
    ]

    report_sections = []
    for title, fn in checks:
        section(title)
        result = fn(con)
        print(result[:2000])
        report_sections.append((title, result))

    report_path = EXPORTS / "pre_manuscript_audit_report.md"
    with open(report_path, "w") as f:
        f.write("# Pre-Manuscript Clinical Face-Validity Audit\n\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        f.write("---\n\n")
        for heading, body in report_sections:
            f.write(f"## {heading}\n\n{body}\n\n---\n\n")
        f.write("## Audit Conclusion\n\n")
        f.write("Review each section above. Sections returning `_not found_` indicate "
                "tables that have not yet been materialized on MotherDuck. "
                "Run the full pipeline (scripts 49-53, then 26 --md) before final "
                "manuscript analysis.\n")
    print(f"\n  Report: {report_path}")


def main():
    p = argparse.ArgumentParser(description="56_pre_manuscript_audit.py")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true")
    g.add_argument("--local", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        run_audit(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 56_pre_manuscript_audit.py finished")


if __name__ == "__main__":
    main()
