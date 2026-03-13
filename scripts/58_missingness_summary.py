#!/usr/bin/env python3
"""
58_missingness_summary.py -- Per-column missingness report

Reads manuscript_cohort_v1 (or patient_analysis_resolved_v1 fallback)
and computes NULL counts and percentages for 25+ clinically important
columns, grouped by domain (demographics, pathology, staging, molecular,
imaging, treatment, outcomes, labs).

Outputs:
  exports/manuscript_analysis/missingness_summary.csv
  exports/manuscript_analysis/missingness_report.md
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUT_DIR = os.path.join("exports", "manuscript_analysis")

DOMAIN_COLUMNS = {
    "demographics": [
        "demo_age_final", "demo_sex_final", "demo_race_final",
        "demo_age_raw", "demo_sex_raw", "demo_race_raw",
    ],
    "pathology": [
        "path_histology_final", "path_histology_raw",
        "path_tumor_size_cm_final", "path_tumor_size_cm_raw",
        "path_margin_status_final", "path_ete_grade_final",
        "path_vascular_invasion_final", "path_lvi_final",
        "path_pni_final", "path_capsular_invasion_final",
    ],
    "staging": [
        "staging_ajcc8_t_final", "staging_ajcc8_n_final",
        "staging_ajcc8_m_final", "staging_ajcc8_stage_group_final",
        "staging_ata_risk_final", "staging_macis_score_final",
    ],
    "molecular": [
        "molecular_braf_positive_final", "molecular_ras_positive_final",
        "molecular_tert_positive_final", "molecular_platform_final",
        "molecular_eligible_flag",
    ],
    "imaging": [
        "imaging_tirads_best_score", "imaging_nodule_size_cm",
        "tirads_best_score_v12", "tirads_score_v11",
    ],
    "treatment": [
        "treatment_procedure_type_final", "treatment_rai_dose_final",
        "rai_eligible_flag", "treatment_rai_response_final",
    ],
    "outcomes": [
        "outcome_recurrence_flag", "outcome_recurrence_date",
        "outcome_tg_rising_flag", "survival_eligible_flag",
        "outcome_time_days", "outcome_event",
    ],
    "labs": [
        "lab_tg_nadir", "lab_tg_last_value", "lab_tg_rising_flag",
        "lab_tsh_last", "lab_pth_nadir", "lab_calcium_nadir",
    ],
}


def get_connection(md: bool):
    import duckdb
    if md:
        token = os.environ.get("MOTHERDUCK_TOKEN") or ""
        if not token:
            try:
                import toml
                token = toml.load(".streamlit/secrets.toml").get("MOTHERDUCK_TOKEN", "")
            except Exception:
                pass
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(os.environ.get("LOCAL_DUCKDB_PATH", "thyroid_master.duckdb"))


def _resolve_table(con, preferred: str, fallbacks: list[str]) -> str:
    for tbl in [preferred] + fallbacks:
        try:
            con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
            return tbl
        except Exception:
            continue
    return preferred


def main():
    ap = argparse.ArgumentParser(description="Missingness summary for manuscript cohort")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    con = get_connection(args.md)

    print(f"[58] Missingness summary  ts={TIMESTAMP}")
    print(f"     target={'MotherDuck' if args.md else 'local DuckDB'}")

    src = _resolve_table(con, "manuscript_cohort_v1", [
        "md_manuscript_cohort_v1",
        "patient_analysis_resolved_v1",
        "md_patient_analysis_resolved_v1",
    ])
    total = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    print(f"  source: {src}  rows={total}")

    available_cols = set(
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{src}'"
        ).fetchall()
    )

    rows = []
    for domain, col_list in DOMAIN_COLUMNS.items():
        for col in col_list:
            if col not in available_cols:
                continue
            try:
                null_n = con.execute(
                    f"SELECT COUNT(*) FROM {src} WHERE {col} IS NULL"
                ).fetchone()[0]
            except Exception:
                continue
            rows.append({
                "domain": domain,
                "column": col,
                "total": total,
                "null_count": null_n,
                "null_pct": round(100.0 * null_n / total, 1) if total > 0 else 0,
                "fill_count": total - null_n,
                "fill_pct": round(100.0 * (total - null_n) / total, 1) if total > 0 else 0,
            })

    if not rows:
        print("  WARNING: No matching columns found. Scanning all columns ...")
        for col in sorted(available_cols):
            try:
                null_n = con.execute(
                    f"SELECT COUNT(*) FROM {src} WHERE \"{col}\" IS NULL"
                ).fetchone()[0]
            except Exception:
                continue
            rows.append({
                "domain": "unknown",
                "column": col,
                "total": total,
                "null_count": null_n,
                "null_pct": round(100.0 * null_n / total, 1) if total > 0 else 0,
                "fill_count": total - null_n,
                "fill_pct": round(100.0 * (total - null_n) / total, 1) if total > 0 else 0,
            })

    df = pd.DataFrame(rows).sort_values(["domain", "null_pct"], ascending=[True, False])

    # ── Print summary ────────────────────────────────────────────────
    print(f"\n  {'Domain':<14} {'Column':<42} {'Fill%':>6}  {'NULL':>6}")
    print(f"  {'-'*14} {'-'*42} {'-'*6}  {'-'*6}")
    for _, r in df.iterrows():
        bar = "#" * int(r["fill_pct"] / 5) + "." * (20 - int(r["fill_pct"] / 5))
        print(f"  {r['domain']:<14} {r['column']:<42} {r['fill_pct']:>5.1f}%  {r['null_count']:>6,}")

    # ── Domain summary ───────────────────────────────────────────────
    if len(df) > 0:
        domain_summary = df.groupby("domain").agg(
            columns=("column", "count"),
            avg_fill_pct=("fill_pct", "mean"),
            min_fill_pct=("fill_pct", "min"),
        ).round(1)
        print(f"\n  Domain summary:")
        print(f"  {'Domain':<14} {'Cols':>5} {'Avg Fill%':>10} {'Min Fill%':>10}")
        for dom, r in domain_summary.iterrows():
            print(f"  {dom:<14} {int(r['columns']):>5} {r['avg_fill_pct']:>9.1f}% {r['min_fill_pct']:>9.1f}%")

    # ── Export ───────────────────────────────────────────────────────
    if not args.dry_run:
        csv_path = os.path.join(OUT_DIR, "missingness_summary.csv")
        df.to_csv(csv_path, index=False)
        print(f"\n  Exported: {csv_path}")

        md_path = os.path.join(OUT_DIR, "missingness_report.md")
        with open(md_path, "w") as f:
            f.write(f"# Missingness Report\n\n")
            f.write(f"**Source:** `{src}` | **N:** {total:,} | **Generated:** {TIMESTAMP}\n\n")
            for domain in df["domain"].unique():
                sub = df[df["domain"] == domain]
                f.write(f"## {domain.title()}\n\n")
                f.write(f"| Column | Fill % | NULL Count |\n")
                f.write(f"|--------|-------:|-----------:|\n")
                for _, r in sub.iterrows():
                    f.write(f"| {r['column']} | {r['fill_pct']:.1f}% | {r['null_count']:,} |\n")
                f.write(f"\n")

            if len(df) > 0:
                f.write(f"## Domain Summary\n\n")
                f.write(f"| Domain | Columns | Avg Fill % | Min Fill % |\n")
                f.write(f"|--------|--------:|-----------:|-----------:|\n")
                for dom, r in domain_summary.iterrows():
                    f.write(f"| {dom} | {int(r['columns'])} | {r['avg_fill_pct']:.1f}% | {r['min_fill_pct']:.1f}% |\n")

        print(f"  Exported: {md_path}")

    print(f"\n[58] Done.  {len(df)} columns assessed across {df['domain'].nunique()} domains.")
    con.close()


if __name__ == "__main__":
    main()
