#!/usr/bin/env python3
"""
60_outlier_scan.py -- Clinical outlier detection for manuscript QA

Scans manuscript cohort + longitudinal labs + scoring tables for values
outside plausible clinical bounds.  Creates a flag table for review.

Outputs:
  manuscript_outlier_flags_v1 table
  exports/manuscript_analysis/outlier_scan.csv
  exports/manuscript_analysis/outlier_report.md
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUT_DIR = os.path.join("exports", "manuscript_analysis")

BOUNDS = {
    "tumor_size_cm": (0.1, 15.0),
    "age": (1, 100),
    "tg": (0, 100000),
    "tsh": (0, 500),
    "ln_ratio": (0.0, 1.0),
    "pth": (0, 3000),
    "calcium": (4.0, 15.0),
    "bmi": (10, 80),
    "specimen_weight_g": (0, 5000),
    "ln_examined": (0, 200),
    "ln_positive": (0, 200),
    "macis_score": (0, 30),
    "rai_dose": (10, 1000),
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


def _tbl_exists(con, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def _scan_column(con, table: str, col: str, flag_type: str, lo: float, hi: float) -> list[dict]:
    """Return outlier rows for a numeric column outside [lo, hi]."""
    try:
        rows = con.execute(f"""
            SELECT research_id, CAST("{col}" AS DOUBLE) AS val
            FROM {table}
            WHERE TRY_CAST("{col}" AS DOUBLE) IS NOT NULL
              AND (TRY_CAST("{col}" AS DOUBLE) < {lo}
                   OR TRY_CAST("{col}" AS DOUBLE) > {hi})
        """).fetchall()
    except Exception:
        return []
    return [
        {
            "research_id": int(r[0]),
            "flag_type": flag_type,
            "column": col,
            "value": float(r[1]),
            "threshold_low": lo,
            "threshold_high": hi,
            "source_table": table,
        }
        for r in rows
    ]


def main():
    ap = argparse.ArgumentParser(description="Outlier scan for manuscript cohort")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print plan only")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    con = get_connection(args.md)

    print(f"[60] Outlier scan  ts={TIMESTAMP}")
    print(f"     target={'MotherDuck' if args.md else 'local DuckDB'}")

    cohort = _resolve_table(con, "manuscript_cohort_v1", [
        "md_manuscript_cohort_v1",
        "patient_analysis_resolved_v1",
        "md_patient_analysis_resolved_v1",
    ])
    total = con.execute(f"SELECT COUNT(*) FROM {cohort}").fetchone()[0]
    print(f"  cohort: {cohort}  rows={total}")

    cohort_cols = set(
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{cohort}'"
        ).fetchall()
    )

    all_flags: list[dict] = []

    # ── 1. Cohort-level scans ────────────────────────────────────────
    print("\n  Scanning cohort columns ...")
    col_mapping = {
        "tumor_size_cm": ["path_tumor_size_cm_final", "path_tumor_size_cm_raw",
                          "tumor_size_cm", "max_tumor_size_cm_v10"],
        "age": ["demo_age_final", "demo_age_raw", "age_at_diagnosis"],
        "ln_ratio": ["staging_ln_ratio_final", "ln_ratio"],
        "specimen_weight_g": ["specimen_weight_g"],
        "ln_examined": ["staging_ln_examined_final", "ln_total_examined"],
        "ln_positive": ["staging_ln_positive_final", "ln_total_positive", "ln_positive_v6"],
        "macis_score": ["staging_macis_score_final", "macis_score"],
        "rai_dose": ["treatment_rai_dose_final", "rai_dose_mci"],
    }

    for flag_type, candidates in col_mapping.items():
        lo, hi = BOUNDS[flag_type]
        for col in candidates:
            if col in cohort_cols:
                flags = _scan_column(con, cohort, col, flag_type, lo, hi)
                if flags:
                    all_flags.extend(flags)
                    print(f"    {flag_type} ({col}): {len(flags)} outliers")
                else:
                    print(f"    {flag_type} ({col}): 0 outliers")
                break

    # ── 2. Longitudinal lab scans ────────────────────────────────────
    print("\n  Scanning longitudinal labs ...")
    lab_table = _resolve_table(con, "longitudinal_lab_clean_v1", [
        "md_longitudinal_lab_clean_v1",
    ])
    if _tbl_exists(con, lab_table):
        lab_cols = set(
            r[0] for r in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name='{lab_table}'"
            ).fetchall()
        )
        lab_scans = {
            "tg": ("lab_value", "thyroglobulin", 0, 100000),
            "tsh": ("lab_value", "tsh", 0, 500),
            "pth": ("lab_value", "pth", 0, 3000),
            "calcium": ("lab_value", "calcium", 4.0, 15.0),
        }
        lab_type_col = None
        for c in ("lab_type", "lab_name", "entity_name"):
            if c in lab_cols:
                lab_type_col = c
                break

        val_col = None
        for c in ("lab_value", "result_numeric", "value_numeric"):
            if c in lab_cols:
                val_col = c
                break

        if lab_type_col and val_col:
            for flag_type, (_, lab_name, lo, hi) in lab_scans.items():
                try:
                    rows = con.execute(f"""
                        SELECT research_id, TRY_CAST("{val_col}" AS DOUBLE) AS val
                        FROM {lab_table}
                        WHERE LOWER("{lab_type_col}") = '{lab_name}'
                          AND TRY_CAST("{val_col}" AS DOUBLE) IS NOT NULL
                          AND (TRY_CAST("{val_col}" AS DOUBLE) < {lo}
                               OR TRY_CAST("{val_col}" AS DOUBLE) > {hi})
                    """).fetchall()
                    for r in rows:
                        all_flags.append({
                            "research_id": int(r[0]),
                            "flag_type": f"lab_{flag_type}",
                            "column": val_col,
                            "value": float(r[1]),
                            "threshold_low": lo,
                            "threshold_high": hi,
                            "source_table": lab_table,
                        })
                    print(f"    lab_{flag_type}: {len(rows)} outliers")
                except Exception as e:
                    print(f"    lab_{flag_type}: error ({e})")
        else:
            print(f"    skipped labs (type_col={lab_type_col}, val_col={val_col})")
    else:
        print(f"    {lab_table} not found, skipping lab scans")

    # ── 3. Scoring table scans ───────────────────────────────────────
    print("\n  Scanning scoring tables ...")
    scoring_table = _resolve_table(con, "thyroid_scoring_py_v1", [
        "md_thyroid_scoring_py_v1",
        "thyroid_scoring_systems_v1",
        "md_thyroid_scoring_systems_v1",
    ])
    if _tbl_exists(con, scoring_table):
        scoring_cols = set(
            r[0] for r in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name='{scoring_table}'"
            ).fetchall()
        )
        for col in ("macis_score", "ages_score"):
            if col in scoring_cols:
                lo, hi = (0, 30)
                flags = _scan_column(con, scoring_table, col, f"scoring_{col}", lo, hi)
                if flags:
                    all_flags.extend(flags)
                print(f"    scoring_{col}: {len(flags)} outliers")
    else:
        print(f"    {scoring_table} not found, skipping scoring scans")

    # ── 4. Create flag table ─────────────────────────────────────────
    flag_df = pd.DataFrame(all_flags) if all_flags else pd.DataFrame(
        columns=["research_id", "flag_type", "column", "value",
                 "threshold_low", "threshold_high", "source_table"]
    )

    print(f"\n  Total outlier flags: {len(flag_df)}")
    if len(flag_df) > 0:
        summary = flag_df.groupby("flag_type").size().reset_index(name="count")
        for _, r in summary.iterrows():
            print(f"    {r['flag_type']}: {r['count']}")

    if not args.dry_run:
        con.execute("DROP TABLE IF EXISTS manuscript_outlier_flags_v1")
        if len(flag_df) > 0:
            con.execute(
                "CREATE TABLE manuscript_outlier_flags_v1 AS SELECT * FROM flag_df",
            )
        else:
            con.execute("""
                CREATE TABLE manuscript_outlier_flags_v1 (
                    research_id INTEGER,
                    flag_type   VARCHAR,
                    "column"    VARCHAR,
                    value       DOUBLE,
                    threshold_low  DOUBLE,
                    threshold_high DOUBLE,
                    source_table   VARCHAR
                )
            """)
        print(f"  manuscript_outlier_flags_v1: {len(flag_df)} rows")

    # ── 5. Export ────────────────────────────────────────────────────
    if not args.dry_run:
        csv_path = os.path.join(OUT_DIR, "outlier_scan.csv")
        flag_df.to_csv(csv_path, index=False)
        print(f"  Exported: {csv_path}")

        md_path = os.path.join(OUT_DIR, "outlier_report.md")
        with open(md_path, "w") as f:
            f.write(f"# Outlier Scan Report\n\n")
            f.write(f"**Generated:** {TIMESTAMP} | **Total flags:** {len(flag_df)}\n\n")

            f.write("## Bounds Checked\n\n")
            f.write("| Variable | Low | High |\n|----------|----:|-----:|\n")
            for k, (lo, hi) in BOUNDS.items():
                f.write(f"| {k} | {lo} | {hi} |\n")
            f.write("\n")

            if len(flag_df) > 0:
                f.write("## Flags by Type\n\n")
                summary = flag_df.groupby("flag_type").agg(
                    count=("flag_type", "size"),
                    patients=("research_id", "nunique"),
                    min_value=("value", "min"),
                    max_value=("value", "max"),
                ).reset_index()
                f.write("| Type | Count | Patients | Min | Max |\n")
                f.write("|------|------:|---------:|----:|----:|\n")
                for _, r in summary.iterrows():
                    f.write(f"| {r['flag_type']} | {r['count']} | {r['patients']} "
                            f"| {r['min_value']:.2f} | {r['max_value']:.2f} |\n")
                f.write("\n")

                f.write("## Sample Outliers (top 20)\n\n")
                f.write("| research_id | type | column | value | bounds |\n")
                f.write("|------------:|------|--------|------:|--------|\n")
                for _, r in flag_df.head(20).iterrows():
                    f.write(f"| {r['research_id']} | {r['flag_type']} | {r['column']} "
                            f"| {r['value']:.2f} | [{r['threshold_low']}, {r['threshold_high']}] |\n")
            else:
                f.write("## No outliers detected.\n")

        print(f"  Exported: {md_path}")

    print(f"\n[60] Done.  {len(flag_df)} outlier flags across "
          f"{flag_df['flag_type'].nunique() if len(flag_df) > 0 else 0} types.")
    con.close()


if __name__ == "__main__":
    main()
