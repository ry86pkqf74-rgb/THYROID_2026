#!/usr/bin/env python3
"""
57_freeze_manuscript_cohort.py -- Freeze manuscript cohort snapshot

Creates a frozen, versioned snapshot of the analysis-resolved layer
for manuscript use.  Generates a CONSORT-style cohort flow table
tracking sequential exclusions.

Outputs:
  manuscript_cohort_v1          -- frozen patient table
  manuscript_cohort_flow_v1     -- step/n/excluded/reason
  exports/manuscript_cohort_freeze/  -- CSV + JSON manifest
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
OUT_DIR = os.path.join("exports", "manuscript_cohort_freeze")


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


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def main():
    ap = argparse.ArgumentParser(description="Freeze manuscript cohort snapshot")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--md", action="store_true", help="Use MotherDuck")
    grp.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL only")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    con = get_connection(args.md)
    git_sha = _git_sha()

    print(f"[57] Freeze manuscript cohort  ts={TIMESTAMP}  sha={git_sha}")
    print(f"     target={'MotherDuck' if args.md else 'local DuckDB'}")

    # ── 1. Resolve source table ──────────────────────────────────────
    src = "patient_analysis_resolved_v1"
    try:
        n_check = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        print(f"  source: {src}  rows={n_check}")
    except Exception:
        src = "md_patient_analysis_resolved_v1" if args.md else src
        try:
            n_check = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
            print(f"  source (fallback): {src}  rows={n_check}")
        except Exception:
            print("  ERROR: patient_analysis_resolved_v1 not found.  Run script 48 first.")
            sys.exit(1)

    # ── 2. Create frozen snapshot ────────────────────────────────────
    freeze_sql = f"""
    DROP TABLE IF EXISTS manuscript_cohort_v1;
    CREATE TABLE manuscript_cohort_v1 AS
    SELECT *,
           '{TIMESTAMP}'            AS freeze_timestamp,
           '{git_sha}'              AS freeze_git_sha,
           'v1'                     AS resolved_layer_version
    FROM {src};
    """
    print("\n  Creating manuscript_cohort_v1 ...")
    if args.dry_run:
        print(freeze_sql)
    else:
        for stmt in [s.strip() for s in freeze_sql.split(";") if s.strip()]:
            con.execute(stmt)

    total = con.execute("SELECT COUNT(*) FROM manuscript_cohort_v1").fetchone()[0]
    print(f"    frozen rows: {total}")

    # ── 3. Cohort flow ───────────────────────────────────────────────
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='manuscript_cohort_v1'"
    ).fetchall()]

    flow_steps = [
        ("total",             "TRUE",                                      "None"),
        ("analysis_eligible", "analysis_eligible_flag IS TRUE",            "Not analysis-eligible"),
    ]
    if "path_histology_raw" in cols:
        flow_steps.append(("cancer_histology", "path_histology_raw IS NOT NULL", "No histology"))
    elif "path_histology_final" in cols:
        flow_steps.append(("cancer_histology", "path_histology_final IS NOT NULL", "No histology"))

    if "molecular_eligible_flag" in cols:
        flow_steps.append(("molecular_tested", "molecular_eligible_flag IS TRUE", "No molecular test"))
    if "rai_eligible_flag" in cols:
        flow_steps.append(("rai_treated", "rai_eligible_flag IS TRUE", "No RAI treatment"))

    tg_col = None
    for c in ("lab_tg_nadir", "tg_nadir", "lab_tg_last_value"):
        if c in cols:
            tg_col = c
            break
    if tg_col:
        flow_steps.append(("tg_available", f"{tg_col} IS NOT NULL", "No Tg data"))

    tirads_col = None
    for c in ("imaging_tirads_best_score", "tirads_best_score_v12", "tirads_score_v11"):
        if c in cols:
            tirads_col = c
            break
    if tirads_col:
        flow_steps.append(("tirads_available", f"{tirads_col} IS NOT NULL", "No TIRADS data"))

    flow_rows = []
    prev_n = total
    for step_name, condition, reason in flow_steps:
        n = con.execute(
            f"SELECT COUNT(*) FROM manuscript_cohort_v1 WHERE {condition}"
        ).fetchone()[0]
        excluded = prev_n - n
        flow_rows.append({
            "step": step_name,
            "n": n,
            "excluded": excluded,
            "reason": reason if excluded > 0 else "",
        })
        prev_n = n

    flow_df = pd.DataFrame(flow_rows)
    print("\n  Cohort flow:")
    for _, r in flow_df.iterrows():
        exc = f"  (-{r['excluded']} {r['reason']})" if r["excluded"] > 0 else ""
        print(f"    {r['step']:25s}  n={r['n']:>6,}{exc}")

    # ── 4. Save flow table ───────────────────────────────────────────
    flow_create = """
    DROP TABLE IF EXISTS manuscript_cohort_flow_v1;
    CREATE TABLE manuscript_cohort_flow_v1 (
        step    VARCHAR,
        n       INTEGER,
        excluded INTEGER,
        reason  VARCHAR
    );
    """
    if not args.dry_run:
        for stmt in [s.strip() for s in flow_create.split(";") if s.strip()]:
            con.execute(stmt)
        for _, r in flow_df.iterrows():
            con.execute(
                "INSERT INTO manuscript_cohort_flow_v1 VALUES (?, ?, ?, ?)",
                [r["step"], int(r["n"]), int(r["excluded"]), r["reason"]],
            )
        print(f"\n  manuscript_cohort_flow_v1: {len(flow_df)} rows")

    # ── 5. Export ────────────────────────────────────────────────────
    cohort_df = con.execute("SELECT * FROM manuscript_cohort_v1").df()
    cohort_csv = os.path.join(OUT_DIR, "manuscript_cohort_v1.csv")
    flow_csv = os.path.join(OUT_DIR, "manuscript_cohort_flow_v1.csv")

    if not args.dry_run:
        cohort_df.to_csv(cohort_csv, index=False)
        flow_df.to_csv(flow_csv, index=False)
        print(f"  Exported: {cohort_csv}  ({len(cohort_df)} rows)")
        print(f"  Exported: {flow_csv}  ({len(flow_df)} rows)")

    manifest = {
        "script": "57_freeze_manuscript_cohort.py",
        "timestamp": TIMESTAMP,
        "git_sha": git_sha,
        "resolved_layer_version": "v1",
        "total_patients": int(total),
        "flow_steps": flow_rows,
        "columns": len(cohort_df.columns),
        "target": "MotherDuck" if args.md else "local",
    }
    manifest_path = os.path.join(OUT_DIR, "manifest.json")
    if not args.dry_run:
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"  Manifest: {manifest_path}")

    print(f"\n[57] Done.  {total:,} patients frozen.")
    con.close()


if __name__ == "__main__":
    main()
