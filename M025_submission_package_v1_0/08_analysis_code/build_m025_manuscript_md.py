#!/usr/bin/env python3
"""Emit 08_analysis_outputs/M025_manuscript_numbers_YYYYMMDD.md from live parquet + snapshots."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

import pandas as pd

PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(PKG, "08_analysis_outputs")


def primary_tr_ge4_snapshot(df: pd.DataFrame):
    """Return sens/spec strings for headline block."""
    sub = df[df["tr_rank"].notna()].copy()
    pred = sub["tr_rank"].to_numpy() >= 4.0
    y = sub["y_mal"].astype(bool).to_numpy()
    tp = int((pred & y).sum())
    fp = int((pred & (~y)).sum())
    tn = int(((~pred) & (~y)).sum())
    fn = int(((~pred) & y).sum())
    sens = tp / max(tp + fn, 1)
    spec = tn / max(tn + fp, 1)
    return tp, fp, tn, fn, sens, spec


def main():
    os.makedirs(OUT, exist_ok=True)
    snap_path = os.path.join(OUT, "m025_run_snapshot.json")
    pq_path = os.path.join(OUT, "m025_analytic_spine.parquet")
    if not os.path.isfile(snap_path) or not os.path.isfile(pq_path):
        raise SystemExit("Run build_m025_tables.py first.")

    snap = json.loads(open(snap_path, encoding="utf-8").read())
    df = pd.read_parquet(pq_path)

    roc_path = os.path.join(OUT, "m025_supp_ROC_summary.csv")
    auc_line = "(ROC summary missing — rerun tables)"
    if os.path.isfile(roc_path):
        with open(roc_path, encoding="utf-8") as fh:
            r = list(csv.DictReader(fh))
            for row in r:
                if row.get("metric") == "roc_auc_ordinal_rank":
                    auc_line = f"AUC (ordinal TI-RADS rank classifier) ≈ **{float(row['value']):.4f}**."

    tp, fp, tn, fn, sens, spec = primary_tr_ge4_snapshot(df)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(OUT, f"M025_manuscript_numbers_{today}.md")
    mal = snap["n_malignant"]

    lines = [
        f"# M025 — manuscript number helper ({today})",
        "",
        f"**Cohort (`cohort_m025_tirads_performance_v1`):** n = {snap['n_cohort_view']:,}",
        f"**Patients with ordinal TR (`tirads_resolved` + worst-score fallback):** n = {snap['n_tr_rank_known']:,}",
        f"**Pathologic malignancies (gold: `is_malignant`):** {mal:,}",
        "",
        "## Primary operative threshold headline (TR ≥ TR4)",
        "",
        "- Sensitivity {:.3f}; specificity {:.3f}.".format(float(sens), float(spec)),
        f"- TP={tp:,} FP={fp:,} FN={fn:,} TN={tn:,}",
        "",
        "## ROC reminder",
        "",
        f"- {auc_line}",
        "",
        "**Operative caveat:** malignancy enrichment at **every TI-RADS stratum vs ACR-illustrative ROM** (`snowflake_trial/reports/m025_tirads_performance.md`).",
        "",
        "## Regenerate",
        "",
        "```bash",
        ".venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_tables.py",
        ".venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_figures.py",
        ".venv/bin/python M025_submission_package_v1_0/08_analysis_code/build_m025_manuscript_md.py",
        "```",
        "",
    ]
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
