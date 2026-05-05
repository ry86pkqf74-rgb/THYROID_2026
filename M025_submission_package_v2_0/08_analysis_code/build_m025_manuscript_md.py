#!/usr/bin/env python3
"""Emit 08_analysis_outputs/M025_manuscript_numbers_YYYYMMDD.md from v2.0 parquet + snapshots."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

import pandas as pd

PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(PKG, "08_analysis_outputs")


def primary_tr_ge4_snapshot(df: pd.DataFrame):
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
    snap_path = os.path.join(OUT, "m025v2_run_snapshot.json")
    pq_path = os.path.join(OUT, "m025_analytic_spine.parquet")
    if not os.path.isfile(snap_path) or not os.path.isfile(pq_path):
        raise SystemExit("Run build_m025_tables.py first.")

    snap = json.loads(open(snap_path, encoding="utf-8").read())
    df = pd.read_parquet(pq_path)
    if "analytic_eligible_strict_acr_pernodule" not in df.columns:
        raise SystemExit("Parquet missing strict-eligibility column")
    sm = df["analytic_eligible_strict_acr_pernodule"].map(
        lambda v: v is True or str(v).lower() in ("true", "t", "1", "yes")
    )
    df_s = df.loc[sm].copy()

    roc_path = os.path.join(OUT, "m025v2_supp_ROC_summary.csv")
    auc_lines = []
    if os.path.isfile(roc_path):
        with open(roc_path, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                m = row.get("metric", "")
                val = row.get("value", "")
                if m == "roc_auc_ordinal_rank_nodule_strict":
                    auc_lines.append(f"- Nodule-level strict AUC (ordinal TR) ≈ **{float(val):.4f}**.")
    roc_p = os.path.join(OUT, "m025v2_supp_ROC_patient_summary.csv")
    if os.path.isfile(roc_p):
        with open(roc_p, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if row.get("metric") == "roc_auc_ordinal_rank_patient_level":
                    auc_lines.append(f"- Patient-level comparator AUC ≈ **{float(row['value']):.4f}**.")

    tp, fp, tn, fn, sens, spec = primary_tr_ge4_snapshot(df_s)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(OUT, f"M025_v2_manuscript_numbers_{today}.md")
    mal_n = int(df_s["y_mal"].astype(bool).sum())

    title = (
        "Patient-level versus nodule-level TI-RADS calibration in a 25-year operative thyroid cohort"
    )

    lines = [
        f"# M025 v2.0 — manuscript number helper ({today})",
        "",
        f"> **Working title:** {title}",
        "",
        f"**Nodule spine:** `cohort_m025_nodule_level_v1` — total rows = {snap.get('n_total_rows', len(df)):,}",
        f"**Strict ACR analytic-eligible nodules:** n = {snap.get('n_strict_acr_analytic_eligible', int(sm.sum())):,}",
        f"**Strict nodules with known TR rank:** n = {int(df_s['tr_rank'].notna().sum()):,}",
        f"**Path-proven malignant nodules (strict):** {mal_n:,}",
        f"**Patient comparator cohort:** `cohort_m025_tirads_performance_v1` — n = {snap.get('patient_cohort_n', '—')}",
        "",
        "## Headline (TR ≥ TR4, strict nodule grain)",
        "",
        "- Sensitivity {:.3f}; specificity {:.3f}.".format(float(sens), float(spec)),
        f"- TP={tp:,} FP={fp:,} FN={fn:,} TN={tn:,}",
        "",
        "## ROC",
        "",
        *([x for x in auc_lines if x] or ["- (ROC summary missing — rerun tables with sklearn)"]),
        "",
        "**Framing:** Patient-level v1.0 analysis (`M025_submission_package_v1_0/`) remains frozen as sister manuscript; v2.0 recovers ACR-expected ROM at TR4/TR5 at nodule grain — see Table 3 / Fig 3b in this package.",
        "",
        "## Regenerate",
        "",
        "```bash",
        ".venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_tables.py",
        ".venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_figures.py",
        ".venv/bin/python M025_submission_package_v2_0/08_analysis_code/build_m025_manuscript_md.py",
        "```",
        "",
    ]
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
