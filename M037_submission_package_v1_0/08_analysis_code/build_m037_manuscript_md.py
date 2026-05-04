#!/usr/bin/env python3
"""Emit 08_analysis_outputs/M037_manuscript_numbers_YYYYMMDD.md from live parquet + snapshot."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd

PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(PKG, "08_analysis_outputs")


def main():
    os.makedirs(OUT, exist_ok=True)
    snap_path = os.path.join(OUT, "m037_run_snapshot.json")
    pq = os.path.join(OUT, "m037_analytic_spine.parquet")
    if not os.path.isfile(snap_path) or not os.path.isfile(pq):
        raise SystemExit("Run build_m037_tables.py first.")

    snap = json.loads(open(snap_path, encoding="utf-8").read())
    df = pd.read_parquet(pq)
    d = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(OUT, f"M037_manuscript_numbers_{d}.md")

    n_fhx = int(df["fhx"].sum()) if "fhx" in df.columns else int(df["pmhx_nlp_family_hx_thyroid"].apply(lambda x: x is True).sum())
    lines = [
        f"# M037 — manuscript number helper ({d})",
        "",
        f"**Cohort (M037 view × CPM join):** n = {snap['n_cohort']:,}",
        f"**LN-positive (AJCC N1+):** {snap['n_ln_pos']:,} ({snap['pct_ln_pos']}%)",
        f"**NLP family hx thyroid (TRUE):** {n_fhx:,}",
        "",
        "## Cowork headline (post–mig_286)",
        "",
        "- Family hx aOR **1.05** (0.74–1.51), p = 0.77 (null association).",
        "- Male sex aOR **1.81**; age OR **0.98**/yr; tumor size OR **1.18**/cm — verify against `Table2b_primary_coef` after rebuild.",
        "",
        "## Regenerate",
        "",
        "```bash",
        ".venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_tables.py",
        ".venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_figures.py",
        ".venv/bin/python M037_submission_package_v1_0/08_analysis_code/build_m037_manuscript_md.py",
        "```",
        "",
    ]
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
