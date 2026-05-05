#!/usr/bin/env python3
"""
m032_mig317_era_stage_post_mig313.py
====================================
mig_317: Recompute M032 Figure 3 era × AJCC8 stage counts (malignant, A–E eras only)
against live MotherDuck cohort view; diff vs frozen v1 CSV; write studies artifacts.

Does not modify M032_submission_package_v1_0/ (frozen).

Run:
  .venv/bin/python scripts/m032_mig317_era_stage_post_mig313.py
"""
from __future__ import annotations

import os
import sys

import duckdb
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from motherduck_client import get_token  # noqa: E402

OUT_DIR = os.path.join(REPO, "studies", "m032_era_stage_v2_post_mig313")
V1_CSV = os.path.join(
    REPO,
    "M032_submission_package_v1_0",
    "06_figures",
    "Fig3_stage_distribution_data.csv",
)

ERA_CASE = """
CASE
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 1999 AND 2004 THEN 'A_1999_2004'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2005 AND 2009 THEN 'B_2005_2009'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2010 AND 2014 THEN 'C_2010_2014'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2015 AND 2019 THEN 'D_2015_2019'
  WHEN YEAR(TRY_CAST(surg_first_date AS DATE)) BETWEEN 2020 AND 2025 THEN 'E_2020_2025'
  ELSE 'F_unknown'
END AS surgery_era
"""

SQL_V2 = f"""
WITH b AS (
  SELECT *,
         {ERA_CASE},
         CASE
           WHEN ajcc8_stage_group = 'I' THEN 'Stage I'
           WHEN ajcc8_stage_group = 'II' THEN 'Stage II'
           WHEN ajcc8_stage_group = 'III' THEN 'Stage III'
           WHEN ajcc8_stage_group IN ('IVA', 'IVB', 'IVC') OR ajcc8_stage_group LIKE 'IV%' THEN 'Stage IV'
           ELSE 'Unknown/Unstaged'
         END AS stage_group
  FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1
  WHERE is_malignant = TRUE AND surgery_era != 'F_unknown'
)
SELECT surgery_era, stage_group, COUNT(*) AS n_v2
FROM b
GROUP BY surgery_era, stage_group
ORDER BY surgery_era, stage_group
"""


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    tok = get_token()
    con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")
    try:
        df_v2 = con.execute(SQL_V2).fetchdf()
    finally:
        con.close()

    df_v2.to_csv(os.path.join(OUT_DIR, "m032_era_stage_v2_live.csv"), index=False)

    df_v1 = pd.read_csv(V1_CSV)
    df_v1 = df_v1.rename(columns={"n": "n_v1"})

    stages = ["Stage I", "Stage II", "Stage III", "Stage IV", "Unknown/Unstaged"]
    eras = sorted(df_v1["surgery_era"].unique().tolist())
    grid = pd.MultiIndex.from_product([eras, stages], names=["surgery_era", "stage_group"]).to_frame(index=False)

    m1 = grid.merge(df_v1, on=["surgery_era", "stage_group"], how="left")
    m = m1.merge(df_v2, on=["surgery_era", "stage_group"], how="left")
    m["n_v1"] = m["n_v1"].fillna(0).astype(int)
    m["n_v2"] = m["n_v2"].fillna(0).astype(int)

    era_tot = m.groupby("surgery_era", as_index=False).agg(
        era_total_v1=("n_v1", "sum"),
        era_total_v2=("n_v2", "sum"),
    )
    m = m.merge(era_tot, on="surgery_era", how="left")
    m["pct_v1_in_era"] = (100.0 * m["n_v1"] / m["era_total_v1"]).round(2)
    m["pct_v2_in_era"] = (100.0 * m["n_v2"] / m["era_total_v2"]).round(2)
    m["delta_n"] = m["n_v2"] - m["n_v1"]
    m["delta_pp"] = (m["pct_v2_in_era"] - m["pct_v1_in_era"]).round(2)
    m["abs_delta_pp"] = m["delta_pp"].abs()
    m["pct_rel_count_change_vs_v1"] = m.apply(
        lambda r: round(100.0 * (r["n_v2"] - r["n_v1"]) / r["n_v1"], 2) if r["n_v1"] > 0 else None,
        axis=1,
    )

    out_xlsx = os.path.join(OUT_DIR, "delta_v1_vs_v2.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        m.sort_values(["surgery_era", "stage_group"]).to_excel(xw, sheet_name="era_x_stage_delta", index=False)
        df_v2.to_excel(xw, sheet_name="v2_live_counts", index=False)
        df_v1.sort_values(["surgery_era", "stage_group"]).to_excel(xw, sheet_name="v1_frozen_fig3", index=False)

    max_pp = float(m["abs_delta_pp"].max())
    worst_pp = m.loc[m["abs_delta_pp"].idxmax()]
    print(f"[OK] Wrote {out_xlsx}")
    print(f"max |Δpp| within-era: {max_pp:.2f} ({worst_pp['surgery_era']}, {worst_pp['stage_group']})")


if __name__ == "__main__":
    main()
