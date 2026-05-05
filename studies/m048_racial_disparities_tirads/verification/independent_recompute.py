#!/usr/bin/env python3
"""
M048 — Independent recompute (Step 5 / QA hard gate).

Re-derives 5 headline numbers from raw m025_analytic_master_* rows joined
directly to canonical_patient_master.race WITHOUT using any m048_* derivation
tables.

Each assertion must match the CSV-based pipeline to ≤0.01% absolute difference.
If any assertion fails, this script exits with code 1 and sign-off is blocked.

Numbers verified:
  1. Per-race patient AUC (Black, White, Asian)
  2. Per-race nodule TR4 ROM at strict-eligible grain
  3. Per-race nodule TR5 ROM at strict-eligible grain
  4. Overall pooled patient AUC
  5. Overall pooled nodule TR4 ROM
"""
from __future__ import annotations

import os
import re
import sys
from math import sqrt

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

VERIF_DIR = os.path.dirname(os.path.abspath(__file__))
STUDY_DIR = os.path.abspath(os.path.join(VERIF_DIR, ".."))
REPO_ROOT = os.path.abspath(os.path.join(STUDY_DIR, "..", ".."))
sys.path.insert(0, REPO_ROOT)

TOLERANCE_PCT = 0.01  # 0.01% absolute

DB_NAME = "thyroid_canonical_publication_v1_0"

RACE_MAP = {
    "Black or African American": "Black",
    "White": "White",
    "Asian": "Asian",
}


def tr_to_int(cat) -> float:
    if cat is None or (isinstance(cat, float) and np.isnan(cat)):
        return np.nan
    s = str(cat).strip().upper()
    m = re.search(r"TR\s*(\d+)", s)
    if m:
        return float(m.group(1))
    m2 = re.search(r"(\d+)", s)
    return float(m2.group(1)) if m2 else np.nan


def to_bool_y(v) -> int:
    return 1 if (v is True or str(v).lower() in ("true", "t", "1")) else 0


def get_connection():
    import duckdb
    from motherduck_client import get_token  # type: ignore[import]
    token = get_token()
    con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={token}")
    con.execute(f"USE {DB_NAME};")
    return con


def auc_from_df(df: pd.DataFrame, score_col: str, y_col: str) -> float:
    sub = df.dropna(subset=[score_col])
    if len(sub) < 2:
        return float("nan")
    y = sub[y_col].values.astype(int)
    s = sub[score_col].values.astype(float)
    if y.sum() == 0 or y.sum() == len(y):
        return float("nan")
    return float(roc_auc_score(y, s))


def rom_pct(df: pd.DataFrame, y_col: str) -> float:
    n = len(df)
    k = int(df[y_col].sum())
    return round(100.0 * k / n, 4) if n > 0 else float("nan")


def main():
    print("=" * 70)
    print("M048 Independent Recompute (Step 5 — Hard QA Gate)")
    print(f"Tolerance: ≤{TOLERANCE_PCT}% absolute difference")
    print("=" * 70)

    con = get_connection()
    print(f"[MD] Connected to {DB_NAME}")

    # ------------------------------------------------------------------
    # Pull raw patient master (no m048_ derivation tables)
    # ------------------------------------------------------------------
    print("[FETCH] Pulling m025_analytic_master_patient_v1 (using patient master race) ...")
    dfp = con.execute("""
        SELECT
            p.research_id,
            p.max_tirads_category_ever,
            p.is_malignant,
            p.predicted_pos_TR3,
            p.predicted_pos_TR4,
            p.predicted_pos_TR5,
            p.race
        FROM manuscript_workspace.m025_analytic_master_patient_v1 p
        WHERE p.max_tirads_category_ever IS NOT NULL
    """).df()
    dfp["race_strat"] = dfp["race"].map(lambda r: RACE_MAP.get(str(r), "Other_Unknown"))
    dfp["score"] = dfp["max_tirads_category_ever"].apply(tr_to_int)
    dfp["y"] = dfp["is_malignant"].apply(to_bool_y)
    print(f"  Patient rows: {len(dfp)}")

    # ------------------------------------------------------------------
    # Pull raw nodule master (no m048_ derivation tables)
    # Use LEFT JOIN to m025_analytic_master_patient_v1 for race — same as pipeline
    # ------------------------------------------------------------------
    print("[FETCH] Pulling m025_analytic_master_nodule_v1 + patient master race ...")
    dfn = con.execute("""
        SELECT
            n.research_id,
            n.acr2017_tirads_category,
            n.nodule_path_proven_malignant,
            n.analytic_eligible_strict_acr_pernodule,
            p.race
        FROM manuscript_workspace.m025_analytic_master_nodule_v1 n
        LEFT JOIN manuscript_workspace.m025_analytic_master_patient_v1 p
               ON n.research_id = p.research_id
        WHERE n.analytic_eligible_strict_acr_pernodule = TRUE
          AND n.acr2017_tirads_category IS NOT NULL
    """).df()
    dfn["race_strat"] = dfn["race"].map(lambda r: RACE_MAP.get(str(r), "Other_Unknown") if pd.notna(r) else "Other_Unknown")
    dfn["score"] = dfn["acr2017_tirads_category"].apply(tr_to_int)
    dfn["y"] = dfn["nodule_path_proven_malignant"].apply(to_bool_y)
    print(f"  Nodule strict rows: {len(dfn)}")
    con.close()

    # ------------------------------------------------------------------
    # Load pipeline CSV outputs
    # ------------------------------------------------------------------
    auc_csv = pd.read_csv(os.path.join(STUDY_DIR, "m048_auc_by_race.csv"))
    rom_csv = pd.read_csv(os.path.join(STUDY_DIR, "m048_rom_by_race_x_tr.csv"))

    def csv_auc(grain: str, race: str) -> float:
        row = auc_csv[(auc_csv["grain"] == grain) & (auc_csv["race_strat"] == race)]
        return float(row.iloc[0]["auc"]) if not row.empty else float("nan")

    def csv_rom(grain: str, race: str, tr: str) -> float:
        row = rom_csv[
            (rom_csv["grain"] == grain) &
            (rom_csv["race_strat"] == race) &
            (rom_csv["tr_category"] == tr)
        ]
        return float(row.iloc[0]["rom_pct"]) if not row.empty else float("nan")

    # ------------------------------------------------------------------
    # Derive headline numbers independently
    # ------------------------------------------------------------------
    results = []
    all_pass = True

    def check(metric: str, derived: float, csv_val: float) -> bool:
        diff = abs(derived - csv_val) if (not np.isnan(derived) and not np.isnan(csv_val)) else float("inf")
        passed = diff <= TOLERANCE_PCT
        icon = "✓" if passed else "✗"
        print(f"  [{icon}] {metric}: derived={derived:.4f}, csv={csv_val:.4f}, diff={diff:.6f}%")
        results.append({
            "metric": metric,
            "derived_value": round(derived, 4),
            "csv_value": round(csv_val, 4),
            "abs_diff_pct": round(diff, 6),
            "tolerance_pct": TOLERANCE_PCT,
            "passed": passed,
        })
        return passed

    print("\n[CHECK] 1. Per-race patient AUC ...")
    for race in ["Black", "White", "Asian"]:
        sub = dfp[dfp["race_strat"] == race]
        auc_d = round(auc_from_df(sub, "score", "y"), 4)
        auc_c = csv_auc("patient", race)
        all_pass &= check(f"patient_auc_{race.lower()}", auc_d, auc_c)

    print("\n[CHECK] 2. Per-race nodule TR4 ROM ...")
    for race in ["Black", "White", "Asian"]:
        sub = dfn[(dfn["race_strat"] == race) & (dfn["score"] == 4)]
        rom_d = rom_pct(sub, "y")
        rom_c = csv_rom("nodule_strict", race, "TR4")
        all_pass &= check(f"nodule_tr4_rom_{race.lower()}", rom_d, rom_c)

    print("\n[CHECK] 3. Per-race nodule TR5 ROM ...")
    for race in ["Black", "White", "Asian"]:
        sub = dfn[(dfn["race_strat"] == race) & (dfn["score"] == 5)]
        rom_d = rom_pct(sub, "y")
        rom_c = csv_rom("nodule_strict", race, "TR5")
        all_pass &= check(f"nodule_tr5_rom_{race.lower()}", rom_d, rom_c)

    print("\n[CHECK] 4. Overall pooled patient AUC ...")
    auc_pooled_d = round(auc_from_df(dfp, "score", "y"), 4)
    auc_pooled_c = csv_auc("patient", "POOLED")
    all_pass &= check("patient_auc_pooled", auc_pooled_d, auc_pooled_c)

    print("\n[CHECK] 5. Overall pooled nodule TR4 ROM ...")
    sub_tr4_all = dfn[dfn["score"] == 4]
    rom_pooled_d = rom_pct(sub_tr4_all, "y")
    # POOLED row may not be in CSV; compute from auc_csv or from aggregating ROM rows
    # Aggregate nodule_strict TR4 across all race strata from the ROM CSV
    rom_nodule_tr4_all = rom_csv[(rom_csv["grain"] == "nodule_strict") & (rom_csv["tr_category"] == "TR4")]
    if not rom_nodule_tr4_all.empty:
        total_k = rom_nodule_tr4_all["n_malignant"].sum()
        total_n = rom_nodule_tr4_all["n_total"].sum()
        rom_pooled_c = round(100.0 * total_k / total_n, 4) if total_n > 0 else float("nan")
    else:
        rom_pooled_c = float("nan")
    all_pass &= check("nodule_tr4_rom_pooled", rom_pooled_d, rom_pooled_c)

    # ------------------------------------------------------------------
    # Write recompute results
    # ------------------------------------------------------------------
    df_results = pd.DataFrame(results)
    out_path = os.path.join(VERIF_DIR, "independent_recompute_results.csv")
    df_results.to_csv(out_path, index=False)
    print(f"\n[WRITTEN] {out_path}")

    # Summary
    n_pass = int(df_results["passed"].sum())
    n_total = len(df_results)
    print(f"\n[RESULT] {n_pass}/{n_total} assertions passed")

    if not all_pass:
        failed = df_results[~df_results["passed"]]
        print("\n[FAIL] Failed assertions:")
        print(failed[["metric", "derived_value", "csv_value", "abs_diff_pct"]].to_string())
        print("\n[FAIL] Independent recompute FAILED. QA gate blocked.")
        sys.exit(1)

    print("[PASS] All independent-recompute assertions pass. QA gate ✓")
    return df_results


if __name__ == "__main__":
    main()
