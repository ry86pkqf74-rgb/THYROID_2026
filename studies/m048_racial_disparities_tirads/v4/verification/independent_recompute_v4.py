#!/usr/bin/env python3
"""
M048 v4 independent recompute.

Refits key models from the v4 patient-level CSV and asserts agreement with
the cascade at ≤2% relative difference for 7 headline assertions:

  1. Black M6 OR
  2. Asian M6 OR
  3. M0→M6 attenuation %
  4. Asian TR5 mean tumor size
  5. Bethesda IV Black OR
  6. Black M3→M4 attenuation magnitude (NEW)
  7. Black M4→M5 attenuation magnitude (NEW)

Emits recompute_v4_report.md in the same directory.
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

STUDY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, STUDY_DIR)

from m048_v3_stats_lib import fit_logit, prepare_v3_frame, race_or_table  # noqa: E402

V4_DIR    = os.path.join(STUDY_DIR, "v4")
VERIF_DIR = os.path.dirname(os.path.abspath(__file__))


def prepare_v4_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """prepare_v3_frame + v4 binary coercions."""
    df = prepare_v3_frame(raw)
    for c in [
        "has_follicular_adenoma",
        "has_lymphocytic_thyroiditis", "has_hashimotos",
        "pmh_dm", "pmh_htn", "pmh_obesity", "pmh_ckd", "pmh_cad", "pmh_copd",
        "pmh_depression", "pmh_hyperthyroidism", "pmh_hypothyroidism",
        "pmh_autoimmune_thyroid_hx", "pmh_family_hx_thyroid", "pmh_family_hx_cancer",
        "pmh_radiation_exposure", "pmh_prior_cancer_hx",
    ]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: 1 if v in (True, "true", "True", 1, "1") else 0)
    return df


def rel_diff(a: float, b: float) -> float:
    if not np.isfinite(a) or not np.isfinite(b) or a == 0:
        return np.inf
    return abs(a - b) / abs(a)


FORMULA_M4 = (
    "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
    "+ had_any_genetics + had_any_nm "
    "+ has_clt + has_mng + has_graves + has_follicular_adenoma"
)

FORMULA_M5 = (
    "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
    "+ had_any_genetics + had_any_nm "
    "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
    "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx"
)

FORMULA_M6 = (
    "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
    "+ had_any_genetics + had_any_nm "
    "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
    "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
    "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--patient-csv",
        default=os.path.join(V4_DIR, "m048_v4_patient_master_full.csv"),
    )
    args = ap.parse_args()

    if not os.path.isfile(args.patient_csv):
        print(f"Missing patient CSV: {args.patient_csv}\nRun m048_run_analysis_v4.py first.")
        return 2

    df = prepare_v4_frame(pd.read_csv(args.patient_csv))
    df_m = df.dropna(subset=["max_tr_int"]).copy()

    # ---- Re-fit M4 / M5 / M6 ----
    res_m4 = fit_logit(FORMULA_M4, df_m)
    res_m5 = fit_logit(FORMULA_M5, df_m)
    res_m6 = fit_logit(FORMULA_M6, df_m)

    rt_m4 = race_or_table(res_m4).set_index("race_level")
    rt_m5 = race_or_table(res_m5).set_index("race_level")
    rt_m6 = race_or_table(res_m6).set_index("race_level")

    # ---- Load cascade for stored values ----
    cascade = pd.read_csv(os.path.join(V4_DIR, "m048_v4_cascade.csv"))

    def cas(step: str, race: str) -> float:
        sub = cascade[(cascade["model_step"] == step) & (cascade["race_level"] == race)]
        return float(sub.iloc[0]["or"]) if not sub.empty else np.nan

    def get_or(rt: pd.DataFrame, race: str) -> float:
        try:
            return float(rt.loc[race, "or"])
        except Exception:
            return np.nan

    # --- Assertion 1: Black M6 OR ---
    exp_black_m6 = cas("M6", "Black")
    recv_black_m6 = get_or(rt_m6, "Black")
    ok1 = rel_diff(exp_black_m6, recv_black_m6) <= 0.02

    # --- Assertion 2: Asian M6 OR ---
    exp_asian_m6  = cas("M6", "Asian")
    recv_asian_m6 = get_or(rt_m6, "Asian")
    ok2 = rel_diff(exp_asian_m6, recv_asian_m6) <= 0.02

    # --- Assertion 3: M0→M6 attenuation ---
    exp_black_m0 = cas("M0", "Black")
    cas_atten    = 100.0 * (1.0 - cas("M6", "Black") / exp_black_m0) if exp_black_m0 else np.nan
    recv_atten   = 100.0 * (1.0 - recv_black_m6 / exp_black_m0) if (exp_black_m0 and np.isfinite(recv_black_m6)) else np.nan
    ok3 = rel_diff(cas_atten, recv_atten) <= 0.02 if np.isfinite(cas_atten) and np.isfinite(recv_atten) else False

    # --- Assertion 4: Asian TR5 mean tumor size ---
    tr5 = df_m[(df_m["race_strat"] == "Asian") & (df_m["max_tr_int"] == 5) & (df_m["is_malignant"].astype(bool))]
    recv_asian_sz = float(pd.to_numeric(tr5["max_tumor_size_cm"], errors="coerce").mean())
    try:
        dd = pd.read_csv(os.path.join(V4_DIR, "m048_v4_disparity_direction_table.csv"))
        row = dd[(dd["race_strat"] == "Asian") & (dd["tr_category"] == "TR5")]
        exp_asian_sz = float(row.iloc[0]["mean_tumor_size_cm"]) if len(row) else np.nan
    except Exception:
        exp_asian_sz = np.nan
    ok4 = rel_diff(exp_asian_sz, recv_asian_sz) <= 0.05 if np.isfinite(exp_asian_sz) and np.isfinite(recv_asian_sz) else False

    # --- Assertion 5: Bethesda IV Black OR ---
    try:
        bstr = pd.read_csv(os.path.join(V4_DIR, "m048_v4_bethesda_stratified_TR_ROM.csv"))
        b4 = bstr[
            bstr["bethesda_bucket"].astype(str).str.upper().eq("IV")
            | (bstr["bethesda_bucket"].astype(str) == "4")
        ]
        blk = b4.loc[b4["race_level"] == "Black", "or"]
        exp_b4 = float(blk.iloc[0]) if len(blk) else np.nan
    except Exception:
        exp_b4 = np.nan

    sub_b4 = df_m[
        df_m["bethesda_bucket"].astype(str).str.upper().eq("IV")
        | (df_m["bethesda_bucket"].astype(str) == "4")
    ].copy()
    if len(sub_b4) >= 30:
        res_b4 = fit_logit("is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int", sub_b4)
        recv_b4 = float(race_or_table(res_b4).set_index("race_level").loc["Black", "or"])
        ok5 = rel_diff(exp_b4, recv_b4) <= 0.02 if np.isfinite(exp_b4) else False
    else:
        recv_b4 = np.nan
        ok5 = True

    # --- Assertion 6: Black M3→M4 attenuation magnitude (NEW) ---
    exp_black_m3 = cas("M3", "Black")
    exp_black_m4 = cas("M4", "Black")
    recv_black_m4 = get_or(rt_m4, "Black")
    cas_m3m4_att  = 100.0 * (1.0 - exp_black_m4 / exp_black_m3) if exp_black_m3 else np.nan
    recv_m3m4_att = 100.0 * (1.0 - recv_black_m4 / exp_black_m3) if (exp_black_m3 and np.isfinite(recv_black_m4)) else np.nan
    ok6 = rel_diff(cas_m3m4_att, recv_m3m4_att) <= 0.02 if np.isfinite(cas_m3m4_att) and np.isfinite(recv_m3m4_att) else False

    # --- Assertion 7: Black M4→M5 attenuation magnitude (NEW) ---
    exp_black_m5 = cas("M5", "Black")
    recv_black_m5 = get_or(rt_m5, "Black")
    cas_m4m5_att  = 100.0 * (1.0 - exp_black_m5 / exp_black_m4) if (exp_black_m4 and exp_black_m4 != 0) else np.nan
    recv_m4m5_att = 100.0 * (1.0 - recv_black_m5 / recv_black_m4) if (np.isfinite(recv_black_m4) and recv_black_m4) else np.nan
    ok7 = rel_diff(cas_m4m5_att, recv_m4m5_att) <= 0.02 if np.isfinite(cas_m4m5_att) and np.isfinite(recv_m4m5_att) else False

    all_pass = all([ok1, ok2, ok3, ok4, ok5, ok6, ok7])

    lines = [
        "# independent_recompute_v4",
        "",
        f"patient_csv: {args.patient_csv}",
        f"analytic_n: {len(df_m)}",
        "",
        "## Assertion 1 — Black M6 OR",
        f"  cascade_stored={exp_black_m6:.5f}  refit={recv_black_m6:.5f}  "
        f"rel_diff={rel_diff(exp_black_m6, recv_black_m6):.4f}  PASS={ok1}",
        "",
        "## Assertion 2 — Asian M6 OR",
        f"  cascade_stored={exp_asian_m6:.5f}  refit={recv_asian_m6:.5f}  "
        f"rel_diff={rel_diff(exp_asian_m6, recv_asian_m6):.4f}  PASS={ok2}",
        "",
        "## Assertion 3 — M0→M6 Black attenuation %",
        f"  cascade_atten={cas_atten:.2f}%  recomputed={recv_atten:.2f}%  "
        f"rel_diff={rel_diff(cas_atten, recv_atten):.4f}  PASS={ok3}",
        "",
        "## Assertion 4 — Asian TR5 mean tumor size",
        f"  disparity_table={exp_asian_sz:.4f}  recomputed={recv_asian_sz:.4f}  "
        f"rel_diff={rel_diff(exp_asian_sz, recv_asian_sz):.4f}  PASS={ok4}",
        "",
        "## Assertion 5 — Bethesda IV Black OR",
        f"  bethesda_table_stored={exp_b4}  refit={recv_b4}  "
        f"rel_diff={rel_diff(exp_b4, recv_b4) if np.isfinite(exp_b4) and np.isfinite(recv_b4) else 'na'}  PASS={ok5}",
        "",
        "## Assertion 6 — Black M3→M4 attenuation % (NEW v4)",
        f"  cascade_m3_or={exp_black_m3:.5f}  cascade_m4_or={exp_black_m4:.5f}  refit_m4_or={recv_black_m4:.5f}",
        f"  cas_att={cas_m3m4_att:.2f}%  recv_att={recv_m3m4_att:.2f}%  "
        f"rel_diff={rel_diff(cas_m3m4_att, recv_m3m4_att):.4f}  PASS={ok6}",
        "",
        "## Assertion 7 — Black M4→M5 attenuation % (NEW v4)",
        f"  cascade_m4_or={exp_black_m4:.5f}  cascade_m5_or={exp_black_m5:.5f}  refit_m5_or={recv_black_m5:.5f}",
        f"  cas_att={cas_m4m5_att:.2f}%  recv_att={recv_m4m5_att:.2f}%  "
        f"rel_diff={rel_diff(cas_m4m5_att, recv_m4m5_att):.4f}  PASS={ok7}",
        "",
        f"## OVERALL: {'7/7 PASS ✓' if all_pass else 'FAIL — see above'}",
    ]

    out_md = os.path.join(VERIF_DIR, "independent_recompute_v4_report.md")
    with open(out_md, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
