#!/usr/bin/env python3
"""
M048 v3 independent recompute: refit key models from patient-level CSV (not from
intermediate m048_v3 regression output tables). Asserts agreement within 2% relative for headlines.
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

V3_DIR = os.path.join(STUDY_DIR, "v3")


def rel_diff(a: float, b: float) -> float:
    if not np.isfinite(a) or not np.isfinite(b) or a == 0:
        return np.inf
    return abs(a - b) / abs(a)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--patient-csv",
        default=os.path.join(V3_DIR, "m048_v3_patient_master_full.csv"),
        help="One row per patient; same schema as m048_v3_patient_master_v1 dump.",
    )
    args = ap.parse_args()
    if not os.path.isfile(args.patient_csv):
        print("Missing patient CSV; run m048_run_analysis_v3.py first.")
        return 2

    df = prepare_v3_frame(pd.read_csv(args.patient_csv))
    df_m = df.dropna(subset=["max_tr_int"]).copy()

    # Bugs B/C: had_any_fna collinear with C(bethesda_bucket)=='missing'; the
    # has_clt/has_mng/has_graves columns are all-zero in this cohort and
    # has_niftp/has_ftump are perfect-separation path-diagnostic indicators.
    # Mirror m6_full from m048_run_analysis_v3.py exactly.
    full_formula = (
        "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
        "+ had_any_genetics + had_any_nm "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
        "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    res_full = fit_logit(full_formula, df_m)
    rt = race_or_table(res_full).set_index("race_level")

    cascade = pd.read_csv(os.path.join(V3_DIR, "m048_v3_attenuation_cascade.csv"))
    m0 = cascade[(cascade["model_step"] == "m0_race_only") & (cascade["race_level"] == "Black")].iloc[0]
    m6 = cascade[(cascade["model_step"] == "m6_full") & (cascade["race_level"] == "Black")].iloc[0]
    exp_black_or = float(m6["or"])
    recv_black_or = float(rt.loc["Black", "or"])
    ok_black = rel_diff(exp_black_or, recv_black_or) <= 0.02

    # Asian full OR assertion
    m6_asian = cascade[(cascade["model_step"] == "m6_full") & (cascade["race_level"] == "Asian")]
    if not m6_asian.empty and "Asian" in rt.index:
        exp_asian_or = float(m6_asian.iloc[0]["or"])
        recv_asian_or = float(rt.loc["Asian", "or"])
        ok_asian = rel_diff(exp_asian_or, recv_asian_or) <= 0.02
    else:
        exp_asian_or = np.nan
        recv_asian_or = np.nan
        ok_asian = False

    m0_or = float(m0["or"])
    atten_obs = 100.0 * (1.0 - recv_black_or / m0_or) if m0_or and np.isfinite(recv_black_or) else np.nan
    cas_atten = 100.0 * (1.0 - float(m6["or"]) / float(m0["or"]))
    ok_atten = rel_diff(atten_obs, cas_atten) <= 0.02 if np.isfinite(atten_obs) and np.isfinite(cas_atten) else False

    # Asian TR5 mean tumor (malignant)
    tr5 = df_m[(df_m["race_strat"] == "Asian") & (df_m["max_tr_int"] == 5) & (df_m["is_malignant"])]
    asian_tr5_sz = float(pd.to_numeric(tr5["max_tumor_size_cm"], errors="coerce").mean())
    dd = pd.read_csv(os.path.join(V3_DIR, "m048_v3_disparity_direction_table.csv"))
    row = dd[(dd["race_strat"] == "Asian") & (dd["tr_category"] == "TR5")]
    exp_sz = float(row.iloc[0]["mean_tumor_size_cm"]) if len(row) else np.nan
    ok_sz = rel_diff(exp_sz, asian_tr5_sz) <= 0.05 if np.isfinite(exp_sz) and np.isfinite(asian_tr5_sz) else False

    bstr = pd.read_csv(os.path.join(V3_DIR, "m048_v3_bethesda_stratified_TR_ROM.csv"))
    b4 = bstr[
        bstr["bethesda_bucket"].astype(str).str.upper().eq("IV")
        | (bstr["bethesda_bucket"].astype(str) == "4")
    ]
    blk = b4.loc[b4["race_level"] == "Black", "or"]
    exp_b4_black = float(blk.iloc[0]) if len(blk) else np.nan
    sub = df_m[
        df_m["bethesda_bucket"].astype(str).str.upper().eq("IV")
        | (df_m["bethesda_bucket"].astype(str) == "4")
    ].copy()
    if len(sub) >= 30:
        res_b = fit_logit("is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int", sub)
        recv_b4 = float(race_or_table(res_b).set_index("race_level").loc["Black", "or"])
        ok_b4 = rel_diff(exp_b4_black, recv_b4) <= 0.02 if np.isfinite(exp_b4_black) else False
    else:
        recv_b4 = np.nan
        ok_b4 = True

    asian_or_rel_diff = rel_diff(exp_asian_or, recv_asian_or) if np.isfinite(exp_asian_or) else np.inf
    lines = [
        f"recompute_black_full_or_rel_diff={rel_diff(exp_black_or, recv_black_or):.4f} pass={ok_black}",
        f"stored_black_full_or={exp_black_or:.5f} refit_black_full_or={recv_black_or:.5f}",
        f"atten_pct_recomputed={atten_obs:.2f} cascade_atten_pct={cas_atten:.2f} pass={ok_atten}",
        f"asian_tr5_mean_tumor_csv={asian_tr5_sz:.4f} disparity_table={exp_sz:.4f} pass={ok_sz}",
        f"bethesda_IV_black_or_stored={exp_b4_black} refit={recv_b4} pass={ok_b4}",
        f"asian_full_or_rel_diff={asian_or_rel_diff:.4f} pass={ok_asian}",
        f"stored_asian_full_or={exp_asian_or:.5f} refit_asian_full_or={recv_asian_or:.5f}",
    ]
    out_md = os.path.join(os.path.dirname(__file__), "independent_recompute_v3_report.md")
    with open(out_md, "w") as f:
        f.write("# independent_recompute_v3\n\n")
        for ln in lines:
            f.write(ln + "\n")
    print("\n".join(lines))
    return 0 if all([ok_black, ok_atten, ok_sz, ok_b4, ok_asian]) else 1


if __name__ == "__main__":
    sys.exit(main())
