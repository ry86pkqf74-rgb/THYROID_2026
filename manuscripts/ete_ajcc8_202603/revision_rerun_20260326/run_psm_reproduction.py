#!/usr/bin/env python3
"""
Reproduce PSM from current exports; writes only to this directory.
Self-contained (no matplotlib import chain).
"""
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from sklearn.linear_model import LogisticRegression

SEED = 42
np.random.seed(SEED)

# staging / revision_rerun_20260326 -> THYROID_2026 repo root (inner)
ROOT = Path(__file__).resolve().parent.parent.parent.parent
OUT = Path(__file__).resolve().parent


def load_expanded():
    rec = pd.read_csv(ROOT / "exports" / "recurrence_full.csv")
    img = pd.read_csv(ROOT / "exports" / "imaging_correlation.csv")
    ptc = pd.read_csv(ROOT / "exports" / "ptc_full.csv")

    rec_ptc = rec[rec["histology_1_type"] == "PTC"].copy()
    rec_ptc = rec_ptc.drop_duplicates(subset=["research_id"], keep="first")

    img_cols = [
        "research_id", "largest_tumor_cm", "ct_pathologic_ln_flag",
        "mri_pathologic_ln_flag", "ct_nodule_flag", "mri_nodule_flag",
        "ct_count", "mri_count", "us_count",
    ]
    img_dedup = img[img_cols].drop_duplicates(subset=["research_id"], keep="first")

    df = rec_ptc.merge(img_dedup, on="research_id", how="left", suffixes=("", "_img"))
    if "largest_tumor_cm_img" in df.columns:
        df["largest_tumor_cm"] = df["largest_tumor_cm"].fillna(df["largest_tumor_cm_img"])
        df.drop(columns=["largest_tumor_cm_img"], inplace=True)

    orig_cols = ["research_id", "ln_examined", "ln_positive", "m_stage_ajcc8"]
    orig = ptc[orig_cols + ["surgery_date"]].copy()
    orig_dedup = orig[orig_cols].drop_duplicates(subset=["research_id"], keep="first")
    df = df.merge(orig_dedup, on="research_id", how="left")

    return df, ptc


def derive_core_vars(df: pd.DataFrame, ptc: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    ete_any = out["tumor_1_extrathyroidal_ext"].astype(str).str.lower().isin(["true", "1", "yes"])
    gross = out["tumor_1_gross_ete"].fillna(0).astype(float).eq(1)
    out["ete_group"] = np.select(
        [gross, ete_any & ~gross, ~ete_any],
        ["Gross ETE", "Microscopic ETE", "No ETE"],
        default="Unknown",
    )
    out["ete_micro"] = out["ete_group"].eq("Microscopic ETE").astype(int)
    out["female"] = out["sex"].eq("Female").astype(int)
    out["n_positive_flag"] = out["n_stage_ajcc8"].fillna("NX").str.startswith("N1").astype(int)

    out["imaging_structural_proxy"] = (
        out["ct_pathologic_ln_flag"].fillna(0).astype(int).eq(1)
        | out["mri_pathologic_ln_flag"].fillna(0).astype(int).eq(1)
    ).astype(int)

    ptc_dates = ptc[["research_id", "surgery_date"]].copy()
    ptc_dates["surgery_date"] = pd.to_datetime(ptc_dates["surgery_date"], errors="coerce")
    reop_map = (
        ptc_dates.dropna(subset=["surgery_date"])
        .groupby("research_id")["surgery_date"]
        .nunique()
        .gt(1)
        .astype(int)
    )
    out["reoperation_proxy"] = out["research_id"].map(reop_map).fillna(0).astype(int)
    out["structural_recurrence"] = (
        out["imaging_structural_proxy"].eq(1) | out["reoperation_proxy"].eq(1)
    ).astype(int)

    out["surgery_date"] = pd.to_datetime(out["surgery_date"], errors="coerce")
    out["tg_last_date"] = pd.to_datetime(out["tg_last_date"], errors="coerce")
    out["last_followup_date"] = out["tg_last_date"].fillna(out["surgery_date"])
    out["dfs_years"] = (
        (out["last_followup_date"] - out["surgery_date"]).dt.days / 365.25
    )
    out["dfs_years"] = out["dfs_years"].clip(lower=0)
    out["dfs_event"] = out["structural_recurrence"].astype(int)

    return out


def propensity_match(df: pd.DataFrame):
    sub = df[df["ete_group"].isin(["No ETE", "Microscopic ETE"])].copy()
    sub["treat"] = sub["ete_micro"].astype(int)
    covars = ["age_at_surgery", "female", "largest_tumor_cm", "n_positive_flag"]
    sub = sub.dropna(subset=covars + ["structural_recurrence", "dfs_years"])

    X = sub[covars].astype(float).values
    y = sub["treat"].values
    lr = LogisticRegression(max_iter=1000, random_state=SEED)
    lr.fit(X, y)
    ps = lr.predict_proba(X)[:, 1]
    sub["propensity"] = ps

    treated = sub[sub["treat"] == 1].copy().sort_values("propensity")
    control = sub[sub["treat"] == 0].copy().sort_values("propensity")
    available_controls = control.index.tolist()

    caliper = 0.05
    pairs = []
    for tidx, trow in treated.iterrows():
        if not available_controls:
            break
        cands = control.loc[available_controls]
        dist = (cands["propensity"] - trow["propensity"]).abs()
        cidx = dist.idxmin()
        if dist.loc[cidx] <= caliper:
            pairs.append((tidx, cidx))
            available_controls.remove(cidx)

    if not pairs:
        return None, None, None, sub

    t_ids = [a for a, _ in pairs]
    c_ids = [b for _, b in pairs]
    matched = pd.concat([sub.loc[t_ids], sub.loc[c_ids]], axis=0).copy()

    t = matched[matched["treat"] == 1]
    c = matched[matched["treat"] == 0]
    tab = pd.crosstab(matched["treat"], matched["structural_recurrence"])
    a = tab.get(1, pd.Series(dtype=float)).get(1, 0) + 0.5
    b = tab.get(1, pd.Series(dtype=float)).get(0, 0) + 0.5
    c0 = tab.get(0, pd.Series(dtype=float)).get(1, 0) + 0.5
    d = tab.get(0, pd.Series(dtype=float)).get(0, 0) + 0.5
    or_est = (a * d) / (b * c0)
    _, p_fisher = fisher_exact(tab.values if tab.shape == (2, 2) else np.array([[0, 0], [0, 0]]))

    def _pval_str(p: float) -> str:
        return "<0.001" if p < 0.001 else f"{p:.3f}"

    effect = pd.DataFrame([{
        "Matched_pairs": len(pairs),
        "NoETE_N": len(c),
        "mETE_N": len(t),
        "NoETE_structural_pct": round(100 * c["structural_recurrence"].mean(), 2),
        "mETE_structural_pct": round(100 * t["structural_recurrence"].mean(), 2),
        "Risk_difference_pct": round(
            100 * (t["structural_recurrence"].mean() - c["structural_recurrence"].mean()), 2
        ),
        "OR_structural_recurrence": round(or_est, 4),
        "Fisher_p": _pval_str(float(p_fisher)),
    }])

    balance_rows = []
    for v in covars:
        m_t = sub.loc[sub["treat"] == 1, v].mean()
        m_c = sub.loc[sub["treat"] == 0, v].mean()
        sd_p = np.sqrt((sub.loc[sub["treat"] == 1, v].var() + sub.loc[sub["treat"] == 0, v].var()) / 2)
        smd_before = (m_t - m_c) / sd_p if sd_p and not np.isnan(sd_p) else np.nan

        mm_t = matched.loc[matched["treat"] == 1, v].mean()
        mm_c = matched.loc[matched["treat"] == 0, v].mean()
        msd_p = np.sqrt((matched.loc[matched["treat"] == 1, v].var() + matched.loc[matched["treat"] == 0, v].var()) / 2)
        smd_after = (mm_t - mm_c) / msd_p if msd_p and not np.isnan(msd_p) else np.nan

        balance_rows.append({
            "Variable": v,
            "SMD_before": round(float(smd_before), 4) if not pd.isna(smd_before) else np.nan,
            "SMD_after": round(float(smd_after), 4) if not pd.isna(smd_after) else np.nan,
        })
    balance = pd.DataFrame(balance_rows)
    return matched, effect, balance, sub


def main():
    df, ptc = load_expanded()
    df = derive_core_vars(df, ptc)
    matched, effect, balance, pool = propensity_match(df)
    covars = ["age_at_surgery", "female", "largest_tumor_cm", "n_positive_flag"]
    sub = pool
    meta = {
        "exports_ROOT": str(ROOT),
        "n_expanded_ptc": len(df),
        "psm_eligible_pool_nonmissing": len(sub),
        "treated_micro_ete_in_pool": int((sub["ete_micro"] == 1).sum()),
        "control_no_ete_in_pool": int((sub["ete_micro"] == 0).sum()),
    }
    if effect is not None:
        meta["matched_pairs"] = int(effect.iloc[0]["Matched_pairs"])
        meta["frozen_audit_pairs_expected"] = 711

    lines = [f"{k}: {v}" for k, v in meta.items()]
    (OUT / "psm_reproduction_summary.txt").write_text("\n".join(lines) + "\n")
    if effect is not None:
        effect.to_csv(OUT / "table6_propensity_matching_effect_rerun.csv", index=False)
    if balance is not None:
        balance.to_csv(OUT / "table6_propensity_matching_balance_rerun.csv", index=False)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
