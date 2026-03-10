#!/usr/bin/env python3
"""
Proposal 2 extension analyses:
1) Structural recurrence endpoint + DFS proxy
2) Propensity score matching (mETE vs No ETE)
3) Tumor-size stratified models
4) Interaction tests (mETE x size, age, nodal)

Uses expanded cohort (all PTC, N~3,278).
Deterministic seed: 42.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import yaml

import statsmodels.api as sm
from sklearn.linear_model import LogisticRegression
from scipy.stats import fisher_exact
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test


SEED = 42
np.random.seed(SEED)

STUDY_DIR = Path(__file__).resolve().parent
ROOT = STUDY_DIR.parent.parent
FIG_DIR = STUDY_DIR / "audit_figures"
TBL_DIR = STUDY_DIR / "audit_tables"
META_PATH = STUDY_DIR / "analysis_metadata.yaml"
FIG_DIR.mkdir(exist_ok=True)
TBL_DIR.mkdir(exist_ok=True)

sns.set_theme(style="whitegrid", context="paper")
plt.rcParams.update({
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "font.family": "sans-serif",
})


def pval_str(p: float) -> str:
    if pd.isna(p):
        return "—"
    return "<0.001" if p < 0.001 else f"{p:.3f}"


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

    # Structural recurrence endpoint:
    # - imaging evidence proxy: pathologic LN on CT/MRI
    # - reoperation proxy: >1 distinct surgery date in source pathology table
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

    # DFS proxy: surgery -> last follow-up (tg_last_date), event by structural endpoint
    out["surgery_date"] = pd.to_datetime(out["surgery_date"], errors="coerce")
    out["tg_last_date"] = pd.to_datetime(out["tg_last_date"], errors="coerce")
    out["last_followup_date"] = out["tg_last_date"].fillna(out["surgery_date"])
    out["dfs_years"] = (
        (out["last_followup_date"] - out["surgery_date"]).dt.days / 365.25
    )
    out["dfs_years"] = out["dfs_years"].clip(lower=0)
    out["dfs_event"] = out["structural_recurrence"].astype(int)

    return out


def structural_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for g in ["No ETE", "Microscopic ETE", "Gross ETE"]:
        sub = df[df["ete_group"] == g]
        if len(sub) == 0:
            continue
        rows.append({
            "ETE_group": g,
            "N": len(sub),
            "Structural_recurrence_n": int(sub["structural_recurrence"].sum()),
            "Structural_recurrence_pct": round(100 * sub["structural_recurrence"].mean(), 2),
            "Imaging_proxy_n": int(sub["imaging_structural_proxy"].sum()),
            "Reoperation_proxy_n": int(sub["reoperation_proxy"].sum()),
            "Median_followup_years": round(sub["dfs_years"].median(), 2),
        })
    return pd.DataFrame(rows)


def propensity_match(df: pd.DataFrame):
    # Isolate mETE effect by comparing mETE vs No ETE only
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
        return None, None, None

    t_ids = [a for a, _ in pairs]
    c_ids = [b for _, b in pairs]
    matched = pd.concat([sub.loc[t_ids], sub.loc[c_ids]], axis=0).copy()
    matched["match_group"] = ["T"] * len(t_ids) + ["C"] * len(c_ids)

    # Effect table
    t = matched[matched["treat"] == 1]
    c = matched[matched["treat"] == 0]
    tab = pd.crosstab(matched["treat"], matched["structural_recurrence"])
    # Haldane-Anscombe for OR stability
    a = tab.get(1, pd.Series()).get(1, 0) + 0.5
    b = tab.get(1, pd.Series()).get(0, 0) + 0.5
    c0 = tab.get(0, pd.Series()).get(1, 0) + 0.5
    d = tab.get(0, pd.Series()).get(0, 0) + 0.5
    or_est = (a * d) / (b * c0)
    _, p_fisher = fisher_exact(tab.values if tab.shape == (2, 2) else np.array([[0, 0], [0, 0]]))

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
        "Fisher_p": pval_str(p_fisher),
    }])

    # Balance (standardized mean differences)
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
    return matched, effect, balance


def plot_matched_dfs(matched: pd.DataFrame):
    if matched is None or matched.empty:
        return None
    fig, ax = plt.subplots(figsize=(8, 6))
    kmf = KaplanMeierFitter()
    colors = {0: "#2ecc71", 1: "#f39c12"}
    labels = {0: "No ETE (matched)", 1: "Microscopic ETE (matched)"}

    for g in [0, 1]:
        sub = matched[matched["treat"] == g]
        if len(sub) < 5:
            continue
        kmf.fit(sub["dfs_years"], event_observed=sub["dfs_event"], label=f"{labels[g]} (n={len(sub)})")
        kmf.plot_survival_function(ax=ax, color=colors[g], linewidth=2)

    a = matched[matched["treat"] == 0]
    b = matched[matched["treat"] == 1]
    if len(a) >= 5 and len(b) >= 5:
        lr = logrank_test(a["dfs_years"], b["dfs_years"], event_observed_A=a["dfs_event"], event_observed_B=b["dfs_event"])
        ax.text(0.02, 0.05, f"Log-rank p = {pval_str(lr.p_value)}", transform=ax.transAxes)

    ax.set_title("Figure 10. Matched DFS by mETE Status (Structural Recurrence Endpoint)")
    ax.set_xlabel("Years from surgery")
    ax.set_ylabel("Disease-free probability")
    plt.tight_layout()
    fig.savefig(FIG_DIR / "fig10_matched_dfs.png")
    fig.savefig(FIG_DIR / "fig10_matched_dfs.pdf")
    plt.close(fig)
    return True


def logistic_or(df: pd.DataFrame, formula_vars: list[str], interaction_var: str | None = None):
    dat = df.copy()
    dat = dat.dropna(subset=["structural_recurrence"] + formula_vars)
    if dat["structural_recurrence"].nunique() < 2 or len(dat) < 30:
        return None
    X = dat[formula_vars].astype(float).copy()
    if interaction_var is not None:
        X[interaction_var] = X["ete_micro"] * X[interaction_var.replace("ete_micro_x_", "")]
    X = sm.add_constant(X, has_constant="add")
    y = dat["structural_recurrence"].astype(int)
    try:
        fit = sm.Logit(y, X).fit(disp=False)
    except Exception:
        return None
    return fit


def stratified_models(df: pd.DataFrame) -> pd.DataFrame:
    # Requested strata: <=1, 1-2, 2-4 cm
    sub = df[df["ete_group"].isin(["No ETE", "Microscopic ETE"])].copy()
    sub["size_group"] = pd.cut(
        sub["largest_tumor_cm"],
        bins=[0, 1, 2, 4],
        labels=["<=1 cm", "1-2 cm", "2-4 cm"],
        include_lowest=True,
        right=True,
    )
    rows = []
    for grp in ["<=1 cm", "1-2 cm", "2-4 cm"]:
        dat = sub[sub["size_group"] == grp].copy()
        fit = logistic_or(dat, ["ete_micro", "age_at_surgery", "female", "n_positive_flag"])
        if fit is None or "ete_micro" not in fit.params.index:
            rows.append({
                "Size_group": grp, "N": len(dat), "Event_n": int(dat["structural_recurrence"].sum()),
                "mETE_OR": np.nan, "CI": "—", "p": "—",
            })
            continue
        b = fit.params["ete_micro"]
        se = fit.bse["ete_micro"]
        rows.append({
            "Size_group": grp,
            "N": len(dat),
            "Event_n": int(dat["structural_recurrence"].sum()),
            "mETE_OR": round(float(np.exp(b)), 4),
            "CI": f"({np.exp(b-1.96*se):.2f}-{np.exp(b+1.96*se):.2f})",
            "p": pval_str(float(fit.pvalues["ete_micro"])),
        })
    return pd.DataFrame(rows)


def interaction_tests(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["ete_group"].isin(["No ETE", "Microscopic ETE"])].copy()
    tests = [
        ("ete_micro_x_largest_tumor_cm", "largest_tumor_cm"),
        ("ete_micro_x_age_at_surgery", "age_at_surgery"),
        ("ete_micro_x_n_positive_flag", "n_positive_flag"),
    ]
    rows = []
    base_vars = ["ete_micro", "age_at_surgery", "female", "largest_tumor_cm", "n_positive_flag"]
    for name, rhs in tests:
        fit = logistic_or(sub, base_vars, interaction_var=name)
        if fit is None or name not in fit.params.index:
            rows.append({"Interaction": name, "OR": np.nan, "CI": "—", "p": "—"})
            continue
        b = fit.params[name]
        se = fit.bse[name]
        rows.append({
            "Interaction": name.replace("ete_micro_x_", "mETE x "),
            "OR": round(float(np.exp(b)), 4),
            "CI": f"({np.exp(b-1.96*se):.2f}-{np.exp(b+1.96*se):.2f})",
            "p": pval_str(float(fit.pvalues[name])),
        })
    return pd.DataFrame(rows)


def update_metadata(df, structural_tbl, psm_effect, psm_balance, strata, inter):
    if META_PATH.exists():
        meta = yaml.safe_load(META_PATH.read_text()) or {}
    else:
        meta = {}

    ext = {
        "endpoint_extension": {
            "definition": {
                "structural_recurrence": "imaging pathologic LN flag (CT/MRI) OR reoperation proxy (>1 surgery_date in source pathology table)",
                "dfs": "time from surgery_date to tg_last_date (or surgery_date if missing), censored by structural endpoint status at follow-up",
            },
            "counts": {
                "N_total": int(len(df)),
                "structural_events": int(df["structural_recurrence"].sum()),
                "imaging_proxy_events": int(df["imaging_structural_proxy"].sum()),
                "reoperation_proxy_events": int(df["reoperation_proxy"].sum()),
            },
            "tables": [
                "table5_structural_endpoint.csv",
                "table6_propensity_matching_effect.csv",
                "table6_propensity_matching_balance.csv",
                "table7_stratified_models.csv",
                "table8_interaction_tests.csv",
            ],
            "figures": ["fig10_matched_dfs.png"],
        }
    }
    if psm_effect is not None and not psm_effect.empty:
        ext["endpoint_extension"]["psm_summary"] = psm_effect.iloc[0].to_dict()

    ext["endpoint_extension"]["stratified_summary"] = strata.to_dict(orient="records")
    ext["endpoint_extension"]["interaction_summary"] = inter.to_dict(orient="records")

    meta.update(ext)
    META_PATH.write_text(yaml.dump(meta, default_flow_style=False, sort_keys=False))


def main():
    print("=" * 72)
    print("PROPOSAL 2 EXTENSION — TRUE ENDPOINT + PSM + STRATA + INTERACTIONS")
    print("=" * 72)

    df, ptc = load_expanded()
    df = derive_core_vars(df, ptc)
    print(f"N expanded: {len(df)}")
    print(f"Structural events: {int(df['structural_recurrence'].sum())}")

    t5 = structural_table(df)
    t5.to_csv(TBL_DIR / "table5_structural_endpoint.csv", index=False)
    print("Saved table5_structural_endpoint.csv")

    matched, psm_effect, psm_balance = propensity_match(df)
    if psm_effect is not None:
        psm_effect.to_csv(TBL_DIR / "table6_propensity_matching_effect.csv", index=False)
        psm_balance.to_csv(TBL_DIR / "table6_propensity_matching_balance.csv", index=False)
        print("Saved propensity matching tables")
        plot_matched_dfs(matched)
        print("Saved fig10_matched_dfs.png/.pdf")
    else:
        print("PSM produced no matched set under current caliper.")

    t7 = stratified_models(df)
    t7.to_csv(TBL_DIR / "table7_stratified_models.csv", index=False)
    print("Saved table7_stratified_models.csv")

    t8 = interaction_tests(df)
    t8.to_csv(TBL_DIR / "table8_interaction_tests.csv", index=False)
    print("Saved table8_interaction_tests.csv")

    update_metadata(df, t5, psm_effect, psm_balance, t7, t8)
    print("Updated analysis_metadata.yaml with endpoint extension block")

    print("\nKey outputs:")
    print(t5.to_string(index=False))
    if psm_effect is not None:
        print("\nPSM effect:")
        print(psm_effect.to_string(index=False))
    print("\nStratified:")
    print(t7.to_string(index=False))
    print("\nInteractions:")
    print(t8.to_string(index=False))


if __name__ == "__main__":
    main()
