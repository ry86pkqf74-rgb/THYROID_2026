#!/usr/bin/env python3
"""H2 v3: BigQuery cohort pull, Tables 1–5 stats, hypopara logistic, Fisher, figures.

Requires: GOOGLE_APPLICATION_CREDENTIALS → thyroid-pub-loader-key.json
Random seed: 42
"""
from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from google.cloud import bigquery
from scipy.stats import chi2_contingency, fisher_exact, kruskal, mannwhitneyu
import statsmodels.api as sm
from statsmodels.stats.contingency_tables import Table2x2

warnings.filterwarnings("ignore", category=UserWarning)
np.random.seed(42)
RNG = np.random.default_rng(42)

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_canonical"
HERE = Path(__file__).resolve().parent
FIG_DIR = HERE / "figures_v3"
CREDS = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/loganglosser/Desktop/Thyroid Motherduck To GC migration/_creds/thyroid-pub-loader-key.json",
)

SQL = """
SELECT
  c.research_id,
  c.race,
  c.sex,
  c.age_at_surgery,
  c.gland_weight_final_g,
  c.syn_left_lobe_volume_cc,
  c.syn_right_lobe_volume_cc,
  c.ct_substernal_extension_any,
  c.mri_substernal_extension_any,
  c.n_us_exams,
  c.n_fna_episodes,
  c.prm_first_fna_days_from_surg,
  c.molecular_tested_confirmed,
  c.bethesda_max_preop_2015,
  c.syn_hashimoto,
  c.syn_graves,
  c.syn_chronic_thyroiditis,
  c.syn_follicular_adenoma,
  c.syn_colloid_nodule,
  c.syn_hyperplastic_nodules,
  c.comp_hypoparathyroidism_confirmed,
  c.comp_hypoparathyroidism_transient,
  c.comp_hypoparathyroidism_permanent,
  c.comp_hypoparathyroidism_preexisting,
  c.comp_hypocalcemia_confirmed,
  c.comp_hypocalcemia_timing_window,
  c.comp_hypocalcemia_clinical_preexisting,
  c.comp_rln_injury_confirmed,
  c.comp_rln_injury_preop,
  c.comp_vc_paralysis_confirmed,
  c.comp_vc_paralysis_preop,
  c.comp_vc_paresis_confirmed,
  b.any_concomitant_malignant
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` c
LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_patient_rollup_v1` b
  ON c.research_id = b.research_id
WHERE c.syn_multinodular_goiter IS TRUE
"""


def p_fmt(p: float) -> str:
    if p < 1e-4:
        return f"{p:.2e}"
    if p < 0.001:
        return "<0.001"
    return f"{p:.4f}".rstrip("0").rstrip(".")


def med_iqr(s: pd.Series) -> str:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) == 0:
        return "—"
    q1, med, q3 = s.quantile([0.25, 0.5, 0.75])
    return f"{med:.1f} ({q1:.1f}–{q3:.1f})"


def kw_groups(df: pd.DataFrame, value_col: str, group_col: str = "race") -> tuple[float, float]:
    """Kruskal–Wallis H, p; exclude null race."""
    d = df.dropna(subset=[value_col, group_col])
    groups = [g[value_col].values for _, g in d.groupby(group_col, dropna=False)]
    groups = [x[~np.isnan(x.astype(float))] for x in groups]  # type: ignore
    groups = [g for g in groups if len(g) > 0]
    if len(groups) < 2:
        return float("nan"), float("nan")
    h, p = kruskal(*groups)
    return float(h), float(p)


def chi2_cat_by_race(df: pd.DataFrame, col: str) -> tuple[float, float, int]:
    """Chi-square test of independence race × col (categorical stringified)."""
    d = df.dropna(subset=["race"]).copy()
    d["_c"] = d[col].astype(str).replace({"None": "NULL", "nan": "NULL"})
    tab = pd.crosstab(d["race"], d["_c"])
    if tab.size == 0 or tab.shape[0] < 2 or tab.shape[1] < 2:
        return float("nan"), float("nan"), 0
    chi2, p, dof, _ = chi2_contingency(tab.values)
    return float(chi2), float(p), int(dof)


def chi2_binary_by_race(df: pd.DataFrame, col: str) -> tuple[float, float]:
    d = df.dropna(subset=["race"]).copy()
    d["_b"] = d[col].fillna(False).astype(bool)
    tab = pd.crosstab(d["race"], d["_b"])
    if tab.shape[1] < 2:
        # single column — no variation
        return float("nan"), float("nan")
    chi2, p, _, _ = chi2_contingency(tab.values)
    return float(chi2), float(p)


def main() -> None:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    client = bigquery.Client(project=PROJECT)
    df = client.query(SQL).to_dataframe()
    assert len(df) == 6075, len(df)

    df["substernal_any"] = (
        df["ct_substernal_extension_any"].fillna(False).astype(bool)
        | df["mri_substernal_extension_any"].fillna(False).astype(bool)
    )
    df["sex_display"] = df["sex"].str.title()
    df["preop_hypocalc"] = (
        (df["comp_hypocalcemia_timing_window"].astype(str) == "pre_surgery")
        | (df["comp_hypocalcemia_clinical_preexisting"].fillna(False).astype(bool))
    )
    df["any_concomitant_malignant"] = df["any_concomitant_malignant"].fillna(False).astype(bool)

    stats_out: dict = {"n_cohort": len(df)}

    # --- Omnibus tests Table 1-style
    d_race = df[df["race"].notna()].copy()

    # Sex × race
    tab_sex = pd.crosstab(d_race["race"], d_race["sex"])
    chi2_sex, p_sex, dof_sex, _ = chi2_contingency(tab_sex.values)
    stats_out["table1_chi2_sex_x_race"] = {"chi2": chi2_sex, "p": p_sex, "dof": dof_sex}

    # Continuous: age, weight, lobe volumes
    for col, label in [
        ("age_at_surgery", "age"),
        ("gland_weight_final_g", "gland_weight_g"),
        ("syn_left_lobe_volume_cc", "l_vol"),
        ("syn_right_lobe_volume_cc", "r_vol"),
    ]:
        h, p = kw_groups(d_race, col, "race")
        stats_out[f"kw_{label}"] = {"H": h, "p": p}

    # Substernal any × race
    chi2_sub, p_sub = chi2_binary_by_race(d_race, "substernal_any")
    stats_out["chi2_substernal_x_race"] = {"chi2": chi2_sub, "p": p_sub}

    # CT substernal × race
    chi2_ct, p_ct = chi2_binary_by_race(d_race, "ct_substernal_extension_any")
    stats_out["chi2_ct_substernal_x_race"] = {"chi2": chi2_ct, "p": p_ct}

    # Table 2-style: substernal × race within sex
    for tag in ("female", "male"):
        sub = d_race[d_race["sex"] == tag]
        chi2_s, p_s = chi2_binary_by_race(sub, "substernal_any")
        stats_out[f"chi2_substernal_x_race_among_{tag}"] = {"chi2": chi2_s, "p": p_s, "n": int(len(sub))}

    # Pairwise MWU Bonferroni: Black vs White for key continuous (primary contrast)
    major = ["Black or African American", "White"]
    pairs_bw = []
    for col in ["gland_weight_final_g", "syn_left_lobe_volume_cc", "syn_right_lobe_volume_cc", "age_at_surgery"]:
        a = pd.to_numeric(d_race.loc[d_race["race"] == major[0], col], errors="coerce").dropna()
        b = pd.to_numeric(d_race.loc[d_race["race"] == major[1], col], errors="coerce").dropna()
        _, p = mannwhitneyu(a, b, alternative="two-sided")
        pairs_bw.append({"contrast": col, "p_uncorrected": float(p)})
    stats_out["mwu_black_vs_white"] = pairs_bw

    # Table 3a KW / chi2
    d_race["mol_pos"] = d_race["molecular_tested_confirmed"].fillna(False).astype(bool)
    for col, lab in [("n_us_exams", "n_us"), ("n_fna_episodes", "n_fna")]:
        h, p = kw_groups(d_race, col, "race")
        stats_out[f"kw_table3_{lab}"] = {"H": h, "p": p}
    chi2_mol, p_mol = chi2_binary_by_race(d_race, "mol_pos")
    stats_out["chi2_molecular_x_race"] = {"chi2": chi2_mol, "p": p_mol}

    fna_pos = d_race[
        (pd.to_numeric(d_race["prm_first_fna_days_from_surg"], errors="coerce").notna())
        & (pd.to_numeric(d_race["prm_first_fna_days_from_surg"], errors="coerce") > 0)
        & (pd.to_numeric(d_race["prm_first_fna_days_from_surg"], errors="coerce") < 10000)
    ]
    h, p = kw_groups(fna_pos, "prm_first_fna_days_from_surg", "race")
    stats_out["kw_prm_fna_days_pos_only"] = {"H": h, "p": p, "n": len(fna_pos)}

    # Table 3b pathology flags — chi2 per flag
    for flag in [
        "syn_hashimoto",
        "syn_graves",
        "syn_follicular_adenoma",
        "syn_chronic_thyroiditis",
        "any_concomitant_malignant",
    ]:
        chi2_c, p_c = chi2_binary_by_race(d_race, flag)
        stats_out[f"chi2_{flag}_x_race"] = {"chi2": chi2_c, "p": p_c}

    # Table 5 — hypopara × race
    chi2_hp, p_hp = chi2_binary_by_race(d_race, "comp_hypoparathyroidism_confirmed")
    stats_out["chi2_hypopara_x_race"] = {"chi2": chi2_hp, "p": p_hp}

    # Fisher Black/AA vs White RLN
    def fish_rln_vc(outcome_col: str) -> dict:
        blk = d_race[d_race["race"] == "Black or African American"]
        wht = d_race[d_race["race"] == "White"]
        a = int(blk[outcome_col].fillna(False).sum())  # Black events
        b = int(len(blk) - a)
        c = int(wht[outcome_col].fillna(False).sum())
        d = int(len(wht) - c)
        # [[Black event, Black no], [White event, White no]]
        oddsratio, p = fisher_exact([[a, b], [c, d]])
        tab = Table2x2([[a, b], [c, d]])
        try:
            ci_l, ci_u = tab.oddsratio_confint()
        except Exception:
            ci_l, ci_u = float("nan"), float("nan")
        return {"a": a, "b": b, "c": c, "d": d, "or": float(oddsratio), "p": float(p), "ci95": (float(ci_l), float(ci_u))}

    stats_out["fisher_rln_baa_white"] = fish_rln_vc("comp_rln_injury_confirmed")
    stats_out["fisher_vc_paralysis_baa_white"] = fish_rln_vc("comp_vc_paralysis_confirmed")

    # Hypopara logistic (reference White, Female)
    log = d_race.copy()
    log["y"] = log["comp_hypoparathyroidism_confirmed"].fillna(False).astype(int)
    medians = log[["age_at_surgery", "gland_weight_final_g"]].median(numeric_only=True)
    log["age_at_surgery"] = pd.to_numeric(log["age_at_surgery"], errors="coerce").fillna(medians["age_at_surgery"])
    log["gland_weight_final_g"] = pd.to_numeric(log["gland_weight_final_g"], errors="coerce").fillna(
        medians["gland_weight_final_g"]
    )
    log["ct_substernal_extension_any"] = log["ct_substernal_extension_any"].fillna(False).astype(int)
    race_d = pd.get_dummies(log["race"], prefix="race", dtype=float)
    if "race_White" in race_d.columns:
        race_d = race_d.drop(columns=["race_White"])
    sex_d = pd.get_dummies(log["sex"], prefix="sex", dtype=float)
    if "sex_female" in sex_d.columns:
        sex_d = sex_d.drop(columns=["sex_female"])
    X = pd.concat([race_d, sex_d], axis=1)
    X["age_at_surgery"] = pd.to_numeric(log["age_at_surgery"], errors="coerce").astype(float)
    X["gland_weight_final_g"] = pd.to_numeric(log["gland_weight_final_g"], errors="coerce").astype(float)
    X["ct_substernal_extension_any"] = pd.to_numeric(log["ct_substernal_extension_any"], errors="coerce").astype(float)
    X = X.astype(np.float64)
    X = sm.add_constant(X, has_constant="add")
    try:
        model = sm.Logit(log["y"], X)
        res = model.fit(method="lbfgs", maxiter=200, disp=False)
        params = res.params
        conf = res.conf_int()
        ors = np.exp(params)
        ci = np.exp(conf)
        stats_out["hypopara_logit"] = {
            "converged": bool(res.mle_retvals["converged"]) if "mle_retvals" in dir(res) else True,
            "n": int(len(log)),
            "events": int(log["y"].sum()),
            "coef": {k: float(v) for k, v in params.items()},
            "or": {k: float(v) for k, v in ors.items()},
            "ci95": {k: [float(ci.loc[k, 0]), float(ci.loc[k, 1])] for k in ci.index},
            "pvalues": {k: float(v) for k, v in res.pvalues.items()},
            "aic": float(res.aic),
        }
    except Exception as e:
        stats_out["hypopara_logit"] = {"error": str(e)}

    # Forest: substernal ~ race×sex, ref White female (cells with n≥30 only)
    forest_df = d_race.copy()
    forest_df["y"] = forest_df["substernal_any"].astype(int)
    forest_df["race_c"] = forest_df["race"].fillna("Unknown")
    forest_df["sex_c"] = forest_df["sex"].fillna("unknown")
    cell_n = forest_df.groupby(["race_c", "sex_c"], dropna=False).size().reset_index(name="cell_n")
    min_cell = 30
    levels = []
    for _, row in cell_n.iterrows():
        r, s, n = row["race_c"], row["sex_c"], row["cell_n"]
        if r == "White" and s == "female":
            continue  # reference
        if n < min_cell:
            continue
        colname = f"RS_{str(r).replace(' ', '_')}_{s}"
        mask = (forest_df["race_c"] == r) & (forest_df["sex_c"] == s)
        forest_df[colname] = mask.astype(np.float64)
        levels.append((colname, r, s, int(n)))
    level_names = [t[0] for t in levels]
    Xf = sm.add_constant(forest_df[level_names].astype(np.float64), has_constant="add")
    try:
        rf = sm.Logit(forest_df["y"], Xf).fit(method="lbfgs", maxiter=300, disp=False)
        forest_rows = []
        for lab, rlab, slab, cn in levels:
            lo_e = float(rf.conf_int().loc[lab, 0])
            hi_e = float(rf.conf_int().loc[lab, 1])
            if lo_e < -50:
                lo_e = -50
            if hi_e > 50:
                hi_e = 50
            forest_rows.append(
                {
                    "label": f"{rlab} — {slab} (n={cn})",
                    "or": float(np.exp(rf.params[lab])),
                    "lo": float(np.exp(lo_e)),
                    "hi": float(np.exp(hi_e)),
                    "p": float(rf.pvalues[lab]),
                }
            )
        stats_out["forest_substernal_race_sex"] = forest_rows
        stats_out["forest_aic"] = float(rf.aic)
        stats_out["forest_min_cell_n"] = min_cell
    except Exception as e:
        stats_out["forest_substernal_race_sex"] = [{"error": str(e)}]

    class _SafeEnc(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, float) and (np.isnan(o) or np.isinf(o)):
                return None
            return super().default(o)

    with open(HERE / "h2_v3_stats.json", "w") as f:
        json.dump(stats_out, f, indent=2, cls=_SafeEnc)

    # ---------- Figures ----------
    sns.set_theme(style="whitegrid", font_scale=1.05)
    # Fig 1 violin weight by race (exclude n<11)
    race_counts = df[df["race"].notna()]["race"].value_counts()
    keep_races = race_counts[race_counts >= 11].index.tolist()
    v1 = df[df["race"].isin(keep_races) & df["gland_weight_final_g"].notna()].copy()
    v1["race"] = pd.Categorical(v1["race"], categories=sorted(keep_races, key=lambda x: -race_counts[x]))
    fig, ax = plt.subplots(figsize=(11, 5.5))
    sns.violinplot(data=v1, x="race", y="gland_weight_final_g", ax=ax, inner="box", cut=0)
    ax.set_yscale("log")
    ax.set_ylabel("Gland weight (g), log scale")
    ax.set_xlabel("Race")
    plt.xticks(rotation=35, ha="right")
    ax.set_title("Figure 1. Gland weight distribution by race (n≥11 per group)")
    plt.tight_layout()
    fig.savefig(FIG_DIR / "figure_1_gland_weight_by_race.png", dpi=300, bbox_inches="tight")
    fig.savefig(FIG_DIR / "figure_1_gland_weight_by_race.svg", bbox_inches="tight")
    plt.close()

    # Fig 2 box FNA days >0
    f2 = fna_pos.copy()
    f2 = f2[f2["race"].isin(keep_races)]
    fig, ax = plt.subplots(figsize=(11, 5.5))
    order = sorted(f2["race"].unique(), key=lambda x: -len(f2[f2["race"] == x]))
    sns.boxplot(
        data=f2,
        x="race",
        y="prm_first_fna_days_from_surg",
        order=order,
        ax=ax,
        showfliers=False,
    )
    ax.set_yscale("log")
    ax.set_ylabel("Days from first preop FNA to surgery (positive only; log scale)")
    ax.set_xlabel("Race")
    plt.xticks(rotation=35, ha="right")
    # annotate medians per race (Section 3.5 style)
    meds = f2.groupby("race")["prm_first_fna_days_from_surg"].median()
    for i, r in enumerate(order):
        if r in meds.index:
            ax.text(i, meds[r] * 1.15, f"{int(meds[r])}d", ha="center", fontsize=8)
    ax.set_title("Figure 2. Time from first preoperative FNA to surgery by race")
    plt.tight_layout()
    fig.savefig(FIG_DIR / "figure_2_fna_days_by_race.png", dpi=300, bbox_inches="tight")
    fig.savefig(FIG_DIR / "figure_2_fna_days_by_race.svg", bbox_inches="tight")
    plt.close()

    # Fig 3 forest
    fr = stats_out.get("forest_substernal_race_sex", [])
    if fr and "error" not in fr[0]:
        fig, ax = plt.subplots(figsize=(8, max(6, len(fr) * 0.35)))
        ys = np.arange(len(fr))
        ors = [row["or"] for row in fr]
        los = [row["lo"] for row in fr]
        his = [row["hi"] for row in fr]
        ax.errorbar(ors, ys, xerr=[np.array(ors) - np.array(los), np.array(his) - np.array(ors)], fmt="o", capsize=3)
        ax.axvline(1.0, color="gray", ls="--")
        ax.set_yticks(ys)
        ax.set_yticklabels([row["label"] for row in fr])
        ax.set_xlabel("Odds ratio vs White female (substernal extension, CT or MRI)")
        ax.set_title("Figure 3. Forest plot: substernal extension by race × sex")
        ax.set_xscale("log")
        plt.tight_layout()
        fig.savefig(FIG_DIR / "figure_3_substernal_forest_race_sex.png", dpi=300, bbox_inches="tight")
        fig.savefig(FIG_DIR / "figure_3_substernal_forest_race_sex.svg", bbox_inches="tight")
        plt.close()

    print("Wrote", HERE / "h2_v3_stats.json", "and figures in", FIG_DIR)


if __name__ == "__main__":
    main()
