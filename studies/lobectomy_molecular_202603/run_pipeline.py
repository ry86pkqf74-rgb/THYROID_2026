#!/usr/bin/env python3
"""
End-to-end cohort extraction + stats for lobectomy vs total thyroidectomy (2–4 cm, imaging N0).

Run from repo root THYROID_2026/:
  .venv/bin/python studies/lobectomy_molecular_202603/run_pipeline.py

Requires LOCAL_DB_PATH (or secrets.toml).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "outputs" / "lobectomy_molecular_202603"
SQL_PATH = Path(__file__).resolve().parent / "sql" / "01_cohort_base.sql"

sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402
from utils.statistical_analysis import ThyroidStatisticalAnalyzer  # noqa: E402


def _boolish(s: pd.Series) -> pd.Series:
    def one(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, bool):
            return bool(x)
        t = str(x).strip().lower()
        if t in ("true", "1", "t", "yes"):
            return True
        if t in ("false", "0", "f", "no"):
            return False
        return np.nan

    return s.map(one)


MALIGNANT_KEYWORDS = (
    "carcinoma",
    "metastatic",
    "lymphoma",
    "sarcoma",
)


def path_malignant(h: pd.Series) -> pd.Series:
    def m(x):
        if pd.isna(x) or str(x).strip() in ("", "None"):
            return np.nan
        s = str(x).lower()
        if "niftp" in s or "nifgp" in s:
            return False
        if s.strip() == "ptc" or s.startswith("ptc "):
            return True
        return any(k in s for k in MALIGNANT_KEYWORDS)

    return h.map(m)


def engineer(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    hr = _boolish(out["preop_high_risk_marker_raw"])
    rc = out["preop_result_class"].astype(str).str.lower()
    out["genetics_guided"] = (
        (rc.isin(["suspicious", "positive"])) | (hr == True)  # noqa: E712
    ) & out["preop_molecular_platform"].notna()
    out["genetics_guided"] = out["genetics_guided"].fillna(False)
    out["has_preop_molecular"] = out["preop_molecular_platform"].notna()
    out["bethesda_cat"] = pd.to_numeric(out["fna_bethesda_final"], errors="coerce")
    bc = out["bethesda_cat"]
    out["bethesda_grp"] = pd.cut(
        bc,
        bins=[0, 2, 3, 4, 7],
        labels=["I_II", "III", "IV", "V_VI"],
        include_lowest=True,
    ).astype(str)
    cond3 = bc == 3
    cond45 = bc.isin([4, 5, 6])
    out["bethesda_III_vs_IV_V"] = "other_NA"
    out.loc[cond3.fillna(False), "bethesda_III_vs_IV_V"] = "III"
    out.loc[cond45.fillna(False), "bethesda_III_vs_IV_V"] = "IV_V"
    out["platform_preop"] = out["preop_molecular_platform"].fillna("none")
    out["path_malignant_flag"] = path_malignant(out["histology_final"])
    out["path_high_ata"] = out["ata_risk_category"].isin(["high", "intermediate"])
    out["path_high_outcome"] = (out["path_malignant_flag"] == True) | (out["path_high_ata"] == True)  # noqa: E712
    out["genetics_high_risk"] = (
        out["has_preop_molecular"]
        & (
            rc.isin(["suspicious", "positive"])
            | (hr == True)  # noqa: E712
        )
    )
    mask_mol = out["has_preop_molecular"]
    kg = pd.Series(np.nan, index=out.index, dtype=object)
    kg.loc[mask_mol] = np.where(
        out.loc[mask_mol, "genetics_high_risk"],
        "high",
        "not_high",
    )
    out["kappa_pair_genetics"] = kg
    has_path = out["path_malignant_flag"].notna() | out["ata_risk_category"].notna()
    hp = has_path.fillna(False)
    kp = pd.Series(np.nan, index=out.index, dtype=object)
    kp.loc[hp] = np.where(out.loc[hp, "path_high_outcome"], "high", "not_high")
    out["kappa_pair_path"] = kp
    return out


def cohort_flow_counts(con) -> dict:
    """Approximate CONSORT-style counts (local DuckDB)."""
    q = """
    WITH first_op AS (
        SELECT research_id, procedure_normalized,
            COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native) AS sx_date_episode,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native) NULLS LAST
            ) AS rn
        FROM operative_episode_detail_v2
    ),
    first_ep AS (
        SELECT * FROM first_op
        WHERE rn = 1 AND procedure_normalized IN ('hemithyroidectomy', 'total_thyroidectomy')
    ),
    spine AS (
        SELECT p.research_id,
            COALESCE(fe.sx_date_episode, p.surg_first_date, p.first_surgery_date) AS sx,
            p.path_m_stage_raw, p.histology_final
        FROM patient_analysis_resolved_v1 p
        JOIN first_ep fe ON p.research_id = fe.research_id
        WHERE COALESCE(p.surg_first_date, p.first_surgery_date) IS NOT NULL
    ),
    pre_sz AS (
        SELECT i.research_id, MAX(i.max_dimension_cm) AS mx
        FROM imaging_nodule_master_v1 i
        JOIN spine s ON i.research_id = s.research_id
        WHERE i.exam_date IS NOT NULL AND s.sx IS NOT NULL AND i.exam_date <= s.sx
        GROUP BY i.research_id
    ),
    asm AS (
        SELECT s.research_id,
            COALESCE(pr.mx, p.imaging_nodule_size_cm) AS sz,
            s.path_m_stage_raw, s.histology_final, p.imaging_nodule_size_cm AS pat_img
        FROM spine s
        JOIN patient_analysis_resolved_v1 p ON s.research_id = p.research_id
        LEFT JOIN pre_sz pr ON s.research_id = pr.research_id
    )
    SELECT
        COUNT(*) AS n_spine,
        SUM(CASE WHEN sz BETWEEN 2 AND 4 THEN 1 ELSE 0 END) AS n_size_2_4,
        SUM(CASE WHEN UPPER(TRIM(COALESCE(path_m_stage_raw,''))) IN ('M1','1')
            OR LOWER(TRIM(COALESCE(path_m_stage_raw,''))) = 'm1' THEN 1 ELSE 0 END) AS n_m1
    FROM asm
    """
    row = con.execute(q).fetchone()
    return {"n_first_hemi_or_tt_with_date": row[0], "n_with_size_2_4_cm": row[1], "n_m1_in_that_spine": row[2]}


def fit_logit_formula(df: pd.DataFrame, formula: str):
    import statsmodels.formula.api as smf

    sub = df.dropna(subset=["total_thyroidectomy", "age_at_surgery", "exact_size_cm_primary"])
    if len(sub) < 40:
        return None, f"too few rows for formula: {len(sub)}"
    try:
        model = smf.logit(formula, data=sub).fit(disp=0, maxiter=200)
    except Exception as exc:
        return None, str(exc)
    summ = model.summary2()
    params = model.params
    ci = model.conf_int()
    rows = []
    for name in params.index:
        if name == "Intercept":
            continue
        or_ = float(np.exp(params[name]))
        lo, hi = float(np.exp(ci.loc[name, 0])), float(np.exp(ci.loc[name, 1]))
        rows.append(
            {
                "predictor": name,
                "OR": or_,
                "ci_lower": lo,
                "ci_upper": hi,
                "p_value": float(model.pvalues[name]),
            }
        )
    or_df = pd.DataFrame(rows)
    auc = None
    try:
        from sklearn.metrics import roc_auc_score

        auc = float(roc_auc_score(sub["total_thyroidectomy"], model.predict()))
    except Exception:
        pass
    return {"model": model, "or_table": or_df, "n": len(sub), "auc": auc, "formula": formula}, None


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "tables").mkdir(exist_ok=True)
    (OUT / "figures").mkdir(exist_ok=True)

    client = MotherDuckClient.for_env("prod")
    con = client.connect_rw()
    sql = SQL_PATH.read_text()
    df = con.execute(sql).fetchdf()
    df.to_csv(OUT / "cohort_analytic_v1.csv", index=False)

    flow = cohort_flow_counts(con)
    con.close()

    dfe = engineer(df)
    dfe.to_csv(OUT / "analytic_ready_v1.csv", index=False)

    n_tt = int(dfe["total_thyroidectomy"].sum())
    n_lo = int(len(dfe) - n_tt)
    mal = dfe["path_malignant_flag"] == True  # noqa: E712
    n_cancer = int(mal.sum())
    summary = {
        "n_analytic": len(dfe),
        "n_total_thyroidectomy": n_tt,
        "n_lobectomy_group": n_lo,
        "n_path_malignant_true": n_cancer,
        "n_preop_molecular": int(dfe["has_preop_molecular"].sum()),
        "n_genetics_guided_true": int(dfe["genetics_guided"].sum()),
        "n_completion_true": int(dfe["completion_after_initial_lobe"].sum()),
        "cohort_flow_hints": flow,
        "outputs": str(OUT),
    }
    (OUT / "cohort_summary.json").write_text(json.dumps(summary, indent=2))

    # Table 1
    con_local = duckdb.connect(":memory:")
    con_local.register("cohort", dfe)
    ana = ThyroidStatisticalAnalyzer(con_local)
    t1_cols_cont = ["age_at_surgery", "exact_size_cm_primary"]
    t1_cols_cat = [
        "sex",
        "bethesda_III_vs_IV_V",
        "has_preop_molecular",
        "genetics_guided",
        "platform_preop",
        "path_malignant_flag",
        "ata_risk_category",
    ]
    present_cat = [c for c in t1_cols_cat if c in dfe.columns]
    t1_df, t1_meta = ana.generate_table_one(
        data=dfe,
        groupby_col="total_thyroidectomy",
        continuous_vars=t1_cols_cont,
        categorical_vars=present_cat,
    )
    t1_df.to_csv(OUT / "tables" / "table1.csv")
    t1_df.to_markdown(OUT / "tables" / "table1.md")
    (OUT / "tables" / "table1_meta.json").write_text(json.dumps({k: str(v) for k, v in t1_meta.items() if k != "tableone_object"}, indent=2))

    # Hypothesis tests vs surgery type
    feats = ["age_at_surgery", "exact_size_cm_primary", "sex", "bethesda_III_vs_IV_V", "has_preop_molecular", "genetics_guided"]
    htests = ana.run_hypothesis_tests(dfe, "total_thyroidectomy", feats, correction="fdr_bh")
    htests.to_csv(OUT / "tables" / "univariable_tests.csv", index=False)

    # Multivariable logistic (formula API)
    formula_main = (
        "total_thyroidectomy ~ age_at_surgery + C(sex) + exact_size_cm_primary "
        "+ C(bethesda_III_vs_IV_V) + has_preop_molecular + genetics_guided"
    )
    res, err = fit_logit_formula(dfe, formula_main)
    if err:
        (OUT / "tables" / "logistic_multivariable_error.txt").write_text(err)
    else:
        res["or_table"].to_csv(OUT / "tables" / "logistic_multivariable_main.csv", index=False)
        try:
            res["or_table"].to_latex(OUT / "tables" / "logistic_multivariable_main.tex", index=False)
        except Exception:
            pass
        with open(OUT / "tables" / "logistic_multivariable_meta.json", "w") as f:
            json.dump({"n": res["n"], "auc": res["auc"], "formula": res["formula"]}, f, indent=2)

        # Forest plot
        fp = res["or_table"].rename(
            columns={"predictor": "label", "OR": "estimate", "ci_lower": "ci_lower", "ci_upper": "ci_upper"}
        )
        try:
            fig = ThyroidStatisticalAnalyzer.create_forest_plot(
                fp.assign(p_value=fp["p_value"]),
                title="Multivariable logistic: total thyroidectomy vs lobectomy",
            )
            fig.write_image(str(OUT / "figures" / "forest_multivariable.png"), scale=2)
        except Exception as exc:
            (OUT / "figures" / "forest_note.txt").write_text(f"Plotly/Kaleido export failed: {exc}\n")
            try:
                import matplotlib.pyplot as plt

                plot_df = fp.sort_values("estimate", ascending=True).reset_index(drop=True)
                y = range(len(plot_df))
                plt.figure(figsize=(8, max(3, len(plot_df) * 0.35)))
                plt.axvline(1.0, color="gray", linestyle="--", linewidth=1)
                plt.errorbar(
                    plot_df["estimate"],
                    y,
                    xerr=[
                        plot_df["estimate"] - plot_df["ci_lower"],
                        plot_df["ci_upper"] - plot_df["estimate"],
                    ],
                    fmt="o",
                    capsize=3,
                )
                plt.yticks(y, plot_df["label"])
                plt.xlabel("Odds ratio")
                plt.title("Multivariable logistic: total thyroidectomy vs lobectomy")
                plt.tight_layout()
                plt.savefig(OUT / "figures" / "forest_multivariable_mpl.png", dpi=200)
                plt.close()
            except Exception as exc2:
                with open(OUT / "figures" / "forest_note.txt", "a") as fh:
                    fh.write(f"Matplotlib fallback failed: {exc2}\n")

    # Platform interaction (full sample — often singular)
    formula_ix = (
        "total_thyroidectomy ~ age_at_surgery + C(sex) + exact_size_cm_primary "
        "+ has_preop_molecular * C(platform_preop)"
    )
    res_ix, err_ix = fit_logit_formula(dfe, formula_ix)
    if err_ix:
        (OUT / "tables" / "logistic_platform_interaction_error.txt").write_text(err_ix)
    else:
        res_ix["or_table"].to_csv(OUT / "tables" / "logistic_platform_interaction.csv", index=False)

    # Stratified: ThyroSeq-only / Afirma-only among those with preop molecular
    kappa_rows = []
    from sklearn.metrics import cohen_kappa_score

    for plat, sub in dfe[dfe["has_preop_molecular"]].groupby("preop_molecular_platform"):
        g = sub["kappa_pair_genetics"].dropna()
        p = sub.loc[g.index, "kappa_pair_path"].dropna()
        common = g.index.intersection(p.index)
        if len(common) < 5:
            kappa_rows.append({"platform": plat, "n": len(sub), "kappa": np.nan, "agreement_pct": np.nan})
            continue
        g2 = sub.loc[common, "kappa_pair_genetics"]
        p2 = sub.loc[common, "kappa_pair_path"]
        try:
            k = cohen_kappa_score(g2, p2)
            agree = (g2.values == p2.values).mean()
            kappa_rows.append({"platform": str(plat), "n_pairs": len(common), "kappa": k, "agreement_pct": agree})
        except Exception:
            kappa_rows.append({"platform": str(plat), "n_pairs": len(common), "kappa": np.nan, "agreement_pct": np.nan})

    all_k = dfe.dropna(subset=["kappa_pair_genetics", "kappa_pair_path"])
    if len(all_k) >= 5:
        k = cohen_kappa_score(all_k["kappa_pair_genetics"], all_k["kappa_pair_path"])
        agree = (all_k["kappa_pair_genetics"].values == all_k["kappa_pair_path"].values).mean()
        kappa_rows.append({"platform": "all_preop_molecular", "n_pairs": len(all_k), "kappa": k, "agreement_pct": agree})

    pd.DataFrame(kappa_rows).to_csv(OUT / "tables" / "kappa_by_platform.csv", index=False)

    # Sensitivity: pathologic size 2–4 cm
    dfp = dfe[
        dfe["exact_size_cm_path_sensitivity"].between(2.0, 4.0, inclusive="both")
    ].copy()
    dfp.to_csv(OUT / "analytic_ready_v1_pathsize_2_4.csv", index=False)
    res_sens, err_sens = fit_logit_formula(dfp, formula_main)
    err_extra = ""
    if res_sens and not err_sens:
        mret = getattr(res_sens["model"], "mle_retvals", {}) or {}
        if mret.get("converged") is False:
            err_extra = "Model failed to converge (likely quasi-separation in path-size subset)."
        elif not np.isfinite(res_sens["model"].llf):
            err_extra = "Non-finite log-likelihood in path-size subset."
        else:
            res_sens["or_table"].to_csv(
                OUT / "tables" / "logistic_multivariable_path_size_sensitivity.csv", index=False
            )
            with open(OUT / "tables" / "logistic_path_size_sensitivity_meta.json", "w") as f:
                json.dump({"n": res_sens["n"], "auc": res_sens["auc"], "formula": res_sens["formula"]}, f, indent=2)
    if err_sens or err_extra:
        msg = "\n".join(filter(None, [err_sens or "", err_extra]))
        (OUT / "tables" / "logistic_path_size_sensitivity_skipped.txt").write_text(msg)

    # Sankey (Plotly)
    try:
        import plotly.graph_objects as go

        # Aggregate flows
        dfe["surg_label"] = np.where(dfe["total_thyroidectomy"] == 1, "Total thyroidectomy", "Lobectomy")
        dfe["mol_simple"] = np.where(
            ~dfe["has_preop_molecular"],
            "No preop molecular",
            np.where(dfe["genetics_high_risk"], "Preop molecular high-risk", "Preop molecular not high-risk"),
        )
        dfe["comp"] = np.where(dfe["completion_after_initial_lobe"], "Completion later", "No completion code")
        flowcounts = dfe.groupby(["mol_simple", "surg_label", "comp"], dropna=False).size().reset_index(name="n")
        labels = sorted(
            set(flowcounts["mol_simple"]) | set(flowcounts["surg_label"]) | set(flowcounts["comp"])
        )
        lab_ix = {l: i for i, l in enumerate(labels)}
        src, tgt, val = [], [], []
        for _, r in flowcounts.iterrows():
            a, b, c, n = r["mol_simple"], r["surg_label"], r["comp"], r["n"]
            src.append(lab_ix[a])
            tgt.append(lab_ix[b])
            val.append(n)
        for _, r in flowcounts.iterrows():
            a, b, c, n = r["mol_simple"], r["surg_label"], r["comp"], r["n"]
            src.append(lab_ix[b])
            tgt.append(lab_ix[c])
            val.append(n)
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(label=labels, pad=15, thickness=20),
                    link=dict(source=src, target=tgt, value=val),
                )
            ]
        )
        fig.update_layout(title_text="Genetics (preop) → surgery → completion (coded)", font_size=12)
        fig.write_html(OUT / "figures" / "sankey_genetics_surgery_completion.html")
    except Exception as exc:
        (OUT / "figures" / "sankey_note.txt").write_text(str(exc))

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
