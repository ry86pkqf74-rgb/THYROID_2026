#!/usr/bin/env python3
"""
M044 ETE manuscript: MotherDuck → analytic parquet (+CPM merges), statsmodels GLMs,
Cox sensitivity, forest plot (matplotlib), QA size strata, openpyxl workbook refresh.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from lifelines import CoxPHFitter
from openpyxl import load_workbook
from scipy import stats

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "m044"
FIG_DIR = REPO_ROOT / "figures"
XLSX = REPO_ROOT / "M044_ETE_tables.xlsx"

MASTER_ANALYTIC_SQL = r"""
WITH cohort AS (
  SELECT
    c.*,
    CASE
      WHEN c.ete_grade_final IN ('false','absent')   THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'               THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group,
    CASE
      WHEN c.lvi_grade ILIKE 'extensiv%'             THEN 'extensive'
      WHEN c.lvi_grade IN ('present','preesent')    THEN 'present'
      WHEN c.lvi_grade = 'focal'                    THEN 'focal'
      WHEN c.lvi_grade IS NULL                       THEN 'missing'
      WHEN c.lvi_grade IN ('indeterminate','indetermiante','indeeterminate','indeterminent','suspicious','x','c/a','no','n/s')
                                                     THEN 'indeterminate'
      ELSE 'indeterminate'
    END AS lvi_clean,
    COALESCE(c.vascular_invasion_final, 'missing') AS vasc_clean
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
),
ln AS (
  SELECT research_id,
    MAX(ln_total_examined)             AS ln_examined,
    MAX(ln_total_positive)             AS ln_positive,
    MAX(ln_central_examined)           AS ln_central_examined,
    MAX(ln_central_positive)           AS ln_central_positive,
    MAX(ln_lateral_left_positive)      AS ln_lateral_left_positive,
    MAX(ln_lateral_right_positive)     AS ln_lateral_right_positive,
    MAX(ln_bilateral_lateral_positive) AS ln_bilateral_lateral_positive,
    MAX(ln_level_vi_positive)          AS ln_level_vi_positive,
    MAX(ln_level_vii_positive)         AS ln_level_vii_positive,
    MAX(ln_extranodal_extension)       AS ln_ene
  FROM manuscript_workspace.ln_master_rollup_v1
  GROUP BY research_id
),
reop AS (
  SELECT research_id,
    MAX(n_surgeries) AS n_surgeries,
    MAX(second_surgery_date) AS second_surgery_date,
    MAX(days_between_first_second_surgery) AS days_to_2nd,
    MAX(completion_reason) AS completion_reason,
    MAX(completion_reason_confidence) AS completion_reason_confidence,
    MAX(completion_histology_type) AS completion_histology_type,
    MAX(op_reoperative_any) AS op_reoperative_any
  FROM manuscript_workspace.cohort_m040_reoperative_v1
  GROUP BY research_id
),
rec AS (
  SELECT
    research_id,
    recurrence_path_proven,
    recurrence_imaging_then_path_confirmed,
    recurrence_status_final,
    days_to_path_proven,
    recurrence_imaging_suspicious_date
  FROM main.canonical_recurrence_resolved_v1
)
SELECT
  c.research_id,
  c.ete_group,
  c.ete_grade_final,
  c.age_at_surgery, c.sex, c.histology_final, c.tumor_size_cm,
  c.ajcc8_t_stage, c.ajcc8_n_stage, c.ajcc8_stage_group,
  c.surg_first_date,
  c.followup_years, c.overall_survival_years, c.death_occurred,
  c.lvi_clean, c.vasc_clean, c.lvi_grade,
  ln.ln_lateral_left_positive, ln.ln_lateral_right_positive, ln.ln_bilateral_lateral_positive,
  CASE WHEN ln.ln_central_positive > 0 THEN 1 ELSE 0 END AS central_pos_flag,
  CASE WHEN COALESCE(ln.ln_lateral_left_positive,0) > 0
         OR COALESCE(ln.ln_lateral_right_positive,0) > 0
         OR COALESCE(ln.ln_bilateral_lateral_positive,0) > 0 THEN 1 ELSE 0 END AS lateral_pos_flag,
  c.rai_received_flag,
  c.any_recurrence_flag,
  rec.recurrence_path_proven, rec.recurrence_status_final,
  rec.days_to_path_proven,
  reop.n_surgeries, reop.days_to_2nd
FROM cohort c
LEFT JOIN ln USING (research_id)
LEFT JOIN reop USING (research_id)
LEFT JOIN rec USING (research_id);
"""


CPM_EXTRA_SQL = """
SELECT
  research_id::VARCHAR AS research_id_cpm_key,
  race, bmi_combined, multifocal_flag_path, bilateral_disease_flag, aggressive_variant_flag,
  margin_involved_any, closest_margin_mm, syn_hashimoto, syn_graves,
  pmhx_nlp_diabetes, pmhx_nlp_hypertension, pmhx_nlp_hypothyroidism, pmhx_nlp_obesity,
  braf_positive_final, tert_positive_final, ras_positive_final, ret_positive_unified,
  surg_total_thyroidectomy, ages_score
FROM main.canonical_patient_master;
"""


def tri_cat(series: pd.Series | None) -> pd.Categorical | None:
    if series is None:
        return None

    def one(v: Any) -> str:
        if pd.isna(v):
            return "missing"
        if isinstance(v, (bool, np.bool_)):
            return "true" if bool(v) else "false"
        sv = str(v).strip().lower()
        if sv in {"", "nan", "none"}:
            return "missing"
        if sv in {"1", "true", "yes", "t", "x"}:
            return "true"
        return "false"

    z = series.map(one)
    return pd.Categorical(z.astype(str), categories=["missing", "false", "true"])


def normalize_rid_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def cox_extract_contrast(sdf: pd.DataFrame, substring: str) -> dict[str, Any]:
    m = sdf[sdf["covariate"].astype(str).str.contains(substring, regex=False)]
    if len(m) != 1:
        return {"_match_error": len(m)}
    r = m.iloc[0]
    return {
        "hr": float(r["exp(coef)"]),
        "hr_ci_low": float(r["exp(coef) lower 95%"]),
        "hr_ci_high": float(r["exp(coef) upper 95%"]),
        "p": float(r["p"]),
        "covariate_row": str(r["covariate"]),
    }


def histology_grouped(h: Any) -> str:
    if h is None or (isinstance(h, float) and np.isnan(h)):
        return "other"
    t = str(h).strip().lower()
    if t == "" or t == "nan":
        return "other"
    if "medullary" in t or t == "mtc" or "(mtc" in t:
        return "MTC-like"
    follicular_hit = ("follicular" in t) and ("papillary" not in t)
    if follicular_hit or t in {"ftc", "hurthle", "hürthle", "hcc", "oncocytic", "niftp"}:
        return "follicular-like"
    if "papillary" in t or t == "ptc" or t.startswith("ptc"):
        return "PTC"
    return "other"


def encode_rai_01(series: pd.Series) -> pd.Series:
    """Map RAI-received to 0/1; undocumented / non-positive → 0 (see analysis note)."""

    nu = pd.to_numeric(series, errors="coerce")
    s = series.astype(str).str.strip().str.lower()
    m_true = (nu == 1).fillna(False)
    txt_ok = (~s.isin({"nan", "none", "", "<nat>"})) & s.isin(
        ("1", "true", "t", "yes", "x")
    )
    ex_true = series.apply(lambda v: v is True)
    return (m_true | txt_ok.fillna(False) | ex_true).astype(int)


def build_primary_prep(df: pd.DataFrame) -> pd.DataFrame:
    d = df[
        df["ete_group"].isin(["No/negative ETE", "Microscopic ETE", "Gross ETE"])
    ].copy()
    d["age10"] = pd.to_numeric(d["age_at_surgery"], errors="coerce") / 10.0
    sx = d["sex"].astype(str).str.strip().str.lower()
    sx = sx.replace({"female": "female", "male": "male"})
    d["sex_lc"] = sx.mask(~sx.isin(["female", "male"]))
    d["histology_grouped"] = d["histology_final"].apply(histology_grouped)
    ns = d["ajcc8_n_stage"]
    d["ajcc8_n_fac"] = np.where(ns.isna(), "missing", ns.astype(str))
    d["tumor_size_cm"] = pd.to_numeric(d["tumor_size_cm"], errors="coerce")
    d["y_pp"] = d["recurrence_path_proven"].astype(bool).astype(int)
    d["y_img"] = (
        (d["recurrence_status_final"] == "imaging_only_unconfirmed").astype(int)
    )
    d["y_comp"] = d["recurrence_status_final"].isin(
        ["path_proven", "imaging_only_unconfirmed"]
    ).astype(int)
    d["y_any"] = d["any_recurrence_flag"].astype(bool).astype(int)

    d["ete_group"] = pd.Categorical(
        d["ete_group"], categories=["Microscopic ETE", "No/negative ETE", "Gross ETE"]
    )
    d["sex_lc"] = pd.Categorical(d["sex_lc"], categories=["female", "male"])
    d["ajcc8_n_fac"] = pd.Categorical(
        d["ajcc8_n_fac"], categories=["N0", "N1a", "N1b", "Nx", "missing"]
    )
    d["histology_grouped"] = pd.Categorical(
        d["histology_grouped"],
        categories=["PTC", "follicular-like", "MTC-like", "other"],
    )
    d["lvi_clean"] = pd.Categorical(
        d["lvi_clean"],
        categories=["missing", "present", "extensive", "focal", "indeterminate"],
    )
    d["vasc_clean"] = pd.Categorical(
        d["vasc_clean"],
        categories=["missing", "present_ungraded", "focal", "extensive", "indeterminate"],
    )
    d["rai_received_flag"] = encode_rai_01(d["rai_received_flag"])

    if "multifocal_flag_path" in d.columns:
        d["multifocal_flag_path_fac"] = tri_cat(d["multifocal_flag_path"])
        d["bilateral_disease_fac"] = tri_cat(d["bilateral_disease_flag"])
        d["aggressive_variant_fac"] = tri_cat(d["aggressive_variant_flag"])
        d["margin_involved_fac"] = tri_cat(d["margin_involved_any"])
        d["braf_positive_fac"] = tri_cat(d["braf_positive_final"])
        d["surg_tt_fac"] = tri_cat(d["surg_total_thyroidectomy"])
    return d


PRIMARY_FORMULA = (
    "y_pp ~ C(ete_group, Treatment(reference='Microscopic ETE'))"
    " + age10 + C(sex_lc, Treatment(reference='female')) + tumor_size_cm"
    " + C(ajcc8_n_fac, Treatment(reference='N0'))"
    " + C(histology_grouped, Treatment(reference='PTC')) + rai_received_flag"
    " + C(lvi_clean, Treatment(reference='missing'))"
    " + C(vasc_clean, Treatment(reference='missing'))"
)

PRIMARY_FORMULA_CPH = PRIMARY_FORMULA.replace("y_pp ~ ", "").strip()


def _glm_metrics(m: Any) -> dict[str, Any]:
    fr = m.model.data.frame  # noqa: SLF001
    endog = m.model.endog
    lhs = str(m.model.formula).strip().split("~", 1)[0].strip()
    n_events = int(np.sum(endog))
    null = smf.glm(f"{lhs} ~ 1", data=fr, family=sm.families.Binomial()).fit()

    lr_stat = 2.0 * (m.llf - null.llf)
    lr_p = float(1.0 - stats.chi2.cdf(lr_stat, df=int(m.df_model)))
    pr2 = float(1.0 - m.llf / null.llf) if null.llf != 0 else float("nan")
    return {
        "n_obs": int(m.nobs),
        "n_events": n_events,
        "llf": float(m.llf),
        "aic": float(m.aic),
        "pseudo_r2_mcfadden": pr2,
        "lr_vs_null_chi2": float(lr_stat),
        "lr_vs_null_pvalue": lr_p,
    }


def coef_table_glm(m: Any) -> pd.DataFrame:
    ci = np.clip(np.asarray(m.conf_int()), -25.0, 25.0)
    coef = np.asarray(m.params)
    se = np.asarray(m.bse)
    pv = np.asarray(m.pvalues)
    return pd.DataFrame(
        {
            "term": m.params.index.astype(str),
            "coef_logit": coef,
            "se": se,
            "or": np.exp(coef),
            "or_ci_low": np.exp(ci[:, 0]),
            "or_ci_high": np.exp(ci[:, 1]),
            "pvalue": pv,
        }
    )


def model_frame_for_primary(df_merged: pd.DataFrame) -> pd.DataFrame:
    d = build_primary_prep(df_merged)
    d = d.dropna(subset=["sex_lc"])
    assert isinstance(d.index, pd.Index)
    return d


def crude_pairwise(d_model: pd.DataFrame) -> dict[str, Any]:
    sub = d_model.dropna(subset=["y_pp"]).copy()

    def or_for(group: str) -> tuple[float, float, float]:
        mic = sub["ete_group"].astype(str) == "Microscopic ETE"
        alt = sub["ete_group"].astype(str) == group
        mm = sub.loc[mic | alt, ["y_pp"]].copy()
        mm["x"] = (sub.loc[mic | alt, "ete_group"].astype(str) == group).astype(int)
        glm = smf.glm("y_pp ~ x", data=mm, family=sm.families.Binomial()).fit()
        coef = glm.params["x"]
        row = glm.conf_int().loc["x"]
        return float(np.exp(coef)), float(np.exp(row[0])), float(np.exp(row[1]))

    g_or, g_lo, g_hi = or_for("Gross ETE")
    n_or, n_lo, n_hi = or_for("No/negative ETE")
    return {
        "crude_pp_gross_vs_microscopic_or": g_or,
        "crude_pp_gross_vs_microscopic_ci_low": g_lo,
        "crude_pp_gross_vs_microscopic_ci_high": g_hi,
        "crude_pp_noneg_vs_microscopic_or": n_or,
        "crude_pp_noneg_vs_microscopic_ci_low": n_lo,
        "crude_pp_noneg_vs_microscopic_ci_high": n_hi,
    }


def export_parquet(force: bool) -> Path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from _md_connect import connect_locked  # noqa: E402

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = DATA_DIR / "analytic_file_v1.parquet"
    if out.exists() and not force:
        print(f"[export] reuse {out}")
        return out

    print("[export] MotherDuck master analytic + CPM …")
    con = connect_locked()
    df_main = con.execute(MASTER_ANALYTIC_SQL).df()
    if len(df_main) != 4128:
        raise SystemExit(f"expected 4128 rows, got {len(df_main)}")
    df_cpm = con.execute(CPM_EXTRA_SQL).df()
    df_main["research_id_norm"] = normalize_rid_series(df_main["research_id"])
    df_cpm["research_id_norm"] = pd.to_numeric(
        df_cpm["research_id_cpm_key"], errors="coerce"
    ).astype("Int64")
    df_cpm = df_cpm.drop(columns=["research_id_cpm_key"]).drop_duplicates(
        subset=["research_id_norm"], keep="first"
    )
    merged = df_main.merge(df_cpm, on="research_id_norm", how="left")
    merged["multifocal_flag_path_fac"] = tri_cat(merged["multifocal_flag_path"])
    merged["bilateral_disease_fac"] = tri_cat(merged["bilateral_disease_flag"])
    merged["aggressive_variant_fac"] = tri_cat(merged["aggressive_variant_flag"])
    merged["margin_involved_fac"] = tri_cat(merged["margin_involved_any"])
    merged["braf_positive_fac"] = tri_cat(merged["braf_positive_final"])
    merged["surg_tt_fac"] = tri_cat(merged["surg_total_thyroidectomy"])
    merged.to_parquet(out, index=False)
    print(f"[export] wrote {out}")
    return out


def size_panel_clean(frame: pd.DataFrame) -> pd.DataFrame:
    # fixed implementation without buggy lambda
    def ete_three_row(gf):
        if pd.isna(gf):
            return None
        s = str(gf).strip().lower()
        if s in {"false", "absent"}:
            return "No/negative ETE"
        if s == "microscopic":
            return "Microscopic ETE"
        if s == "gross":
            return "Gross ETE"
        return None

    x = frame.copy()
    x["ete3"] = x["ete_grade_final"].map(ete_three_row)
    x = x[x["ete3"].notna()]
    ts = pd.to_numeric(x["tumor_size_cm"], errors="coerce")

    def bsz(v):
        if pd.isna(v):
            return None
        if v <= 1:
            return "<=1"
        if v <= 2:
            return "1.1-2"
        if v <= 4:
            return "2.1-4"
        return ">4"

    x["bin"] = ts.map(bsz)
    x = x[x["bin"].notna()]
    rows = []
    for (eg, bn), grp in x.groupby(["ete3", "bin"]):
        n = len(grp)
        ev = int(grp["recurrence_path_proven"].astype(bool).sum())
        rows.append(
            {"ete_group": eg, "size_bin": bn, "n": n, "events": ev, "rate_pct": round(100 * ev / n, 2)}
        )
    return pd.DataFrame(rows)


def expected_size_rates() -> dict[tuple[str, str], float]:
    return {
        ("Microscopic ETE", "<=1"): 1.1,
        ("Microscopic ETE", "1.1-2"): 2.7,
        ("Microscopic ETE", "2.1-4"): 2.3,
        ("Microscopic ETE", ">4"): 5.6,
        ("Gross ETE", "<=1"): 2.6,
        ("Gross ETE", "1.1-2"): 4.1,
        ("Gross ETE", "2.1-4"): 7.0,
        ("Gross ETE", ">4"): 8.6,
        ("No/negative ETE", "<=1"): 3.8,
        ("No/negative ETE", "1.1-2"): 11.4,
        ("No/negative ETE", "2.1-4"): 7.0,
        ("No/negative ETE", ">4"): 3.8,
    }


def run_all_models(df_merged: pd.DataFrame) -> dict[str, Any]:
    dm = model_frame_for_primary(df_merged)
    out: dict[str, Any] = {}
    out["primary_n"] = len(dm)
    out["primary_events_pp"] = int(dm["y_pp"].sum())
    out["drop_sex_missing_n"] = int(
        len(build_primary_prep(df_merged)) - len(dm)
    )
    out["crude"] = crude_pairwise(dm)

    primary = smf.glm(PRIMARY_FORMULA, data=dm, family=sm.families.Binomial()).fit()
    out["primary"] = {
        "metrics": _glm_metrics(primary),
        "coef": coef_table_glm(primary),
        "formula": PRIMARY_FORMULA,
    }

    img = PRIMARY_FORMULA.replace("y_pp", "y_img")
    m_img = smf.glm(img, data=dm, family=sm.families.Binomial()).fit()
    out["secondary_imaging_only"] = {
        "metrics": _glm_metrics(m_img),
        "coef": coef_table_glm(m_img),
    }

    comp = PRIMARY_FORMULA.replace("y_pp", "y_comp")
    m_comp = smf.glm(comp, data=dm, family=sm.families.Binomial()).fit()
    out["secondary_composite"] = {
        "metrics": _glm_metrics(m_comp),
        "coef": coef_table_glm(m_comp),
    }

    d_s1 = dm.loc[pd.to_numeric(dm["followup_years"], errors="coerce") > 0].copy()
    m_s1 = smf.glm(PRIMARY_FORMULA, data=d_s1, family=sm.families.Binomial()).fit()
    out["S1_exclude_zero_followup"] = {
        "n": len(d_s1),
        "metrics": _glm_metrics(m_s1),
        "coef": coef_table_glm(m_s1),
    }

    sfd = pd.to_datetime(dm["surg_first_date"], errors="coerce")
    win = (
        (sfd >= pd.Timestamp("1999-01-01"))
        & (sfd <= pd.Timestamp("2024-12-31"))
    ).fillna(False)
    d_s2 = dm.loc[win].copy()
    m_s2 = smf.glm(PRIMARY_FORMULA, data=d_s2, family=sm.families.Binomial()).fit()
    out["S2_surgery_date_1999_2024"] = {
        "n": len(d_s2),
        "metrics": _glm_metrics(m_s2),
        "coef": coef_table_glm(m_s2),
    }

    f_s3 = (
        "y_pp ~ C(ete_group, Treatment(reference='Microscopic ETE'))"
        " + age10 + C(sex_lc, Treatment(reference='female')) + tumor_size_cm"
        " + central_pos_flag + lateral_pos_flag"
        " + C(histology_grouped, Treatment(reference='PTC')) + rai_received_flag"
        " + C(lvi_clean, Treatment(reference='missing'))"
        " + C(vasc_clean, Treatment(reference='missing'))"
    )
    m_s3 = smf.glm(f_s3, data=dm, family=sm.families.Binomial()).fit()
    out["S3_ln_flags_substitute_n_stage"] = {
        "metrics": _glm_metrics(m_s3),
        "coef": coef_table_glm(m_s3),
    }

    dp = dm.copy()
    lc = dm["lvi_clean"].astype(str)
    vc = dm["vasc_clean"].astype(str)
    dp["lvi_pooled"] = (
        lc.isin(["present", "extensive", "focal"])
        | (vc.ne("missing") & vc.ne("nan") & vc.ne("indeterminate"))
    ).astype(int)
    f_s4 = PRIMARY_FORMULA.replace(
        "+ C(lvi_clean, Treatment(reference='missing'))"
        " + C(vasc_clean, Treatment(reference='missing'))",
        "+ lvi_pooled",
    )
    m_s4 = smf.glm(f_s4, data=dp, family=sm.families.Binomial()).fit()
    ct_s4 = coef_table_glm(m_s4)
    row_pv = ct_s4.loc[ct_s4["term"] == "lvi_pooled", "coef_logit"]
    pooled_coef = float(row_pv.iloc[0]) if len(row_pv) else float("nan")
    pooled_or = float(np.exp(pooled_coef)) if np.isfinite(pooled_coef) else np.nan
    pooled_p = float(ct_s4.loc[ct_s4["term"] == "lvi_pooled", "pvalue"].iloc[0]) if len(
        ct_s4.loc[ct_s4["term"] == "lvi_pooled"]
    ) else float("nan")

    protective = pooled_coef < -1e-6 and pooled_p < 0.05
    risk_increasing_sig = pooled_coef > 1e-6 and pooled_p < 0.05
    out["S4_pooled_lvi_collapsed_missing_as_absent"] = {
        "formula": f_s4,
        "metrics": _glm_metrics(m_s4),
        "coef": ct_s4,
        "protective_association_at_alpha005": protective,
        "pooled_positive_risk_at_alpha005": risk_increasing_sig,
        "pooled_coef_logit": pooled_coef,
        "pooled_or": pooled_or,
        "lvi_pooled_pvalue": pooled_p,
    }

    dor = dm.copy()
    vmap = {
        "missing": 0,
        "indeterminate": 1,
        "focal": 2,
        "present_ungraded": 3,
        "extensive": 4,
    }
    dor["vasc_ord"] = dor["vasc_clean"].astype(str).map(vmap).astype(float)
    f_s5 = (
        "y_pp ~ C(ete_group, Treatment(reference='Microscopic ETE'))"
        " + age10 + C(sex_lc, Treatment(reference='female')) + tumor_size_cm"
        " + C(ajcc8_n_fac, Treatment(reference='N0'))"
        " + C(histology_grouped, Treatment(reference='PTC')) + rai_received_flag"
        " + C(lvi_clean, Treatment(reference='missing')) + vasc_ord"
    )
    m_s5 = smf.glm(f_s5, data=dor, family=sm.families.Binomial()).fit()
    out["S5_vascular_invasion_linear_ordinal_scalar"] = {
        "formula": f_s5,
        "metrics": _glm_metrics(m_s5),
        "coef": coef_table_glm(m_s5),
        "ordering_note": (
            "vasc_ord: missing=0 indeterminate=1 focal=2 "
            "present_ungraded=3 extensive=4; single linear coefficient"
        ),
    }

    dm6 = dm.loc[dm["ete_grade_final"].astype(str) != "true"].copy()
    m_s6 = smf.glm(PRIMARY_FORMULA, data=dm6, family=sm.families.Binomial()).fit()
    out["S6_drop_ete_grade_true"] = {
        "n": len(dm6),
        "n_removed": len(dm) - len(dm6),
        "metrics": _glm_metrics(m_s6),
        "coef": coef_table_glm(m_s6),
    }

    f_s7 = (
        PRIMARY_FORMULA.rstrip()
        + " + C(multifocal_flag_path_fac, Treatment(reference='missing'))"
        " + C(bilateral_disease_fac, Treatment(reference='missing'))"
        " + C(margin_involved_fac, Treatment(reference='missing'))"
        " + C(aggressive_variant_fac, Treatment(reference='missing'))"
        " + C(braf_positive_fac, Treatment(reference='missing'))"
        " + C(surg_tt_fac, Treatment(reference='missing'))"
    )
    m_s7 = smf.glm(f_s7, data=dm, family=sm.families.Binomial()).fit()
    pr_g = coef_table_glm(primary)
    au_g = coef_table_glm(m_s7)
    pr_gross = pr_g[pr_g["term"].str.contains("Gross ETE", regex=False)]
    au_gross = au_g[au_g["term"].str.contains("Gross ETE", regex=False)]
    g1 = float(pr_gross["coef_logit"].iloc[0]) if len(pr_gross) else float("nan")
    g2 = float(au_gross["coef_logit"].iloc[0]) if len(au_gross) else float("nan")
    out["S7_cpm_extended_covariates"] = {
        "formula": f_s7,
        "metrics": _glm_metrics(m_s7),
        "coef": au_g,
        "gross_vs_micro_coef_primary": g1,
        "gross_vs_micro_coef_augmented": g2,
        "change_note": (
            "coefficient strengthened vs primary" if g2 > g1 + 1e-6
            else "coefficient attenuated vs primary" if g2 < g1 - 1e-6
            else "essentially unchanged"
        ),
    }

    m_s8 = smf.glm(
        PRIMARY_FORMULA.replace("y_pp", "y_any"), data=dm, family=sm.families.Binomial()
    ).fit()
    out["S8_legacy_any_recurrence_flag"] = {
        "metrics": _glm_metrics(m_s8),
        "coef": coef_table_glm(m_s8),
    }

    # Cox PH
    dc = dm.copy()
    dc = dc.loc[
        pd.notna(dc["surg_first_date"])
        & (pd.to_numeric(dc["followup_years"], errors="coerce") > 0)
    ].copy()
    fu = pd.to_numeric(dc["followup_years"], errors="coerce")
    dtp_arr = pd.to_numeric(dc["days_to_path_proven"], errors="coerce").to_numpy(dtype=float)
    ev_s = dc["y_pp"].astype(int)
    fu_arr = fu.to_numpy(dtype=float)
    cens = fu_arr * 365.25
    ev_arr = ev_s.to_numpy(dtype=int)
    event_time = np.where(np.isfinite(dtp_arr), dtp_arr, cens)
    time_days_arr = np.where(ev_arr == 1, event_time, cens)
    dc["time_days"] = time_days_arr.astype(float)
    dc = dc.loc[np.isfinite(dc["time_days"]) & (dc["time_days"] > 0)].copy()
    dc = dc.dropna(subset=["age10", "tumor_size_cm"]).reset_index(drop=True)

    cat_for_cox = (
        "ete_group",
        "sex_lc",
        "ajcc8_n_fac",
        "histology_grouped",
        "lvi_clean",
        "vasc_clean",
    )
    for c in cat_for_cox:
        if c in dc.columns:
            dc[c] = dc[c].astype(str)

    cph = CoxPHFitter(penalizer=0.0001)
    cph.fit(
        dc,
        duration_col="time_days",
        event_col="y_pp",
        formula=PRIMARY_FORMULA_CPH,
    )
    summ_df = cph.summary.reset_index()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    summ_df.to_csv(DATA_DIR / "m044_cox_primary_summary.csv", index=False)

    gross_hr = cox_extract_contrast(summ_df, "Gross ETE")
    noneg_hr = cox_extract_contrast(summ_df, "No/negative ETE")

    out["Cox_surgery_date_known_positive_fu"] = {
        "n": len(dc),
        "events": int(dc["y_pp"].sum()),
        "coef_table_path": str(DATA_DIR / "m044_cox_primary_summary.csv"),
        "gross_vs_microscopic_hr": gross_hr,
        "noneg_vs_microscopic_hr": noneg_hr,
    }

    # No/negative subgroup
    sub = df_merged.loc[df_merged["ete_group"] == "No/negative ETE"].copy()
    sub["tumor_size_cm"] = pd.to_numeric(sub["tumor_size_cm"], errors="coerce")
    ns = sub["ajcc8_n_stage"]
    sub["ajcc8_n_fac"] = np.where(ns.isna(), "missing", ns.astype(str))
    sub["ajcc8_n_fac"] = pd.Categorical(
        sub["ajcc8_n_fac"], categories=["N0", "N1a", "N1b", "Nx", "missing"]
    )
    sub["y_comp"] = sub["recurrence_status_final"].isin(
        ["path_proven", "imaging_only_unconfirmed"]
    ).astype(int)
    sub["rai_received_flag"] = encode_rai_01(sub["rai_received_flag"])
    sub["n_surgeries"] = pd.to_numeric(sub["n_surgeries"], errors="coerce").fillna(1.0)
    sub["ge2"] = (sub["n_surgeries"] >= 2).astype(int)
    sub["days_per_100"] = pd.to_numeric(sub["days_to_2nd"], errors="coerce").fillna(0.0) / 100.0
    sg_form = (
        "y_comp ~ tumor_size_cm + C(ajcc8_n_fac, Treatment(reference='N0'))"
        " + central_pos_flag + lateral_pos_flag + rai_received_flag"
        " + ge2 + days_per_100"
    )
    m_sg = smf.glm(sg_form, data=sub, family=sm.families.Binomial()).fit()
    out["subgroup_no_neg_ete_composite"] = {
        "n": len(sub),
        "events": int(sub["y_comp"].sum()),
        "formula": sg_form,
        "metrics": _glm_metrics(m_sg),
        "coef": coef_table_glm(m_sg),
    }

    # Size QA
    qa_tbl = size_panel_clean(df_merged)
    qa_flags = []
    exp = expected_size_rates()
    for _, r in qa_tbl.iterrows():
        key = (r["ete_group"], str(r["size_bin"]))
        ex = exp.get(key)
        if ex is None:
            continue
        if abs(float(r["rate_pct"]) - ex) > 0.5:
            qa_flags.append(
                f"{key}: got {r['rate_pct']}% expected {ex}% (n={r['n']})"
            )
    out["size_strata_qa"] = {"table": qa_tbl, "flags": qa_flags}

    return out


def forest_plot_primary(primary_coef: pd.DataFrame, out_png: Path, out_csv: Path) -> None:
    tab = primary_coef[~primary_coef["term"].str.contains("Intercept", na=False)].copy()
    tab = tab.sort_values("or").reset_index(drop=True)
    ete_hit = tab["term"].astype(str).str.contains("ete_group", regex=False)
    colors = np.where(ete_hit, "#c0392b", "#34495e")
    yy = np.arange(len(tab))
    fig, ax = plt.subplots(figsize=(10, max(6, len(tab) * 0.22)))
    ax.axvline(1.0, color="gray", lw=1)
    for j in range(len(tab)):
        row = tab.iloc[j]
        ax.plot([row["or_ci_low"], row["or_ci_high"]], [j, j], color="#bdc3c7", lw=1.5)
        ax.plot(row["or"], j, "o", color=colors[j], markersize=6)
    ax.set_yticks(yy)
    ax.set_yticklabels(tab["term"], fontsize=8)
    ax.set_xlabel("Adjusted OR (95% CI)")
    ax.set_title("M044 primary model — path-proven recurrence")
    fig.tight_layout()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=200)
    plt.close(fig)
    tab.to_csv(out_csv, index=False)


def _find_table3_row(ws: Any, prefix: str) -> int | None:
    for r in range(1, ws.max_row + 2):
        v = ws.cell(row=r, column=1).value
        if v and str(v).startswith(prefix):
            return r
    return None


def _set_or_row(ws: Any, row: int, coef_row: pd.Series) -> None:
    ws.cell(row=row, column=2, value=float(coef_row["or"]))
    ws.cell(row=row, column=3, value=float(coef_row["or_ci_low"]))
    ws.cell(row=row, column=4, value=float(coef_row["or_ci_high"]))
    ws.cell(row=row, column=5, value=float(coef_row["pvalue"]))


def _coef_one(coef: pd.DataFrame, *needles: str) -> pd.Series | None:
    mask = pd.Series(True, index=coef.index)
    for n in needles:
        mask &= coef["term"].astype(str).str.contains(n, regex=False, na=False)
    hits = coef.loc[mask]
    if len(hits) == 1:
        return hits.iloc[0]
    return None


def fill_table3_excel(ws: Any, coef: pd.DataFrame) -> None:
    specs: list[tuple[str, tuple[str, ...]]] = [
        ("No/negative ETE", ("ete_group", "No/negative")),
        ("Gross ETE", ("ete_group", "Gross")),
        ("Age, per 10 years", ("age10",)),
        ("Sex (male", ("sex_lc", "[T.male")),
        ("Tumor size", ("tumor_size_cm",)),
        ("AJCC N1a", ("ajcc8_n_fac", "N1a")),
        ("AJCC N1b", ("ajcc8_n_fac", "N1b")),
        ("AJCC Nx", ("ajcc8_n_fac", "Nx")),
        ("AJCC N missing", ("ajcc8_n_fac", "missing")),
        ("Histology — Follicular-like", ("histology_grouped", "follicular")),
        ("Histology — MTC-like", ("histology_grouped", "MTC")),
        ("RAI received", ("rai_received_flag",)),
        ("Lymphatic — present", ("lvi_clean", "present")),
        ("Lymphatic — extensive", ("lvi_clean", "extensive")),
        ("Lymphatic — focal", ("lvi_clean", "focal")),
        ("Lymphatic — indeterminate", ("lvi_clean", "indeterminate")),
        ("Vascular — present_ungraded", ("vasc_clean", "present_ungraded")),
        ("Vascular — focal", ("vasc_clean", "focal")),
        ("Vascular — extensive", ("vasc_clean", "extensive")),
        ("Vascular — indeterminate", ("vasc_clean", "indeterminate")),
    ]
    for prefix, reqs in specs:
        rr = _find_table3_row(ws, prefix)
        if rr is None:
            continue
        prow = _coef_one(coef, *reqs)
        if prow is None:
            ws.cell(row=rr, column=6, value=f"TERM_NOT_FOUND:{prefix}:{reqs}")
            continue
        _set_or_row(ws, rr, prow)


def _append_coef_block(
    ws: Any, start_row: int, title: str, metrics: dict[str, Any], ctab: pd.DataFrame
) -> int:
    from openpyxl.styles import Font

    r = start_row
    ws.cell(row=r, column=1, value=title).font = Font(bold=True)
    r += 1
    ws.cell(row=r, column=1, value=json.dumps(metrics, indent=2, default=str))
    r += 1
    for _, row in ctab.iterrows():
        ws.cell(row=r, column=1, value=row["term"])
        ws.cell(row=r, column=2, value=float(row["or"]))
        ws.cell(row=r, column=3, value=float(row["or_ci_low"]))
        ws.cell(row=r, column=4, value=float(row["or_ci_high"]))
        ws.cell(row=r, column=5, value=float(row["pvalue"]))
        r += 1
    return r + 1


def update_workbook(bundle: dict[str, Any]) -> None:
    from openpyxl.styles import Font

    coef = bundle["primary"]["coef"]
    wb = load_workbook(XLSX)

    ws3 = wb["Table 3 — Multivariable"]
    ws3.cell(
        row=2,
        column=1,
        value=(
            "Reference = Microscopic ETE | Binomial GLM | excludes unknown sex | "
            "scripts/m044_ete_fit_models.py"
        ),
    )
    fill_table3_excel(ws3, coef)

    ws_m = wb["Model outputs"]
    for rr in ws_m.iter_rows():
        for c in rr:
            c.value = None

    r = 1
    seq = [
        ("Primary — path proven", "primary"),
        ("Secondary — imaging_only_unconfirmed", "secondary_imaging_only"),
        ("Secondary — composite", "secondary_composite"),
        ("S1 exclude FU==0", "S1_exclude_zero_followup"),
        ("S2 surgery date 1999–2024", "S2_surgery_date_1999_2024"),
        ("S3 LN flags replace N-stage", "S3_ln_flags_substitute_n_stage"),
        ("S4 pooled LVI (artifact)", "S4_pooled_lvi_collapsed_missing_as_absent"),
        ("S5 vascular ordinal scalar", "S5_vascular_invasion_linear_ordinal_scalar"),
        ("S6 drop ete true", "S6_drop_ete_grade_true"),
        ("S7 CPM-augmented", "S7_cpm_extended_covariates"),
        ("S8 legacy any recurrence flag", "S8_legacy_any_recurrence_flag"),
    ]
    for title, key in seq:
        blk = bundle[key]
        met = dict(blk.get("metrics", {}))
        for k_extra in ("n", "formula", "n_removed", "change_note", "ordering_note"):
            if k_extra in blk:
                met[k_extra] = blk[k_extra]
        r = _append_coef_block(ws_m, r, title, met, blk["coef"])

    sgk = bundle["subgroup_no_neg_ete_composite"]
    sg_met = dict(sgk.get("metrics", {}))
    sg_met["events"] = sgk["events"]
    sg_met["formula"] = sgk["formula"]
    r = _append_coef_block(ws_m, r, "Subgroup no/negative composite", sg_met, sgk["coef"])
    cx = bundle["Cox_surgery_date_known_positive_fu"]
    ws_m.cell(row=r, column=1, value="Cox PH (surgery date known, FU>0)").font = Font(bold=True)
    r += 1
    slim = {k: v for k, v in cx.items() if k != "summary"}
    ws_m.cell(row=r, column=1, value=json.dumps(slim, indent=2, default=str))
    r += 1
    cdf = pd.read_csv(DATA_DIR / "m044_cox_primary_summary.csv")
    cov_col = "covariate" if "covariate" in cdf.columns else cdf.columns[0]
    for _, row in cdf.iterrows():
        ws_m.cell(row=r, column=1, value=row[cov_col])
        ws_m.cell(row=r, column=2, value=float(row["exp(coef)"]))
        ws_m.cell(row=r, column=3, value=float(row["exp(coef) lower 95%"]))
        ws_m.cell(row=r, column=4, value=float(row["exp(coef) upper 95%"]))
        ws_m.cell(row=r, column=5, value=float(row["p"]))
        r += 1

    if "Figures" not in wb.sheetnames:
        wb.create_sheet("Figures")
    wfig = wb["Figures"]
    wfig.cell(row=1, column=1, value="figures/m044_forest_primary.png")
    wfig.cell(row=2, column=1, value="figures/m044_forest_primary_data.csv")

    qa = wb["QA"]
    nxt = qa.max_row + 2
    proto = bundle["S4_pooled_lvi_collapsed_missing_as_absent"]
    qa.cell(row=nxt, column=1, value="S4 pooled-LVI artifact check")
    qa.cell(
        row=nxt,
        column=2,
        value=(
            f"OR pooled={proto.get('pooled_or')} "
            f"p={proto.get('lvi_pooled_pvalue')}"
        ),
    )
    qa.cell(
        row=nxt,
        column=3,
        value=(
            f"protective_sig={proto.get('protective_association_at_alpha005')}; "
            f"pooled_OR>1_sig={proto.get('pooled_positive_risk_at_alpha005')}"
        ),
    )
    qs = bundle["size_strata_qa"]["flags"]
    nxt += 1
    if qs:
        for i, ftxt in enumerate(qs):
            qa.cell(row=nxt + i, column=1, value=f"SIZE_STRAT_MISMATCH: {ftxt}")
    else:
        qa.cell(row=nxt, column=1, value="SIZE_STRAT QA: within ±0.5 ppt")

    wb.save(XLSX)
    print(f"[xlsx] saved {XLSX}")


def patch_manuscript_md(bundle: dict[str, Any]) -> None:
    import re as _re

    path_md = REPO_ROOT / "M044_ETE_manuscript_draft.md"
    txt = path_md.read_text(encoding="utf-8")
    crude = bundle["crude"]
    coef = bundle["primary"]["coef"]
    g_adj = _coef_one(coef, "ete_group", "Gross")
    nn_adj = _coef_one(coef, "ete_group", "No/negative")

    cg = crude["crude_pp_gross_vs_microscopic_or"]
    cgl = crude["crude_pp_gross_vs_microscopic_ci_low"]
    cgh = crude["crude_pp_gross_vs_microscopic_ci_high"]
    cn = crude["crude_pp_noneg_vs_microscopic_or"]
    cnl = crude["crude_pp_noneg_vs_microscopic_ci_low"]
    cnh = crude["crude_pp_noneg_vs_microscopic_ci_high"]

    def _adj_txt(row: pd.Series | None, label: str) -> str:
        if row is None:
            return f"{label} adjusted OR=TBD"
        return (
            f"{label} adjusted OR {float(row['or']):.2f} "
            f"(95% CI {float(row['or_ci_low']):.2f}–{float(row['or_ci_high']):.2f}; "
            f"p={float(row['pvalue']):.4g})"
        )

    adj_gross = _adj_txt(g_adj, "Gross vs microscopic ETE:")
    adj_noneg = _adj_txt(nn_adj, "No/negative vs microscopic ETE:")
    cx = bundle["Cox_surgery_date_known_positive_fu"]
    gh = cx.get("gross_vs_microscopic_hr")

    gross_hr_note = ""
    if isinstance(gh, dict) and "hr" in gh:
        gross_hr_note = (
            f" A Cox proportional hazards sensitivity analysis (restricted to patients with documented "
            f"surgery date and positive follow-up; n={cx['n']}) estimated HR="
            f"{gh['hr']:.2f} (95% CI {gh['hr_ci_low']:.2f}–{gh['hr_ci_high']:.2f}; p={gh['p']:.4g}) "
            f"for gross vs microscopic ETE."
        )

    proto = bundle["S4_pooled_lvi_collapsed_missing_as_absent"]
    pooled_pv = proto.get("lvi_pooled_pvalue")
    pooled_pv_s = "" if pooled_pv is None or (isinstance(pooled_pv, float) and np.isnan(pooled_pv)) else f"{float(pooled_pv):.4g}"
    por = proto.get("pooled_or")
    por_s = "" if por is None or (isinstance(por, float) and np.isnan(por)) else f"{float(por):.2f}"
    pooled_bits = (
        f"In the pre-specified pooled lymphovascular sensitivity model (missing treated as absent for the "
        f"pooled binary), the pooled coefficient had adjusted OR={por_s} "
        f"(p={pooled_pv_s}). "
    )
    if proto.get("protective_association_at_alpha005"):
        pooled_bits += (
            "Under α=0.05, this pooled-negative-coefficient construction reproduced a statistically significant "
            "**inverse-risk (OR<1)** association, consistent with the pooled-LVI artifact described in prior "
            "literature."
        )
    elif proto.get("pooled_positive_risk_at_alpha005"):
        pooled_bits += (
            "Instead, this pooled construction produced a statistically significant **elevated-odds** association "
            "(OR>1), not a protective association; it therefore does **not** reconstruct the classic "
            "**protective** pooled-LVI artifact, though it still mixes lymphatic/vascular signal and treats "
            "missing as absent."
        )
    else:
        pooled_bits += (
            "A statistically significant pooled coefficient (either protective or risk-increasing) was not "
            "observed at α=0.05 under this specification."
        )

    pm = bundle["primary"]["metrics"]

    mv_par = (
        f"In the primary logistic regression of path-proven recurrence (covariates: age per decade, sex, "
        f"tumor size (cm), AJCC 8 N stage with explicit missing category, grouped histology, RAI receipt, "
        f"and separated lymphatic and vascular invasion categories),\n\n{adj_gross}\n\n{adj_noneg}\n\n"
        f"(McFadden pseudo-R²={pm['pseudo_r2_mcfadden']:.4f}; "
        f"n={pm['n_obs']}, events={pm['n_events']}; likelihood-ratio χ²="
        f"{pm['lr_vs_null_chi2']:.2f} vs intercept-only)."
        f"{gross_hr_note}\n\n"
        "Detailed coefficients appear in Table 3 and Supplement tables.\n\n"
        f"{pooled_bits}\n"
    )

    core = (
        f"The crude path-proven odds ratio for gross ETE vs microscopic ETE was {cg:.2f} "
        f"(95% CI {cgl:.2f}–{cgh:.2f}); the crude odds ratio for no/negative ETE vs microscopic ETE was "
        f"{cn:.2f} (95% CI {cnl:.2f}–{cnh:.2f}). "
    )
    txt, n12 = _re.subn(
        r"The crude path-proven odds ratio for gross ETE vs microscopic ETE was .*?;"
        r" the crude odds ratio for no/negative ETE vs microscopic ETE was .*?\.\s*"
        r"(?=The legacy `any_recurrence_flag`)",
        core,
        txt,
        count=1,
        flags=_re.S,
    )

    txt, n3 = _re.subn(
        r"(### Multivariable analysis\n\n).+?(\n\n### Tumor-size)",
        r"\1" + mv_par + r"\2",
        txt,
        count=1,
        flags=_re.S,
    )
    if n12 != 1 or n3 != 1:
        raise SystemExit(f"manuscript regex replace failed: crude_block={n12}, multivariable={n3}")

    txt, dsc = _re.subn(
        r"gross ETE was associated with approximately 2\.5-fold higher pathology-proven recurrence than "
        r"microscopic ETE on both crude and adjusted analyses",
        (
            "gross ETE was associated with substantially higher pathology-proven recurrence than microscopic ETE "
            "on crude analysis; logistic adjustment attenuated the gross-vs-microscopic odds ratio (Table 3), "
            "whereas Cox regression on documented surgery-interval follow-up retained elevated hazard gross vs microscopic"
            " (Results)."
        ),
        txt,
        count=1,
    )
    del dsc

    path_md.write_text(txt, encoding="utf-8")
    print(f"[md] patched {path_md}")


def main() -> None:
    ap = argparse.ArgumentParser(description="M044 ETE analytic export + modeling")
    ap.add_argument("--force", action="store_true", help="re-export parquet from MotherDuck")
    ap.add_argument(
        "--skip-md-export",
        action="store_true",
        help="use existing data/m044/analytic_file_v1.parquet",
    )
    args = ap.parse_args()

    if args.skip_md_export:
        pq = DATA_DIR / "analytic_file_v1.parquet"
        if not pq.exists():
            raise SystemExit("--skip-md-export but parquet missing")
    else:
        pq = export_parquet(force=args.force)

    df = pd.read_parquet(pq)
    if len(df) != 4128:
        raise SystemExit(f"parquet rows {len(df)} != 4128")

    bundle = run_all_models(df)
    fg_png = FIG_DIR / "m044_forest_primary.png"
    fg_csv = FIG_DIR / "m044_forest_primary_data.csv"
    forest_plot_primary(bundle["primary"]["coef"], fg_png, fg_csv)
    print(f"[figures] wrote {fg_png} {fg_csv}")

    update_workbook(bundle)
    patch_manuscript_md(bundle)
    print("[done]")


if __name__ == "__main__":
    main()
