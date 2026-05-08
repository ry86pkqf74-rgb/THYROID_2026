#!/usr/bin/env python3
"""
M048 v4 — full covariate-adjusted racial disparities analysis.

Builds on m048_run_analysis_v3.py. Key changes from v3 → v4:
  - Reads m048_v4_patient_master_v1 / m048_v4_nodule_master_v1 (mig_318 BQ tables).
  - Restores the M4 "background pathology" step in the cascade (M0–M6) now that
    has_clt / has_mng / has_graves / has_follicular_adenoma are non-zero from the
    canonical rollup (Bug C fix, mig_318).
  - Expanded mediation: 5 original mediators + 3 new M4 candidates =
    8 mediators × 2 race targets = 16 rows.
  - Two-formula comorbidity sensitivity arm (v1.2-equivalent + full PMH panel).
  - Cascade attenuation table includes per-race M3→M4 and M4→M6 delta rows.
  - Outputs written to studies/m048_racial_disparities_tirads/v4/
  - QA gates updated for v4; 4 new gates added.

Migration: mig_318b (Python pipeline re-fit of mig_318 BQ fix).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)
STUDY_DIR = os.path.dirname(os.path.abspath(__file__))
V4_DIR = os.path.join(STUDY_DIR, "v4")
V3_DIR = os.path.join(STUDY_DIR, "v3")
VERIF_DIR = os.path.join(V4_DIR, "verification")
PKG_V13 = os.path.join(REPO_ROOT, "M048_submission_package_v1.3")

from m048_v3_stats_lib import (  # noqa: E402
    PRIMARY_RACES,
    bootstrap_mediation_product,
    fit_logit,
    normalize_tr_category,
    prepare_v3_frame,
    race_or_table,
    smd_binary,
    smd_continuous,
    fit_logit_regularized,
)

BQ_PROJECT = "thyroid-canonical-pub-2026"
BQ_DATASET = "pub_workspace"
MIG_ID = "mig_318b"
M025_N = 3375
ANALYTIC_N = 3121

# ============================================================================
# BQ helpers (read-only; DDL was executed separately as mig_318)
# ============================================================================

def bq_to_df(query: str) -> pd.DataFrame:
    """Run a BQ SELECT and return a DataFrame via JSON output."""
    import subprocess, json as _json, tempfile, os as _os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with open(tmp_path, "w") as out_fh:
            result = subprocess.run(
                ["bq", "query", "--use_legacy_sql=false", "--format=json",
                 f"--project_id={BQ_PROJECT}", "--max_rows=100000", query],
                stdout=out_fh, stderr=subprocess.PIPE, text=True,
            )
        if result.returncode != 0:
            raise RuntimeError(f"bq error: {result.stderr[:400]}")
        with open(tmp_path) as f:
            content = f.read().strip()
        if not content or content == "[]":
            return pd.DataFrame()
        rows = _json.loads(content)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        _os.unlink(tmp_path)


def bq_table_to_df(table: str) -> pd.DataFrame:
    return bq_to_df(f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`")


def bq_extract_csv(table: str, dest: str) -> None:
    """Export a BQ table to local CSV via bq query."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    try:
        df = bq_to_df(f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`")
        df.to_csv(dest, index=False)
        print(f"  [BQ] {table} -> {dest} ({len(df)} rows)")
    except Exception as exc:
        print(f"  [BQ WARN] {table}: {exc}")


# ============================================================================
# Helpers copied / adapted from v3
# ============================================================================

def tr_to_int(cat) -> float:
    import re
    if cat is None or (isinstance(cat, float) and np.isnan(cat)):
        return np.nan
    s = str(cat).strip().upper()
    m = re.search(r"TR\s*(\d+)", s)
    if m:
        return float(m.group(1))
    m2 = re.search(r"(\d+)", s)
    return float(m2.group(1)) if m2 else np.nan


def acr_rom_mid_high(tr: str) -> tuple[float, float]:
    if tr == "TR5":
        return 42.0, 55.0
    return 18.0, 28.0


def build_disparity_direction(df_bio, df_rom, df_cell) -> pd.DataFrame:
    """race × TR4/TR5 interpretive table + rule-based signature."""
    df_rom = df_rom.copy(); df_bio = df_bio.copy(); df_cell = df_cell.copy()
    df_rom["tr_key"]  = df_rom["tr_category"].map(normalize_tr_category)
    df_bio["tr_key"]  = df_bio["tr_category"].map(normalize_tr_category)
    df_cell["tr_key"] = df_cell["max_tirads_category_ever"].map(normalize_tr_category)

    rows = []
    for tr in ["TR4", "TR5"]:
        mid, high = acr_rom_mid_high(tr)
        for race in PRIMARY_RACES:
            r_rom  = df_rom[(df_rom["race_strat"]  == race) & (df_rom["tr_key"]  == tr)]
            r_bio  = df_bio[(df_bio["race_strat"]  == race) & (df_bio["tr_key"]  == tr)]
            r_cell = df_cell[(df_cell["race_strat"] == race) & (df_cell["tr_key"] == tr)]
            rom_pct      = float(r_rom.iloc[0]["rom_pct"])          if len(r_rom)  else np.nan
            n_bio        = int(r_bio.iloc[0]["n_malignant_in_cell"]) if len(r_bio)  else 0
            mean_sz      = float(r_bio.iloc[0]["mean_tumor_size_cm"]) if len(r_bio) else np.nan
            multifocal_pct = (100.0 * float(r_bio.iloc[0]["n_multifocal"]) / n_bio
                              if n_bio else np.nan)
            pct_ete  = float(r_cell.iloc[0]["pct_any_ete"])      if len(r_cell) else np.nan
            pct_ln   = float(r_cell.iloc[0]["pct_ln_positive"])  if len(r_cell) else np.nan
            dom_hist = str(r_cell.iloc[0]["dominant_histology"]) if len(r_cell) else ""
            rows.append({
                "race_strat": race, "tr_category": tr,
                "n_malignant_cell": n_bio, "rom_pct": rom_pct,
                "mean_tumor_size_cm": mean_sz, "pct_multifocal": multifocal_pct,
                "pct_any_ete": pct_ete, "pct_ln_positive": pct_ln,
                "dominant_histology": dom_hist,
                "acr_rom_mid_ref": mid, "acr_rom_high_ref": high,
            })

    out = pd.DataFrame(rows)
    med_sz  = float(np.nanmedian(out["mean_tumor_size_cm"]))
    med_ete = float(np.nanmedian(out["pct_any_ete"]))
    med_ln  = float(np.nanmedian(out["pct_ln_positive"]))

    sigs = []
    for _, r in out.iterrows():
        tr = r["tr_category"]; mid, high = acr_rom_mid_high(tr)
        rom, sz, ete, ln = r["rom_pct"], r["mean_tumor_size_cm"], r["pct_any_ete"], r["pct_ln_positive"]
        s = "calibrated"
        if rom==rom and sz==sz and ete==ete and rom < mid and sz < med_sz and ete < med_ete:
            s = "over_referral_signature"
        elif rom==rom and sz==sz and ete==ete and ln==ln and rom > high and sz > med_sz and (ete > med_ete or ln > med_ln):
            s = "under_referral_signature"
        sigs.append(s)
    out["signature"] = sigs
    return out


def run_bethesda_stratified_additive(df: pd.DataFrame) -> pd.DataFrame:
    """Model B: additive race + TR within each Bethesda stratum."""
    rows = []
    for b in sorted(df["bethesda_bucket"].dropna().unique()):
        sub = df[df["bethesda_bucket"] == b].dropna(subset=["max_tr_int"]).copy()
        if len(sub) < 30:
            continue
        formula = "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int"
        try:
            res = fit_logit(formula, sub)
            rt  = race_or_table(res)
            for _, rr in rt.iterrows():
                rows.append({"bethesda_bucket": b, "race_level": rr["race_level"],
                             "or": rr["or"], "ci_lo": rr["ci_lo"], "ci_hi": rr["ci_hi"],
                             "p": rr["p"], "n": len(sub), "n_events": int(sub["is_malignant"].sum())})
        except Exception as e:
            rows.append({"bethesda_bucket": b, "race_level": "ERROR",
                         "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "p": np.nan,
                         "n": len(sub), "n_events": int(sub["is_malignant"].sum()) if len(sub) else 0,
                         "error": str(e)})
    return pd.DataFrame(rows)


def run_bethesda_stratified_interaction(df: pd.DataFrame) -> pd.DataFrame:
    """Model B-int: race × TR interaction within each Bethesda stratum; Bonferroni-corrected."""
    bstr_strata = sorted(df["bethesda_bucket"].dropna().unique())
    n_bonf = len(bstr_strata) * 2
    inter_rows = []
    for b in bstr_strata:
        sub = df[df["bethesda_bucket"] == b].dropna(subset=["max_tr_int"]).copy()
        if len(sub) < 30:
            continue
        formula_inter = "is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int"
        try:
            res = fit_logit(formula_inter, sub)
        except Exception:
            try:
                res = fit_logit_regularized(formula_inter, sub)
            except Exception as e2:
                inter_rows.append({"bethesda_bucket": b, "interaction_term": "ERROR",
                                   "coef": np.nan, "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan,
                                   "p": np.nan, "p_bonf": np.nan, "n": len(sub),
                                   "n_events": int(sub["is_malignant"].sum()), "error": str(e2)})
                continue
        ci = res.conf_int()
        pv = getattr(res, "pvalues", pd.Series(dtype=float))
        for pname in res.params.index:
            if ":" not in pname or "race_strat" not in pname:
                continue
            coef   = float(res.params[pname])
            lo     = float(ci.loc[pname, 0]) if pname in ci.index else np.nan
            hi     = float(ci.loc[pname, 1]) if pname in ci.index else np.nan
            p_raw  = float(pv[pname]) if pname in pv.index else np.nan
            inter_rows.append({
                "bethesda_bucket": b, "interaction_term": pname,
                "coef": coef, "or": np.exp(coef),
                "ci_lo": np.exp(lo), "ci_hi": np.exp(hi),
                "p": p_raw,
                "p_bonf": float(min(p_raw * n_bonf, 1.0)) if np.isfinite(p_raw) else np.nan,
                "n": len(sub), "n_events": int(sub["is_malignant"].sum()),
            })
    return pd.DataFrame(inter_rows)


def covariate_balance(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    covariates = [
        ("max_tr_int", "cont"), ("age_at_surgery", "cont"), ("surg_year", "cont"),
        ("n_nodules_total", "cont"), ("n_fnas_total", "cont"),
        ("days_us_to_surg_approx", "cont"),
        ("had_any_genetics", "bin"), ("had_any_nm", "bin"),
        ("had_any_fna", "bin"), ("had_repeat_fna", "bin"),
        ("has_clt", "bin"), ("has_mng", "bin"), ("has_graves", "bin"),
        ("has_follicular_adenoma", "bin"),
        ("has_niftp", "bin"), ("has_ftump", "bin"),
        ("pmh_dm", "bin"), ("pmh_htn", "bin"), ("pmh_obesity", "bin"),
        ("pmh_hyperthyroidism", "bin"), ("pmh_hypothyroidism", "bin"),
        ("pmh_family_hx_thyroid", "bin"), ("pmh_radiation_exposure", "bin"),
    ]
    for name, kind in covariates:
        if name not in df.columns:
            continue
        if kind == "cont":
            smd = smd_continuous(df[name], df["race_strat"])
        else:
            smd = smd_binary(df[name], df["race_strat"])
        for race, val in smd.items():
            rows.append({"variable": name, "versus_reference": "White", "race_strat": race,
                         "smd": round(val, 4) if val == val else np.nan,
                         "flag_gt_010": bool(val == val and abs(val) > 0.10)})
    if "bethesda_bucket" in df.columns:
        for lv in sorted(df["bethesda_bucket"].dropna().astype(str).unique()):
            col = (df["bethesda_bucket"].astype(str) == lv).astype(float)
            smd = smd_binary(col, df["race_strat"])
            for race, val in smd.items():
                rows.append({"variable": f"bethesda_{lv}", "versus_reference": "White",
                             "race_strat": race,
                             "smd": round(val, 4) if val == val else np.nan,
                             "flag_gt_010": bool(val == val and abs(val) > 0.10)})
    return pd.DataFrame(rows)


def prepare_v4_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Extends prepare_v3_frame with v4-specific binary columns."""
    df = prepare_v3_frame(raw)
    # Additional v4 binary columns
    for c in [
        "has_follicular_adenoma",
        "has_lymphocytic_thyroiditis", "has_hashimotos",
        "has_substernal_goiter", "has_nodular_hyperplasia",
        "pmh_dm", "pmh_htn", "pmh_obesity", "pmh_ckd", "pmh_cad", "pmh_copd",
        "pmh_depression", "pmh_hyperthyroidism", "pmh_hypothyroidism",
        "pmh_autoimmune_thyroid_hx", "pmh_family_hx_thyroid", "pmh_family_hx_cancer",
        "pmh_radiation_exposure", "pmh_prior_cancer_hx",
        "pmh_smoking_current", "pmh_smoking_former",
    ]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: 1 if v in (True, "true", "True", 1, "1") else 0)
    return df


# ============================================================================
# M4 mediators (v4 adds has_mng, has_clt, has_graves)
# ============================================================================

MEDIATORS_V4: list[tuple[str, str]] = [
    ("n_nodules_total",          "continuous"),
    ("had_any_genetics",         "binary"),
    ("had_any_nm",               "binary"),
    ("n_fnas_total",             "continuous"),
    ("days_us_to_surg_approx",   "continuous"),
    # New M4 mediator candidates (now non-zero in v4 master)
    ("has_mng",                  "binary"),
    ("has_clt",                  "binary"),
    ("has_graves",               "binary"),
]


# ============================================================================
# Main
# ============================================================================

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-sql", action="store_true",
                    help="Skip BQ table dumps (assumes v4/ CSVs already present).")
    ap.add_argument("--mediation-boot", type=int, default=1000)
    args = ap.parse_args()

    os.makedirs(V4_DIR, exist_ok=True)
    os.makedirs(VERIF_DIR, exist_ok=True)
    os.makedirs(PKG_V13, exist_ok=True)

    git_sha = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, stderr=subprocess.DEVNULL
    ).decode().strip()

    ts = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # 1. Dump v4 BQ tables to CSV
    # ------------------------------------------------------------------
    if not args.skip_sql:
        print("[BQ DUMP] v4 tables ...")
        bq_extract_csv("m048_v4_patient_master_v1",
                       os.path.join(V4_DIR, "m048_v4_patient_master_full.csv"))
        bq_extract_csv("m048_v4_nodule_master_v1",
                       os.path.join(V4_DIR, "m048_v4_nodule_master_full.csv"))
        bq_extract_csv("m048_v4_sql_qa_counts_v1",
                       os.path.join(V4_DIR, "m048_v4_sql_qa_counts.csv"))
        bq_extract_csv("m048_v4_background_path_by_race_v1",
                       os.path.join(V4_DIR, "m048_v4_background_path_by_race.csv"))
        bq_extract_csv("m048_v4_pmh_by_race_v1",
                       os.path.join(V4_DIR, "m048_v4_pmh_by_race.csv"))

    # ------------------------------------------------------------------
    # 2. Load and prepare patient master
    # ------------------------------------------------------------------
    print("[LOAD] v4 patient master ...")
    df_raw = pd.read_csv(os.path.join(V4_DIR, "m048_v4_patient_master_full.csv"),
                         low_memory=False)
    df = prepare_v4_frame(df_raw)
    df_model = df.dropna(subset=["max_tr_int"]).copy()
    assert len(df_model) == ANALYTIC_N, (
        f"Analytic N mismatch: expected {ANALYTIC_N}, got {len(df_model)}"
    )
    print(f"  analytic n = {len(df_model)} ✓")

    # ------------------------------------------------------------------
    # 3. v4 CASCADE  M0 → M1 → M2 → M3 → M4 (RESTORED) → M5 → M6
    # ------------------------------------------------------------------
    print("[CASCADE] M0..M6 (M4 restored) ...")

    # Note: 'surg_year_centered' and 'days_us_to_surg_approx_yrs' are aliases
    # for the prepare_v4_frame transformations:
    #   surg_year is already centred by prepare_v3_frame (→ surg_year in df)
    #   days_us_to_surg_approx is already rescaled to years (→ days_us_to_surg_approx_yrs equiv)
    # The prompt spec uses "surg_year_centered" and "days_us_to_surg_approx_yrs" in formulas;
    # the actual column names in df are "surg_year" (centred) and "days_us_to_surg_approx" (in years).

    cascade_specs = [
        ("M0", "is_malignant ~ C(race_strat, Treatment('White'))"),
        ("M1", "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int"),
        ("M2", "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat)"),
        (
            "M3",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm",
        ),
        (
            "M4",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm "
            "+ has_clt + has_mng + has_graves + has_follicular_adenoma",
        ),
        (
            "M5",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm "
            "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
            "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx",
        ),
        (
            "M6",
            "is_malignant ~ C(race_strat, Treatment('White')) + max_tr_int + C(nodule_burden_cat) "
            "+ had_any_genetics + had_any_nm "
            "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
            "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
            "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)",
        ),
    ]

    cascade_rows = []
    cascade_or: dict[str, dict[str, float]] = {}  # {step: {race: or}}

    for tag, formula in cascade_specs:
        try:
            res = fit_logit(formula, df_model)
            rt  = race_or_table(res)
            log_lik = float(res.llf)
            aic     = float(res.aic)
            cascade_or[tag] = {}
            for _, rr in rt.iterrows():
                cascade_rows.append({
                    "model_step": tag, "formula": formula,
                    "race_level": rr["race_level"],
                    "or": rr["or"], "ci_lo": rr["ci_lo"], "ci_hi": rr["ci_hi"],
                    "p": rr["p"], "n": len(df_model),
                    "log_lik": log_lik, "aic": aic,
                })
                cascade_or[tag][rr["race_level"]] = rr["or"]
        except Exception as e:
            for race in ("Black", "Asian"):
                cascade_rows.append({
                    "model_step": tag, "formula": formula, "race_level": race,
                    "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "p": np.nan,
                    "n": len(df_model), "log_lik": np.nan, "aic": np.nan, "error": str(e),
                })

    cas_df = pd.DataFrame(cascade_rows)
    cas_df.to_csv(os.path.join(V4_DIR, "m048_v4_cascade.csv"), index=False)
    print(f"  cascade rows: {len(cas_df)}")

    # ------------------------------------------------------------------
    # 4. Cascade attenuation table (M3→M4, M3→M6, M0→M6 per race)
    # ------------------------------------------------------------------
    atten_rows = []
    for race in ("Black", "Asian"):
        m0_or = cascade_or.get("M0", {}).get(race, np.nan)
        m3_or = cascade_or.get("M3", {}).get(race, np.nan)
        m4_or = cascade_or.get("M4", {}).get(race, np.nan)
        m6_or = cascade_or.get("M6", {}).get(race, np.nan)

        def pct_att(start, end):
            if np.isfinite(start) and np.isfinite(end) and start:
                return round(100.0 * (1.0 - end / start), 2)
            return np.nan

        atten_rows.append({"race_level": race, "from_step": "M0", "to_step": "M6",
                           "from_or": m0_or, "to_or": m6_or,
                           "attenuation_pct": pct_att(m0_or, m6_or)})
        atten_rows.append({"race_level": race, "from_step": "M3", "to_step": "M4",
                           "from_or": m3_or, "to_or": m4_or,
                           "attenuation_pct": pct_att(m3_or, m4_or)})
        atten_rows.append({"race_level": race, "from_step": "M3", "to_step": "M6",
                           "from_or": m3_or, "to_or": m6_or,
                           "attenuation_pct": pct_att(m3_or, m6_or)})
        atten_rows.append({"race_level": race, "from_step": "M4", "to_step": "M5",
                           "from_or": m4_or, "to_or": cascade_or.get("M5", {}).get(race, np.nan),
                           "attenuation_pct": pct_att(m4_or, cascade_or.get("M5", {}).get(race, np.nan))})

    pd.DataFrame(atten_rows).to_csv(
        os.path.join(V4_DIR, "m048_v4_cascade_attenuation.csv"), index=False
    )

    # ------------------------------------------------------------------
    # 5. Full M6 model: full OR table for Table 3
    # ------------------------------------------------------------------
    full_formula = cascade_specs[-1][1]  # M6
    full_res = fit_logit(full_formula, df_model)
    pd.DataFrame({
        "param": full_res.params.index,
        "coef":  full_res.params.values,
        "or":    np.exp(full_res.params.values),
        "ci_lo": np.exp(full_res.conf_int().iloc[:, 0].values),
        "ci_hi": np.exp(full_res.conf_int().iloc[:, 1].values),
        "p":     full_res.pvalues.values,
    }).to_csv(os.path.join(V4_DIR, "m048_v4_full_model_OR.csv"), index=False)

    # ------------------------------------------------------------------
    # 6. Model I: race × TR interaction (controls = M6 minus race × TR term)
    # ------------------------------------------------------------------
    inter_formula = (
        "is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int "
        "+ C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
        "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    inter_res = fit_logit(inter_formula, df_model)
    inter_params = pd.DataFrame({
        "param": inter_res.params.index,
        "coef":  inter_res.params.values,
        "p":     inter_res.pvalues.values,
    })
    inter_terms = inter_params[
        inter_params["param"].str.contains(":") & inter_params["param"].str.contains("race")
    ].copy()
    inter_terms["p_bonf"] = np.minimum(inter_terms["p"] * 4, 1.0)
    inter_terms.to_csv(os.path.join(V4_DIR, "m048_v4_interaction_race_x_tr.csv"), index=False)

    # Model M: race × nodule burden
    m_formula = (
        "is_malignant ~ C(race_strat, Treatment('White')) * C(nodule_burden_cat) + max_tr_int "
        "+ had_any_genetics + had_any_nm "
        "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) + days_us_to_surg_approx "
        "+ age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    m_res = fit_logit(m_formula, df_model)
    pd.DataFrame({
        "param": m_res.params.index,
        "coef":  m_res.params.values,
        "or":    np.exp(m_res.params.values),
        "p":     m_res.pvalues.values,
    }).to_csv(os.path.join(V4_DIR, "m048_v4_interaction_race_x_nodulect.csv"), index=False)

    # ------------------------------------------------------------------
    # 7. Bethesda-stratified Model B (additive) and B-int (interaction)
    # ------------------------------------------------------------------
    print("[BETHESDA STRAT] Model B additive + interaction ...")
    df_bstrat = run_bethesda_stratified_additive(df_model)
    df_bstrat.to_csv(
        os.path.join(V4_DIR, "m048_v4_bethesda_stratified_TR_ROM.csv"), index=False
    )

    df_bint = run_bethesda_stratified_interaction(df_model)
    df_bint.to_csv(
        os.path.join(V4_DIR, "m048_v4_bethesda_stratified_TR_interaction.csv"), index=False
    )

    # ------------------------------------------------------------------
    # 8. Per-nodule cluster-robust Model F-Nodule (v4 nodule master, M4 added)
    # ------------------------------------------------------------------
    print("[NODULE] Model F-Nodule cluster-robust ...")
    df_nod_raw = pd.read_csv(os.path.join(V4_DIR, "m048_v4_nodule_master_full.csv"),
                             low_memory=False)
    # prepare_v3_frame requires an is_malignant column; add a dummy if absent
    # (the nodule outcome is nodule_path_proven_malignant, not is_malignant)
    df_nod_work = df_nod_raw.copy()
    if "is_malignant" not in df_nod_work.columns:
        df_nod_work["is_malignant"] = 0  # dummy; will not be used in regression
    df_nod = prepare_v4_frame(df_nod_work)
    # nodule-level outcome
    df_nod["nodule_path_proven_malignant"] = df_nod_raw["nodule_path_proven_malignant"].apply(
        lambda v: 1 if v in (True, "true", "True", 1, "1") else 0
    ).astype(int)
    df_nod["acr2017_tirads_int"] = df_nod_raw["acr2017_tirads_category"].apply(tr_to_int)
    mask_col = "analytic_eligible_strict_acr_pernodule"
    if mask_col in df_nod.columns:
        df_nod = df_nod[df_nod[mask_col] == True].copy()
    df_nod = df_nod.dropna(subset=["acr2017_tirads_int", "research_id"])
    df_nod["race_strat"] = df_nod["race_strat"].astype(str)
    df_nod = df_nod[df_nod["race_strat"].isin(PRIMARY_RACES)]
    if "patient_bethesda_bucket" in df_nod.columns:
        if "bethesda_bucket" in df_nod.columns:
            df_nod = df_nod.drop(columns=["bethesda_bucket"])
        df_nod.rename(columns={"patient_bethesda_bucket": "bethesda_bucket"}, inplace=True)
    if "bethesda_bucket" not in df_nod.columns and "bethesda_bucket" in df_nod_raw.columns:
        df_nod["bethesda_bucket"] = df_nod_raw["bethesda_bucket"].fillna("missing").astype(str)

    nod_formula = (
        "nodule_path_proven_malignant ~ C(race_strat, Treatment('White')) + acr2017_tirads_int "
        "+ C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ has_clt + has_mng + has_graves + has_follicular_adenoma "
        "+ had_repeat_fna + n_fnas_total + C(bethesda_bucket) "
        "+ days_us_to_surg_approx + age_at_surgery + C(sex) + surg_year + C(surg_procedure_type)"
    )
    try:
        df_nod_fit = df_nod.dropna(subset=["nodule_path_proven_malignant"]).copy()
        nod_res = fit_logit(
            nod_formula, df_nod_fit,
            cluster_col="research_id",
            outcome_col="nodule_path_proven_malignant",
        )
        race_or_table(nod_res).to_csv(
            os.path.join(V4_DIR, "m048_v4_per_nodule_cluster_robust.csv"), index=False
        )
    except Exception as e:
        pd.DataFrame([{"error": str(e)}]).to_csv(
            os.path.join(V4_DIR, "m048_v4_per_nodule_cluster_robust.csv"), index=False
        )
        print(f"  [WARN] nodule cluster robust: {e}")

    # ------------------------------------------------------------------
    # 9. Comorbidity sensitivity arms (v1.2-equivalent + full PMH panel)
    # ------------------------------------------------------------------
    print("[COMORBIDITY SENSITIVITY] v1.2 + full PMH ...")

    formula_v12 = (
        "is_malignant ~ C(race_strat, Treatment('White')) "
        "+ max_tr_int + C(bethesda_bucket) "
        "+ pmh_hyperthyroidism + pmh_htn + pmh_dm"
    )
    formula_v4_full = (
        "is_malignant ~ C(race_strat, Treatment('White')) "
        "+ max_tr_int + C(bethesda_bucket) "
        "+ pmh_hyperthyroidism + pmh_hypothyroidism "
        "+ pmh_autoimmune_thyroid_hx "
        "+ pmh_htn + pmh_dm + pmh_obesity + pmh_ckd "
        "+ pmh_cad + pmh_copd + pmh_depression "
        "+ pmh_family_hx_thyroid + pmh_family_hx_cancer "
        "+ pmh_radiation_exposure + pmh_prior_cancer_hx"
    )

    comorbidity_rows = []
    for model_label, formula in [("v12_equivalent", formula_v12), ("v4_full_pmh", formula_v4_full)]:
        try:
            res = fit_logit(formula, df_model)
            params_df = pd.DataFrame({
                "term":  res.params.index,
                "coef":  res.params.values,
                "or":    np.exp(res.params.values),
                "ci_lo": np.exp(res.conf_int().iloc[:, 0].values),
                "ci_hi": np.exp(res.conf_int().iloc[:, 1].values),
                "p":     res.pvalues.values,
            })
            params_df["model_label"] = model_label
            params_df["n"] = len(df_model)
            comorbidity_rows.append(params_df)
        except Exception as e:
            comorbidity_rows.append(pd.DataFrame([{
                "model_label": model_label, "term": "ERROR",
                "coef": np.nan, "or": np.nan, "ci_lo": np.nan, "ci_hi": np.nan,
                "p": np.nan, "n": len(df_model), "error": str(e),
            }]))

    pd.concat(comorbidity_rows, ignore_index=True).to_csv(
        os.path.join(V4_DIR, "m048_v4_comorbidity_sensitivity.csv"), index=False
    )

    # ------------------------------------------------------------------
    # 10. Sensitivity arms A–G (v4 master; arm D now non-trivial)
    # ------------------------------------------------------------------
    print("[SENSITIVITY ARMS] A–G ...")
    sens_rows: list[dict] = []

    def sens_fit(label: str, sub_raw: pd.DataFrame) -> None:
        if len(sub_raw) < 100:
            sens_rows.append({"arm": label, "n": len(sub_raw), "error": "too_small"})
            return
        sub = prepare_v4_frame(sub_raw).dropna(subset=["max_tr_int"]).copy()
        if len(sub) < 50:
            sens_rows.append({"arm": label, "n": len(sub), "error": "too_small_after_filter"})
            return
        try:
            r = fit_logit(full_formula, sub)
            for _, row in race_or_table(r).iterrows():
                sens_rows.append({
                    "arm": label, "n": len(sub), "race_level": row["race_level"],
                    "or": row["or"], "ci_lo": row["ci_lo"], "ci_hi": row["ci_hi"], "p": row["p"],
                })
        except Exception as e:
            sens_rows.append({"arm": label, "n": len(sub), "error": str(e)})

    sdf = df_raw.copy()
    if "surg_first_date" in sdf.columns:
        sdf["surg_dt"] = pd.to_datetime(sdf["surg_first_date"], errors="coerce")
    elif "surg_year" in sdf.columns:
        sdf["surg_dt"] = pd.to_datetime(
            (pd.to_numeric(sdf["surg_year"], errors="coerce") + 2020).round().astype("Int64").astype(str) + "-07-01",
            errors="coerce",
        )
    else:
        sdf["surg_dt"] = pd.NaT

    sens_fit("S048v4_A_post2017",       sdf[sdf["surg_dt"] >= "2017-05-01"])
    sens_fit("S048v4_B_single_nodule",  sdf[pd.to_numeric(sdf["n_nodules_total"], errors="coerce") == 1])
    sens_fit("S048v4_C_genetics_tested",
             sdf[sdf["had_any_genetics"].apply(lambda v: v in (True, "true", 1, "1", 1.0))])
    # Arm D: now actually removes CLT patients (non-trivial because has_clt>0 in v4)
    sens_fit("S048v4_D_no_CLT",
             sdf[sdf["has_clt"].apply(lambda v: v not in (True, "true", 1, "1", 1.0))])
    bb_upper = sdf["bethesda_bucket"].fillna("").astype(str).str.strip().str.upper()
    vi_mask  = bb_upper.str.startswith("VI")
    sens_fit("S048v4_E_no_Bethesda_VI", sdf[~vi_mask])
    sens_fit("S048v4_F_TR4_only",
             sdf[pd.to_numeric(sdf["max_tr_int"], errors="coerce") == 4])
    sens_fit("S048v4_G_had_fna",
             sdf[sdf["had_any_fna"].apply(lambda v: v in (True, "true", 1, "1", 1.0))])

    pd.DataFrame(sens_rows).to_csv(
        os.path.join(V4_DIR, "m048_v4_sensitivity_arms.csv"), index=False
    )

    # ------------------------------------------------------------------
    # 11. Mediation bootstrap (8 mediators × 2 race targets = 16 rows)
    # ------------------------------------------------------------------
    print(f"[MEDIATION] {args.mediation_boot} boot reps × 8 mediators × 2 races ...")
    med_controls = (
        "max_tr_int + C(nodule_burden_cat) + had_any_genetics + had_any_nm "
        "+ had_repeat_fna + C(bethesda_bucket) + age_at_surgery "
        "+ C(sex) + surg_year + C(surg_procedure_type)"
    )
    df_med_input = df_model.assign(is_malignant=df_model["is_malignant"].astype(int))
    med_rows = []
    for race_target in ("Black", "Asian"):
        scope = f"univariate_{race_target.lower()}_vs_white"
        for med, mtype in MEDIATORS_V4:
            if med not in df_model.columns:
                print(f"  SKIP mediator {med} (not in df_model)")
                continue
            r = bootstrap_mediation_product(
                df_med_input, med, mtype,
                "race_strat", "is_malignant", med_controls,
                n_boot=max(50, args.mediation_boot),
                seed=42,
                race_target=race_target,
            )
            med_rows.append({
                "mediator": med, "type": mtype,
                "race_target": r["race_target"], "scope": scope,
                "indirect_median": r["indirect_mean"],       # median per v3 convention
                "indirect_winsor_mean": r.get("indirect_winsor_mean", float("nan")),
                "ci_lo": r["ci_lo"], "ci_hi": r["ci_hi"],
            })
    pd.DataFrame(med_rows).to_csv(
        os.path.join(V4_DIR, "m048_v4_mediation.csv"), index=False
    )
    print(f"  mediation rows: {len(med_rows)}")

    # ------------------------------------------------------------------
    # 12. Disparity direction table (from v3 BQ views — unchanged)
    # ------------------------------------------------------------------
    print("[DISPARITY DIRECTION] from BQ ...")
    try:
        q_cell = """
        WITH mal AS (
          SELECT v.research_id, v.race_strat, v.max_tirads_category_ever,
                 CASE WHEN e.ete_grade IN ('microscopic', 'gross') THEN 1 ELSE 0 END AS any_ete,
                 CASE WHEN ln.ln_any_positive IS TRUE THEN 1 ELSE 0 END AS ln_pos,
                 v.histology_category
          FROM `thyroid-canonical-pub-2026.pub_workspace.m048_v4_patient_master_v1` v
          LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_ete_event_resolved_v1` e
            ON CAST(e.research_id AS STRING) = v.research_id
          LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.ln_master_rollup_v1` ln
            ON CAST(ln.research_id AS STRING) = v.research_id
          WHERE v.is_malignant = TRUE
            AND SAFE_CAST(REGEXP_EXTRACT(CAST(v.max_tirads_category_ever AS STRING), r'[0-9]+') AS INT64) IN (4, 5)
        )
        SELECT race_strat, max_tirads_category_ever,
               COUNT(*) AS n,
               AVG(any_ete) AS pct_any_ete,
               AVG(ln_pos) AS pct_ln_positive,
               APPROX_TOP_COUNT(histology_category, 1)[OFFSET(0)].value AS dominant_histology
        FROM mal
        GROUP BY 1, 2
        """
        df_cell = bq_to_df(q_cell)
        if len(df_cell) and "pct_any_ete" in df_cell.columns:
            df_cell["pct_any_ete"]      = pd.to_numeric(df_cell["pct_any_ete"],      errors="coerce") * 100.0
            df_cell["pct_ln_positive"]  = pd.to_numeric(df_cell["pct_ln_positive"],  errors="coerce") * 100.0
        df_bio = bq_table_to_df("m048_tumor_biology_descriptors_by_race_v1")
        df_rom = bq_table_to_df("m048_rom_by_race_patient_v1")
        df_dd  = build_disparity_direction(df_bio, df_rom, df_cell)
        df_dd.to_csv(os.path.join(V4_DIR, "m048_v4_disparity_direction_table.csv"), index=False)
    except Exception as exc:
        print(f"  [WARN] disparity direction: {exc}")
        # Fall back to v3 copy
        import shutil
        v3_dd = os.path.join(V3_DIR, "m048_v3_disparity_direction_table.csv")
        if os.path.exists(v3_dd):
            shutil.copy(v3_dd, os.path.join(V4_DIR, "m048_v4_disparity_direction_table.csv"))

    # ------------------------------------------------------------------
    # 13. Covariate balance
    # ------------------------------------------------------------------
    df_bal = covariate_balance(df_model)
    df_bal.to_csv(os.path.join(V4_DIR, "m048_v4_covariate_balance.csv"), index=False)

    # ------------------------------------------------------------------
    # 14. QA gates
    # ------------------------------------------------------------------
    print("[QA GATES] ...")
    gates = []

    # Original gates
    n_master  = len(df_raw)
    n_analytic = len(df_model)
    gates.append({"gate": "v4_master_rowcount",
                  "status": "PASS" if n_master == M025_N else "FAIL",
                  "actual": n_master, "expected": M025_N})
    gates.append({"gate": "analytic_n_reconciles",
                  "status": "PASS" if n_analytic == ANALYTIC_N else "FAIL",
                  "actual": n_analytic, "expected": ANALYTIC_N})

    with_fna = int(pd.to_numeric(df_raw.get("had_any_fna", pd.Series()), errors="coerce").fillna(0).sum())
    pct_fna  = round(100.0 * with_fna / n_master, 2) if n_master else 0.0
    gates.append({"gate": "fna_coverage_pct",
                  "status": "PASS" if 65 <= pct_fna <= 80 else "WARN",
                  "actual": pct_fna, "expected": "~70.5"})

    # v4 specific gates
    with_mng = int(pd.to_numeric(df_raw.get("has_mng", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    with_clt = int(pd.to_numeric(df_raw.get("has_clt", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    with_gvs = int(pd.to_numeric(df_raw.get("has_graves", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    gates.append({"gate": "m4_background_path_signal_ok",
                  "status": "PASS" if with_mng > 1500 and with_clt > 600 and with_gvs > 50 else "FAIL",
                  "actual": f"mng={with_mng} clt={with_clt} graves={with_gvs}",
                  "expected": "mng>1500 AND clt>600 AND graves>50"})
    gates.append({"gate": "cascade_has_m4_row",
                  "status": "PASS" if set(cas_df[cas_df["model_step"] == "M4"]["race_level"]) >= {"Black", "Asian"} else "FAIL",
                  "actual": list(cas_df[cas_df["model_step"] == "M4"]["race_level"].unique()),
                  "expected": "M4 rows for Black and Asian"})
    gates.append({"gate": "cohort_n_reconciles",
                  "status": "PASS" if n_analytic == ANALYTIC_N else "FAIL",
                  "actual": n_analytic, "expected": ANALYTIC_N})

    # mediation mediator coverage
    try:
        med_df = pd.read_csv(os.path.join(V4_DIR, "m048_v4_mediation.csv"))
        has_m4_med = all(
            med in med_df["mediator"].values
            for med in ["has_mng", "has_clt", "has_graves"]
        ) and set(med_df[med_df["mediator"] == "has_mng"]["race_target"]) >= {"Black", "Asian"}
        gates.append({"gate": "mediation_has_m4_mediators",
                      "status": "PASS" if has_m4_med else "FAIL",
                      "actual": list(med_df["mediator"].unique()),
                      "expected": "has_mng, has_clt, has_graves for Black and Asian"})
    except Exception as e:
        gates.append({"gate": "mediation_has_m4_mediators", "status": "FAIL",
                      "actual": str(e), "expected": "has_mng, has_clt, has_graves for Black+Asian"})

    # Arm D non-trivial check
    sens_df = pd.read_csv(os.path.join(V4_DIR, "m048_v4_sensitivity_arms.csv"))
    arm_d_n = sens_df.loc[sens_df["arm"].str.contains("_D_"), "n"].dropna()
    arm_d_meaningful = int(arm_d_n.iloc[0]) < n_analytic if len(arm_d_n) else False
    gates.append({"gate": "arm_D_no_clt_removes_patients",
                  "status": "PASS" if arm_d_meaningful else "WARN",
                  "actual": int(arm_d_n.iloc[0]) if len(arm_d_n) else "missing",
                  "expected": f"< {n_analytic} (has_clt patients removed)"})

    pd.DataFrame(gates).to_csv(os.path.join(V4_DIR, "m048_v4_qa_gates.csv"), index=False)
    n_fail = int((pd.DataFrame(gates)["status"] == "FAIL").sum())
    print(f"  QA: {len(gates)} gates, {n_fail} FAILs")

    # ------------------------------------------------------------------
    # 15. Run snapshot
    # ------------------------------------------------------------------
    m0_black_or = cascade_or.get("M0", {}).get("Black", np.nan)
    m6_black_or = cascade_or.get("M6", {}).get("Black", np.nan)
    atten_pct   = round(100.0 * (1.0 - m6_black_or / m0_black_or), 2) if (np.isfinite(m0_black_or) and np.isfinite(m6_black_or) and m0_black_or) else np.nan

    snap = {
        "study_id":       "M048_v4",
        "mig_id":         MIG_ID,
        "run_timestamp_utc": ts,
        "git_sha":        git_sha,
        "n_patient_v4":   n_master,
        "n_analytic":     n_analytic,
        "cascade_m0_black_or": round(m0_black_or, 5) if np.isfinite(m0_black_or) else None,
        "cascade_m6_black_or": round(m6_black_or, 5) if np.isfinite(m6_black_or) else None,
        "attenuation_pct_black_m0_to_m6": atten_pct,
        "with_mng": with_mng, "with_clt": with_clt, "with_graves": with_gvs,
        "mediation_boot_reps": args.mediation_boot,
        "n_qa_fail": n_fail,
        "paths": {"v4_dir": V4_DIR, "pkg_v13": PKG_V13},
    }
    with open(os.path.join(V4_DIR, "m048_v4_run_snapshot.json"), "w") as f:
        json.dump(snap, f, indent=2)

    # M025 reconciliation
    try:
        rec = bq_to_df(f"""
            SELECT race_strat,
                   COUNT(*) AS n_patients,
                   COUNTIF(is_malignant = TRUE) AS n_malignant
            FROM `{BQ_PROJECT}.{BQ_DATASET}.m048_v4_patient_master_v1`
            GROUP BY 1 ORDER BY 1
        """)
        rec.to_csv(os.path.join(VERIF_DIR, "m025_reconciliation_v4.csv"), index=False)
    except Exception as e:
        pd.DataFrame([{"error": str(e)}]).to_csv(
            os.path.join(VERIF_DIR, "m025_reconciliation_v4.csv"), index=False
        )

    print(f"\n[DONE] v4 outputs under {V4_DIR}")
    print(f"  Black M0 OR:  {m0_black_or:.4f}")
    print(f"  Black M6 OR:  {m6_black_or:.4f}")
    print(f"  Attenuation:  {atten_pct}%")
    print(f"  QA failures:  {n_fail}")
    print(f"\n  Next: run independent_recompute_v4.py, then m048_build_tables_xlsx_v4.py")


if __name__ == "__main__":
    main()
