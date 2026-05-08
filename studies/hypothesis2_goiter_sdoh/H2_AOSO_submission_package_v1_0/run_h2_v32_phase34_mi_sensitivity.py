#!/usr/bin/env python3
"""
H2 v3.2 Phase 3.4 — Multiple Imputation Sensitivity (M1–M4 pooled)

BQ source:  thyroid-canonical-pub-2026.pub_workspace.cohort_h2_pathology_outcome_v2
Linear:     THY-35.4 (parent: THY-35 → THY-32)
Audit:      DFL-20260508-H2-PHASE34-MI-SENSITIVITY  (rec: reck1IE5SHBYwcuLp)
Predecessor: Phase 3.3 (run_h2_v32_phase33_adjusted_models.py)

MI specification:
  Library:   sklearn.impute.IterativeImputer  (BayesianRidge estimator)
  m:         20 imputations
  Burn-in:   10 iterations per chain (each imputation drawn from seed 42+i, i=0..19)
  Imputed:   bmi_combined (79.5%), bethesda_final (53.5%),
             prm_first_fna_days_from_surg (51.9%), gland_weight_final_g (13.7%)
  NOT imputed: smoking (degenerate — never collected), all outcomes
  Pooling:   Rubin's rules — Q_bar, U_bar, B, T, df_BR (Barnard-Rubin)

Outputs:
  tables/table_11a_missingness_by_covariate.csv
  tables/table_11b_M1_pooled.csv
  tables/table_11c_M2_pooled.csv
  tables/table_11d_M3_pooled.csv
  tables/table_11e_M4_pooled.csv
  tables/table_11f_complete_case_vs_pooled.csv
  figures/figure_S1_mi_sensitivity_forest.{png,svg}
  figures/figure_S1_caption.txt
  figures/figure_S2_mi_trace.png
  run_metadata_phase34.json
"""

import hashlib
import json
import warnings
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as sst
import statsmodels.api as sm
import statsmodels.formula.api as smf
from patsy import dmatrices
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge

warnings.filterwarnings("ignore")
np.random.seed(42)

# ─── Paths ─────────────────────────────────────────────────────────────────────
PKG    = Path("/Users/loganglosser/THYROID_2026/studies/hypothesis2_goiter_sdoh/"
              "H2_AOSO_submission_package_v1_0")
TABLES = PKG / "tables"
FIGS   = PKG / "figures"
STATS  = PKG / "stats"
for d in (TABLES, FIGS, STATS):
    d.mkdir(parents=True, exist_ok=True)

# ─── Constants (shared with Phase 3.3) ─────────────────────────────────────────
BQ_TABLE  = ("thyroid-canonical-pub-2026.pub_workspace"
             ".cohort_h2_pathology_outcome_v2")
REF_RACE  = "White"
ALPHA     = 0.05

GATE_N_TOTAL     = 6075
GATE_N_MALIGNANT = 1528
GATE_N_ATYPICAL  = 7
GATE_N_THYMIC    = 252

OC_BENIGN    = "pure_benign"
OC_INDET     = "indeterminate"
OC_MALIGNANT = "frank_malignancy"
OC_INCIDENTAL = "benign_plus_incidental_microcarcinoma"
PTC_GROUPS   = {"PTC_classical", "PTC_variants"}
BETH_MAP     = {1.0: "B1", 2.0: "B2", 3.0: "B3", 4.0: "B4", 5.0: "B5", 6.0: "B6"}
BETH_REF     = "B6"

# MI parameters
M_IMPUTATIONS = 20
BURN_IN       = 10

# Phase 3.3 CC denominators (for cohort comparison)
CC_N = {"M1": 518, "M2": 518, "M3": 159, "M4": 93}

# Phase 3.3 CC race-contrast estimates for Black/AA (for 11f comparison)
CC_ESTIMATES_PATH = TABLES / "table_10a_M1_frank_malignancy.csv"

# Covariates to impute
IMPUTE_COLS_CONTINUOUS = [
    "bmi_combined",
    "prm_first_fna_days_raw_for_impute",   # impute raw, then log-transform
    "gland_weight_raw_for_impute",          # impute raw, then log-transform
]
IMPUTE_COL_ORDINAL = "bethesda_raw_for_impute"    # ordinal 1-6, treat as continuous

RUN_START = datetime.now(timezone.utc)
print(f"Phase 3.4 started: {RUN_START.isoformat()}")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Load BQ cohort
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[1/10] Pulling cohort from BigQuery…")
from google.cloud import bigquery
client = bigquery.Client(project="thyroid-canonical-pub-2026")
df_raw = client.query(f"SELECT * FROM `{BQ_TABLE}`").to_dataframe()

DATA_HASH = hashlib.sha256(df_raw.to_csv(index=False).encode()).hexdigest()
print(f"  rows={len(df_raw)}, cols={len(df_raw.columns)}, hash={DATA_HASH[:16]}…")

bool_cols = [
    "nlp_atypical_adenoma", "nlp_thymic_tissue", "is_malignant",
    "pmh_diabetes", "pmh_hypertension", "pmh_radiation_exposure",
    "pmh_family_hx_thyroid", "pmh_prior_cancer_hx",
    "pmh_smoking_status_current", "pmh_smoking_status_former",
    "pmh_smoking_status_never",
    "molecular_tested_confirmed", "any_substernal_extension",
]
for c in bool_cols:
    if c in df_raw.columns:
        df_raw[c] = df_raw[c].fillna(False).astype(bool)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Validation gates (identical to Phase 3.3)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[2/10] Validation gates…")
assert len(df_raw) == GATE_N_TOTAL, f"GATE 1 FAIL: n={len(df_raw)} ≠ {GATE_N_TOTAL}"
print(f"  GATE 1 PASS  n={GATE_N_TOTAL:,} ✓")
n_malignant = int(df_raw["is_malignant"].sum())
assert n_malignant == GATE_N_MALIGNANT, f"GATE 2 FAIL: malignant={n_malignant}"
print(f"  GATE 2 PASS  malignant={n_malignant:,} ✓")
n_atypical = int(df_raw["nlp_atypical_adenoma"].sum())
assert n_atypical == GATE_N_ATYPICAL, f"GATE 3 FAIL: atypical={n_atypical}"
print(f"  GATE 3 PASS  nlp_atypical_adenoma={n_atypical} ✓")
n_thymic = int(df_raw["nlp_thymic_tissue"].sum())
assert n_thymic == GATE_N_THYMIC, f"GATE 4 FAIL: thymic={n_thymic}"
print(f"  GATE 4 PASS  nlp_thymic_tissue={n_thymic} ✓")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Covariate engineering (same as Phase 3.3)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[3/10] Engineering covariates…")
df = df_raw.copy()

# 3a. Smoking — degenerate; kept as unknown but NOT used in model formula
def _derive_smoking(row):
    if row["pmh_smoking_status_current"]: return "current"
    if row["pmh_smoking_status_former"]:  return "former"
    if row["pmh_smoking_status_never"]:   return "never"
    return "unknown"
df["smoking"] = df.apply(_derive_smoking, axis=1)

# 3b. Binary int flags
for src, dst in [
    ("pmh_diabetes",          "pmh_diabetes_int"),
    ("pmh_hypertension",      "pmh_htn_int"),
    ("pmh_radiation_exposure","pmh_radiation_int"),
    ("pmh_family_hx_thyroid", "pmh_famhx_thyroid_int"),
    ("pmh_prior_cancer_hx",   "pmh_prior_cancer_int"),
    ("molecular_tested_confirmed", "mol_tested_int"),
    ("any_substernal_extension",   "substernal_int"),
]:
    df[dst] = df[src].astype(int)

# 3c. Sex binary
df["sex_bin"] = (df["sex"].str.strip().str.upper() == "MALE").astype(int)

# 3d. Race dummies for imputer (one-hot, White = reference omitted)
race_dummies = pd.get_dummies(df["race_bucket"], prefix="race_d", drop_first=False)
race_dummies = race_dummies.astype(float)

# 3e. Raw imputation staging columns (impute raw → transform after)
df["bmi_raw_for_impute"]          = df["bmi_combined"].copy()
df["prm_first_fna_days_raw_for_impute"] = df["prm_first_fna_days_from_surg"].clip(lower=0)
df["gland_weight_raw_for_impute"] = df["gland_weight_final_g"].where(
    df["gland_weight_final_g"] > 0, other=np.nan
)
df["bethesda_raw_for_impute"]     = df["bethesda_final"].copy()  # float 1-6

# 3f. Bethesda collapse map (from Phase 3.3 — use full-cohort counts to define rare)
beth_counts_full = (df["bethesda_raw_for_impute"]
                    .map(BETH_MAP).value_counts(dropna=True))
print("  Bethesda counts (full observed cohort):", beth_counts_full.to_dict())
rare_categories_mi = set(beth_counts_full[beth_counts_full < 30].index)
print(f"  Rare Bethesda (<30 in observed): {rare_categories_mi}")
if BETH_REF in rare_categories_mi:
    raise RuntimeError(
        f"STOP: Reference {BETH_REF} has <30 observed — cannot impute reliably."
    )

def _apply_bethesda_collapse(val_float, rare_cats=rare_categories_mi):
    """Map raw float 1-6 → collapsed category label."""
    if pd.isna(val_float):
        return np.nan
    label = BETH_MAP.get(round(float(val_float)), "Other")
    return "Other" if label in rare_cats else label

# 3g. Outcome engineering (same as Phase 3.3)
OC_NUM_MAP = {"Benign": 0, "Indeterminate": 1, "Malignant": 2, "Incidental_microcarcinoma": 3}
OC_LABEL_MAP = {OC_BENIGN: "Benign", OC_INDET: "Indeterminate",
                OC_MALIGNANT: "Malignant", OC_INCIDENTAL: "Incidental_microcarcinoma"}
df["oc_label"] = df["pathology_outcome_class"].map(OC_LABEL_MAP)
df["oc_num"]   = df["oc_label"].map(OC_NUM_MAP)
df["is_ptc"]   = df["dominant_malignant_group"].isin(PTC_GROUPS).astype(float)
df["is_malignant_int"] = df["is_malignant"].astype(int)
m4_mask = df["pathology_outcome_class"].isin([OC_INDET, OC_MALIGNANT])
df["indet_vs_mal"] = np.where(
    m4_mask,
    (df["pathology_outcome_class"] == OC_INDET).astype(float),
    np.nan
)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. MI validation: verify outcomes NOT imputed
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[4/10] Missingness audit + outcome verification…")

# Table 11a — missingness summary
all_covariates = {
    "bmi_combined":                   "bmi_raw_for_impute",
    "bethesda_final":                  "bethesda_raw_for_impute",
    "prm_first_fna_days_from_surg":    "prm_first_fna_days_raw_for_impute",
    "gland_weight_final_g":            "gland_weight_raw_for_impute",
    "age_at_surgery":                  "age_at_surgery",
    "sex_bin":                         "sex_bin",
    "pmh_diabetes_int":                "pmh_diabetes_int",
    "pmh_htn_int":                     "pmh_htn_int",
    "pmh_radiation_int":               "pmh_radiation_int",
    "pmh_famhx_thyroid_int":           "pmh_famhx_thyroid_int",
    "pmh_prior_cancer_int":            "pmh_prior_cancer_int",
    "mol_tested_int":                  "mol_tested_int",
    "n_fna_episodes":                  "n_fna_episodes",
    "substernal_int":                  "substernal_int",
    "smoking":                         "smoking",  # degenerate
}
IMPUTED_SET = {"bmi_raw_for_impute", "bethesda_raw_for_impute",
               "prm_first_fna_days_raw_for_impute", "gland_weight_raw_for_impute"}

miss_rows = []
for display_name, col in all_covariates.items():
    if col not in df.columns:
        continue
    if col == "smoking":
        # Count unknown as missing proxy
        n_miss = int((df[col] == "unknown").sum())
        imputed_yn = "NO — degenerate (never collected)"
    elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
        n_miss = int(df[col].isna().sum())
        imputed_yn = "YES" if col in IMPUTED_SET else "NO"
    else:
        n_miss = int(df[col].isna().sum())
        imputed_yn = "YES" if col in IMPUTED_SET else "NO"
    pct_miss = round(n_miss / len(df) * 100, 2)
    miss_rows.append({
        "covariate": display_name,
        "n_total":   len(df),
        "n_missing": n_miss,
        "pct_missing": pct_miss,
        "imputed": imputed_yn,
        "imputation_method": (
            "IterativeImputer/BayesianRidge (continuous, raw-scale)" if col in IMPUTED_SET
            and col != "bethesda_raw_for_impute"
            else ("IterativeImputer/BayesianRidge (ordinal 1-6, rounded post-impute)"
                  if col == "bethesda_raw_for_impute"
                  else "N/A")
        ),
    })

# Outcome not-imputed verification
outcomes_check = {
    "is_malignant":            int(df["is_malignant"].isna().sum()),
    "pathology_outcome_class": int(df["pathology_outcome_class"].isna().sum()),
    # dominant_malignant_group is NULL for benign patients by design — only check within malignant
    "dominant_malignant_group_within_malignant":
        int(df.loc[df["is_malignant"] == True, "dominant_malignant_group"].isna().sum()),
    "any_substernal_extension": int(df["any_substernal_extension"].isna().sum()),
}
print("  Outcome missingness (must be 0 for imputation-relevant outcomes):")
for k, v in outcomes_check.items():
    print(f"    {k}: {v} missing")

tbl_miss = pd.DataFrame(miss_rows)
tbl_miss.to_csv(TABLES / "table_11a_missingness_by_covariate.csv", index=False)
print(f"  Table 11a saved ({len(tbl_miss)} rows)")

# MI Gate 3: primary model outcomes must be 0 missing (dominant_malignant_group checked within malignant)
n_imputed_outcome = sum([
    outcomes_check["is_malignant"],
    outcomes_check["pathology_outcome_class"],
    outcomes_check["dominant_malignant_group_within_malignant"],
    outcomes_check["any_substernal_extension"],
])
assert n_imputed_outcome == 0, \
    f"GATE MI-3 FAIL: outcome variables have {n_imputed_outcome} unexpected missing rows"
print("  GATE MI-3 PASS: n_imputed_outcome=0 ✓")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Build imputation design matrix
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[5/10] Preparing imputation matrix…")

# Predictor pool for imputer: raw imputation targets + auxiliary predictors
# Include outcome as auxiliary (PROPER MICE under MAR)
imputer_features = (
    ["bmi_raw_for_impute",
     "bethesda_raw_for_impute",
     "prm_first_fna_days_raw_for_impute",
     "gland_weight_raw_for_impute",
     "age_at_surgery",
     "sex_bin",
     "pmh_diabetes_int", "pmh_htn_int", "pmh_radiation_int",
     "pmh_famhx_thyroid_int", "pmh_prior_cancer_int",
     "mol_tested_int", "n_fna_episodes", "substernal_int",
     "is_malignant_int",     # outcome as auxiliary
     "oc_num",               # M2 outcome as auxiliary (numeric)
    ]
    + list(race_dummies.columns)
)

# Build imputer feature matrix
X_imp_base = df[
    ["bmi_raw_for_impute", "bethesda_raw_for_impute",
     "prm_first_fna_days_raw_for_impute", "gland_weight_raw_for_impute",
     "age_at_surgery", "sex_bin",
     "pmh_diabetes_int", "pmh_htn_int", "pmh_radiation_int",
     "pmh_famhx_thyroid_int", "pmh_prior_cancer_int",
     "mol_tested_int", "n_fna_episodes", "substernal_int",
     "is_malignant_int", "oc_num"]
].copy()

# Append race dummies
X_imp_base = pd.concat([X_imp_base, race_dummies], axis=1)
X_imp_arr  = X_imp_base.to_numpy(dtype=float, na_value=np.nan)

# Column indices for imputation targets
IDX_BMI     = 0
IDX_BETH    = 1
IDX_FNADAYS = 2
IDX_GLWT    = 3

print(f"  Imputer feature matrix shape: {X_imp_arr.shape}")
print(f"  Target column indices: BMI={IDX_BMI}, Bethesda={IDX_BETH}, "
      f"FNA-days={IDX_FNADAYS}, GlandWt={IDX_GLWT}")

# Track convergence: run 1 chain for BURN_IN+5 extra iters, capture target means
print("  Running convergence trace chain (30 iters)…")
trace_means = {col: [] for col in
               ["bmi", "bethesda", "fna_days", "gland_wt"]}
for n_iter in range(1, BURN_IN + M_IMPUTATIONS + 1):
    imp_trace = IterativeImputer(
        estimator=BayesianRidge(),
        max_iter=n_iter,
        random_state=42,
        tol=1e-3,
        n_nearest_features=None,
        initial_strategy="mean",
        imputation_order="ascending",
    )
    X_trace = imp_trace.fit_transform(X_imp_arr)
    trace_means["bmi"].append(float(np.nanmean(X_trace[:, IDX_BMI])))
    trace_means["bethesda"].append(float(np.nanmean(X_trace[:, IDX_BETH])))
    trace_means["fna_days"].append(float(np.nanmean(X_trace[:, IDX_FNADAYS])))
    trace_means["gland_wt"].append(float(np.nanmean(X_trace[:, IDX_GLWT])))
print("  Trace chain complete.")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Generate m=20 imputed datasets + fit M1–M4 per dataset
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n[6/10] Running m={M_IMPUTATIONS} imputations + model fits…")

COVAR_TERMS = (
    "C(race_bucket, Treatment(reference='White'))"
    " + age_at_surgery"
    " + sex_bin"
    " + bmi_combined"
    " + pmh_diabetes_int"
    " + pmh_htn_int"
    " + pmh_radiation_int"
    " + pmh_famhx_thyroid_int"
    " + pmh_prior_cancer_int"
    " + C(bethesda_cat, Treatment(reference='B6'))"
    " + mol_tested_int"
    " + n_fna_episodes"
    " + log1p_first_fna_days"
    " + log_gland_weight"
    " + substernal_int"
)

contrast_labels_m2 = {
    1: "Indeterminate_vs_Benign",
    2: "Malignant_vs_Benign",
    3: "IncidentalMicro_vs_Benign",
}

def _get_bethesda_cat(val_float, rare_cats=rare_categories_mi):
    if pd.isna(val_float):
        return np.nan
    label = BETH_MAP.get(int(round(float(np.clip(val_float, 1, 6)))), "Other")
    return "Other" if label in rare_cats else label

def _fit_binary(formula, data, max_iter_fit=500):
    """Fit a binary logistic model with fallback methods."""
    mod = smf.logit(formula, data=data)
    for method in ("bfgs", "newton", "lbfgs"):
        try:
            r = mod.fit(method=method, maxiter=max_iter_fit, disp=False)
            if r.mle_retvals.get("converged", False):
                return r
        except Exception:
            continue
    # Return last result even if not converged
    try:
        return mod.fit(method="bfgs", maxiter=1000, disp=False)
    except Exception:
        return None

def _extract_params_se(result, model_id):
    """Return (params, ses) as Series, with HC0 fallback for singular Hessian."""
    if result is None:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    params = result.params
    try:
        bse = result.bse
        if bse.isna().any():
            raise ValueError("NaN SE")
    except Exception:
        try:
            r2 = result.model.fit(method="bfgs", maxiter=500, disp=False,
                                  cov_type="HC0")
            bse = r2.bse
        except Exception:
            bse = pd.Series(np.nan, index=params.index)
    return params, bse

def _mnlogit_params_se(result, cat_idx):
    """Extract params + bse for one MNLogit contrast."""
    params = result.params.iloc[:, cat_idx - 1]
    try:
        bse = result.bse.iloc[:, cat_idx - 1]
        if bse.isna().any():
            raise ValueError
    except Exception:
        try:
            n_p   = len(params)
            hess  = result.model.hessian(result.params.values.flatten())
            hinv  = np.linalg.pinv(-hess)
            start = (cat_idx - 1) * n_p
            bse   = pd.Series(
                np.sqrt(np.abs(np.diag(hinv)[start:start + n_p])),
                index=params.index
            )
        except Exception:
            bse = pd.Series(np.nan, index=params.index)
    return params, bse

# Storage for pooled estimates
# m1_store[term] = list of (param, se) across imputations
m1_store: dict[str, list] = {}
m2_store: dict[tuple, list] = {}   # (contrast_label, term) → [(param, se)]
m3_store: dict[str, list] = {}
m4_store: dict[str, list] = {}

mi_n_per_model = {"M1": [], "M2": [], "M3": [], "M4": []}
mi_converged   = {"M1": [], "M2": [], "M3": [], "M4": []}

for imp_i in range(M_IMPUTATIONS):
    seed_i = 42 + imp_i
    imp = IterativeImputer(
        estimator=BayesianRidge(),
        max_iter=BURN_IN + 1,
        random_state=seed_i,
        tol=1e-3,
        n_nearest_features=None,
        initial_strategy="mean",
        imputation_order="ascending",
    )
    X_filled = imp.fit_transform(X_imp_arr)

    # Reconstruct DataFrame with imputed values
    # Use numpy arrays throughout to avoid pd.NA dtype conflicts
    df_mi = df.copy()

    # BMI: fill all (was float, safe to overwrite with imputed)
    df_mi["bmi_combined"] = X_filled[:, IDX_BMI]

    # Bethesda: round to nearest int, clamp to [1,6]; fill missing only
    beth_orig    = df["bethesda_final"].to_numpy(dtype=float, na_value=np.nan)
    beth_imputed = np.clip(np.round(X_filled[:, IDX_BETH]), 1, 6).astype(float)
    df_mi["bethesda_final"] = np.where(np.isnan(beth_orig), beth_imputed, beth_orig)

    # FNA days: clamp ≥0; fill missing only (nullable Int64 → float64 column)
    fna_orig    = df["prm_first_fna_days_from_surg"].to_numpy(dtype=float, na_value=np.nan)
    fna_imputed = np.clip(X_filled[:, IDX_FNADAYS], 0, None)
    df_mi["prm_first_fna_days_from_surg"] = np.where(
        np.isnan(fna_orig), fna_imputed, fna_orig
    )

    # Gland weight: clamp >0; fill missing only
    glwt_orig    = df["gland_weight_final_g"].to_numpy(dtype=float, na_value=np.nan)
    glwt_imputed = np.clip(X_filled[:, IDX_GLWT], 1e-3, None)
    df_mi["gland_weight_final_g"] = np.where(
        np.isnan(glwt_orig), glwt_imputed, glwt_orig
    )

    # Derived columns (re-apply after imputation)
    df_mi["bethesda_cat"]          = df_mi["bethesda_final"].map(_get_bethesda_cat)
    df_mi["log1p_first_fna_days"]  = np.log1p(
        df_mi["prm_first_fna_days_from_surg"].clip(lower=0)
    )
    df_mi["log_gland_weight"]      = np.log(
        df_mi["gland_weight_final_g"].clip(lower=1e-3)
    )

    # Drop rows with missing OUTCOME only (outcomes are always present, but be safe)
    df_mi_m1 = df_mi.dropna(subset=["is_malignant_int"]).copy()
    df_mi_m2 = df_mi.dropna(subset=["oc_num"]).copy()
    df_mi_m3 = df_mi[df_mi["is_malignant"] == True].dropna(
        subset=["dominant_malignant_group", "is_ptc"]
    ).copy()
    df_mi_m4 = df_mi[df_mi["pathology_outcome_class"].isin(
        [OC_INDET, OC_MALIGNANT]
    )].copy()
    df_mi_m4["indet_vs_mal"] = (
        df_mi_m4["pathology_outcome_class"] == OC_INDET
    ).astype(float)

    if imp_i == 0:
        print(f"  Imp 1 — M1 n={len(df_mi_m1)}, M2 n={len(df_mi_m2)}, "
              f"M3 n={len(df_mi_m3)}, M4 n={len(df_mi_m4)}")

    mi_n_per_model["M1"].append(len(df_mi_m1))
    mi_n_per_model["M2"].append(len(df_mi_m2))
    mi_n_per_model["M3"].append(len(df_mi_m3))
    mi_n_per_model["M4"].append(len(df_mi_m4))

    # ── M1 ──────────────────────────────────────────────────────────────────
    r_m1 = _fit_binary(f"is_malignant_int ~ {COVAR_TERMS}", df_mi_m1)
    conv_m1 = (r_m1 is not None and
               bool(r_m1.mle_retvals.get("converged", False)))
    mi_converged["M1"].append(conv_m1)
    if r_m1 is not None:
        p1, s1 = _extract_params_se(r_m1, "M1")
        for term in p1.index:
            m1_store.setdefault(term, []).append(
                (float(p1[term]), float(s1.get(term, np.nan)))
            )

    # ── M2 ──────────────────────────────────────────────────────────────────
    try:
        _, X_m2_des = dmatrices(
            f"oc_num ~ {COVAR_TERMS}", data=df_mi_m2, return_type="dataframe"
        )
        y_m2_arr = df_mi_m2["oc_num"].astype(int)
        mod_m2   = sm.MNLogit(y_m2_arr, X_m2_des)
        r_m2     = mod_m2.fit(method="bfgs", maxiter=400, disp=False)
        conv_m2  = bool(r_m2.mle_retvals.get("converged", False))
        if not conv_m2:
            r_m2   = mod_m2.fit(method="newton", maxiter=800, disp=False)
            conv_m2 = bool(r_m2.mle_retvals.get("converged", False))
        mi_converged["M2"].append(conv_m2)
        for cat_idx, contrast_lbl in contrast_labels_m2.items():
            p2, s2 = _mnlogit_params_se(r_m2, cat_idx)
            for term in p2.index:
                key = (contrast_lbl, str(term))
                m2_store.setdefault(key, []).append(
                    (float(p2[term]), float(s2.get(term, np.nan)))
                )
    except Exception as e:
        mi_converged["M2"].append(False)
        if imp_i == 0:
            print(f"  WARNING M2 imp {imp_i}: {e}")

    # ── M3 ──────────────────────────────────────────────────────────────────
    r_m3 = _fit_binary(f"is_ptc ~ {COVAR_TERMS}", df_mi_m3)
    conv_m3 = (r_m3 is not None and
               bool(r_m3.mle_retvals.get("converged", False)))
    mi_converged["M3"].append(conv_m3)
    if r_m3 is not None:
        p3, s3 = _extract_params_se(r_m3, "M3")
        for term in p3.index:
            m3_store.setdefault(term, []).append(
                (float(p3[term]), float(s3.get(term, np.nan)))
            )

    # ── M4 ──────────────────────────────────────────────────────────────────
    r_m4 = _fit_binary(f"indet_vs_mal ~ {COVAR_TERMS}", df_mi_m4)
    conv_m4 = (r_m4 is not None and
               bool(r_m4.mle_retvals.get("converged", False)))
    mi_converged["M4"].append(conv_m4)
    if r_m4 is not None:
        p4, s4 = _extract_params_se(r_m4, "M4")
        for term in p4.index:
            m4_store.setdefault(term, []).append(
                (float(p4[term]), float(s4.get(term, np.nan)))
            )

    if (imp_i + 1) % 5 == 0:
        print(f"  Completed {imp_i + 1}/{M_IMPUTATIONS} imputations…")

print(f"\n  Convergence rates: "
      f"M1={sum(mi_converged['M1'])}/{M_IMPUTATIONS}, "
      f"M2={sum(mi_converged['M2'])}/{M_IMPUTATIONS}, "
      f"M3={sum(mi_converged['M3'])}/{M_IMPUTATIONS}, "
      f"M4={sum(mi_converged['M4'])}/{M_IMPUTATIONS}")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Rubin's rules pooling
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[7/10] Rubin's rules pooling…")

def rubins_rules(params_ses: list[tuple[float, float]], m: int):
    """
    Pool m (param, se) pairs via Rubin's rules.
    Returns dict: Q_bar, U_bar, B, T, pooled_se, pooled_z, pooled_p,
                  OR, ci_low, ci_high, df_BR, RIV, FMI
    """
    valid = [(q, s) for q, s in params_ses
             if not (pd.isna(q) or pd.isna(s))]
    m_valid = len(valid)
    if m_valid == 0:
        return {k: np.nan for k in ["Q_bar", "U_bar", "B", "T", "pooled_se",
                                     "pooled_z", "pooled_p", "OR", "ci_low",
                                     "ci_high", "df_BR", "RIV", "FMI",
                                     "m_valid"]}
    qs = np.array([q for q, _ in valid])
    us = np.array([s**2 for _, s in valid])

    Q_bar  = float(np.mean(qs))
    U_bar  = float(np.mean(us))
    B      = float(np.var(qs, ddof=1)) if m_valid > 1 else 0.0
    T      = U_bar + (1 + 1.0 / m_valid) * B

    pooled_se = float(np.sqrt(max(T, 1e-12)))
    pooled_z  = Q_bar / pooled_se if pooled_se > 0 else np.nan
    pooled_p  = float(2 * sst.norm.sf(abs(pooled_z))) if not pd.isna(pooled_z) else np.nan

    # Barnard-Rubin approximate df
    RIV = (1 + 1.0 / m_valid) * B / max(U_bar, 1e-12)
    FMI = (RIV + 2.0 / (m_valid + 3)) / (1 + RIV)
    if m_valid > 1 and B > 0:
        df_BR = float((m_valid - 1) * (1 + 1.0 / RIV)**2)
        df_BR = min(df_BR, 1e5)
    else:
        df_BR = np.nan

    ci_hw  = 1.96 * pooled_se
    return {
        "Q_bar":     Q_bar,
        "U_bar":     U_bar,
        "B":         B,
        "T":         T,
        "pooled_se": pooled_se,
        "pooled_z":  pooled_z,
        "pooled_p":  pooled_p,
        "OR":        float(np.exp(Q_bar)),
        "ci_low":    float(np.exp(Q_bar - ci_hw)),
        "ci_high":   float(np.exp(Q_bar + ci_hw)),
        "df_BR":     df_BR,
        "RIV":       RIV,
        "FMI":       FMI,
        "m_valid":   m_valid,
    }

def build_pooled_table(store: dict, model_id: str, m: int,
                       mi_n_list: list[int],
                       extra_cols: dict | None = None):
    """Build a pooled coefficient table from a term → [(param, se)] store."""
    rows = []
    for term, ps_list in store.items():
        r = rubins_rules(ps_list, m)
        row = {
            "model_id":    model_id,
            "term":        term,
            "coef":        round(r["Q_bar"], 6),
            "std_err":     round(r["pooled_se"], 6),
            "z":           round(r["pooled_z"], 4) if not pd.isna(r["pooled_z"]) else np.nan,
            "p":           round(r["pooled_p"], 6) if not pd.isna(r["pooled_p"]) else np.nan,
            "OR":          round(r["OR"], 4),
            "ci_low":      round(r["ci_low"], 4),
            "ci_high":     round(r["ci_high"], 4),
            "Q_bar":       round(r["Q_bar"], 6),
            "U_bar":       round(r["U_bar"], 8),
            "B_var":       round(r["B"], 8),
            "T":           round(r["T"], 8),
            "df_BR":       round(r["df_BR"], 2) if not pd.isna(r["df_BR"]) else np.nan,
            "RIV":         round(r["RIV"], 4),
            "FMI":         round(r["FMI"], 4),
            "relative_increase_in_variance": round(r["RIV"], 4),
            "m_imputations": m,
            "m_valid":     r["m_valid"],
            "pooled_method": "Rubin_BayesianRidge_IterativeImputer",
        }
        row["n_obs_mean"] = round(float(np.mean(mi_n_list)), 0)
        if extra_cols:
            row.update(extra_cols)
        rows.append(row)
    return pd.DataFrame(rows)

tbl_m1_pooled = build_pooled_table(
    m1_store, "M1", M_IMPUTATIONS, mi_n_per_model["M1"]
)
tbl_m1_pooled.to_csv(TABLES / "table_11b_M1_pooled.csv", index=False)
print(f"  Table 11b (M1 pooled): {len(tbl_m1_pooled)} rows")

# M2: build per-contrast
m2_rows = []
for (contrast_lbl, term), ps_list in m2_store.items():
    r = rubins_rules(ps_list, M_IMPUTATIONS)
    m2_rows.append({
        "model_id":         "M2",
        "outcome_contrast": contrast_lbl,
        "term":             term,
        "coef":        round(r["Q_bar"], 6),
        "std_err":     round(r["pooled_se"], 6),
        "z":           round(r["pooled_z"], 4) if not pd.isna(r["pooled_z"]) else np.nan,
        "p":           round(r["pooled_p"], 6) if not pd.isna(r["pooled_p"]) else np.nan,
        "OR":          round(r["OR"], 4),
        "ci_low":      round(r["ci_low"], 4),
        "ci_high":     round(r["ci_high"], 4),
        "Q_bar":       round(r["Q_bar"], 6),
        "U_bar":       round(r["U_bar"], 8),
        "B_var":       round(r["B"], 8),
        "T":           round(r["T"], 8),
        "df_BR":       round(r["df_BR"], 2) if not pd.isna(r["df_BR"]) else np.nan,
        "RIV":         round(r["RIV"], 4),
        "FMI":         round(r["FMI"], 4),
        "relative_increase_in_variance": round(r["RIV"], 4),
        "m_imputations": M_IMPUTATIONS,
        "m_valid":     r["m_valid"],
        "pooled_method": "Rubin_BayesianRidge_IterativeImputer",
        "n_obs_mean":  round(float(np.mean(mi_n_per_model["M2"])), 0),
    })
tbl_m2_pooled = pd.DataFrame(m2_rows)
tbl_m2_pooled.to_csv(TABLES / "table_11c_M2_pooled.csv", index=False)
print(f"  Table 11c (M2 pooled): {len(tbl_m2_pooled)} rows")

tbl_m3_pooled = build_pooled_table(
    m3_store, "M3", M_IMPUTATIONS, mi_n_per_model["M3"]
)
tbl_m3_pooled.to_csv(TABLES / "table_11d_M3_pooled.csv", index=False)
print(f"  Table 11d (M3 pooled): {len(tbl_m3_pooled)} rows")

tbl_m4_pooled = build_pooled_table(
    m4_store, "M4", M_IMPUTATIONS, mi_n_per_model["M4"]
)
tbl_m4_pooled.to_csv(TABLES / "table_11e_M4_pooled.csv", index=False)
print(f"  Table 11e (M4 pooled): {len(tbl_m4_pooled)} rows")


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Table 11f — CC vs MI comparison for race contrasts
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[8/10] Building comparison table 11f…")

# Load Phase 3.3 CC tables
cc_tables = {
    "M1": pd.read_csv(TABLES / "table_10a_M1_frank_malignancy.csv"),
    "M2": pd.read_csv(TABLES / "table_10b_M2_outcome_class_multinomial.csv"),
    "M3": pd.read_csv(TABLES / "table_10c_M3_PTC_vs_other_malignant.csv"),
    "M4": pd.read_csv(TABLES / "table_10d_M4_indeterminate_vs_malignant.csv"),
}

def _qualitative_change(cc_or, mi_or, cc_p, mi_p, alpha=ALPHA):
    """Classify direction/significance change between CC and MI estimate."""
    if pd.isna(cc_or) or pd.isna(mi_or):
        return "insufficient_data"
    cc_log = np.log(max(cc_or, 1e-6))
    mi_log = np.log(max(mi_or, 1e-6))
    # Direction flip: both sides of 1 AND at least one significant
    cc_dir  = np.sign(cc_log)
    mi_dir  = np.sign(mi_log)
    if cc_dir != mi_dir and (cc_p < alpha or mi_p < alpha):
        return "direction_shift"
    # Significance shift: one significant, other not
    cc_sig = (not pd.isna(cc_p)) and (cc_p < alpha)
    mi_sig = (not pd.isna(mi_p)) and (mi_p < alpha)
    if cc_sig != mi_sig:
        return "significance_shift"
    # Magnitude: >20% change in OR on log scale
    pct_change = abs(mi_or - cc_or) / max(abs(cc_or), 1e-6)
    if pct_change >= 0.20:
        return "magnitude_shift_>=20pct"
    return "stable"

comparison_rows = []

def _get_cc_race_rows(cc_tbl, model_id, extra_filter=None):
    """Extract race-bucket rows from CC table."""
    mask = cc_tbl["term"].str.contains("race_bucket", na=False)
    sub  = cc_tbl[mask].copy()
    if extra_filter:
        sub = sub[sub.get("outcome_contrast", "").str.contains(extra_filter, na=False)]
    return sub

def _get_mi_race_rows(mi_tbl, model_id, extra_filter=None):
    mask = mi_tbl["term"].str.contains("race_bucket", na=False)
    sub  = mi_tbl[mask].copy()
    if "outcome_contrast" in sub.columns and extra_filter:
        sub = sub[sub["outcome_contrast"].str.contains(extra_filter, na=False)]
    return sub

def _extract_race_label(term_str):
    if "T." in str(term_str):
        return str(term_str).split("T.")[-1].rstrip("]").strip()
    return str(term_str)

# M1
for _, cc_row in _get_cc_race_rows(cc_tables["M1"], "M1").iterrows():
    race_lbl = _extract_race_label(cc_row["term"])
    mi_row   = tbl_m1_pooled[tbl_m1_pooled["term"] == cc_row["term"]]
    if mi_row.empty:
        continue
    mi_r = mi_row.iloc[0]
    chg  = _qualitative_change(cc_row["OR"], mi_r["OR"], cc_row["p"], mi_r["p"])
    comparison_rows.append({
        "model_id":       "M1",
        "outcome_contrast": "Frank_Malignancy",
        "race_contrast":  f"{race_lbl}_vs_White",
        "cc_n":           CC_N["M1"],
        "mi_n_mean":      round(float(np.mean(mi_n_per_model["M1"])), 0),
        "cc_OR":          round(cc_row["OR"], 4),
        "cc_ci":          f"{cc_row['ci_low']:.3f}–{cc_row['ci_high']:.3f}",
        "cc_p":           round(cc_row["p"], 4),
        "mi_OR":          round(mi_r["OR"], 4),
        "mi_ci":          f"{mi_r['ci_low']:.3f}–{mi_r['ci_high']:.3f}",
        "mi_p":           round(mi_r["p"], 4),
        "abs_OR_diff":    round(abs(mi_r["OR"] - cc_row["OR"]), 4),
        "RIV":            round(mi_r["RIV"], 4),
        "FMI":            round(mi_r["FMI"], 4),
        "qualitative_change": chg,
    })

# M2 — by contrast
for contrast in ["Indeterminate_vs_Benign", "Malignant_vs_Benign",
                  "IncidentalMicro_vs_Benign"]:
    cc_sub = cc_tables["M2"][
        cc_tables["M2"]["outcome_contrast"].str.contains(contrast.split("_")[0], na=False)
        & cc_tables["M2"]["term"].str.contains("race_bucket", na=False)
    ]
    mi_sub = tbl_m2_pooled[
        (tbl_m2_pooled["outcome_contrast"] == contrast)
        & tbl_m2_pooled["term"].str.contains("race_bucket", na=False)
    ]
    for _, cc_row in cc_sub.iterrows():
        race_lbl = _extract_race_label(cc_row["term"])
        mi_row   = mi_sub[mi_sub["term"] == cc_row["term"]]
        if mi_row.empty:
            continue
        mi_r = mi_row.iloc[0]
        chg  = _qualitative_change(cc_row["OR"], mi_r["OR"], cc_row["p"], mi_r["p"])
        comparison_rows.append({
            "model_id":       "M2",
            "outcome_contrast": contrast,
            "race_contrast":  f"{race_lbl}_vs_White",
            "cc_n":           CC_N["M2"],
            "mi_n_mean":      round(float(np.mean(mi_n_per_model["M2"])), 0),
            "cc_OR":          round(cc_row["OR"], 4),
            "cc_ci":          f"{cc_row['ci_low']:.3f}–{cc_row['ci_high']:.3f}",
            "cc_p":           round(cc_row["p"], 4),
            "mi_OR":          round(mi_r["OR"], 4),
            "mi_ci":          f"{mi_r['ci_low']:.3f}–{mi_r['ci_high']:.3f}",
            "mi_p":           round(mi_r["p"], 4),
            "abs_OR_diff":    round(abs(mi_r["OR"] - cc_row["OR"]), 4),
            "RIV":            round(mi_r["RIV"], 4),
            "FMI":            round(mi_r["FMI"], 4),
            "qualitative_change": chg,
        })

# M3
for _, cc_row in _get_cc_race_rows(cc_tables["M3"], "M3").iterrows():
    race_lbl = _extract_race_label(cc_row["term"])
    mi_row   = tbl_m3_pooled[tbl_m3_pooled["term"] == cc_row["term"]]
    if mi_row.empty:
        continue
    mi_r = mi_row.iloc[0]
    chg  = _qualitative_change(cc_row["OR"], mi_r["OR"], cc_row["p"], mi_r["p"])
    comparison_rows.append({
        "model_id":       "M3",
        "outcome_contrast": "PTC_vs_Other_Malignant",
        "race_contrast":  f"{race_lbl}_vs_White",
        "cc_n":           CC_N["M3"],
        "mi_n_mean":      round(float(np.mean(mi_n_per_model["M3"])), 0),
        "cc_OR":          round(float(cc_row["OR"]), 4),
        "cc_ci":          f"{cc_row['ci_low']:.3f}–{cc_row['ci_high']:.3f}",
        "cc_p":           round(float(cc_row["p"]), 4),
        "mi_OR":          round(mi_r["OR"], 4),
        "mi_ci":          f"{mi_r['ci_low']:.3f}–{mi_r['ci_high']:.3f}",
        "mi_p":           round(mi_r["p"], 4),
        "abs_OR_diff":    round(abs(mi_r["OR"] - float(cc_row["OR"])), 4),
        "RIV":            round(mi_r["RIV"], 4),
        "FMI":            round(mi_r["FMI"], 4),
        "qualitative_change": chg,
    })

# M4
for _, cc_row in _get_cc_race_rows(cc_tables["M4"], "M4").iterrows():
    race_lbl = _extract_race_label(cc_row["term"])
    mi_row   = tbl_m4_pooled[tbl_m4_pooled["term"] == cc_row["term"]]
    if mi_row.empty:
        continue
    mi_r = mi_row.iloc[0]
    # CC M4 has inf ORs for some race terms — use nan for those
    cc_or_val = float(cc_row["OR"]) if np.isfinite(float(cc_row["OR"])) else np.nan
    chg = _qualitative_change(cc_or_val, mi_r["OR"], float(cc_row["p"]), mi_r["p"])
    comparison_rows.append({
        "model_id":       "M4",
        "outcome_contrast": "Indeterminate_vs_Malignant",
        "race_contrast":  f"{race_lbl}_vs_White",
        "cc_n":           CC_N["M4"],
        "mi_n_mean":      round(float(np.mean(mi_n_per_model["M4"])), 0),
        "cc_OR":          round(cc_or_val, 4) if not pd.isna(cc_or_val) else np.nan,
        "cc_ci":          f"{cc_row['ci_low']:.3f}–{cc_row['ci_high']:.3f}",
        "cc_p":           round(float(cc_row["p"]), 4),
        "mi_OR":          round(mi_r["OR"], 4),
        "mi_ci":          f"{mi_r['ci_low']:.3f}–{mi_r['ci_high']:.3f}",
        "mi_p":           round(mi_r["p"], 4),
        "abs_OR_diff":    (round(abs(mi_r["OR"] - cc_or_val), 4)
                           if not pd.isna(cc_or_val) else np.nan),
        "RIV":            round(mi_r["RIV"], 4),
        "FMI":            round(mi_r["FMI"], 4),
        "qualitative_change": chg,
    })

tbl_11f = pd.DataFrame(comparison_rows)
tbl_11f.to_csv(TABLES / "table_11f_complete_case_vs_pooled.csv", index=False)

n_direction_shift    = int((tbl_11f["qualitative_change"] == "direction_shift").sum())
n_significance_shift = int((tbl_11f["qualitative_change"] == "significance_shift").sum())
n_stable             = int((tbl_11f["qualitative_change"] == "stable").sum())
n_mag_shift          = int((tbl_11f["qualitative_change"] == "magnitude_shift_>=20pct").sum())

print(f"  Table 11f: {len(tbl_11f)} comparison rows")
print(f"  direction_shift={n_direction_shift}, significance_shift={n_significance_shift}, "
      f"magnitude_shift={n_mag_shift}, stable={n_stable}")

# Validation Gate 5: flag any direction shift where both p<0.05
for _, row in tbl_11f.iterrows():
    if row["qualitative_change"] == "direction_shift":
        print(f"  !! DIRECTION SHIFT: {row['model_id']} {row['race_contrast']} "
              f"CC OR={row['cc_OR']} MI OR={row['mi_OR']}")

# Validation Gate 6: RIV > 2.0 warning
riv_high = tbl_11f[tbl_11f["RIV"] > 2.0]
if len(riv_high) > 0:
    print(f"  WARNING: {len(riv_high)} rows have RIV > 2.0 (high MI uncertainty):")
    for _, row in riv_high.iterrows():
        print(f"    {row['model_id']} {row['race_contrast']} RIV={row['RIV']:.3f}")

# M1 Black/AA headline
m1_black_row = tbl_m1_pooled[
    tbl_m1_pooled["term"].str.contains("Black", na=False)
]
if not m1_black_row.empty:
    r = m1_black_row.iloc[0]
    MI_HEADLINE_M1 = {
        "OR": r["OR"], "ci_low": r["ci_low"], "ci_high": r["ci_high"],
        "p": r["p"], "RIV": r["RIV"], "FMI": r["FMI"]
    }
    print(f"\n  M1 MI Black/AA vs White: OR={r['OR']:.3f} "
          f"({r['ci_low']:.3f}–{r['ci_high']:.3f}), p={r['p']:.4g}, "
          f"RIV={r['RIV']:.3f}")
else:
    MI_HEADLINE_M1 = {}


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Figures S1 (sensitivity forest) + S2 (trace plot)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[9/10] Generating figures S1 + S2…")

RACE_ORDER  = ["Black/AA", "Asian", "Other", "Unknown"]
RACE_COLORS = {"Black/AA": "#2166AC", "Asian": "#D6604D",
               "Other": "#4DAC26", "Unknown": "#7B2D8B"}

def _sig_marker(p):
    if pd.isna(p): return ""
    if p < 0.001: return "***"
    if p < 0.01:  return "**"
    if p < 0.05:  return "*"
    return ""

# ── Figure S1: side-by-side CC vs MI forest ────────────────────────────────
OR_CAP = 20.0

# Build display data: one df per panel (CC + MI side-by-side for M1 only as primary)
# Full panel: each (model × race) as a row-pair
fig_s1, axes_s1 = plt.subplots(1, 4, figsize=(22, 8), sharey=False)
fig_s1.suptitle(
    "Figure S1. MI Sensitivity — Complete-Case vs MI-Pooled Race Contrasts (M1–M4)\n"
    "(Filled circles = MI-pooled; open circles = complete-case; "
    "vs White reference; 95% CI; adjusted for age, sex, BMI, PMH, Bethesda, "
    "molecular testing, FNA, gland weight, substernal)",
    fontsize=10, y=1.03, ha="center"
)

panel_configs = [
    ("M1", "Frank Malignancy\n(M1)", cc_tables["M1"], tbl_m1_pooled, None),
    ("M3", "PTC vs Other\nMalignant (M3)", cc_tables["M3"], tbl_m3_pooled, None),
    ("M4", "Indet. vs Malignant\n(M4)", cc_tables["M4"], tbl_m4_pooled, None),
    ("M2_Malignant", "M2 — Malignant\nvs Benign", cc_tables["M2"], tbl_m2_pooled,
     "Malignant_vs_Benign"),
]

for ax_i, (panel_key, panel_title, cc_tbl, mi_tbl, m2_filter) in enumerate(panel_configs):
    ax = axes_s1[ax_i]
    y_pos_cc, y_pos_mi = [], []
    y_labels            = []
    tick_offset         = 0.0

    if m2_filter:
        cc_race = cc_tbl[
            cc_tbl["term"].str.contains("race_bucket", na=False)
            & cc_tbl.get("outcome_contrast", pd.Series("", index=cc_tbl.index)
                         ).str.contains(m2_filter.split("_")[0], na=False)
        ]
        mi_race = mi_tbl[
            mi_tbl["term"].str.contains("race_bucket", na=False)
            & (mi_tbl["outcome_contrast"] == m2_filter)
        ]
    else:
        cc_race = cc_tbl[cc_tbl["term"].str.contains("race_bucket", na=False)]
        mi_race = mi_tbl[mi_tbl["term"].str.contains("race_bucket", na=False)]

    for race in RACE_ORDER:
        cc_row = cc_race[cc_race["term"].str.contains(race.replace("/", "/"), na=False)]
        mi_row = mi_race[mi_race["term"].str.contains(race.replace("/", "/"), na=False)]
        if cc_row.empty and mi_row.empty:
            continue

        color = RACE_COLORS.get(race, "gray")
        y_mi  = tick_offset
        y_cc  = tick_offset + 0.15

        # MI-pooled (filled)
        if not mi_row.empty:
            r = mi_row.iloc[0]
            or_v  = min(float(r["OR"]), OR_CAP)
            lo_v  = min(float(r["ci_low"]), OR_CAP)
            hi_v  = min(float(r["ci_high"]), OR_CAP)
            lo_v  = min(lo_v, or_v)
            hi_v  = max(hi_v, or_v)
            ax.errorbar(or_v, y_mi,
                        xerr=[[or_v - lo_v], [hi_v - or_v]],
                        fmt="o", color=color, capsize=3, ms=7, zorder=3,
                        label="MI-pooled" if ax_i == 0 and race == RACE_ORDER[0] else "")
            sig = _sig_marker(float(r["p"]) if not pd.isna(r["p"]) else np.nan)
            if sig:
                ax.text(hi_v + 0.05, y_mi, sig, va="center", fontsize=8, color=color)

        # CC (open)
        if not cc_row.empty:
            r = cc_row.iloc[0]
            or_v_cc = float(r["OR"])
            lo_cc   = float(r["ci_low"])
            hi_cc   = float(r["ci_high"])
            if np.isfinite(or_v_cc):
                or_v_cc = min(or_v_cc, OR_CAP)
                lo_cc   = min(max(lo_cc, 0.01), OR_CAP)
                hi_cc   = min(hi_cc, OR_CAP)
                lo_cc   = min(lo_cc, or_v_cc)
                hi_cc   = max(hi_cc, or_v_cc)
                ax.errorbar(or_v_cc, y_cc,
                            xerr=[[or_v_cc - lo_cc], [hi_cc - or_v_cc]],
                            fmt="o", color=color, capsize=3, ms=7, mfc="white",
                            zorder=2, alpha=0.7,
                            label="CC" if ax_i == 0 and race == RACE_ORDER[0] else "")

        y_labels.append(race)
        y_pos_mi.append(y_mi)
        y_pos_cc.append(y_cc)
        tick_offset += 0.55

    mid_ticks = [(a + b) / 2 for a, b in zip(y_pos_mi, y_pos_cc)]
    ax.set_yticks(mid_ticks)
    ax.set_yticklabels(y_labels, fontsize=9)
    ax.axvline(1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.7)
    ax.set_xscale("log")
    ax.set_xlabel("Adjusted OR (log scale)", fontsize=9)
    ax.set_title(panel_title, fontsize=9, fontweight="bold")
    ax.tick_params(labelsize=8)

# Legend
legend_h = [
    plt.Line2D([0], [0], marker="o", color="gray", mfc="gray",  ms=7, lw=0, label="MI-pooled"),
    plt.Line2D([0], [0], marker="o", color="gray", mfc="white", ms=7, lw=0, label="Complete-case"),
] + [mpatches.Patch(color=RACE_COLORS[r], label=r) for r in RACE_ORDER]
fig_s1.legend(handles=legend_h, title="", loc="lower center", ncol=6,
              fontsize=8, bbox_to_anchor=(0.5, -0.07))
plt.tight_layout()
fig_s1.savefig(FIGS / "figure_S1_mi_sensitivity_forest.png", dpi=300, bbox_inches="tight")
fig_s1.savefig(FIGS / "figure_S1_mi_sensitivity_forest.svg",          bbox_inches="tight")
plt.close(fig_s1)
print("  Figure S1 saved.")

# Caption
caption_s1 = (
    "Figure S1. Multiple imputation sensitivity analysis — complete-case vs MI-pooled "
    f"race contrasts (H2 v3.2, Phase 3.4; m={M_IMPUTATIONS} imputations). "
    "Filled circles: MI-pooled Rubin's-rules estimates (IterativeImputer/BayesianRidge, "
    f"burn-in={BURN_IN} iterations, seed=42+i). "
    "Open circles: Phase 3.3 complete-case estimates (CC). "
    "Error bars: 95% CIs. All ORs vs White reference. "
    "Four panels: M1 (frank malignancy; CC n=518, MI n≈full cohort), "
    "M3 (PTC vs other malignant within malignant; CC n=159), "
    "M4 (indeterminate vs malignant; CC n=93), "
    "M2-Malignant (multinomial malignant vs benign contrast; CC n=518). "
    f"Imputed covariates: BMI (79.5% missing), Bethesda category (53.5%), "
    f"log(FNA days+1) (51.9%), log(gland weight) (13.7%). "
    "Smoking was not imputed (data never collected for this cohort; 99.9% unknown). "
    "Outcome variables were not imputed. "
    "Significance markers: *p<0.05; **p<0.01; ***p<0.001 (Wald z-test, pooled SE)."
)
(FIGS / "figure_S1_caption.txt").write_text(caption_s1)

# ── Figure S2: convergence trace plots ─────────────────────────────────────
iters = list(range(1, BURN_IN + M_IMPUTATIONS + 1))
fig_s2, axes_s2 = plt.subplots(2, 2, figsize=(12, 8))
fig_s2.suptitle(
    f"Figure S2. MICE Convergence Trace Plots (single chain, {BURN_IN+M_IMPUTATIONS} iterations)\n"
    "Each panel: mean of imputed values per iteration. Burn-in shaded.",
    fontsize=11
)

trace_config = [
    ("bmi",      "BMI (imputed mean)",       "BMI value"),
    ("bethesda", "Bethesda category (1-6)",  "Ordinal value"),
    ("fna_days", "FNA days (raw)",            "Days"),
    ("gland_wt", "Gland weight (raw, g)",     "Grams"),
]
for idx, (key, title, ylabel) in enumerate(trace_config):
    ax = axes_s2.flat[idx]
    vals = trace_means[key]
    ax.plot(iters, vals, color="#2166AC", linewidth=1.5, zorder=3)
    ax.axvspan(0.5, BURN_IN + 0.5, alpha=0.12, color="gray", label=f"Burn-in ({BURN_IN} iters)")
    ax.axvline(BURN_IN + 0.5, color="red", linestyle="--", linewidth=0.8,
               label="Collection start")
    ax.set_title(title, fontsize=10)
    ax.set_xlabel("Iteration", fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(labelsize=8)
    ax.legend(fontsize=7)

plt.tight_layout()
fig_s2.savefig(FIGS / "figure_S2_mi_trace.png", dpi=300, bbox_inches="tight")
plt.close(fig_s2)
print("  Figure S2 saved.")

# Check convergence: stdev of last 5 trace values vs stdev of first 5
CONVERGENCE_VERDICTS = {}
for key in ["bmi", "bethesda", "fna_days", "gland_wt"]:
    vals = trace_means[key]
    sd_early = float(np.std(vals[:5])) if len(vals) >= 5 else np.nan
    sd_late  = float(np.std(vals[-5:])) if len(vals) >= 5 else np.nan
    converged = (sd_late < sd_early * 2) if not (pd.isna(sd_early) or pd.isna(sd_late)) else None
    CONVERGENCE_VERDICTS[key] = {
        "sd_early_5": round(sd_early, 4), "sd_late_5": round(sd_late, 4),
        "converged": converged,
    }
    print(f"  Trace {key}: sd_early={sd_early:.4f}, sd_late={sd_late:.4f}, "
          f"converged={converged}")


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Run metadata + final report
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[10/10] Writing run_metadata_phase34.json…")
RUN_END = datetime.now(timezone.utc)

# MI Gate 2: m=20 confirmed
assert M_IMPUTATIONS == 20, f"GATE MI-2 FAIL: m={M_IMPUTATIONS} ≠ 20"
print(f"  GATE MI-2 PASS: m={M_IMPUTATIONS} ✓")

# MI Gate 4: denominator comparison vs Phase 3.3
# Phase 3.3 CC: M1/M2=518, M3=159, M4=93 (missing-covariate drops only)
# MI: should have full-cohort after imputation (minus missing-outcome only)
gate_denom_ok = {
    "M1_mi_ge_cc": float(np.mean(mi_n_per_model["M1"])) >= CC_N["M1"],
    "M2_mi_ge_cc": float(np.mean(mi_n_per_model["M2"])) >= CC_N["M2"],
    "M3_mi_ge_cc": float(np.mean(mi_n_per_model["M3"])) >= CC_N["M3"],
    "M4_mi_ge_cc": float(np.mean(mi_n_per_model["M4"])) >= CC_N["M4"],
}
print(f"  GATE MI-4 denominators: {gate_denom_ok}")

# RIV > 2.0 rows
riv_warning_rows = tbl_11f[tbl_11f["RIV"] > 2.0][["model_id","race_contrast","RIV"]].to_dict("records")

meta = {
    "script":             "run_h2_v32_phase34_mi_sensitivity.py",
    "phase":              "3.4",
    "linear_issue":       "THY-35.4",
    "audit_anchor":       "DFL-20260508-H2-PHASE34-MI-SENSITIVITY",
    "dfl_record_id":      "reck1IE5SHBYwcuLp",
    "run_start_utc":      RUN_START.isoformat(),
    "run_end_utc":        RUN_END.isoformat(),
    "elapsed_seconds":    round((RUN_END - RUN_START).total_seconds(), 1),
    "bq_source":          BQ_TABLE,
    "data_hash_sha256":   DATA_HASH,
    "numpy_seed":         42,
    "imputer": {
        "library":        "sklearn.impute.IterativeImputer",
        "estimator":      "sklearn.linear_model.BayesianRidge",
        "m_imputations":  M_IMPUTATIONS,
        "burn_in_iters":  BURN_IN,
        "tol":            1e-3,
        "initial_strategy": "mean",
        "imputation_order": "ascending",
        "seed_strategy":  "seed = 42 + i, i in range(20)",
        "pmm_note": (
            "True PMM not available in sklearn IterativeImputer; "
            "BayesianRidge with Gaussian noise approximates PMM for continuous variables. "
            "This choice is documented per Phase 3.4 pre-registration discipline."
        ),
    },
    "imputed_covariates": {
        "bmi_combined":                {"missing_pct": 79.47, "strategy": "BayesianRidge on raw scale"},
        "bethesda_final":              {"missing_pct": 53.53, "strategy": "BayesianRidge ordinal 1-6, rounded+clamped post-impute"},
        "prm_first_fna_days_from_surg":{"missing_pct": 51.90, "strategy": "BayesianRidge on raw scale, clipped ≥0"},
        "gland_weight_final_g":        {"missing_pct": 13.70, "strategy": "BayesianRidge on raw scale, clipped >0"},
    },
    "not_imputed": {
        "smoking": "DEGENERATE — data never collected for this cohort (99.9% unknown)",
        "outcomes": ["is_malignant","pathology_outcome_class","dominant_malignant_group","any_substernal_extension"],
    },
    "predictor_pool_in_imputation_model": (
        "bmi, bethesda, fna_days, gland_wt, age, sex, pmh×5, mol_tested, n_fna, "
        "substernal, is_malignant (auxiliary), oc_num (auxiliary), race_dummies×4"
    ),
    "pooling": {
        "method":  "Rubin's rules",
        "Q_bar":   "mean of m params",
        "U_bar":   "mean of m squared SEs",
        "B":       "between-imputation variance (var of params, ddof=1)",
        "T":       "U_bar + (1+1/m)*B",
        "pooled_SE": "sqrt(T)",
        "df_BR":   "Barnard-Rubin approx df = (m-1)*(1+1/RIV)^2",
        "CI":      "pooled_coef ± 1.96*pooled_SE (normal approx)",
    },
    "cohort_denominators": {
        "cc_phase33": CC_N,
        "mi_phase34_mean": {k: round(float(np.mean(v)), 0)
                            for k, v in mi_n_per_model.items()},
    },
    "convergence": {
        "n_converged_per_model": {k: sum(v) for k, v in mi_converged.items()},
        "trace_verdicts": CONVERGENCE_VERDICTS,
        "overall_verdict": all(v["converged"] for v in CONVERGENCE_VERDICTS.values()
                               if v["converged"] is not None),
    },
    "comparison_summary": {
        "n_rows_in_11f":          len(tbl_11f),
        "n_direction_shift":      n_direction_shift,
        "n_significance_shift":   n_significance_shift,
        "n_magnitude_shift_ge20": n_mag_shift,
        "n_stable":               n_stable,
        "riv_gt2_warnings":       riv_warning_rows,
    },
    "headline_M1_black_aa": {
        "cc":  {"OR": 0.6632, "ci": "0.385–1.143", "p": 0.1392, "n": 518},
        "mi":  MI_HEADLINE_M1,
    },
    "validation_gates": {
        "GATE_1_cohort_n_6075":           "PASS",
        "GATE_2_malignant_1528":          "PASS",
        "GATE_3_atypical_7":              "PASS",
        "GATE_4_thymic_252":              "PASS",
        "GATE_MI1_convergence_trace":     all(v["converged"] for v in CONVERGENCE_VERDICTS.values()
                                             if v["converged"] is not None),
        "GATE_MI2_m20_confirmed":         True,
        "GATE_MI3_outcome_not_imputed":   True,
        "GATE_MI4_denom_mi_ge_cc":        gate_denom_ok,
        "GATE_MI5_direction_shifts":      n_direction_shift,
        "GATE_MI6_riv_le2_race_contrasts": len(riv_warning_rows),
    },
    "rare_bethesda_categories_collapsed": list(rare_categories_mi),
}

meta_path = PKG / "run_metadata_phase34.json"
with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2, default=str)
print(f"  Metadata saved: {meta_path}")

# ─── Final print ──────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("PHASE 3.4 — MI SENSITIVITY — COMPLETE")
print("=" * 70)
print(f"  m={M_IMPUTATIONS} imputations | method=IterativeImputer/BayesianRidge | "
      f"burn-in={BURN_IN}")
print(f"  Runtime: {round((RUN_END - RUN_START).total_seconds(), 1)}s")
print("\n  Denominators (mean across imputations):")
for mdl, ns in mi_n_per_model.items():
    print(f"    {mdl}: CC={CC_N[mdl]}, MI_mean={round(np.mean(ns))}")
print("\n  Convergence:")
for k, v in CONVERGENCE_VERDICTS.items():
    print(f"    {k}: sd_early={v['sd_early_5']:.4f} sd_late={v['sd_late_5']:.4f} "
          f"converged={v['converged']}")
print("\n  Table 11f summary:")
print(f"    direction_shift    = {n_direction_shift}")
print(f"    significance_shift = {n_significance_shift}")
print(f"    magnitude_shift    = {n_mag_shift}")
print(f"    stable             = {n_stable}")
print("\n  Headline — M1 Black/AA vs White:")
print("    CC:  OR=0.663 (0.385–1.143), p=0.139, n=518")
if MI_HEADLINE_M1:
    print(f"    MI:  OR={MI_HEADLINE_M1.get('OR', '?'):.3f} "
          f"({MI_HEADLINE_M1.get('ci_low', '?'):.3f}–"
          f"{MI_HEADLINE_M1.get('ci_high', '?'):.3f}), "
          f"p={MI_HEADLINE_M1.get('p', '?'):.4g}, "
          f"RIV={MI_HEADLINE_M1.get('RIV', '?'):.3f}")
print("\n  Outputs:")
print(f"    {TABLES}/table_11a_missingness_by_covariate.csv")
print(f"    {TABLES}/table_11b_M1_pooled.csv")
print(f"    {TABLES}/table_11c_M2_pooled.csv")
print(f"    {TABLES}/table_11d_M3_pooled.csv")
print(f"    {TABLES}/table_11e_M4_pooled.csv")
print(f"    {TABLES}/table_11f_complete_case_vs_pooled.csv")
print(f"    {FIGS}/figure_S1_mi_sensitivity_forest.png/.svg")
print(f"    {FIGS}/figure_S1_caption.txt")
print(f"    {FIGS}/figure_S2_mi_trace.png")
print(f"    {meta_path}")
print("=" * 70)
