"""M037 Table 2 — Univariable + Multivariable logistic regression for LN positivity predictors.

Outcome: LN_POSITIVE (binary; from cohort view, derived as LN_POSITIVE_FLAG=1 OR LN_TOTAL_POSITIVE>0).
Predictors:
  Continuous: age_at_surgery, tumor_size_cm_max, ln_total_examined
  Binary: sex(female=ref), histology_group(PTC=ref), t_stage(T1a=ref),
          ete_any (none=ref), braf_positive_final, surgery_total

Univariable: each predictor against LN_POSITIVE separately.
Multivariable: all predictors jointly via statsmodels Logit.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m037_table2_logreg.md")
ctx, cur = get_cursor()

print("=== Pulling M037 cohort ===")
cur.execute("""
SELECT
  RESEARCH_ID, AGE_AT_SURGERY, SEX, RACE, HISTOLOGY_GROUP,
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE,
  TUMOR_SIZE_CM_MAX, ETE_GRADE,
  LN_TOTAL_EXAMINED, LN_TOTAL_POSITIVE, LN_POSITIVE,
  SURG_PROCEDURE_TYPE,
  MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL
FROM COHORT_M037_LN_PREDICTORS
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
import numpy as np
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows; LN+ = {df['LN_POSITIVE'].sum():,}")

# Build feature matrix
df['SEX_MALE'] = (df['SEX'] == 'male').astype(int)
df['HIST_NON_PTC'] = (df['HISTOLOGY_GROUP'] != 'PTC').astype(int)
df['T_HIGH'] = df['AJCC8_T_STAGE'].isin(['T3a', 'T3b', 'T4a', 'T4b']).astype(int)
df['ETE_ANY'] = df['ETE_GRADE'].isin(['microscopic', 'gross', 'present_ungraded']).astype(int)
df['BRAF_POS'] = (df['BRAF_POSITIVE_FINAL'] == True).astype(int)
df['SURG_TOTAL'] = (df['SURG_PROCEDURE_TYPE'] == 'total_thyroidectomy').astype(int)

# Numeric coercions
for c in ['AGE_AT_SURGERY', 'TUMOR_SIZE_CM_MAX', 'LN_TOTAL_EXAMINED']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

# y
df['Y'] = df['LN_POSITIVE'].astype(int)

# Predictors list
PREDICTORS = [
    ('AGE_AT_SURGERY', 'Age (per year)'),
    ('SEX_MALE', 'Male sex (vs female)'),
    ('TUMOR_SIZE_CM_MAX', 'Tumor size (per cm)'),
    ('HIST_NON_PTC', 'Non-PTC histology (vs PTC)'),
    ('T_HIGH', 'T3-4 (vs T1-T2)'),
    ('ETE_ANY', 'Any ETE (vs none)'),
    ('BRAF_POS', 'BRAF positive (vs neg)'),
    ('SURG_TOTAL', 'Total thyroidectomy (vs partial)'),
    ('LN_TOTAL_EXAMINED', 'LN examined (per node)'),
]

import statsmodels.api as sm

def fit_logit(X_cols, label):
    sub = df[X_cols + ['Y']].dropna()
    if len(sub) < 50:
        return None, len(sub)
    X = sm.add_constant(sub[X_cols])
    y = sub['Y']
    try:
        model = sm.Logit(y, X).fit(disp=False, maxiter=200)
        return model, len(sub)
    except Exception as e:
        print(f"  fit failed for {label}: {e}")
        return None, len(sub)


def or_ci(model, var):
    if model is None or var not in model.params.index:
        return "—", "—", "—"
    coef = model.params[var]
    se = model.bse[var]
    odds = np.exp(coef)
    ci_lo = np.exp(coef - 1.96 * se)
    ci_hi = np.exp(coef + 1.96 * se)
    p = model.pvalues[var]
    p_str = f"{p:.4f}" if p >= 0.0001 else "<0.0001"
    return f"{odds:.2f}", f"({ci_lo:.2f}–{ci_hi:.2f})", p_str


# Univariable fits
print("\n=== Univariable fits ===")
univar = {}
for col, label in PREDICTORS:
    m, n = fit_logit([col], label)
    or_str, ci_str, p_str = or_ci(m, col)
    univar[col] = (label, n, or_str, ci_str, p_str)
    print(f"  {label}: n={n}  OR={or_str} {ci_str}  p={p_str}")

# Multivariable fit
print("\n=== Multivariable (all predictors) ===")
all_cols = [c for c, _ in PREDICTORS]
mv_model, mv_n = fit_logit(all_cols, 'multivariable')
if mv_model is not None:
    print(f"  n={mv_n}  log-lik={mv_model.llf:.1f}  pseudo R²={mv_model.prsquared:.3f}")

# Render markdown
md = ["# Table 2 — Manuscript M037: Logistic Regression Predictors of Lymph-Node Positivity\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {len(df):,} malignant patients (LN+ = {df['Y'].sum():,}; {100.0*df['Y'].sum()/len(df):.1f}%)\n",
      f"**Source:** THYROID_VALIDATION.PUBLIC.COHORT_M037_LN_PREDICTORS\n",
      f"**Method:** statsmodels Logit; ORs with 95% CI (Wald)\n\n",
      "_Note: Prior M037 Table 1 found BRAF was identical between LN+ and LN- (6.9% in both, p=1.00). Multivariable adjustment may surface effects masked at univariable._\n\n"]

md.append("## Univariable\n\n")
md.append("| Predictor | n | OR | 95% CI | p |\n")
md.append("| --- | --- | --- | --- | --- |\n")
for col, _ in PREDICTORS:
    label, n, or_str, ci_str, p_str = univar[col]
    md.append(f"| {label} | {n:,} | {or_str} | {ci_str} | {p_str} |\n")
md.append("\n")

md.append("## Multivariable (adjusted for all listed predictors)\n\n")
if mv_model is None:
    md.append("Model did not converge or insufficient cases.\n")
else:
    md.append(f"**N = {mv_n:,}; pseudo R² = {mv_model.prsquared:.3f}; log-lik = {mv_model.llf:.1f}**\n\n")
    md.append("| Predictor | aOR | 95% CI | p |\n")
    md.append("| --- | --- | --- | --- |\n")
    for col, label in PREDICTORS:
        or_str, ci_str, p_str = or_ci(mv_model, col)
        md.append(f"| {label} | {or_str} | {ci_str} | {p_str} |\n")
    md.append(f"\n**Constant (intercept):** β = {mv_model.params['const']:.3f}, OR = {np.exp(mv_model.params['const']):.3f}\n")
md.append("\n")

# Methods footnote
md.append("## Methods\n\n")
md.append("- **Outcome:** `LN_POSITIVE` from `COHORT_M037_LN_PREDICTORS` (= LN_POSITIVE_FLAG=1 OR LN_TOTAL_POSITIVE>0; per mig_258/259 caveat, manuscripts requiring numeric LN positivity should restrict to `ln_status_source='both'`).\n")
md.append("- **Predictors:** continuous (age, tumor size, LN examined) entered linearly. Categorical reference levels: female, PTC histology, T1a-T2 (vs T3-T4), no ETE, BRAF negative, partial thyroidectomy.\n")
md.append("- **Method:** Maximum likelihood logistic regression (statsmodels Logit). Wald CI for ORs.\n")
md.append("- **Caveat:** rows with any predictor NULL excluded from multivariable fit (complete-case). Sensitivity analysis with multiple imputation recommended for publication.\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
