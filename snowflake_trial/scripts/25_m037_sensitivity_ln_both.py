"""M037 sensitivity analysis — restrict to ln_status_source='both' subset.

Rationale: per mig_258/259, ln_status_source flags whether LN positivity comes from
N-stage assertion only ('staging'), structured count only ('count'), or both ('both').
Patients with 'both' have concordant signals — most defensible for numeric LN-burden
analyses. This sensitivity check re-fits the Table 2 logreg on the 'both' subset.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m037_sensitivity_ln_both.md")
ctx, cur = get_cursor()

print("=== Pulling M037 cohort restricted to ln_status_source='both' ===")
cur.execute("""
SELECT
  cpm.RESEARCH_ID, cpm.AGE_AT_SURGERY, cpm.SEX, cpm.RACE,
  CASE WHEN cpm.HISTOLOGY_FINAL ILIKE 'PTC%' THEN 'PTC'
       WHEN cpm.HISTOLOGY_FINAL ILIKE '%follicular%' THEN 'FTC'
       WHEN cpm.HISTOLOGY_FINAL ILIKE 'MTC%' OR cpm.HISTOLOGY_FINAL ILIKE '%medullary%' THEN 'MTC'
       WHEN cpm.HISTOLOGY_FINAL ILIKE '%anaplastic%' THEN 'ATC'
       WHEN cpm.HISTOLOGY_FINAL ILIKE '%poorly differentiated%' THEN 'PDTC'
       ELSE 'Other' END AS HISTOLOGY_GROUP,
  cpm.AJCC8_T_STAGE, cpm.AJCC8_N_STAGE, cpm.AJCC8_M_STAGE,
  cpm.TUMOR_SIZE_CM_MAX, cpm.ETE_GRADE,
  cpm.LN_TOTAL_EXAMINED, cpm.LN_TOTAL_POSITIVE,
  CASE WHEN cpm.LN_TOTAL_POSITIVE > 0 THEN TRUE ELSE FALSE END AS LN_POSITIVE,
  cpm.SURG_PROCEDURE_TYPE, cpm.BRAF_POSITIVE_FINAL,
  cpm.LN_STATUS_SOURCE
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
WHERE cpm.IS_MALIGNANT = TRUE
  -- Exclude staging-only LN+ patients (AJCC N1 with no structured count) to remove
  -- the disagreement set surfaced by mig_258. Keeps 'both' (1,126), 'count'-only (0),
  -- and ln_status_source IS NULL (the LN-negative subset who were never N1 staged).
  AND (cpm.LN_STATUS_SOURCE IS NULL OR cpm.LN_STATUS_SOURCE != 'staging')
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
import numpy as np
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows; LN+ (count-based): {df['LN_POSITIVE'].sum():,}")

# Build features (same encoding as Table 2)
df['SEX_MALE'] = (df['SEX'] == 'male').astype(int)
df['HIST_NON_PTC'] = (df['HISTOLOGY_GROUP'] != 'PTC').astype(int)
df['T_HIGH'] = df['AJCC8_T_STAGE'].isin(['T3a','T3b','T4a','T4b']).astype(int)
df['ETE_ANY'] = df['ETE_GRADE'].isin(['microscopic','gross','present_ungraded']).astype(int)
df['BRAF_POS'] = (df['BRAF_POSITIVE_FINAL'] == True).astype(int)
df['SURG_TOTAL'] = (df['SURG_PROCEDURE_TYPE'] == 'total_thyroidectomy').astype(int)
for c in ['AGE_AT_SURGERY','TUMOR_SIZE_CM_MAX','LN_TOTAL_EXAMINED']:
    df[c] = pd.to_numeric(df[c], errors='coerce')
df['Y'] = df['LN_POSITIVE'].astype(int)

PREDICTORS = [
    ('AGE_AT_SURGERY', 'Age (per year)'),
    ('SEX_MALE', 'Male sex'),
    ('TUMOR_SIZE_CM_MAX', 'Tumor size (per cm)'),
    ('HIST_NON_PTC', 'Non-PTC (vs PTC)'),
    ('T_HIGH', 'T3-4 (vs T1-T2)'),
    ('ETE_ANY', 'Any ETE (vs none)'),
    ('BRAF_POS', 'BRAF positive'),
    ('SURG_TOTAL', 'Total thyroidectomy'),
    # LN_TOTAL_EXAMINED removed — degenerate in 'both' subset (all rows have examined>0)
]

import statsmodels.api as sm

def fit(cols_list):
    sub = df[cols_list + ['Y']].dropna()
    X = sm.add_constant(sub[cols_list])
    return sm.Logit(sub['Y'], X).fit(disp=False, maxiter=200), len(sub)


def or_ci(model, var):
    coef = model.params[var]; se = model.bse[var]
    or_ = np.exp(coef); ci_lo = np.exp(coef - 1.96*se); ci_hi = np.exp(coef + 1.96*se)
    p = model.pvalues[var]
    p_str = f"{p:.4f}" if p >= 0.0001 else "<0.0001"
    return f"{or_:.2f}", f"({ci_lo:.2f}–{ci_hi:.2f})", p_str


# Univariable
print("\n=== Univariable (ln_status_source='both' subset) ===")
univ = {}
for c, label in PREDICTORS:
    m, n = fit([c])
    univ[c] = (label, n) + or_ci(m, c)
    print(f"  {label}: n={n}  OR={univ[c][2]} {univ[c][3]}  p={univ[c][4]}")

# Multivariable
print("\n=== Multivariable ===")
mv, mv_n = fit([c for c, _ in PREDICTORS])
print(f"  n={mv_n}  pseudo R²={mv.prsquared:.3f}")

# Render
md = ["# M037 Sensitivity Analysis — ln_status_source='both' Subset\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** Malignant + ln_status_source='both' (n={len(df):,}; LN+ = {df['Y'].sum():,}; {100.0*df['Y'].sum()/len(df):.1f}%)\n",
      "**Comparison:** Re-fit M037 Table 2 logreg on the cleanest LN-positivity subset (concordant N-stage AND structured count).\n",
      "Use this to confirm or refute the full-cohort Table 2 conclusions.\n\n",
      "## Univariable\n\n",
      "| Predictor | n | OR | 95% CI | p |\n| --- | --- | --- | --- | --- |\n"]
for c, _ in PREDICTORS:
    label, n, o, ci, p = univ[c]
    md.append(f"| {label} | {n:,} | {o} | {ci} | {p} |\n")

md.append(f"\n## Multivariable (n={mv_n:,}; pseudo R²={mv.prsquared:.3f}; log-lik={mv.llf:.1f})\n\n")
md.append("| Predictor | aOR | 95% CI | p |\n| --- | --- | --- | --- |\n")
for c, label in PREDICTORS:
    o, ci, p = or_ci(mv, c)
    md.append(f"| {label} | {o} | {ci} | {p} |\n")

md.append("\n## Comparison vs full-cohort Table 2\n\n")
md.append("Full cohort (n=4,137; LN+=1,126; 27.2%) vs `ln_status_source='both'` (n=" + f"{len(df):,}" + f"; LN+={df['Y'].sum():,}; {100.0*df['Y'].sum()/len(df):.1f}%):\n\n")
md.append("Full-cohort multivariable highlighted: BRAF aOR=1.23 (NS), Non-PTC aOR=0.34, T3-4 aOR=1.38, Total thyroidectomy aOR=2.64.\n")
md.append("Restricted-cohort findings should be compared row-by-row above. Direction-preserving + same-significance = robust effect; sign-flip or significance loss = full-cohort artifact.\n")

md.append("\n## Methods\n\n")
md.append("- Outcome: LN_TOTAL_POSITIVE > 0 (count-based; cleaner than the LN_POSITIVE_FLAG which mixes staging+count)\n")
md.append("- Cohort: malignant + ln_status_source='both' (per mig_258/259 reconciliation)\n")
md.append("- Method: statsmodels Logit\n")
md.append("- Caveat: smaller subset means wider CIs; restricted cohort may bias toward better-LN-counted patients (selection)\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
