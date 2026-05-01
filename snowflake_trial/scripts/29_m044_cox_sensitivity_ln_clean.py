"""M044 Cox PH sensitivity — same model on cleaner LN cohort (excludes ln_status_source='staging' only)."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m044_cox_sensitivity_ln_clean.md")
ctx, cur = get_cursor()

print("=== Pulling M044 cohort restricted to ln_status_source != 'staging' ===")
cur.execute("""
SELECT
  RESEARCH_ID, AGE_AT_SURGERY, SEX, ETE_GRADE, HISTOLOGY_FINAL,
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE, TUMOR_SIZE_CM_MAX,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG, BRAF_POSITIVE_FINAL,
  ANY_RECURRENCE_FLAG, TIME_TO_RECURRENCE_DAYS, FOLLOWUP_YEARS,
  LN_STATUS_SOURCE
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE
  AND ETE_GRADE IN ('none','microscopic','gross')
  AND (LN_STATUS_SOURCE IS NULL OR LN_STATUS_SOURCE != 'staging')
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
import numpy as np
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows pre-NaN-drop")

for c in ['AGE_AT_SURGERY','TUMOR_SIZE_CM_MAX','FOLLOWUP_YEARS','TIME_TO_RECURRENCE_DAYS']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df['EVENT'] = df['ANY_RECURRENCE_FLAG'].fillna(False).astype(bool).astype(int)
df['DURATION_DAYS'] = np.where(
    df['EVENT'] == 1,
    df['TIME_TO_RECURRENCE_DAYS'],
    df['FOLLOWUP_YEARS'] * 365.25
)
df = df[df['DURATION_DAYS'] > 0].copy()
df['DURATION_YEARS'] = df['DURATION_DAYS'] / 365.25

df['SEX_MALE'] = (df['SEX'] == 'male').astype(int)
df['ETE_MICRO'] = (df['ETE_GRADE'] == 'microscopic').astype(int)
df['ETE_GROSS'] = (df['ETE_GRADE'] == 'gross').astype(int)
df['T_HIGH'] = df['AJCC8_T_STAGE'].isin(['T3a','T3b','T4a','T4b']).astype(int)
df['N_POS'] = df['AJCC8_N_STAGE'].isin(['N1a','N1b']).astype(int)
df['BRAF'] = (df['BRAF_POSITIVE_FINAL'] == True).astype(int)
df['SURG_TOTAL'] = (df['SURG_PROCEDURE_TYPE'] == 'total_thyroidectomy').astype(int)
df['RAI'] = (df['RAI_RECEIVED_FLAG'] == True).astype(int)

analysis = df[['DURATION_YEARS','EVENT','AGE_AT_SURGERY','SEX_MALE','ETE_MICRO','ETE_GROSS',
               'T_HIGH','N_POS','BRAF','SURG_TOTAL','RAI','TUMOR_SIZE_CM_MAX']].dropna()
print(f"  {len(analysis):,} rows after dropping NaN; events = {analysis['EVENT'].sum():,}")

from lifelines import CoxPHFitter
cph = CoxPHFitter()
cph.fit(analysis, duration_col='DURATION_YEARS', event_col='EVENT')
print(f"  Cox c-index: {cph.concordance_index_:.3f}")

# Render
md = ["# M044 Cox PH — Sensitivity on cleaner LN cohort (ln_status_source ≠ 'staging')\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {len(analysis):,} malignant + ETE-graded + ln_status_source ≠ 'staging'; {analysis['EVENT'].sum():,} recurrence events\n",
      f"**Comparison:** Same model as `m044_cox_ph.md` but restricted to remove staging-only LN+ patients\n\n",
      "## Cox PH multivariable\n\n",
      "| Predictor | HR | 95% CI | p |\n| --- | --- | --- | --- |\n"]

summary = cph.summary
labels = {
    'AGE_AT_SURGERY': 'Age (per year)',
    'SEX_MALE': 'Male sex',
    'ETE_MICRO': 'ETE microscopic (vs none)',
    'ETE_GROSS': 'ETE gross (vs none)',
    'T_HIGH': 'T3-T4',
    'N_POS': 'N1',
    'BRAF': 'BRAF positive',
    'SURG_TOTAL': 'Total thyroidectomy',
    'RAI': 'RAI received',
    'TUMOR_SIZE_CM_MAX': 'Tumor size (per cm)',
}
for var in summary.index:
    hr = summary.loc[var,'exp(coef)']; ci_lo = summary.loc[var,'exp(coef) lower 95%']
    ci_hi = summary.loc[var,'exp(coef) upper 95%']; p = summary.loc[var,'p']
    p_str = f"{p:.4f}" if p >= 0.0001 else "<0.0001"
    md.append(f"| {labels.get(var, var)} | {hr:.2f} | ({ci_lo:.2f}–{ci_hi:.2f}) | {p_str} |\n")

md.append(f"\n**c-index:** {cph.concordance_index_:.3f}; AIC = {cph.AIC_partial_:.1f}; partial log-lik = {cph.log_likelihood_:.1f}\n\n")
md.append("## Compare vs full M044 (m044_cox_ph.md, n=2,626)\n\n")
md.append("Direction-preserved + same significance = robust. Sign-flip / significance change = full-cohort artifact.\n")
md.append("Particular interest: ETE microscopic + gross HR direction; tumor size; total thyroidectomy.\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
