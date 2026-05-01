"""M044 Cox PH with interaction terms — does ETE effect vary by T-stage / tumor size / N-stage?"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m044_cox_interactions.md")
ctx, cur = get_cursor()

cur.execute("""
SELECT RESEARCH_ID, AGE_AT_SURGERY, SEX, ETE_GRADE, HISTOLOGY_FINAL,
       AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE, TUMOR_SIZE_CM_MAX,
       SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG, BRAF_POSITIVE_FINAL,
       ANY_RECURRENCE_FLAG, TIME_TO_RECURRENCE_DAYS, FOLLOWUP_YEARS
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND ETE_GRADE IN ('none','microscopic','gross')
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
import numpy as np
df = pd.DataFrame(rows, columns=cols)
for c in ['AGE_AT_SURGERY','TUMOR_SIZE_CM_MAX','TIME_TO_RECURRENCE_DAYS','FOLLOWUP_YEARS']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df['EVENT'] = df['ANY_RECURRENCE_FLAG'].fillna(False).astype(bool).astype(int)
df['DURATION_DAYS'] = np.where(df['EVENT'] == 1, df['TIME_TO_RECURRENCE_DAYS'], df['FOLLOWUP_YEARS']*365.25)
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
df['LARGE_TUMOR'] = (df['TUMOR_SIZE_CM_MAX'] >= 4.0).astype(int)

# Interaction terms
df['ETE_X_THIGH'] = df['ETE_GROSS'] * df['T_HIGH']
df['ETE_X_NPOS']  = df['ETE_GROSS'] * df['N_POS']
df['ETE_X_LARGE'] = df['ETE_GROSS'] * df['LARGE_TUMOR']

base_cols = ['DURATION_YEARS','EVENT','AGE_AT_SURGERY','SEX_MALE','ETE_MICRO','ETE_GROSS',
             'T_HIGH','N_POS','BRAF','SURG_TOTAL','RAI','TUMOR_SIZE_CM_MAX']

from lifelines import CoxPHFitter

results = {}

# Model 1: base (already in Round 7)
m1 = df[base_cols].dropna()
print(f"=== Model 1 (base): n={len(m1)}, events={int(m1['EVENT'].sum())} ===")
cph1 = CoxPHFitter(); cph1.fit(m1, duration_col='DURATION_YEARS', event_col='EVENT')
results['base'] = (cph1, len(m1), int(m1['EVENT'].sum()))

# Model 2: + ETE_GROSS × T_HIGH (with penalizer to prevent collinearity issues)
m2 = df[base_cols + ['ETE_X_THIGH']].dropna()
print(f"=== Model 2 (+ ETE_gross × T_high): n={len(m2)} ===")
try:
    cph2 = CoxPHFitter(penalizer=0.01); cph2.fit(m2, duration_col='DURATION_YEARS', event_col='EVENT')
    results['ete_x_thigh'] = (cph2, len(m2), int(m2['EVENT'].sum()))
except Exception as e:
    print(f"  Model 2 skipped (collinearity): {str(e)[:120]}")

# Model 3: + ETE_GROSS × N_POS
m3 = df[base_cols + ['ETE_X_NPOS']].dropna()
print(f"=== Model 3 (+ ETE_gross × N_pos): n={len(m3)} ===")
try:
    cph3 = CoxPHFitter(penalizer=0.01); cph3.fit(m3, duration_col='DURATION_YEARS', event_col='EVENT')
    results['ete_x_npos'] = (cph3, len(m3), int(m3['EVENT'].sum()))
except Exception as e:
    print(f"  Model 3 skipped (collinearity): {str(e)[:120]}")

# Model 4: + ETE_GROSS × LARGE_TUMOR (LARGE_TUMOR enters as own covariate; with penalizer)
m4 = df[base_cols + ['LARGE_TUMOR','ETE_X_LARGE']].dropna()
print(f"=== Model 4 (+ ETE_gross × large_tumor): n={len(m4)} ===")
try:
    cph4 = CoxPHFitter(penalizer=0.05); cph4.fit(m4, duration_col='DURATION_YEARS', event_col='EVENT')
    results['ete_x_large'] = (cph4, len(m4), int(m4['EVENT'].sum()))
except Exception as e:
    print(f"  Model 4 skipped (collinearity): {str(e)[:120]}")

# Render
md = ["# M044 Cox PH — Interaction Terms\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      "Tests whether ETE's effect on recurrence varies by T-stage, N-stage, or tumor size.\n",
      "Comparison vs base model (`m044_cox_ph.md`): if interaction term significant, ETE's effect depends on the moderator.\n\n"]

for label, (model, n, events) in results.items():
    md.append(f"## Model — {label}\n\n")
    md.append(f"**N = {n:,}; events = {events}; c-index = {model.concordance_index_:.3f}; AIC = {model.AIC_partial_:.1f}**\n\n")
    md.append("| Predictor | HR | 95% CI | p |\n| --- | --- | --- | --- |\n")
    for var in model.summary.index:
        hr = model.summary.loc[var,'exp(coef)']
        ci_lo = model.summary.loc[var,'exp(coef) lower 95%']
        ci_hi = model.summary.loc[var,'exp(coef) upper 95%']
        p = model.summary.loc[var,'p']
        p_str = f"{p:.4f}" if p >= 0.0001 else "<0.0001"
        md.append(f"| {var} | {hr:.2f} | ({ci_lo:.2f}–{ci_hi:.2f}) | {p_str} |\n")
    md.append("\n")

md.append("## AIC comparison\n\n| Model | n | events | AIC | c-index |\n| --- | --- | --- | --- | --- |\n")
for label, (model, n, events) in results.items():
    md.append(f"| {label} | {n:,} | {events} | {model.AIC_partial_:.1f} | {model.concordance_index_:.3f} |\n")
md.append("\nLower AIC = better fit. ΔAIC > 2 = meaningful improvement; > 10 = strongly preferred.\n\n")

md.append("## Methods\n\n")
md.append("- Same base predictors as `m044_cox_ph.md` Cox model. Each interaction model adds 1 product term.\n")
md.append("- LARGE_TUMOR = TUMOR_SIZE_CM_MAX ≥ 4.0 (clinically relevant cutoff per AJCC).\n")
md.append("- Interactions of interest: ETE_GROSS × T_HIGH (does gross ETE matter more in T3-4?), ETE_GROSS × N_POS (LN-positive amplifier?), ETE_GROSS × LARGE_TUMOR (size + invasion synergy).\n")
md.append("- p-value on interaction term tests H0: no synergy beyond main effects.\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
