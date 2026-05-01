"""M044 Cox Proportional Hazards — time-to-recurrence by ETE strata."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m044_cox_ph.md")
ctx, cur = get_cursor()

print("=== Pulling M044 cohort with time-to-recurrence ===")
cur.execute("""
SELECT
  RESEARCH_ID, AGE_AT_SURGERY, SEX, ETE_GRADE,
  HISTOLOGY_FINAL,
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE,
  TUMOR_SIZE_CM_MAX,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG,
  BRAF_POSITIVE_FINAL,
  ANY_RECURRENCE_FLAG, TIME_TO_RECURRENCE_DAYS,
  FOLLOWUP_YEARS
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND ETE_GRADE IN ('none','microscopic','gross')
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
import numpy as np
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows pre-filter")

# Build duration + event columns
df['AGE_AT_SURGERY'] = pd.to_numeric(df['AGE_AT_SURGERY'], errors='coerce')
df['TUMOR_SIZE_CM_MAX'] = pd.to_numeric(df['TUMOR_SIZE_CM_MAX'], errors='coerce')
df['FOLLOWUP_YEARS'] = pd.to_numeric(df['FOLLOWUP_YEARS'], errors='coerce')
df['TIME_TO_RECURRENCE_DAYS'] = pd.to_numeric(df['TIME_TO_RECURRENCE_DAYS'], errors='coerce')

df['EVENT'] = df['ANY_RECURRENCE_FLAG'].fillna(False).astype(bool).astype(int)
df['DURATION_DAYS'] = np.where(
    df['EVENT'] == 1,
    df['TIME_TO_RECURRENCE_DAYS'],
    df['FOLLOWUP_YEARS'] * 365.25
)
df = df[df['DURATION_DAYS'] > 0].copy()
df['DURATION_YEARS'] = df['DURATION_DAYS'] / 365.25

# Encode covariates
df['SEX_MALE'] = (df['SEX'] == 'male').astype(int)
df['ETE_MICRO'] = (df['ETE_GRADE'] == 'microscopic').astype(int)
df['ETE_GROSS'] = (df['ETE_GRADE'] == 'gross').astype(int)
df['T_HIGH'] = df['AJCC8_T_STAGE'].isin(['T3a','T3b','T4a','T4b']).astype(int)
df['N_POS'] = df['AJCC8_N_STAGE'].isin(['N1a','N1b']).astype(int)
df['BRAF'] = (df['BRAF_POSITIVE_FINAL'] == True).astype(int)
df['SURG_TOTAL'] = (df['SURG_PROCEDURE_TYPE'] == 'total_thyroidectomy').astype(int)
df['RAI'] = (df['RAI_RECEIVED_FLAG'] == True).astype(int)

# Drop NaN in any covariate
analysis = df[['DURATION_YEARS','EVENT','AGE_AT_SURGERY','SEX_MALE',
               'ETE_MICRO','ETE_GROSS','T_HIGH','N_POS','BRAF','SURG_TOTAL','RAI',
               'TUMOR_SIZE_CM_MAX']].dropna()
print(f"  {len(analysis):,} rows after dropping NaN; events = {analysis['EVENT'].sum():,}")

from lifelines import CoxPHFitter, KaplanMeierFitter
from lifelines.statistics import multivariate_logrank_test

# Cox PH
print("\n=== Cox PH multivariable ===")
cph = CoxPHFitter()
cph.fit(analysis, duration_col='DURATION_YEARS', event_col='EVENT')

# Kaplan-Meier by ETE strata
print("\n=== Log-rank test across ETE strata ===")
strata = df.dropna(subset=['DURATION_YEARS','EVENT','ETE_GRADE'])
lr = multivariate_logrank_test(strata['DURATION_YEARS'], strata['ETE_GRADE'], strata['EVENT'])
print(f"  log-rank statistic = {lr.test_statistic:.2f}; p = {lr.p_value:.4f}")

# Per-strata KM
km_data = {}
for ete in ['none','microscopic','gross']:
    sub = strata[strata['ETE_GRADE'] == ete]
    if len(sub) < 10:
        continue
    kmf = KaplanMeierFitter()
    kmf.fit(sub['DURATION_YEARS'], sub['EVENT'], label=ete)
    km_data[ete] = {
        'n': len(sub),
        'events': int(sub['EVENT'].sum()),
        'median_followup': sub['DURATION_YEARS'].median(),
        'survival_at_5y': kmf.predict(5.0),
        'survival_at_10y': kmf.predict(10.0),
    }

# Render
md = ["# Manuscript M044 — Cox Proportional Hazards: Time-to-Recurrence by ETE\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {len(analysis):,} malignant patients (ETE in none/micro/gross), {analysis['EVENT'].sum():,} recurrence events\n",
      f"**Method:** lifelines CoxPHFitter; events = any_recurrence_flag (post-mig_255 hybrid B′/A′)\n\n",
      "_Note: Time-to-event uses time_to_recurrence_days for events, followup_years*365.25 for censored. mig_255 cleared 740 flag/timing mismatches; ln_status_source not yet integrated as covariate (would tighten LN N1 measurement)._\n\n"]

# Cox PH summary
md.append("## Cox PH multivariable\n\n")
md.append("| Predictor | HR | 95% CI | p |\n")
md.append("| --- | --- | --- | --- |\n")
summary = cph.summary
for var in summary.index:
    hr = summary.loc[var, 'exp(coef)']
    ci_lo = summary.loc[var, 'exp(coef) lower 95%']
    ci_hi = summary.loc[var, 'exp(coef) upper 95%']
    p = summary.loc[var, 'p']
    p_str = f"{p:.4f}" if p >= 0.0001 else "<0.0001"
    label = {
        'AGE_AT_SURGERY': 'Age (per year)',
        'SEX_MALE': 'Male sex',
        'ETE_MICRO': 'ETE microscopic (vs none)',
        'ETE_GROSS': 'ETE gross (vs none)',
        'T_HIGH': 'T3-T4 (vs T1-T2)',
        'N_POS': 'N1 (vs N0)',
        'BRAF': 'BRAF positive',
        'SURG_TOTAL': 'Total thyroidectomy',
        'RAI': 'RAI received',
        'TUMOR_SIZE_CM_MAX': 'Tumor size (per cm)',
    }.get(var, var)
    md.append(f"| {label} | {hr:.2f} | ({ci_lo:.2f}–{ci_hi:.2f}) | {p_str} |\n")
md.append(f"\n**Concordance index:** {cph.concordance_index_:.3f}; AIC = {cph.AIC_partial_:.1f}; partial log-lik = {cph.log_likelihood_:.1f}\n\n")

# Log-rank
md.append("## Log-rank test across ETE strata\n\n")
md.append(f"- Statistic: {lr.test_statistic:.2f}\n- p-value: {lr.p_value:.4f}\n\n")

# KM per strata
md.append("## Kaplan-Meier per ETE stratum\n\n")
md.append("| ETE | n | events | median followup (yr) | 5-yr surv | 10-yr surv |\n")
md.append("| --- | --- | --- | --- | --- | --- |\n")
for ete, d in km_data.items():
    md.append(f"| {ete} | {d['n']:,} | {d['events']:,} | {d['median_followup']:.1f} | {d['survival_at_5y']:.3f} | {d['survival_at_10y']:.3f} |\n")

# Methods
md.append("\n## Methods\n\n")
md.append("- **Outcome:** any_recurrence_flag (post-mig_255 hybrid B′/A′ disposition; path_proven flips applied)\n")
md.append("- **Time:** time_to_recurrence_days for events, followup_years*365.25 for censored\n")
md.append("- **Covariates:** age, sex, ETE strata, T-stage, N-stage, BRAF, surgery type, RAI, tumor size\n")
md.append("- **Software:** lifelines.CoxPHFitter (semiparametric Cox model)\n")
md.append("- **Caveats:** complete-case; no time-varying covariates; no competing risks adjustment; ln_status_source filter not applied (would tighten N-stage measurement to ln_status_source='both' subset)\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
