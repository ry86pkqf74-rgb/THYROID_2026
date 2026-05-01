"""M044 manuscript figures — KM curves data + Forest plot data export.

Outputs CSV-style data for downstream plotting:
  reports/m044_km_curves_data.csv  — per-strata survival probabilities at fixed timepoints
  reports/m044_forest_plot_data.csv — HRs + 95% CI from Cox model for each predictor
"""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT_DIR = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
ctx, cur = get_cursor()

print("=== Pulling M044 cohort ===")
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

# === KM curves data (Tier 6 #1) ===
from lifelines import KaplanMeierFitter

km_records = []
TIME_POINTS = [0.5, 1, 2, 3, 5, 7, 10, 15]  # years
for ete in ['none', 'microscopic', 'gross']:
    sub = df[df['ETE_GRADE'] == ete]
    if len(sub) < 10: continue
    kmf = KaplanMeierFitter()
    kmf.fit(sub['DURATION_YEARS'], sub['EVENT'], label=ete)
    for t in TIME_POINTS:
        survival = kmf.predict(t)
        ci = kmf.confidence_interval_survival_function_
        try:
            row_ci = ci.iloc[(ci.index - t).abs().argsort()[:1]]
            ci_lo = row_ci.iloc[0, 0]
            ci_hi = row_ci.iloc[0, 1]
        except Exception:
            ci_lo, ci_hi = float('nan'), float('nan')
        # Number at risk: patients with duration >= t
        n_at_risk = (sub['DURATION_YEARS'] >= t).sum()
        n_events_so_far = ((sub['DURATION_YEARS'] <= t) & (sub['EVENT'] == 1)).sum()
        km_records.append({
            'ete_strata': ete, 'n_total': len(sub), 'time_years': t,
            'survival': round(survival, 4),
            'ci_lo': round(ci_lo, 4) if not np.isnan(ci_lo) else None,
            'ci_hi': round(ci_hi, 4) if not np.isnan(ci_hi) else None,
            'n_at_risk': int(n_at_risk),
            'cumulative_events': int(n_events_so_far),
        })

km_df = pd.DataFrame(km_records)
km_df.to_csv(OUT_DIR / "m044_km_curves_data.csv", index=False)
print(f"  KM curves data: {len(km_records)} rows -> reports/m044_km_curves_data.csv")

# === Forest plot data (Tier 6 #2) ===
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

from lifelines import CoxPHFitter
cph = CoxPHFitter()
cph.fit(analysis, duration_col='DURATION_YEARS', event_col='EVENT')

forest_records = []
labels = {
    'AGE_AT_SURGERY': 'Age (per year)',
    'SEX_MALE': 'Male sex',
    'ETE_MICRO': 'ETE microscopic vs none',
    'ETE_GROSS': 'ETE gross vs none',
    'T_HIGH': 'T3-T4 vs T1-T2',
    'N_POS': 'N1 vs N0',
    'BRAF': 'BRAF positive',
    'SURG_TOTAL': 'Total thyroidectomy',
    'RAI': 'RAI received',
    'TUMOR_SIZE_CM_MAX': 'Tumor size (per cm)',
}
for var in cph.summary.index:
    forest_records.append({
        'predictor': labels.get(var, var),
        'hr': round(cph.summary.loc[var,'exp(coef)'], 3),
        'ci_lo': round(cph.summary.loc[var,'exp(coef) lower 95%'], 3),
        'ci_hi': round(cph.summary.loc[var,'exp(coef) upper 95%'], 3),
        'p_value': round(cph.summary.loc[var,'p'], 4) if cph.summary.loc[var,'p'] >= 0.0001 else '<0.0001',
        'log_hr': round(cph.summary.loc[var,'coef'], 3),
        'se': round(cph.summary.loc[var,'se(coef)'], 3),
    })

forest_df = pd.DataFrame(forest_records)
forest_df.to_csv(OUT_DIR / "m044_forest_plot_data.csv", index=False)
print(f"  Forest plot data: {len(forest_records)} rows -> reports/m044_forest_plot_data.csv")
print(f"  c-index = {cph.concordance_index_:.3f}; n = {len(analysis)}; events = {analysis['EVENT'].sum()}")

# Companion markdown summary
md_path = OUT_DIR / "m044_figures_data_README.md"
md_path.write_text(f"""# M044 Manuscript Figures Data

**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}

## Files

- `m044_km_curves_data.csv` — Kaplan-Meier survival probabilities per ETE strata at {len(TIME_POINTS)} timepoints (0.5/1/2/3/5/7/10/15 years). Columns: ete_strata, n_total, time_years, survival, ci_lo, ci_hi, n_at_risk, cumulative_events. Plot directly with matplotlib/ggplot for Figure 2 of M044 manuscript.

- `m044_forest_plot_data.csv` — Cox PH multivariable HRs with 95% CI for forest plot (Figure 3). Columns: predictor, hr, ci_lo, ci_hi, p_value, log_hr (for plotting on log scale), se.

## Cox model
n = {len(analysis):,}; events = {analysis['EVENT'].sum():,}; c-index = {cph.concordance_index_:.3f}; AIC = {cph.AIC_partial_:.1f}

## KM strata sizes
""")
for ete in ['none','microscopic','gross']:
    sub = df[df['ETE_GRADE'] == ete]
    md_path.write_text(md_path.read_text() + f"- **ETE {ete}:** n={len(sub):,}, events={int(sub['EVENT'].sum())}\n")
print(f"  README: {md_path}")
