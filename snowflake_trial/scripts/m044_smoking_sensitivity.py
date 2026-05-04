"""M044 sensitivity: add smoking covariate to primary logistic, check whether
gross-vs-microscopic ETE aOR materially shifts.
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import duckdb
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")

# Pull M044 cohort with NLP smoking via CPM JOIN (cohort view doesn't yet carry smoking — would need mig_287)
df = md.execute("""
SELECT
  c.research_id,
  c.ete_grade_final,
  c.histology_final,
  pm.tumor_size_cm_max AS tumor_size_cm,
  c.age_at_surgery,
  c.sex,
  c.ajcc8_t_stage,
  c.ajcc8_n_stage,
  COALESCE(c.any_recurrence_flag, FALSE) AS recurrence_any,
  pm.pmhx_nlp_smoking_status,
  pm.nsqip_smoker
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
LEFT JOIN main.canonical_patient_master pm USING (research_id)
""").fetch_df()

print(f"Cohort: {len(df)} pts")
print(f"  smoking known via NLP: {df['pmhx_nlp_smoking_status'].notna().sum()}")
print(f"  smoking known via NSQIP: {df['nsqip_smoker'].notna().sum()}")

# Build smoking_combined: prefer NLP, fall back to NSQIP
def smk_cat(row):
    if pd.notna(row['pmhx_nlp_smoking_status']):
        return row['pmhx_nlp_smoking_status']
    if pd.notna(row['nsqip_smoker']):
        return f"nsqip_{row['nsqip_smoker']}"
    return None

df['smoking_combined'] = df.apply(smk_cat, axis=1)
print(f"  smoking_combined known: {df['smoking_combined'].notna().sum()}")

# Filter to strict-DTC + path-proven recurrence outcome
strict_dtc = df['histology_final'].str.contains('PTC|FTC|Hurthle|High-grade|Metastatic|Poorly', case=False, na=False)
df = df[strict_dtc & df['ete_grade_final'].isin(['none','absent','false','microscopic','gross'])].copy()
print(f"After strict-DTC filter: n={len(df)}")

# Recode ETE
def ete_grp(v):
    v = str(v).lower()
    if v in ('none','absent','false'): return 'No/negative ETE'
    if v == 'microscopic': return 'Microscopic ETE'
    if v == 'gross': return 'Gross ETE'
    return None

df['ete_group'] = df['ete_grade_final'].apply(ete_grp)
df = df[df['ete_group'].notna()].copy()
df['ete_group'] = pd.Categorical(df['ete_group'], categories=['Microscopic ETE', 'No/negative ETE', 'Gross ETE'])
df['y_pp'] = df['recurrence_any'].astype(bool).astype(int)

# Run 3 models: baseline (no smoking), + smoking NLP only, + smoking combined
def fit_model(df_, smk_term, label=""):
    import math
    cols = ['y_pp','ete_group','age_at_surgery','sex','ajcc8_n_stage','tumor_size_cm']
    if smk_term: cols.append(smk_term)
    d = df_[cols].dropna()
    formula = "y_pp ~ C(ete_group, Treatment(reference='Microscopic ETE')) + age_at_surgery + C(sex) + C(ajcc8_n_stage) + tumor_size_cm"
    if smk_term:
        formula += f" + C({smk_term})"
    print(f"  [{label}] formula: {formula}")
    print(f"  [{label}] complete-case n={len(d)} events={int(d['y_pp'].sum())}")
    try:
        m = smf.glm(formula, data=d, family=sm.families.Binomial()).fit(disp=0)
        print(f"  [{label}] params: {list(m.params.index)}")
        for name in m.params.index:
            if 'Gross ETE' in name:
                coef = m.params[name]; se = m.bse[name]; p = m.pvalues[name]
                or_ = math.exp(coef); lo = math.exp(coef - 1.96*se); hi = math.exp(coef + 1.96*se)
                return {'n':len(d),'events':int(d['y_pp'].sum()),'aOR':or_,'lo':lo,'hi':hi,'p':p}
        return {'n':len(d),'events':int(d['y_pp'].sum()),'error':'no Gross ETE param found'}
    except Exception as e:
        return {'error':str(e),'n':len(d),'events':int(d['y_pp'].sum())}

baseline = fit_model(df, None, 'baseline')
nlp_only = fit_model(df, 'pmhx_nlp_smoking_status', 'nlp_only')
combined = fit_model(df, 'smoking_combined', 'combined')
print("baseline result:", baseline)
print("nlp_only result:", nlp_only)
print("combined result:", combined)

md.close()

report = [
    "# M044 sensitivity — Gross-vs-Microscopic aOR with smoking covariate",
    "**Generated:** 2026-05-04",
    f"**Cohort:** strict-DTC + ETE in (none/micro/gross), n={len(df)}",
    f"**Outcome:** path-proven recurrence (n_events={df['y_pp'].sum()})",
    "",
    "## Gross-vs-Microscopic aOR — sensitivity to smoking covariate",
    "",
    "| Spec | n | events | aOR | 95% CI | p |",
    "|---|---:|---:|---:|---|---:|",
]
def row(label, r):
    if r and 'aOR' in r:
        return f"| {label} | {r['n']} | {r['events']} | {r['aOR']:.2f} | ({r['lo']:.2f}-{r['hi']:.2f}) | {r['p']:.4g} |"
    return f"| {label} | {r.get('n','?')} | {r.get('events','?')} | ERR | {r.get('error','no result')} | |"

report.append(row("Baseline (no smoking)", baseline))
report.append(row("+ NLP smoking", nlp_only))
report.append(row("+ smoking_combined (NLP+NSQIP)", combined))

report.extend([
    "",
    "## Interpretation",
    "",
    "If aOR remains close to v1.1 baseline (~2.08) across all 3 specs, smoking is NOT confounding the ETE effect. M044 v1.1 primary spec stands as-is.",
    "If aOR shifts >25%, smoking should be added to primary model (manuscript revision needed).",
    "",
])
(REPORTS / "m044_smoking_sensitivity_20260504.md").write_text("\n".join(report))
print(f"\n[saved] {REPORTS}/m044_smoking_sensitivity_20260504.md")
print("=== DONE ===")
