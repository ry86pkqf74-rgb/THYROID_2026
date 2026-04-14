"""ETE manuscript numeric verification: ordinal regression + PSM against frozen cohort CSV."""
import pandas as pd, numpy as np, json, sys
from statsmodels.miscmodels.ordinal_model import OrderedModel
from sklearn.linear_model import LogisticRegression
from scipy.stats import fisher_exact

SEED = 42
np.random.seed(SEED)

df = pd.read_csv('artifacts/ete_verification/ete_final_cohort_N3278.csv')

# ------------------------------------------------------------------
# PART 1: Coverage / no-gap audit
# ------------------------------------------------------------------
primary_vars = ['research_id','age_at_surgery','sex','ete_group','ete_micro','ete_gross',
                'risk_ord','t_stage_ajcc8','overall_stage_ajcc8','female',
                't_stage_ajcc7','overall_stage_ajcc7','largest_tumor_cm','ln_ratio',
                'recurrence_risk_band']
coverage = []
for v in primary_vars:
    coverage.append({'variable': v, 'non_null': int(df[v].notna().sum()),
                     'missing': int(df[v].isna().sum()),
                     'pct_missing': round(100.0*df[v].isna().sum()/len(df), 2)})
cov_df = pd.DataFrame(coverage)
cov_df.to_csv('artifacts/ete_verification/coverage_audit.csv', index=False)
print("=== COVERAGE AUDIT ===")
print(cov_df.to_string(index=False))

# ------------------------------------------------------------------
# PART 2: Ordinal regression (Table 3 Primary, CC expanded)
# ------------------------------------------------------------------
preds = ['ete_micro','ete_gross','age_at_surgery','female','largest_tumor_cm','ln_ratio']
df2 = df.copy()
df2['ln_ratio'] = df2['ln_ratio'].fillna(df2['ln_ratio'].mean())
mod_df = df2.dropna(subset=['risk_ord'] + preds).copy()
print(f"\nOrdinal CC N = {len(mod_df)}")

y = mod_df['risk_ord'].astype(int)
X = mod_df[preds].astype(float)
mod = OrderedModel(y, X, distr='logit')
res = mod.fit(method='bfgs', disp=False, maxiter=300)

ord_results = []
for var in preds:
    b, se = res.params[var], res.bse[var]
    OR = np.exp(b)
    lo, hi = np.exp(b-1.96*se), np.exp(b+1.96*se)
    p = res.pvalues[var]
    ord_results.append({'variable': var, 'OR': OR, 'CI_lo': lo, 'CI_hi': hi, 'p': p})
ord_df = pd.DataFrame(ord_results)
ord_df.to_csv('artifacts/ete_verification/reproduced_ordinal_regression.csv', index=False)

# Compare to frozen
frozen = pd.read_csv('studies/proposal2_ete_staging/audit_tables/table3_ordinal_regression.csv')
frozen = frozen[frozen.Subgroup == 'Primary (CC, expanded)'].set_index('Variable')

print(f"\n=== ORDINAL REGRESSION COMPARISON ===")
print(f"{'Variable':<20} {'OR reprod':>10} {'OR frozen':>10} {'d_OR':>10} {'match':>7}")
for var in preds:
    OR_r = ord_df.loc[ord_df.variable==var,'OR'].iloc[0]
    OR_f = frozen.loc[var,'OR']
    delta = abs(OR_r - OR_f)
    ok = (delta < 0.01) or (var == 'ete_gross' and delta/max(OR_f,1) < 0.01)
    print(f"{var:<20} {OR_r:>10.4f} {OR_f:>10.4f} {delta:>10.4g} {'PASS' if ok else 'FAIL':>7}")

# ------------------------------------------------------------------
# PART 3: PSM 1:1 nearest-neighbour caliper=0.05 (Table 6)
# ------------------------------------------------------------------
print(f"\n=== PSM REPRODUCTION ===")
psm_df = df[df['ete_group'].isin(['No ETE','Microscopic ETE'])].copy()
psm_df = psm_df.sort_values('research_id', kind='mergesort').reset_index(drop=True)
psm_df['treat'] = (psm_df['ete_group']=='Microscopic ETE').astype(int)
psm_feats = ['age_at_surgery','female','largest_tumor_cm']
psm_df2 = psm_df.dropna(subset=psm_feats+['risk_ord','treat']).copy()
print(f"PSM eligible N: {len(psm_df2)} (treated={psm_df2.treat.sum()}, control={(psm_df2.treat==0).sum()})")

lr = LogisticRegression(max_iter=1000, random_state=SEED, solver='lbfgs')
lr.fit(psm_df2[psm_feats].values, psm_df2['treat'].values)
psm_df2['propensity'] = lr.predict_proba(psm_df2[psm_feats].values)[:,1]

caliper = 0.05
treated = psm_df2[psm_df2.treat==1].copy().sort_values(['propensity','research_id'], kind='mergesort').reset_index(drop=True)
control = psm_df2[psm_df2.treat==0].copy().sort_values(['propensity','research_id'], kind='mergesort').reset_index(drop=True)

matched_t, matched_c = [], []
used_c = set()
for _, trow in treated.iterrows():
    cands = control[~control.index.isin(used_c)].copy()
    if len(cands)==0: break
    dist = (cands['propensity'] - trow['propensity']).abs()
    within = dist <= caliper
    if not within.any(): continue
    cands_w = cands[within]
    dist_w = dist[within]
    order = pd.DataFrame({'dist': dist_w.values, 'rid': cands_w['research_id'].astype(str).values,
                          'idx': cands_w.index.values}).sort_values(['dist','rid'], kind='mergesort')
    best_idx = int(order.iloc[0]['idx'])
    matched_t.append(trow['research_id'])
    matched_c.append(control.loc[best_idx,'research_id'])
    used_c.add(best_idx)

n_pairs = len(matched_t)
mt = psm_df2[psm_df2.research_id.isin(matched_t)]
mc = psm_df2[psm_df2.research_id.isin(matched_c)]
# Endpoint: structural recurrence proxy risk_ord >= 2 ("high")
rec_t = (mt['risk_ord'] >= 2).sum()
rec_c = (mc['risk_ord'] >= 2).sum()

# Fisher + Haldane-Anscombe OR
table = [[rec_t, len(mt)-rec_t],[rec_c, len(mc)-rec_c]]
a,b = table[0]; c,d = table[1]
OR_ha = ((a+0.5)*(d+0.5))/((b+0.5)*(c+0.5))
_, p_fish = fisher_exact(table, alternative='two-sided')
print(f"Pairs: {n_pairs}")
print(f"Treated events: {rec_t}/{len(mt)} = {100*rec_t/len(mt):.2f}%")
print(f"Control events: {rec_c}/{len(mc)} = {100*rec_c/len(mc):.2f}%")
print(f"Haldane-Anscombe OR: {OR_ha:.4f}")
print(f"Fisher exact p: {p_fish:.4f}")

# Compare to frozen Table 6 effect
frozen_effect = pd.read_csv('studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_effect.csv')
print("\nFrozen Table 6 effect:")
print(frozen_effect.to_string(index=False))

# Save reproduced
pd.DataFrame([{'n_pairs': n_pairs, 'treated_events': int(rec_t), 'control_events': int(rec_c),
               'OR_HA': OR_ha, 'fisher_p': p_fish}]).to_csv(
    'artifacts/ete_verification/reproduced_psm_effect.csv', index=False)

# ------------------------------------------------------------------
# PART 4: Dump final summary JSON
# ------------------------------------------------------------------
summary = {
    'cohort_N': int(len(df)),
    'ordinal_CC_N': int(len(mod_df)),
    'ordinal_mete_OR_reproduced': float(ord_df.loc[ord_df.variable=='ete_micro','OR'].iloc[0]),
    'ordinal_mete_OR_frozen': float(frozen.loc['ete_micro','OR']),
    'psm_pairs_reproduced': int(n_pairs),
    'psm_OR_HA_reproduced': float(OR_ha),
    'psm_fisher_p_reproduced': float(p_fish),
    'coverage_primary_vars_pct_complete': round(100*(1 - df[['age_at_surgery','sex','ete_group','risk_ord']].isna().any(axis=1).mean()), 4),
    'seed': SEED,
}
with open('artifacts/ete_verification/verification_summary.json','w') as f:
    json.dump(summary, f, indent=2)
print("\n=== SUMMARY ===")
print(json.dumps(summary, indent=2))
