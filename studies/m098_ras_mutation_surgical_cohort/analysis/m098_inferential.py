"""M098 Inferential analyses with FDR + multivariable logistic regression."""
import pandas as pd, numpy as np, warnings
from scipy import stats
from statsmodels.stats.multitest import multipletests
import statsmodels.api as sm
warnings.filterwarnings('ignore')

BASE = '/sessions/stoic-practical-gates/mnt/outputs/studies/m098_ras_mutation_surgical_cohort'
df = pd.read_csv(f'{BASE}/data/m098_analytic.csv')

def auto_cat_test(x, y, name):
    """χ² for categorical×categorical, switching to Fisher if any expected <5 or n<30."""
    tab = pd.crosstab(x, y).values
    if tab.size==0 or (tab.shape[0]<2) or (tab.shape[1]<2):
        return name, 'NA', np.nan, 'insufficient'
    try:
        chi2, p, dof, exp = stats.chi2_contingency(tab)
        use_fisher = (exp.min() < 5) or (tab.sum() < 30)
        if use_fisher and tab.shape==(2,2):
            _, p_f = stats.fisher_exact(tab)
            return name, f'Fisher 2×2', p_f, 'fisher'
        return name, f'χ² ({dof}df) = {chi2:.2f}', p, 'chi2'
    except Exception as e:
        return name, f'err:{e}', np.nan, 'err'

def auto_num_test(x, group, name, kind=None):
    """Mann-Whitney (2 groups) or Kruskal-Wallis (≥3)."""
    levels = group.dropna().unique()
    samples = [pd.to_numeric(x[group==l],errors='coerce').dropna() for l in levels]
    samples = [s for s in samples if len(s)>=2]
    if len(samples)<2: return name,'insufficient',np.nan,'na'
    if len(samples)==2:
        u,p = stats.mannwhitneyu(samples[0], samples[1], alternative='two-sided')
        return name, f'MW U={u:.0f}', p, 'mannwhitney'
    else:
        h,p = stats.kruskal(*samples)
        return name, f'KW H={h:.2f}', p, 'kruskal'

results = []

# Make 2x2 dichotomies
df['malig'] = (df.histology_class=='Malignant').astype(int)
df['malig_or_border'] = df.histology_class.isin(['Malignant','Borderline']).astype(int)
df['ata25_high'] = (df.ata_2025=='high').astype(int)
df['ata25_intHigh'] = df.ata_2025.isin(['intermediate','high']).astype(int)
df['tt'] = (df.procedure_type=='total_thyroidectomy').astype(int)

# Restrict to single-gene patients for gene comparisons
sg = df[df.gene_single.isin(['NRAS','HRAS','KRAS'])]

# ---- The 21+ comparison family ----
results.append(auto_cat_test(sg.gene_single, sg.malig, 'Gene (single-only) × Malignancy'))
results.append(auto_cat_test(sg.gene_single, sg.malig_or_border, 'Gene (single-only) × ROM (malig+border)'))
results.append(auto_cat_test(df.comut_group, df.malig, 'Co-mutation group × Malignancy'))
results.append(auto_cat_test(df.any_comut, df.malig, 'Any co-mutation × Malignancy'))
results.append(auto_cat_test(df.tert.fillna(False), df.malig, 'TERT (any) × Malignancy'))
results.append(auto_cat_test(df.braf.fillna(False), df.malig, 'BRAF (any) × Malignancy'))
ata_sub = df[df.ata_2025.isin(['low','intermediate','high'])]
results.append(auto_cat_test(ata_sub.comut_group, ata_sub.ata25_high, 'Co-mutation × ATA-2025 high'))
results.append(auto_cat_test(ata_sub.comut_group, ata_sub.ata25_intHigh, 'Co-mutation × ATA-2025 int-or-high'))
results.append(auto_cat_test(ata_sub.tert, ata_sub.ata25_high, 'TERT × ATA-2025 high'))
results.append(auto_cat_test(df.comut_group, df.tt, 'Co-mutation × Surgery (TT)'))
ln_sub = df[df.ln_positive.notna()]
results.append(auto_cat_test(ln_sub.comut_group, ln_sub.ln_positive.astype(int), 'Co-mutation × LN positive'))
results.append(auto_cat_test(df.bethesda_name.fillna('Missing'), df.malig, 'Bethesda × Malignancy'))

# Continuous comparisons
results.append(auto_num_test(df.age, df.gene_single.where(df.gene_single.isin(['NRAS','HRAS','KRAS'])), 'Age × Gene (single-only)'))
results.append(auto_num_test(df.age, df.comut_group, 'Age × Co-mutation'))
results.append(auto_num_test(df.age, df.histology_class.where(df.histology_class.isin(['Malignant','Benign'])), 'Age × Malignant-vs-Benign'))
mal_df = df[df.histology_class=='Malignant']
results.append(auto_num_test(mal_df.tumor_size, mal_df.gene_single.where(mal_df.gene_single.isin(['NRAS','HRAS','KRAS'])), 'Tumor size × Gene (malignant subset)'))
results.append(auto_num_test(mal_df.tumor_size, mal_df.comut_group, 'Tumor size × Co-mutation (malignant)'))
results.append(auto_num_test(mal_df.tumor_size, mal_df.ata_2025.where(mal_df.ata_2025.isin(['low','intermediate','high'])), 'Tumor size × ATA-2025 (malignant)'))
results.append(auto_num_test(df.ras_vaf, df.histology_class.where(df.histology_class.isin(['Malignant','Benign'])), 'Max RAS VAF × Malignant-vs-Benign'))
results.append(auto_num_test(df.ras_vaf, df.comut_group, 'Max RAS VAF × Co-mutation'))
results.append(auto_num_test(df.ras_vaf, df.gene_single.where(df.gene_single.isin(['NRAS','HRAS','KRAS'])), 'Max RAS VAF × Gene'))

# Apply BH FDR
pvals = [r[2] for r in results]
valid_idx = [i for i,p in enumerate(pvals) if not (np.isnan(p))]
valid_p = [pvals[i] for i in valid_idx]
rej, p_adj, _, _ = multipletests(valid_p, alpha=0.05, method='fdr_bh')
adj_arr = [np.nan]*len(pvals)
for j, i in enumerate(valid_idx):
    adj_arr[i] = p_adj[j]

# Build results df
df_res = pd.DataFrame([{
    'comparison':r[0], 'test':r[1], 'p_raw':r[2], 'p_fdr_bh':a, 'method':r[3],
    'sig_fdr_05': (not np.isnan(a)) and (a < 0.05)
} for r,a in zip(results, adj_arr)])
df_res.to_csv(f'{BASE}/analysis/inferential_results.csv', index=False)
print('--- INFERENTIAL RESULTS (FDR-adjusted) ---')
print(df_res.to_string(index=False))
print()

# ============ Multivariable logistic regression ============
def fit_logit(y_name, X_cols, label, df_in=None, ref_levels=None):
    print(f'\n=== {label} ===')
    if df_in is None: df_in = df
    sub = df_in.dropna(subset=[y_name]+X_cols).copy()
    if len(sub)==0:
        print('  no data'); return None
    # One-hot encode
    Xparts = []
    for col in X_cols:
        if sub[col].dtype == 'object' or sub[col].dtype.name=='category':
            ref = ref_levels.get(col) if ref_levels else None
            d = pd.get_dummies(sub[col], prefix=col, drop_first=False)
            if ref and f'{col}_{ref}' in d.columns: d = d.drop(columns=[f'{col}_{ref}'])
            else: d = d.iloc[:, 1:]
            Xparts.append(d.astype(float))
        else:
            Xparts.append(pd.to_numeric(sub[col],errors='coerce').rename(col).to_frame().astype(float))
    X = pd.concat(Xparts, axis=1)
    X = sm.add_constant(X)
    y = sub[y_name].astype(int)
    try:
        m = sm.Logit(y, X, missing='drop').fit(disp=0, maxiter=300)
        summ = pd.DataFrame({
            'coef':m.params, 'se':m.bse,
            'OR':np.exp(m.params),
            'OR_lo': np.exp(m.params - 1.96*m.bse),
            'OR_hi': np.exp(m.params + 1.96*m.bse),
            'p':m.pvalues
        })
        summ.to_csv(f'{BASE}/analysis/logit_{label.replace(" ","_").replace("(","").replace(")","")}.csv')
        print(summ.round(3).to_string())
        return m
    except Exception as e:
        print(f'  failed: {e}')
        return None

# Outcome 1: Malignancy
fit_logit('malig', ['gene_priority','comut_group','age','bethesda','ras_vaf'],
          'Outcome1_Malignancy',
          ref_levels={'gene_priority':'NRAS','comut_group':'Isolated'})

# Outcome 2: ATA-2025 int-or-high (scored only)
ata_df = df[df.ata_2025.isin(['low','intermediate','high'])].copy()
fit_logit('ata25_intHigh', ['gene_priority','comut_group','age','ras_vaf'],
          'Outcome2_ATA25_intHigh', df_in=ata_df,
          ref_levels={'gene_priority':'NRAS','comut_group':'Isolated'})

# Outcome 3: Any LN positive (with LN data)
ln_df = df[df.ln_positive.notna()].copy()
ln_df['ln_positive_int'] = ln_df.ln_positive.astype(int)
fit_logit('ln_positive_int', ['gene_priority','comut_group','age','tumor_size'],
          'Outcome3_LN_positive', df_in=ln_df,
          ref_levels={'gene_priority':'NRAS','comut_group':'Isolated'})

# Outcome 4: Total thyroidectomy
fit_logit('tt', ['gene_priority','comut_group','age','bethesda','tumor_size'],
          'Outcome4_TotalThyroidectomy',
          ref_levels={'gene_priority':'NRAS','comut_group':'Isolated'})

# Save summary stats for write-up
with open(f'{BASE}/analysis/summary_for_writeup.txt','w') as f:
    f.write(df_res.to_string(index=False))
    f.write('\n')
