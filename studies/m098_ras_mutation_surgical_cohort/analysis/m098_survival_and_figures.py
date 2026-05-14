"""M098 survival analysis, sensitivity analyses, and figures."""
import pandas as pd, numpy as np, os, warnings
from scipy import stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from statsmodels.stats.multitest import multipletests
import statsmodels.api as sm
warnings.filterwarnings('ignore')

BASE = '/sessions/stoic-practical-gates/mnt/outputs/studies/m098_ras_mutation_surgical_cohort'
df = pd.read_csv(f'{BASE}/data/m098_analytic.csv')
df['malig'] = (df.histology_class=='Malignant').astype(int)
df['malig_or_border'] = df.histology_class.isin(['Malignant','Borderline']).astype(int)

# ========== Survival analysis ==========
print('=== SURVIVAL ANALYSIS ===')
# Recurrence as any (path or imaging suspicious)
df['rec_any'] = (df.rec_path_proven.fillna(False) | df.rec_img_susp.fillna(False)).astype(int)
df['rec_pathonly'] = df.rec_path_proven.fillna(False).astype(int)

# Incidence rate per 100 person-years (full cohort, full follow-up)
total_py = pd.to_numeric(df.followup_years, errors='coerce').fillna(0).sum()
n_path_rec_full = df.rec_pathonly.sum()
n_any_rec_full = df.rec_any.sum()
if total_py > 0:
    ir_pp_full = 100*n_path_rec_full/total_py
    ir_any_full = 100*n_any_rec_full/total_py
    print(f'Full cohort: {n_path_rec_full} path-proven recurrences / {total_py:.1f} PY = {ir_pp_full:.2f} per 100 PY')
    print(f'Full cohort: {n_any_rec_full} any recurrences (path+imaging) / {total_py:.1f} PY = {ir_any_full:.2f} per 100 PY')

# ≥1y follow-up analytic subset
sub = df[df.followup_years >= 1.0].copy()
n_sub = len(sub)
print(f'\nAnalytic subset (FU ≥1y): n={n_sub}')
py_sub = sub.followup_years.sum()
n_rec_sub = sub.rec_pathonly.sum()
n_any_sub = sub.rec_any.sum()
print(f'  Path-proven recurrences: {n_rec_sub}  Any: {n_any_sub}  PY: {py_sub:.1f}')
print(f'  Median FU: {sub.followup_years.median():.2f}y  IQR: {sub.followup_years.quantile(.25):.2f}-{sub.followup_years.quantile(.75):.2f}')

# KM curves stratified by co-mutation group
fig, ax = plt.subplots(figsize=(8,6))
colors = {'Isolated':'#2c7fb8','RAS+BRAF':'#f57c00','RAS+TERT':'#d62728','RAS+TERT+BRAF':'#9467bd'}
for cm, g in sub.groupby('comut_group'):
    if len(g)<3: continue
    kmf = KaplanMeierFitter()
    kmf.fit(g.followup_years, g.rec_any, label=f'{cm} (n={len(g)}, events={int(g.rec_any.sum())})')
    kmf.plot_survival_function(ax=ax, ci_show=True, color=colors.get(cm,'gray'))
ax.set_xlabel('Years from surgery')
ax.set_ylabel('Recurrence-free probability (any: path or imaging)')
ax.set_title(f'KM recurrence-free survival, ≥1y FU subset (n={n_sub})')
ax.legend(loc='lower left', fontsize=9)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f'{BASE}/figures/figure_6_km_recurrence.png', dpi=300)
plt.savefig(f'{BASE}/figures/figure_6_km_recurrence.svg')
plt.close()
print('Saved KM figure')

# ========== Sensitivity analyses ==========
print('\n=== SENSITIVITY ANALYSES ===')
sens_results = []
def sens(name, sub):
    if len(sub)<10: return
    # gene × malig (Fisher / χ²)
    sg = sub[sub.gene_single.isin(['NRAS','HRAS','KRAS'])]
    tab = pd.crosstab(sg.gene_single, sg.malig)
    if tab.shape==(3,2):
        chi2,p,dof,exp=stats.chi2_contingency(tab.values)
        sens_results.append({'sensitivity':name,'comparison':'Gene×Malig','n':len(sg),'p':p})
    # co-mut × malig
    cm_tab = pd.crosstab(sub.comut_group, sub.malig)
    if cm_tab.shape[0]>=2 and cm_tab.shape[1]==2:
        chi2,p,dof,exp=stats.chi2_contingency(cm_tab.values)
        sens_results.append({'sensitivity':name,'comparison':'Comut×Malig','n':len(sub),'p':p})

sens('1. Drop MTC', df[df.histology_final != 'MTC'])
dtc_only = df[df.histology_final.isin(['PTC','follicular carcinoma','NIFTP','FTUMP']) | df.histology_final.isna()]
sens('2. DTC-only (drop MTC/PDTC/HG)', dtc_only)
sens('3. Drop NIFTP+FTUMP', df[~df.histology_final.isin(['NIFTP','FTUMP'])])
sens('4a. ThyroSeq only', df[df.mol_platform.str.contains('ThyroSeq',na=False)])
sens('4b. Afirma only', df[df.mol_platform.str.contains('Afirma',na=False)])
df_complete = df.dropna(subset=['age','bethesda','gene_priority'])
sens('5. Complete cases', df_complete)

sens_df = pd.DataFrame(sens_results)
sens_df.to_csv(f'{BASE}/analysis/sensitivity_results.csv', index=False)
print(sens_df.to_string(index=False))

# ========== Figures ==========
print('\n=== FIGURES ===')

# Figure 1: Cohort flow (CONSORT)
fig, ax = plt.subplots(figsize=(10,8))
ax.set_xlim(0,10); ax.set_ylim(0,12)
ax.axis('off')
boxes = [
    (5, 11, 'Thyroid registry\nn = 10,871'),
    (5, 9.5, 'RAS+ surgical cohort (ras_positive_final = TRUE\nand surg_first_date IS NOT NULL)\nn = 292'),
    (1.5, 7, 'Malignant\nn = 176 (60.3%)\nPTC: 98, FTC: 67,\nMTC: 5, PDTC: 5, HG-DTC: 1'),
    (4.5, 7, 'Borderline\nn = 22 (7.5%)\nNIFTP: 17, FTUMP: 5'),
    (7.5, 7, 'Benign\nn = 94 (32.2%)'),
    (1.5, 4, 'ATA-2015: scored = 96\n(High 84, Int 12, Low 0)\nUncalculable: 85\nNot scored: 111'),
    (5, 4, 'ATA-2025: scored = 170\n(High 84, Int 85, Low 1)\nUncalculable: 11\nNot scored: 111'),
    (8.5, 4, 'Reclassification (n=181)\nUp: 79 (43.6%)\nSame: 64 (35.4%)\nDown: 38 (21.0%)'),
    (5, 1.5, 'Total thyroidectomy: 149 (51.0%)\nHemithyroidectomy: 142 (48.6%)\nIsthmusectomy: 1 (0.3%)\nLateral neck dissection: 7 (2.4%)'),
]
for x,y,text in boxes:
    ax.add_patch(plt.Rectangle((x-1.5, y-0.6), 3, 1.2, facecolor='#e7f0fa', edgecolor='#2c7fb8', linewidth=1.2))
    ax.text(x, y, text, ha='center', va='center', fontsize=8)
# arrows
for (x1,y1),(x2,y2) in [((5,10.4),(5,10.1)), ((5,8.9),(1.5,7.6)), ((5,8.9),(4.5,7.6)), ((5,8.9),(7.5,7.6)),
                         ((1.5,6.4),(1.5,4.6)), ((4.5,6.4),(5,4.6)), ((4.5,6.4),(8.5,4.6)),
                         ((5,3.4),(5,2.1))]:
    ax.annotate('', xy=(x2,y2), xytext=(x1,y1), arrowprops=dict(arrowstyle='->', color='#2c7fb8'))
ax.set_title('M098 — Cohort flow (CONSORT-style)', fontsize=11, fontweight='bold')
plt.savefig(f'{BASE}/figures/figure_1_cohort_flow.png', dpi=300, bbox_inches='tight')
plt.savefig(f'{BASE}/figures/figure_1_cohort_flow.svg', bbox_inches='tight')
plt.close()

# Figure 2: Co-mutation × ATA-2025 stacked bar (proportions)
fig, ax = plt.subplots(figsize=(8,6))
ata_df = df[df.ata_2025.isin(['low','intermediate','high','uncalculable'])]
ct = pd.crosstab(ata_df.comut_group, ata_df.ata_2025, normalize='index')*100
# Order: low, intermediate, high, uncalculable
order = [c for c in ['low','intermediate','high','uncalculable'] if c in ct.columns]
ct = ct[order]
ct.plot(kind='bar', stacked=True, ax=ax, color=['#2c7fb8','#ffd93d','#d62728','#999999'], width=0.7)
ax.set_ylabel('% of patients (in co-mutation group)')
ax.set_xlabel('Co-mutation group')
ax.set_title('Co-mutation group × ATA-2025 risk category')
ax.legend(title='ATA-2025', loc='upper right')
for i, cm in enumerate(ct.index):
    n = (ata_df.comut_group==cm).sum()
    ax.text(i, 102, f'n={n}', ha='center', fontsize=9)
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig(f'{BASE}/figures/figure_2_comut_ata25.png', dpi=300)
plt.savefig(f'{BASE}/figures/figure_2_comut_ata25.svg')
plt.close()

# Figure 3: ATA Reclassification Sankey-style flow
fig, ax = plt.subplots(figsize=(10,6))
# 2015 → 2025 flows (data from cross-tab in table 8)
flows = [
    ('high','high',46), ('high','intermediate',38),
    ('intermediate','high',5),('intermediate','intermediate',7),
    ('uncalculable','high',33),('uncalculable','intermediate',40),('uncalculable','low',1),('uncalculable','uncalculable',11),
]
src_y = {'high':3, 'intermediate':2, 'uncalculable':1}
dst_y = {'high':3,'intermediate':2,'low':1,'uncalculable':0.3}
color_map={'high':'#d62728','intermediate':'#ffd93d','uncalculable':'#999999','low':'#2c7fb8'}
for s,d,n in flows:
    ax.plot([0,1],[src_y[s], dst_y[d]], color=color_map[s], alpha=0.5, linewidth=n*0.6)
    ax.text(0.5, (src_y[s]+dst_y[d])/2, f'n={n}', fontsize=8, ha='center',
            bbox=dict(facecolor='white', edgecolor='gray', alpha=0.8))
for k,v in src_y.items():
    ax.text(-0.1, v, f'2015: {k}', ha='right', va='center', fontsize=10, fontweight='bold')
for k,v in dst_y.items():
    ax.text(1.1, v, f'2025: {k}', ha='left', va='center', fontsize=10, fontweight='bold')
ax.set_xlim(-1, 2)
ax.set_ylim(0, 4)
ax.axis('off')
ax.set_title('ATA 2015 (3-tier) → ATA 2025 (4-tier) Reclassification (n=181)\nUp: 79, Same: 64, Down: 38', fontsize=11)
plt.tight_layout()
plt.savefig(f'{BASE}/figures/figure_3_ata_reclassification.png', dpi=300)
plt.savefig(f'{BASE}/figures/figure_3_ata_reclassification.svg')
plt.close()

# Figure 4: Per-gene malignancy rates with 95% CI
fig, ax = plt.subplots(figsize=(7,5))
sg = df[df.gene_single.isin(['NRAS','HRAS','KRAS'])]
agg = []
for g in ['NRAS','HRAS','KRAS']:
    s = sg[sg.gene_single==g]
    n = len(s); k = s.malig.sum()
    if n>0:
        p = k/n
        # Wilson 95% CI
        z=1.96
        denom=1+z**2/n; centre=(p+z**2/(2*n))/denom
        margin = z*np.sqrt(p*(1-p)/n + z**2/(4*n**2))/denom
        lo, hi = max(0,centre-margin), min(1,centre+margin)
        agg.append((g,n,k,100*p,100*lo,100*hi))
ag = pd.DataFrame(agg, columns=['gene','n','k_malig','pct','ci_lo','ci_hi'])
xpos = np.arange(len(ag))
bars = ax.bar(xpos, ag.pct, color=['#4472c4','#ed7d31','#a5a5a5'], edgecolor='black')
ax.errorbar(xpos, ag.pct, yerr=[ag.pct-ag.ci_lo, ag.ci_hi-ag.pct], fmt='none', color='black', capsize=8)
ax.set_xticks(xpos); ax.set_xticklabels(ag.apply(lambda r: f'{r.gene}\n(n={r.n})', axis=1))
ax.set_ylabel('% malignant')
ax.set_ylim(0,100)
ax.set_title('Malignancy rate by RAS gene (single-gene–only patients)\nwith Wilson 95% CI')
for x, pct in zip(xpos, ag.pct):
    ax.text(x, pct+2, f'{pct:.0f}%', ha='center', fontsize=10, fontweight='bold')
plt.tight_layout()
plt.savefig(f'{BASE}/figures/figure_4_gene_malig_rate.png', dpi=300)
plt.savefig(f'{BASE}/figures/figure_4_gene_malig_rate.svg')
plt.close()

# Figure 5: VAF distribution by co-mutation group
fig, ax = plt.subplots(figsize=(8,5))
vaf_df = df.dropna(subset=['ras_vaf'])
groups = ['Isolated','RAS+BRAF','RAS+TERT','RAS+TERT+BRAF']
data = [vaf_df[vaf_df.comut_group==g].ras_vaf.values*100 for g in groups]
labels = [f'{g}\n(n={len(d)})' for g,d in zip(groups, data)]
bp = ax.boxplot(data, labels=labels, patch_artist=True, widths=0.55)
for patch, color in zip(bp['boxes'], ['#4472c4','#ed7d31','#a5a5a5','#9467bd']):
    patch.set_facecolor(color); patch.set_alpha(0.65)
ax.set_ylabel('Max RAS VAF (%)')
ax.set_title('Distribution of maximum RAS variant allele frequency\nby co-mutation group')
ax.set_ylim(0, 100)
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig(f'{BASE}/figures/figure_5_vaf_by_comut.png', dpi=300)
plt.savefig(f'{BASE}/figures/figure_5_vaf_by_comut.svg')
plt.close()

# Figure 7: Forest plot from Outcome 1 logistic
fig, ax = plt.subplots(figsize=(9,5))
try:
    logit_df = pd.read_csv(f'{BASE}/analysis/logit_Outcome1_Malignancy.csv', index_col=0)
    logit_df = logit_df[~logit_df.index.isin(['const'])]
    y = np.arange(len(logit_df))
    ax.errorbar(logit_df['OR'], y, xerr=[logit_df['OR']-logit_df['OR_lo'], logit_df['OR_hi']-logit_df['OR']],
                fmt='o', color='black', capsize=4, markersize=6)
    ax.axvline(1, color='red', linestyle='--', alpha=0.5)
    ax.set_yticks(y); ax.set_yticklabels(logit_df.index)
    ax.set_xlabel('Odds ratio (95% CI)')
    ax.set_xscale('log')
    ax.set_title('Multivariable logistic regression — Outcome: Malignancy')
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f'{BASE}/figures/figure_7_forest_malignancy.png', dpi=300)
    plt.savefig(f'{BASE}/figures/figure_7_forest_malignancy.svg')
    plt.close()
except Exception as e:
    print(f'Forest plot failed: {e}')

print('\nAll figures saved to', f'{BASE}/figures/')
import os
for f in sorted(os.listdir(f'{BASE}/figures/')):
    print(f' - {f}')
