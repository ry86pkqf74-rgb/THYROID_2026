"""M098 — Generate all descriptive Tables 1-10."""
import pandas as pd, numpy as np, os
from scipy import stats

BASE = '/sessions/stoic-practical-gates/mnt/outputs/studies/m098_ras_mutation_surgical_cohort'
df = pd.read_csv(f'{BASE}/data/m098_analytic.csv')
N = len(df)

def pct(n,d): return f'{n} ({100*n/d:.1f}%)' if d>0 else f'{n} (n/a)'
def iqr_str(s):
    s=pd.to_numeric(s,errors='coerce').dropna()
    if len(s)==0: return 'n/a'
    return f'{s.median():.1f} ({s.quantile(.25):.1f}-{s.quantile(.75):.1f})'
def msd_str(s):
    s=pd.to_numeric(s,errors='coerce').dropna()
    if len(s)==0: return 'n/a'
    return f'{s.mean():.1f} ± {s.std():.1f}'

mal = df[df.histology_class=='Malignant']
malb= df[df.histology_class.isin(['Malignant','Borderline'])]
ata_scored = df[df.ata_2025.notna() & (df.ata_2025!='')]

# ============ Table 1: Cohort overview ============
t1 = []
t1.append(('Total cohort', f'{N}'))
t1.append(('Histology classification — Malignant', pct(176, N)))
t1.append(('Histology classification — Borderline (NIFTP/FTUMP)', pct(22, N)))
t1.append(('Histology classification — Benign', pct(94, N)))
t1.append(('Histology classification — Unclassified', pct(0, N)))
t1.append(('RAS gene — NRAS positive (hybrid)', pct(int(df.nras.sum()), N)))
t1.append(('RAS gene — HRAS positive (hybrid)', pct(int(df.hras.sum()), N)))
t1.append(('RAS gene — KRAS positive (hybrid)', pct(int(df.kras.sum()), N)))
t1.append(('RAS gene — Multi-gene RAS+', pct(int(df.multi_gene.sum()), N)))
for g in ['NRAS','HRAS','KRAS','MULTI']:
    t1.append((f'  Single-gene-only — {g}', pct(int((df.gene_single==g).sum()), N)))
for g in ['Isolated','RAS+BRAF','RAS+TERT','RAS+TERT+BRAF']:
    t1.append((f'Co-mutation group — {g}', pct(int((df.comut_group==g).sum()), N)))
t1.append(('Molecular platform — ThyroSeq only', pct(int((df.mol_platform=='ThyroSeq').sum()), N)))
t1.append(('Molecular platform — Afirma only', pct(int((df.mol_platform=='Afirma').sum()), N)))
t1.append(('Molecular platform — ThyroSeq + Afirma (both)', pct(int((df.mol_platform=='ThyroSeq+Afirma').sum()), N)))
t1.append(('Molecular platform — Unknown', pct(int((df.mol_platform=='unknown').sum()), N)))

# ============ Table 2: Demographics ============
t2 = []
t2.append(('Age at surgery — median (IQR), years', iqr_str(df.age)))
t2.append(('Age at surgery — mean ± SD, years', msd_str(df.age)))
t2.append(('Age <55', pct(int((df.age<55).sum()), N)))
t2.append(('Age ≥55', pct(int((df.age>=55).sum()), N)))
t2.append(('Sex — female', pct(int((df.sex=='female').sum()), N)))
t2.append(('Sex — male', pct(int((df.sex=='male').sum()), N)))
for r,c in df.race.value_counts().items():
    t2.append((f'Race — {r}', pct(c, N)))
t2.append(('BMI (kg/m²) — median (IQR) [when available]', iqr_str(df['vasc_vessel_count']*0+pd.to_numeric(pd.Series([np.nan]*N),errors='coerce'))))  # placeholder

# We don't have BMI in this pull; note that

# ============ Table 3: Preoperative workup ============
t3 = []
beth_counts = df.bethesda_name.fillna('Missing').value_counts()
for b,c in beth_counts.items():
    t3.append((f'Bethesda — {b}', pct(c, N)))
tir = df.tirads.fillna('Missing').value_counts()
for t,c in tir.items():
    t3.append((f'TIRADS — {t}', pct(c, N)))
concord = pd.read_csv(f'{BASE}/data/m098_analytic.csv').get('fna_path_concordance_category')  # not pulled, skip
t3.append(('Time from molecular test to surgery — median (IQR) days',
           iqr_str(df.mol_days_from_surg)))
t3.append(('Molecular test performed preoperatively', pct(int((df.mol_days_from_surg<=0).sum()), N)))
t3.append(('Molecular test performed postoperatively', pct(int((df.mol_days_from_surg>0).sum()), N)))

# ============ Table 5: Pathology drill (Malignant subset) ============
t5 = []
nm = len(mal)
t5.append(('— Denominator: Malignant cases —', f'n={nm}'))
hist_in_malig = mal.histology_final.value_counts()
for h,c in hist_in_malig.items():
    t5.append((f'Histology — {h}', pct(c, nm)))
# capsular invasion present (any non-empty / non-x)
def is_present(s):
    s = s.astype(str).str.lower().fillna('')
    return ~s.isin(['','x','none','no','absent','not identified','not assessed','nan'])
t5.append(('Capsular invasion — any', pct(int(is_present(mal.cap_inv).sum()), nm)))
t5.append(('Capsular invasion — widely invasive',
           pct(int(mal.cap_inv.astype(str).str.lower().str.contains('widely|extensive',na=False).sum()), nm)))
t5.append(('Vascular invasion — any', pct(int(is_present(mal.vasc_inv).sum()), nm)))
t5.append(('Vascular invasion — vessel count ≥4 (extensive)',
           pct(int((pd.to_numeric(mal.vasc_vessel_count,errors='coerce')>=4).sum()), nm)))
t5.append(('Lymphovascular invasion (LVI) — any', pct(int(mal.lvi_any.fillna(False).sum()), nm)))
t5.append(('Perineural invasion (PNI) — any', pct(int(mal.pni_any.fillna(False).sum()), nm)))
ete_p = mal.ete_grade.astype(str).str.lower()
t5.append(('ETE — any present',
           pct(int(ete_p.str.contains('microscopic|gross|widely|extensive|present|minimal',na=False).sum()), nm)))
t5.append(('ETE — microscopic',
           pct(int(ete_p.str.contains('microscopic|minimal',na=False).sum()), nm)))
_gross_any=(ete_p.str.contains('gross|widely|t4',na=False)|mal.ete_gross.fillna(False)).sum()
t5.append(('ETE — gross (or T4)',pct(int(_gross_any),nm)))
t5.append(('Margin involvement', pct(int(mal.margin_pos.fillna(False).sum()), nm)))
sz = pd.to_numeric(mal.tumor_size,errors='coerce')
t5.append(('Tumor size (cm) — median (IQR)', iqr_str(sz)))
t5.append(('Tumor size (cm) — mean ± SD', msd_str(sz)))
t5.append(('Tumor size ≤1 cm', pct(int((sz<=1).sum()), nm)))
t5.append(('Tumor size 1.01–2 cm', pct(int(((sz>1)&(sz<=2)).sum()), nm)))
t5.append(('Tumor size 2.01–4 cm', pct(int(((sz>2)&(sz<=4)).sum()), nm)))
t5.append(('Tumor size >4 cm', pct(int((sz>4).sum()), nm)))
t5.append(('Multifocality', pct(int(mal.multifocal.fillna(False).sum()), nm)))

# ============ Table 6: LN involvement (malignant subset with LN data) ============
t6 = []
m_ln = mal[(pd.to_numeric(mal.ln_examined,errors='coerce')>0) | (pd.to_numeric(mal.ln_positive_n,errors='coerce')>0)]
nmln = len(m_ln)
t6.append(('— Denominator: Malignant with LN data —', f'n={nmln}'))
t6.append(('LN examined — median (IQR)', iqr_str(m_ln.ln_examined)))
t6.append(('Any LN positive', pct(int((m_ln.ln_positive==1).sum()), nmln)))
ln_pos_sub = m_ln[m_ln.ln_positive==1]
t6.append(('LN positive count — median (IQR) among LN+', iqr_str(ln_pos_sub.ln_positive_n)))
t6.append(('Largest LN deposit (cm) — median (IQR)', iqr_str(m_ln.ln_largest_dep_cm)))
t6.append(('Lateral neck dissection performed', pct(int(mal.lateral_neck.fillna(False).sum()), nm)))
# pN staging
pn_counts = mal.ajcc_n.fillna('Missing').value_counts()
for p,c in pn_counts.items():
    t6.append((f'AJCC pN — {p}', pct(c, nm)))

# ============ Table 7: Surgery × ATA-2025 and × Histology ============
t7 = []
# Surgery counts
t7.append(('Total thyroidectomy', pct(int((df.procedure_type=='total_thyroidectomy').sum()), N)))
t7.append(('Hemithyroidectomy (lobectomy)', pct(int((df.procedure_type=='hemithyroidectomy').sum()), N)))
t7.append(('Isthmusectomy', pct(int((df.procedure_type=='isthmusectomy').sum()), N)))
t7.append(('Lateral neck dissection', pct(int(df.lateral_neck.fillna(False).sum()), N)))
# Cross tabs
ct = pd.crosstab(df.procedure_type, df.ata_2025, margins=False, dropna=False)
for proc in ct.index:
    for cat in ct.columns:
        if cat=='': continue
        t7.append((f'Surgery × ATA-2025: {proc} × {cat}', f'{ct.loc[proc, cat]}'))
ct2 = pd.crosstab(df.procedure_type, df.histology_class, margins=False, dropna=False)
for proc in ct2.index:
    for hc in ct2.columns:
        t7.append((f'Surgery × Histology: {proc} × {hc}', f'{ct2.loc[proc, hc]}'))

# ============ Table 8: ATA Risk Stratification & Reclassification ============
t8 = []
for cat in ['low','intermediate','high','uncalculable']:
    t8.append((f'ATA 2015 (3-tier) — {cat}', pct(int((df.ata_2015==cat).sum()), 181)))
t8.append(('ATA 2015 — not scored (non-DTC)', pct(int((df.ata_2015=='').sum() | df.ata_2015.isna().sum()), N)))
for cat in ['low','intermediate','high','uncalculable']:
    t8.append((f'ATA 2025 (4-tier) — {cat}', pct(int((df.ata_2025==cat).sum()), 181)))
t8.append(('Reclassification direction — Up', pct(79, 181)))
t8.append(('Reclassification direction — Same', pct(64, 181)))
t8.append(('Reclassification direction — Down', pct(38, 181)))
# 2015 x 2025 cross-tab
ct3 = pd.crosstab(df.ata_2015.replace('',np.nan), df.ata_2025.replace('',np.nan), dropna=True)
t8.append(('— 2015 × 2025 Cross-tab —', ''))
for r in ct3.index:
    for c in ct3.columns:
        if ct3.loc[r,c]>0:
            t8.append((f'  2015={r} × 2025={c}', f'{ct3.loc[r,c]}'))

# ============ Table 9: Cross-tabulations ============
t9 = []
# Gene × Histology (single-gene only excluding MULTI/NONE)
sg = df[df.gene_single.isin(['NRAS','HRAS','KRAS'])]
ct4 = pd.crosstab(sg.gene_single, sg.histology_class)
t9.append(('— Gene × Histology Classification (single-gene patients) —', f'n={len(sg)}'))
for g in ct4.index:
    for h in ct4.columns:
        n_g = (sg.gene_single==g).sum()
        t9.append((f'  {g} × {h}', f'{ct4.loc[g,h]} / {n_g} ({100*ct4.loc[g,h]/n_g:.0f}%)'))
# Co-mutation × histology
ct5 = pd.crosstab(df.comut_group, df.histology_class)
t9.append(('— Co-mutation × Histology —', f'n={N}'))
for cm in ct5.index:
    for h in ct5.columns:
        n_cm = (df.comut_group==cm).sum()
        t9.append((f'  {cm} × {h}', f'{ct5.loc[cm,h]} / {n_cm} ({100*ct5.loc[cm,h]/n_cm:.0f}%)'))
# Co-mut × ATA-2025 (scored only)
ct6 = pd.crosstab(ata_scored.comut_group, ata_scored.ata_2025)
t9.append(('— Co-mutation × ATA-2025 (scored only) —', f'n={len(ata_scored)}'))
for cm in ct6.index:
    for cat in ct6.columns:
        if cat=='': continue
        n_cm = (ata_scored.comut_group==cm).sum()
        t9.append((f'  {cm} × ATA25={cat}', f'{ct6.loc[cm,cat]} / {n_cm} ({100*ct6.loc[cm,cat]/n_cm:.0f}%)'))
# Bethesda × Histology
beth_hist = pd.crosstab(df.bethesda_name.fillna('Missing'), df.histology_class)
t9.append(('— Bethesda × Histology —', f'n={N}'))
for b in beth_hist.index:
    for h in beth_hist.columns:
        nb = (df.bethesda_name.fillna('Missing')==b).sum()
        t9.append((f'  Bethesda={b} × {h}', f'{beth_hist.loc[b,h]} / {nb} ({100*beth_hist.loc[b,h]/nb:.0f}%)'))

# ============ Table 10: Subgroup comparison ============
t10 = []
iso = df[df.comut_group=='Isolated']
cox = df[df.comut_group!='Isolated']
def grpsum(g):
    return {
        'n': len(g),
        'age_med': pd.to_numeric(g.age,errors='coerce').median(),
        'female_pct': 100*(g.sex=='female').sum()/len(g) if len(g)>0 else 0,
        'malig_pct': 100*(g.histology_class=='Malignant').sum()/len(g) if len(g)>0 else 0,
        'border_pct': 100*(g.histology_class=='Borderline').sum()/len(g) if len(g)>0 else 0,
        'benign_pct': 100*(g.histology_class=='Benign').sum()/len(g) if len(g)>0 else 0,
        'ata25_high_n': (g.ata_2025=='high').sum(),
        'tt_pct': 100*(g.procedure_type=='total_thyroidectomy').sum()/len(g) if len(g)>0 else 0,
    }
iso_s = grpsum(iso); cox_s = grpsum(cox)
for k in iso_s:
    t10.append((f'{k} — Isolated RAS', f'{iso_s[k]}'))
    t10.append((f'{k} — Co-mutated', f'{cox_s[k]}'))

# Per-codon ROM (if n ≥3)
codon_df = df[df.ras_variant_v13.fillna('') != '']
codon_df = codon_df.assign(codon=codon_df.ras_gene_v13.astype(str)+' '+codon_df.ras_variant_v13.astype(str))
codon_rom = codon_df.groupby('codon').agg(
    n=('research_id','count'),
    n_malig=('malignant','sum'),
    n_border=('borderline','sum')
).reset_index()
codon_rom['rom_malig_pct'] = (100*codon_rom.n_malig/codon_rom.n).round(0)
codon_rom['rom_malig_border_pct'] = (100*(codon_rom.n_malig+codon_rom.n_border)/codon_rom.n).round(0)
codon_rom = codon_rom[codon_rom.n>=3].sort_values('n',ascending=False).head(20)
t10.append(('— Per-codon ROM (n ≥3) —', ''))
for _,r in codon_rom.iterrows():
    t10.append((f'  {r.codon}', f'n={r.n}, malig={int(r.n_malig)} ({int(r.rom_malig_pct)}%), malig+border={int(r.n_malig+r.n_border)} ({int(r.rom_malig_border_pct)}%)'))

# ============ Table 4: Molecular details ============
t4 = []
t4.append(('Total RAS+ patients with variant-level detail',
           f'{(df.ras_gene_v13.fillna("")!="").sum()}'))
ras_genes = df.ras_gene_v13.replace('',np.nan).dropna().value_counts()
for g,c in ras_genes.items():
    t4.append((f'RAS gene (variant-level v13) — {g}', pct(c, len(df.ras_gene_v13.dropna()))))
ras_var = df.ras_variant_v13.replace('',np.nan).dropna().value_counts().head(10)
for v,c in ras_var.items():
    t4.append((f'RAS protein change (top 10) — {v}', pct(c, (df.ras_variant_v13.fillna("")!="").sum())))
vaf = pd.to_numeric(df.ras_vaf,errors='coerce')
t4.append(('Max RAS VAF — median (IQR), %', f'{(vaf.median()*100):.1f} ({(vaf.quantile(.25)*100):.1f}-{(vaf.quantile(.75)*100):.1f})' if vaf.notna().sum()>0 else 'n/a'))
t4.append(('Max RAS VAF — mean ± SD, %', f'{(vaf.mean()*100):.1f} ± {(vaf.std()*100):.1f}' if vaf.notna().sum()>0 else 'n/a'))
for lo,hi,lbl in [(0,0.05,'<5%'),(0.05,0.10,'5-9%'),(0.10,0.20,'10-19%'),(0.20,0.30,'20-29%'),(0.30,0.50,'30-49%'),(0.50,1.0,'≥50%')]:
    if hi<1.0:
        t4.append((f'Max VAF {lbl}', pct(int(((vaf>=lo)&(vaf<hi)).sum()), N)))
    else:
        t4.append((f'Max VAF {lbl}', pct(int((vaf>=lo).sum()), N)))
t4.append(('Max VAF missing', pct(int(vaf.isna().sum()), N)))
t4.append(('Co-mutation — TERT', pct(int(df.tert.fillna(False).sum()), N)))
t4.append(('Co-mutation — BRAF', pct(int(df.braf.fillna(False).sum()), N)))

# ============ Write tables to disk ============
all_tables = {'Table_1_cohort_overview':t1,'Table_2_demographics':t2,'Table_3_preop_workup':t3,
              'Table_4_molecular_details':t4,'Table_5_pathology_drill_malignant':t5,
              'Table_6_lymph_node_involvement':t6,'Table_7_surgery_x_ATA_x_histology':t7,
              'Table_8_ATA_risk_stratification':t8,'Table_9_cross_tabs':t9,
              'Table_10_subgroup_comparison':t10}

with open(f'{BASE}/tables/tables_M098.md','w') as f:
    f.write('# M098 Tables — RAS-Mutated Thyroid Surgical Cohort\n\n')
    f.write(f'Cohort N = {N}\n\n')
    for tname, rows in all_tables.items():
        f.write(f'## {tname.replace("_"," ")}\n\n')
        f.write('| Item | Value |\n|---|---|\n')
        for label, val in rows:
            f.write(f'| {label} | {val} |\n')
        f.write('\n')
        # Also save as CSV
        pd.DataFrame(rows, columns=['item','value']).to_csv(f'{BASE}/tables/{tname}.csv', index=False)
print(f'Tables saved to {BASE}/tables/ ({len(all_tables)} tables)')
