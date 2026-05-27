"""M098 — RAS-Mutated Thyroid Surgical Cohort: full analysis pipeline."""
import json, os, sys
import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.stats.multitest import multipletests
import statsmodels.api as sm

BASE = '/sessions/stoic-practical-gates/mnt/outputs/studies/m098_ras_mutation_surgical_cohort'
SRC  = '/sessions/stoic-practical-gates/mnt/.claude/projects/-Users-lgm5maxmac-Library-Application-Support-Claude-local-agent-mode-sessions-46b48217-471c-440c-9c88-e09e16c0cdb0-59105ecc-e1bd-41b4-945f-690e25b3e108-local-7df05749-e874-46e2-930f-19b66e33c2f0-outp-4qa6hg/a5ff39bc-92ac-4921-a09f-2130b7a4fc27/tool-results/mcp-e9a2042f-4b89-4e45-b727-c411d9b438e2-execute_sql_readonly-1778717679950.txt'

COLS = [
    'research_id','age','sex','race',
    'histology_class','histology_final',
    'bethesda','bethesda_name','tirads','tumor_size',
    'total_thyroidectomy','procedure_type','lateral_neck',
    'mol_platform','mol_days_from_surg',
    'nras','hras','kras','braf','tert',
    'gene_priority','gene_single','comut_group',
    'ata_2015','ata_2025','recl_dir',
    'cap_inv','vasc_inv','vasc_vessel_count',
    'lvi_any','pni_any','ete_grade','ete_gross',
    'multifocal','margin_pos','r_class',
    'ln_examined','ln_positive_n','ln_largest_dep_cm','ln_positive',
    'ajcc_t','ajcc_n','ajcc_m','distant_mets',
    'followup_years','vital_status','survival_event',
    'rec_path_proven','rec_img_susp','rec_status','days_to_rec',
    'ras_vaf','ras_gene_v13','ras_variant_v13'
]

# ---------- Load data ----------
with open(SRC) as f:
    data = json.load(f)
rows = [r['f'][0]['v'].split('|') for r in data['rows']]
df = pd.DataFrame(rows, columns=COLS)

# Type conversions
to_num   = ['age','bethesda','tumor_size','mol_days_from_surg','vasc_vessel_count',
            'ln_examined','ln_positive_n','ln_largest_dep_cm','ln_positive',
            'followup_years','days_to_rec','ras_vaf']
to_bool  = ['total_thyroidectomy','lateral_neck','nras','hras','kras','braf','tert',
            'lvi_any','pni_any','ete_gross','multifocal','margin_pos','distant_mets',
            'survival_event','rec_path_proven','rec_img_susp']
for c in to_num:
    df[c] = pd.to_numeric(df[c], errors='coerce')
for c in to_bool:
    df[c] = df[c].map({'true':True,'false':False,'':np.nan})

# Derived
df['malignant']   = (df['histology_class']=='Malignant').astype(int)
df['borderline']  = (df['histology_class']=='Borderline').astype(int)
df['benign']      = (df['histology_class']=='Benign').astype(int)
df['malig_or_border'] = df['malignant'] | df['borderline']
df['ata25_intHigh'] = df['ata_2025'].map({'low':0,'intermediate':1,'high':1,'uncalculable':np.nan,'':np.nan})
df['any_comut']     = (df['comut_group']!='Isolated').astype(int)
df['multi_gene']    = (df['gene_single']=='MULTI').astype(int)

print(f'Loaded N={len(df)}')
print('Histology class distribution:', df['histology_class'].value_counts().to_dict())
print('Gene single distribution:', df['gene_single'].value_counts().to_dict())
print('Comut distribution:', df['comut_group'].value_counts().to_dict())
print('ATA-2015 distribution:', df['ata_2015'].value_counts().to_dict())
print('ATA-2025 distribution:', df['ata_2025'].value_counts().to_dict())
print('Recl direction:', df['recl_dir'].value_counts().to_dict())
print('Procedure:', df['procedure_type'].value_counts().to_dict())

# Save the analytic dataset
df.to_csv(f'{BASE}/data/m098_analytic.csv', index=False)
print(f'\nSaved analytic dataset: {BASE}/data/m098_analytic.csv  ({len(df.columns)} cols × {len(df)} rows)')
