"""M098 §6 variant-level cleanup pipeline.
§6.1 — drop biologically impossible gene-protein pairs
§6.2 — drop variants where source raw text contradicts the call (negation regex within ±100 chars)
§6.3 — normalize OCR errors in the protein column
§6.4 — recover variants from raw text where the parser dropped them
"""
import json, re, os
import pandas as pd

SRC = '/sessions/stoic-practical-gates/mnt/.claude/projects/-Users-lgm5maxmac-Library-Application-Support-Claude-local-agent-mode-sessions-46b48217-471c-440c-9c88-e09e16c0cdb0-59105ecc-e1bd-41b4-945f-690e25b3e108-local-7df05749-e874-46e2-930f-19b66e33c2f0-outp-4qa6hg/a5ff39bc-92ac-4921-a09f-2130b7a4fc27/tool-results/mcp-e9a2042f-4b89-4e45-b727-c411d9b438e2-execute_sql_readonly-1778720481252.txt'
BASE = '/sessions/stoic-practical-gates/mnt/outputs/studies/m098_ras_mutation_surgical_cohort'

COLS = ['research_id','molecular_episode_id','test_date_native','platform','bethesda_category',
        'overall_result_class','rom_descriptor','rom_percent_point',
        'gene','protein','cdna','af_pct','source_call','gene_mutations_raw']

with open(SRC) as f:
    data = json.load(f)
rows = []
for r in data['rows']:
    v = r['f'][0]['v']
    parts = v.split('|', maxsplit=len(COLS)-1)
    while len(parts) < len(COLS): parts.append('')
    rows.append(parts)
df = pd.DataFrame(rows, columns=COLS)
df['af_pct'] = pd.to_numeric(df['af_pct'], errors='coerce')
df['rom_percent_point'] = pd.to_numeric(df['rom_percent_point'], errors='coerce')
print(f'Raw variant rows pulled: {len(df)}')
print(f'Distinct patients with variant data: {df["research_id"].nunique()}')
print(f'Gene distribution (raw):'); print(df['gene'].value_counts().head(15).to_string())

# Mark all rows with status flags
df['drop_reason'] = ''
df['protein_norm_status'] = ''
df['protein_normalized'] = df['protein'].astype(str)

# §6.3 — OCR normalization map (applied first so impossibility filter sees the cleaned protein)
ocr_map = [
    (re.compile(r'^p\.?O61R$', re.I), 'p.Q61R'),            # O→Q
    (re.compile(r'^p\.?Q6IR$', re.I), 'p.Q61R'),            # I→1
    (re.compile(r'^p\.?Q[BGS]1R$', re.I), 'p.Q61R'),        # B/G/S→6
    (re.compile(r'^p\.?QG1Kc?$', re.I), 'p.Q61K'),          # G→6, drop trailing c
    (re.compile(r'^pG[iI]3R$', re.I), 'p.Q13R'),            # G→Q, I→1
    (re.compile(r'^pQ61K$', re.I), 'p.Q61K'),               # insert missing dot
    (re.compile(r'^pQ61R$', re.I), 'p.Q61R'),               # insert missing dot
    (re.compile(r'^pG12$', re.I), 'TRUNCATED'),             # flag truncated
]
def normalize_protein(p):
    p = (p or '').strip()
    if not p: return p, ''
    for pat, repl in ocr_map:
        if pat.match(p):
            if repl == 'TRUNCATED':
                return p, 'truncated_AA_missing'
            return repl, 'OCR_normalized'
    return p, ''

norm_results = df['protein'].apply(normalize_protein)
df['protein_normalized'] = norm_results.apply(lambda x: x[0])
df.loc[norm_results.apply(lambda x: x[1]) != '', 'protein_norm_status'] = norm_results.apply(lambda x: x[1])
print(f'\n§6.3 OCR normalization: {(df["protein_norm_status"]=="OCR_normalized").sum()} rows normalized; {(df["protein_norm_status"]=="truncated_AA_missing").sum()} flagged truncated')

# §6.1 — Biologically impossible gene-protein pairs
def is_impossible(gene, protein):
    g = (gene or '').upper().strip()
    p = (protein or '').upper().strip()
    if not g or not p: return False
    # RAS family should not have BRAF V600 / TERT C228/C250
    if g in ('NRAS','HRAS','KRAS'):
        if 'V600' in p or 'C228' in p or 'C250' in p:
            return True
    # BRAF should not have Q61/G12/G13 (RAS codons)
    if g == 'BRAF':
        if 'Q61' in p or 'G12' in p or 'G13' in p:
            return True
    # TERT should be promoter only (C228/C250); flag protein not matching
    if g == 'TERT':
        if 'C228' not in p and 'C250' not in p:
            return True
    return False

mask_impossible = df.apply(lambda r: is_impossible(r['gene'], r['protein_normalized']), axis=1)
df.loc[mask_impossible, 'drop_reason'] += 'parser_bug_impossible;'
print(f'§6.1 impossible gene-protein pairs flagged: {int(mask_impossible.sum())}')

# §6.2 — Negation cue filter within ±100 char window of gene mention
NEG_PATTERNS = [
    re.compile(r'(?:\b|^)(negative|not detected|not identified|undetected)\b', re.I),
    re.compile(r'(?:\b|^)(not ordered|not tested|no result|test not)\b', re.I),
    re.compile(r'(?:\b|^)(negative for|no clinically significant|none of the|absence of|free of)\b', re.I),
]
POS_PATTERNS = [
    re.compile(r'\b(positive|detected|identified|present)\b', re.I),
]
def window_contradicts(gene_text, raw):
    if not raw or not gene_text: return False
    raw_u = raw.upper()
    g = (gene_text or '').upper().strip()
    if not g: return False
    idx = raw_u.find(g)
    if idx == -1: return False
    win = raw[max(0,idx-100):idx+100+len(g)]
    has_neg = any(p.search(win) for p in NEG_PATTERNS)
    has_pos = any(p.search(win) for p in POS_PATTERNS)
    return has_neg and not has_pos

mask_neg = df.apply(lambda r: window_contradicts(r['gene'], r['gene_mutations_raw']), axis=1)
df.loc[mask_neg, 'drop_reason'] += 'negation_in_source;'
print(f'§6.2 negation-cue contradictions flagged: {int(mask_neg.sum())}')

# §6.4 — Recover variants from raw text for rows that were parser-bug dropped
# (Conservative: only for §6.1-dropped rows, look for explicit Q61R/Q61K/G12X patterns in raw)
RAS_CODON_RE = re.compile(r'\b(NRAS|HRAS|KRAS)\b[^a-z0-9]{1,30}(p\.?\s*(?:Q61[RK]|G1[23][A-Z]|Q22K))', re.I)
def recover_from_raw(gene, raw):
    if not raw: return None
    m = RAS_CODON_RE.search(raw)
    if not m: return None
    cg = m.group(1).upper()
    cp = m.group(2).replace(' ', '').upper()
    if not cp.startswith('P.'):
        cp = 'P.' + cp.lstrip('P').lstrip('.').upper()
    return f'{cg}|{cp}'

recovered = 0
for i, r in df[mask_impossible].iterrows():
    rec = recover_from_raw(r['gene'], r['gene_mutations_raw'])
    if rec:
        cg, cp = rec.split('|')
        df.at[i, 'gene'] = cg
        df.at[i, 'protein_normalized'] = cp.title().replace('P.','p.')
        df.at[i, 'drop_reason'] = ''   # un-drop the row
        df.at[i, 'protein_norm_status'] = 'recovered_from_raw'
        recovered += 1
print(f'§6.4 recovered from raw text: {recovered} rows')

# Final analytic flag
df['analytic_keep'] = df['drop_reason'] == ''
print(f'\nFinal: analytic_keep = {int(df["analytic_keep"].sum())} of {len(df)} rows ({100*df["analytic_keep"].mean():.1f}%)')
print('\nKept gene distribution:')
print(df[df.analytic_keep]['gene'].value_counts().head(15).to_string())
print('\nTop kept protein changes (top 15):')
print(df[df.analytic_keep & df.protein_normalized.ne('')]['protein_normalized'].value_counts().head(15).to_string())

# Save variant-level long table
out_cols = ['research_id','molecular_episode_id','test_date_native','platform','bethesda_category',
            'overall_result_class','rom_descriptor','rom_percent_point',
            'gene','protein','protein_normalized','protein_norm_status','cdna','af_pct','source_call',
            'drop_reason','analytic_keep']
df_out = df[out_cols].copy()
df_out.to_csv(f'{BASE}/data/m098_variant_long.csv', index=False)
print(f'\nSaved: {BASE}/data/m098_variant_long.csv ({len(df_out)} rows × {len(out_cols)} cols)')

# QC summary
qc_summary = {
    'raw_rows_pulled': len(df),
    'distinct_patients_with_variants': int(df['research_id'].nunique()),
    'ocr_normalized': int((df['protein_norm_status']=='OCR_normalized').sum()),
    'truncated_AA_missing': int((df['protein_norm_status']=='truncated_AA_missing').sum()),
    'impossible_gene_protein': int(mask_impossible.sum()),
    'negation_contradicted': int(mask_neg.sum()),
    'recovered_from_raw': recovered,
    'final_analytic_kept': int(df['analytic_keep'].sum())
}
pd.Series(qc_summary).to_csv(f'{BASE}/data/m098_variant_long_qc_summary.csv', header=False)
print('\nQC summary:')
for k,v in qc_summary.items(): print(f'  {k}: {v}')
