import pandas as pd, json
df = pd.read_parquet('/Users/loganglosser/THYROID_2026/processed/us_nodules_tirads.parquet')
out = {
    'rows': len(df),
    'cols': list(df.columns),
    'has_note_text': 'note_text' in df.columns,
    'has_note_row_id': 'note_row_id' in df.columns,
    'has_research_id': 'research_id' in df.columns,
    'dtypes': {c: str(df[c].dtype) for c in df.columns},
}
if 'note_text' in df.columns:
    s = df['note_text'].dropna().astype(str)
    out['note_text_nonnull'] = int(len(s))
    out['note_text_mean_chars'] = int(s.str.len().mean()) if len(s) else 0
    out['note_text_max_chars'] = int(s.str.len().max()) if len(s) else 0
    out['note_text_sample'] = s.iloc[0][:500] if len(s) else ''
print(json.dumps(out, indent=2, default=str))
