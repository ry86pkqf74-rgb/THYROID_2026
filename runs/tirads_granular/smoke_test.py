"""Smoke test: send one real TIRADS note to the vLLM endpoint, print the full JSON response."""
import json, os, sys, pathlib, textwrap
import pandas as pd
import openai

BASE = pathlib.Path('/Users/loganglosser/THYROID_2026')
PROMPT = (BASE / 'llm_extraction/prompts/tirads_granular_extraction_v1.txt').read_text()

df = pd.read_parquet(BASE / 'processed/us_nodules_tirads.parquet')
# find a row that has a real us_1 body and impression and at least one nodule
mask = df['us_1'].astype(str).str.len().gt(200) & df['us_1_impression'].astype(str).str.len().gt(50) & df['nodule_1'].astype(str).str.len().gt(30)
sample = df[mask].head(1).iloc[0]

note_parts = [str(sample['us_1'])]
note_parts.append('\n\nIMPRESSION:\n' + str(sample['us_1_impression']))
nodule_cols = [f'nodule_{i}' for i in range(1, 11)] + [f'n{i}' for i in range(11, 15)]
tr_cols = [f'n{i}_tr' for i in range(1, 15)]
nodule_lines = []
for nc, tc in zip(nodule_cols, tr_cols):
    n = sample.get(nc)
    t = sample.get(tc)
    if pd.notna(n) and str(n).strip():
        nodule_lines.append(f'- {nc}: {n} (TR: {t})')
if nodule_lines:
    note_parts.append('\n\nNODULE BREAKDOWN:\n' + '\n'.join(nodule_lines))
note_text = '\n'.join(note_parts)

print('=== INPUT ROW ===', flush=True)
print('research_id:', sample['research_id'])
print('us_1_date:', sample.get('us_1_date'))
print('note_text len:', len(note_text))
print('note_text preview:')
print(textwrap.indent(note_text[:1200], '  '))
print('...' if len(note_text) > 1200 else '')
print()

client = openai.OpenAI(base_url=os.environ['VLLM_URL'], api_key='EMPTY')
resp = client.chat.completions.create(
    model='qwen2.5-32b',
    messages=[
        {'role': 'system', 'content': '/no_think\n' + PROMPT},
        {'role': 'user', 'content': note_text[:6000]},
    ],
    temperature=0.0,
    max_tokens=1500,
    response_format={'type': 'json_object'},
)

print('=== RAW RESPONSE ===', flush=True)
content = resp.choices[0].message.content
print(content)
print()
print('=== PARSED JSON ===', flush=True)
try:
    parsed = json.loads(content)
    print(json.dumps(parsed, indent=2)[:5000])
    print()
    print('=== SUMMARY ===')
    ents = parsed.get('entities', [])
    print('n_entities:', len(ents))
    by_type = {}
    for e in ents:
        by_type.setdefault(e.get('entity_type'), []).append(e.get('entity_value'))
    for t, vs in sorted(by_type.items()):
        print(f'  {t}: {vs}')
except Exception as e:
    print('JSON PARSE FAILED:', e)

print('\n=== USAGE ===')
print('prompt_tokens:', resp.usage.prompt_tokens)
print('completion_tokens:', resp.usage.completion_tokens)
print('total_tokens:', resp.usage.total_tokens)
