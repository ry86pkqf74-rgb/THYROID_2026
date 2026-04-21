"""Replay the driver's exact call for 1886_us1 to see finish_reason + full output."""
import json, pandas as pd, openai, os
from pathlib import Path

BASE = Path('/Users/loganglosser/THYROID_2026')
PROMPT = (BASE / 'llm_extraction/prompts/tirads_granular_extraction_v1.txt').read_text()

df = pd.read_parquet(BASE / 'processed/remaining/tirads_us_reports_smoke3.parquet')
for _, r in df.iterrows():
    print(f"\n========== {r['note_row_id']} ({len(r['note_text'])} chars) ==========")
    client = openai.OpenAI(base_url=os.environ['VLLM_URL'], api_key='EMPTY')
    resp = client.chat.completions.create(
        model='qwen2.5-32b',
        messages=[
            {'role':'system','content':'/no_think\n' + PROMPT},
            {'role':'user','content': r['note_text'][:6000]},
        ],
        temperature=0,
        max_tokens=1500,
        response_format={'type':'json_object'},
    )
    ch = resp.choices[0]
    print(f"finish_reason: {ch.finish_reason}")
    print(f"usage: prompt={resp.usage.prompt_tokens} completion={resp.usage.completion_tokens}")
    content = ch.message.content or ''
    print(f"content length: {len(content)}")
    # try parse
    try:
        parsed = json.loads(content)
        ents = parsed.get('entities', [])
        print(f"PARSED OK  n_entities={len(ents)}")
    except json.JSONDecodeError as e:
        print(f"PARSE FAILED: {e}")
        print("LAST 400 chars of response:")
        print(repr(content[-400:]))
