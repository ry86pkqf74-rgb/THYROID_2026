import json, pandas as pd
from pathlib import Path
p = Path('/Users/loganglosser/THYROID_2026/runs/tirads_granular/smoke3_output/note_entities_llm_tirads_granular.ckpt.jsonl')
lines = p.read_text().splitlines()
print(f'=== {len(lines)} records ===\n')
for i, line in enumerate(lines, 1):
    obj = json.loads(line)
    rj = obj.get('result_json') or ''
    note_id = obj.get('note_row_id')
    try:
        parsed = json.loads(rj)
        ents = parsed.get('entities', [])
        parse_err = parsed.get('parse_error', False)
    except Exception as e:
        parsed, ents, parse_err = None, [], f'outer parse err: {e}'
    status = 'PARSE_ERR' if parse_err else 'OK'
    print(f'--- record {i}: {note_id}  date={obj.get("note_date")}  status={status}  n_entities={len(ents)} ---')
    if parse_err:
        print('  parse_error raw (first 400):', rj[:400])
    else:
        # counts by entity_type
        by_type = {}
        for e in ents:
            by_type.setdefault(e['entity_type'], []).append(e.get('entity_value'))
        for t, vals in sorted(by_type.items()):
            print(f'  {t}: {vals}')
    print()
