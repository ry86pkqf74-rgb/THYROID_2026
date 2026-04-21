import json
from pathlib import Path
ckpt = Path("runs/tirads_granular/smoke3_output/note_entities_llm_tirads_granular.ckpt.jsonl")
print(f"file: {ckpt}  size={ckpt.stat().st_size}B  lines={sum(1 for _ in ckpt.open())}")
print("=" * 90)
for i, line in enumerate(ckpt.read_text().splitlines()):
    rec = json.loads(line)
    rid = rec.get("note_row_id")
    raw = rec.get("result_json") or ""
    parsed = None
    parse_err = None
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError as e:
        parse_err = str(e)
    print(f"\n--- REC {i}: {rid} ---")
    print(f"  result_json_len={len(raw)}")
    if parse_err:
        print(f"  *** PARSE ERROR: {parse_err}")
        print(f"  raw[:400]: {raw[:400]!r}")
        print(f"  raw[-200:]: {raw[-200:]!r}")
        continue
    if isinstance(parsed, dict) and parsed.get("parse_error"):
        print(f"  *** DRIVER FLAGGED parse_error")
        print(f"  raw[:400]: {(parsed.get('raw') or '')[:400]}")
        continue
    n_total = 0
    if isinstance(parsed, dict):
        for k, v in parsed.items():
            if isinstance(v, list):
                n_total += len(v)
                print(f"  {k}: {len(v)} items")
            else:
                vs = str(v)
                if len(vs) > 80: vs = vs[:80] + "..."
                print(f"  {k}: {vs}")
    print(f"  TOTAL entities: {n_total}")
