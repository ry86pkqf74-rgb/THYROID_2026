#!/usr/bin/env python3
"""Distribution of any key fields in a results jsonl."""
import json
import sys
import collections

path = sys.argv[1]
fields = sys.argv[2:]
recs = [json.loads(l) for l in open(path)]
print(f"n={len(recs)}")
for f in fields:
    c = collections.Counter((r.get("parsed_json") or {}).get(f) for r in recs)
    print(f"{f}: {c.most_common(8)}")
