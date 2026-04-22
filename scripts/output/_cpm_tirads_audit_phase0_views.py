#!/usr/bin/env python3
"""
Phase 0 supplement: scan all VIEW definitions (views_readable, manuscript_workspace,
main, tier2) for TIRADS-column references. View references count as READERS for the
audit (downstream consumers).
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

OUT = Path(__file__).resolve().parent
INV = json.loads((OUT / "_cpm_tirads_audit_inventory.json").read_text())
all_cols = [c["name"] for c in INV["audit_columns"]] + [
    c["name"] for c in INV["nlp_columns"]
]

con = MotherDuckClient(
    MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
).connect_rw()

views = con.execute(
    """
    SELECT table_schema, table_name, view_definition
    FROM information_schema.views
    WHERE table_schema IN ('views_readable', 'manuscript_workspace', 'main', 'tier2')
    """
).fetchall()

view_readers: dict[str, list[str]] = defaultdict(list)
for schema, name, defn in views:
    if defn is None:
        continue
    for c in all_cols:
        if re.search(rf"\b{re.escape(c)}\b", defn):
            view_readers[c].append(f"{schema}.{name}")

# Sort and write
out_obj = {
    c: sorted(set(view_readers.get(c, []))) for c in all_cols if view_readers.get(c)
}
(OUT / "_cpm_tirads_audit_view_readers.json").write_text(json.dumps(out_obj, indent=2))

print(f"Views scanned: {len(views)}")
print(f"Columns referenced in view defs: {len(out_obj)}")
for c, lst in sorted(out_obj.items(), key=lambda kv: -len(kv[1]))[:25]:
    print(f"  {c}: {len(lst)} views — {', '.join(lst[:4])}{'...' if len(lst) > 4 else ''}")
