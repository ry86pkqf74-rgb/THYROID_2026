#!/usr/bin/env python3
"""Phase 2 recon: pull every cohort view that touches a CPM TIRADS column.

Identifies the actual targets (resolves the prompt's m011_*/m045_* wildcards),
dumps each view definition, and lists which CPM TIRADS columns each view
references plus how many times.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

OUT = Path(__file__).resolve().parent
con = MotherDuckClient(
    MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
).connect_rw()

# 53 CPM audit cols from pre-B coverage table
audit_cols = [
    r[0] for r in con.execute(
        "SELECT column_name FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1"
    ).fetchall()
]

# Pull all manuscript_workspace + views_readable views
views = con.execute(
    """
    SELECT table_schema, table_name, view_definition
    FROM information_schema.views
    WHERE table_schema IN ('manuscript_workspace', 'views_readable', 'main', 'tier2')
    """
).fetchall()

hits: dict[str, dict] = {}
for schema, name, defn in views:
    if defn is None:
        continue
    refs: dict[str, int] = {}
    for c in audit_cols:
        n = len(re.findall(rf"\b{re.escape(c)}\b", defn))
        if n:
            refs[c] = n
    if refs:
        hits[f"{schema}.{name}"] = {
            "n_distinct_cols_referenced": len(refs),
            "total_references": sum(refs.values()),
            "cols": refs,
            "definition_chars": len(defn),
        }

# Save view defs to disk for archival + manual edit
defs_dir = OUT / "_partB_phase2_view_defs"
defs_dir.mkdir(exist_ok=True)
for fq, _ in hits.items():
    schema, name = fq.split(".", 1)
    defn = next(d for s, n, d in views if f"{s}.{n}" == fq)
    (defs_dir / f"{name}.before.sql").write_text(
        f"-- View: {fq}\n-- Pulled: 2026-04-21 Part B Phase 2 recon\n\n{defn}\n"
    )

(OUT / "_partB_phase2_recon.json").write_text(json.dumps(hits, indent=2))

print(f"Total views with CPM TIRADS references: {len(hits)}")
print()
for fq, h in sorted(hits.items(), key=lambda kv: -kv[1]["n_distinct_cols_referenced"]):
    print(f"  {fq}")
    print(f"    distinct cols: {h['n_distinct_cols_referenced']}, total refs: {h['total_references']}, def chars: {h['definition_chars']}")
    for c, n in sorted(h["cols"].items(), key=lambda kv: -kv[1]):
        print(f"      {c} ({n}x)")
    print()
