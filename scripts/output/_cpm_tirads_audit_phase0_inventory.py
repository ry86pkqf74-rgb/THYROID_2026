#!/usr/bin/env python3
"""
CPM TIRADS audit — Phase 0/1 helper: pull the full column inventory for CPM,
filter to TIRADS-related columns, and emit a JSON inventory + markdown table
for downstream Phase 2 cell-level comparison and Phase 3 classification.

Read-only. Writes only to scripts/output/.
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
DB = "thyroid_canonical_publication_v1_0"

TIRADS_REGEX = re.compile(
    r"(tirads|laterality_v271b|imaging_laterality_rollup|max_tirads|preop_tirads"
    r"|imaging_updated_tirads|worst_tirads_category|imaging_tirads"
    r"|pathology_vs_imaging_laterality)",
    re.IGNORECASE,
)

NLP_REGEX = re.compile(r"^nlp_(tirads|imaging|usnodule)_", re.IGNORECASE)


def main() -> None:
    client = MotherDuckClient(MotherDuckConfig(database=DB))
    con = client.connect_rw()

    cols = con.execute(
        """
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
          AND table_schema = 'main'
          AND table_name = 'canonical_patient_master'
        ORDER BY ordinal_position
        """
    ).fetchall()

    all_cols = [
        {"name": c[0], "type": c[1], "nullable": c[2], "pos": c[3]} for c in cols
    ]

    tirads_cols = [c for c in all_cols if TIRADS_REGEX.search(c["name"])]
    nlp_cols = [c for c in tirads_cols if NLP_REGEX.search(c["name"])]
    audit_cols = [c for c in tirads_cols if not NLP_REGEX.search(c["name"])]

    # Per-column populated counts (NOT NULL count) on the entire CPM
    pop_counts: dict[str, int] = {}
    for c in audit_cols + nlp_cols:
        n = con.execute(
            f'SELECT COUNT(*) - COUNT(CASE WHEN "{c["name"]}" IS NULL THEN 1 END) FROM main.canonical_patient_master'
        ).fetchone()[0]
        pop_counts[c["name"]] = n

    out = {
        "database": DB,
        "table": "main.canonical_patient_master",
        "total_columns": len(all_cols),
        "tirads_related_total": len(tirads_cols),
        "audit_columns": [
            {**c, "n_populated": pop_counts[c["name"]]} for c in audit_cols
        ],
        "nlp_columns": [
            {**c, "n_populated": pop_counts[c["name"]]} for c in nlp_cols
        ],
    }
    (OUT / "_cpm_tirads_audit_inventory.json").write_text(json.dumps(out, indent=2))

    # Markdown table for the agent
    lines = ["# CPM TIRADS column inventory", ""]
    lines.append(f"- Total CPM columns: **{len(all_cols)}**")
    lines.append(f"- TIRADS-related columns (regex match): **{len(tirads_cols)}**")
    lines.append(f"- → Audit-target columns (excluding NLP_*): **{len(audit_cols)}**")
    lines.append(f"- → NLP-coverage columns (excluded from audit): **{len(nlp_cols)}**")
    lines.append("")
    lines.append("## Audit columns")
    lines.append("")
    lines.append("| pos | column_name | data_type | n_populated |")
    lines.append("|---:|---|---|---:|")
    for c in audit_cols:
        lines.append(
            f"| {c['pos']} | `{c['name']}` | {c['type']} | {pop_counts[c['name']]} |"
        )
    lines.append("")
    lines.append("## NLP-coverage columns (excluded from audit)")
    lines.append("")
    lines.append("| pos | column_name | data_type | n_populated |")
    lines.append("|---:|---|---|---:|")
    for c in nlp_cols:
        lines.append(
            f"| {c['pos']} | `{c['name']}` | {c['type']} | {pop_counts[c['name']]} |"
        )
    (OUT / "_cpm_tirads_audit_inventory.md").write_text("\n".join(lines) + "\n")

    print("OK")
    print(f"audit cols: {len(audit_cols)}  nlp cols: {len(nlp_cols)}")


if __name__ == "__main__":
    main()
