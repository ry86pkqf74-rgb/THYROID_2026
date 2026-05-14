#!/usr/bin/env python3
"""Prompt 11 — decompose ORPHAN_BUILDER objects into VIEW / FROZEN / REAL_TABLE.

Reads ``studies/bq_pub_authoritative_builders_20260514.json`` and prints counts.
Convention (BigQuery corpus from ``bq_pub_object_list_snapshot_20260514``):

* ``pub_views_readable`` — presentation layer; ORPHAN rows here are **VIEW (OK)**.
* ``pub_semantic`` — ``vw_*`` / ``*_VIEW_*`` safe surfaces are **VIEW (OK)**;
  ``release_manifest_v1`` is a real table.
* ``pub_canonical`` — name heuristics ``_VIEW_``, ``VW_``, ``V_`` → VIEW;
  ``__readme`` and ``canonical_patient_master_v1_9`` → frozen;
  remaining ORPHAN rows → **real table backlog**.

Re-run after refreshing the authoritative-builders JSON.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PATH = REPO / "studies" / "bq_pub_authoritative_builders_20260514.json"


def classify(ds: str, tid: str) -> str:
    if ds == "pub_views_readable":
        return "VIEW_READABLE_LAYER"
    if ds == "pub_semantic":
        tl = tid.lower()
        if tid.startswith("vw_") or "_view_" in tl or tl.endswith("_view_v1"):
            return "VIEW_SEMANTIC"
        return "REAL_TABLE"
    u = tid.upper()
    if "_VIEW" in u or u.startswith("VW_") or u.startswith("V_"):
        return "VIEW_CANONICAL_DDL"
    if tid in ("__readme", "canonical_patient_master_v1_9"):
        return "FROZEN"
    n = tid.lower()
    if any(
        m in n
        for m in ("_pre_mig", "pre_mig_", "_archive", "_backup", "_freeze", "_snapshot")
    ):
        return "FROZEN"
    return "REAL_TABLE"


def main() -> None:
    d = json.loads(PATH.read_text(encoding="utf-8"))
    orph: list[tuple[str, str]] = []
    for k, v in d["objects"].items():
        if v.get("authoritative_builder") != "ORPHAN_BUILDER":
            continue
        ds, tid = k.split(".", 1)
        orph.append((ds, tid))

    c = Counter(classify(ds, t) for ds, t in orph)
    view_total = sum(c[x] for x in c if x.startswith("VIEW"))
    print("ORPHAN_BUILDER total:", len(orph))
    print("  VIEW (OK):", view_total, dict((k, c[k]) for k in sorted(c) if k.startswith("VIEW")))
    print("  FROZEN SNAPSHOT (OK):", c["FROZEN"])
    print("  REAL TABLE — no resolved builder (backlog):", c["REAL_TABLE"])


if __name__ == "__main__":
    main()
