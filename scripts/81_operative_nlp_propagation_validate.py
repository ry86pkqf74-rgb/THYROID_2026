#!/usr/bin/env python3
"""
81_operative_nlp_propagation_validate.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Validate and report operative NLP field coverage in MotherDuck canonical tables.

This script is a *validation companion* to script 71 (which runs the actual
UPDATE).  It confirms that operative NLP-enriched fields are correctly
propagated into operative_episode_detail_v2 (and its md_* mirror), then
writes a detailed coverage report.

Operative NLP fields assessed:
  rln_monitoring_flag          – nerve-monitoring mentioned in op note
  rln_finding_raw              – RLN finding text
  parathyroid_autograft_flag   – auto-transplantation noted
  gross_ete_flag               – gross ETE observed intraoperatively
  local_invasion_flag          – any local invasion (strap, trachea, etc.)
  tracheal_involvement_flag    – tracheal contact/invasion
  esophageal_involvement_flag  – esophageal contact/invasion
  strap_muscle_involvement_flag– strap muscle resection/invasion
  reoperative_field_flag       – reoperative field noted
  drain_flag                   – surgical drain placed
  operative_findings_raw       – any free-text operative finding

Gap classification:
  Category A – Source-linked and materialized (reliable)
  Category B – NOT_PARSED (default=FALSE, meaning NOT confirmed-negative)
  Category C – Source-absent (entity type not in vocabulary)

Usage:
    .venv/bin/python scripts/81_operative_nlp_propagation_validate.py [--md] [--local]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / f"final_md_optimization_20260314"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Field definitions ────────────────────────────────────────────────────────
OPERATIVE_FIELDS = [
    # (field_name, category, notes)
    ("rln_monitoring_flag",          "B", "Script 22 default=FALSE; script 71 propagates NLP nerve_monitoring"),
    ("rln_finding_raw",              "B", "Script 22 default=NULL; script 71 propagates NLP rln_finding"),
    ("parathyroid_autograft_flag",   "B", "Script 22 default=FALSE; script 71 propagates parathyroid_autograft"),
    ("gross_ete_flag",               "B", "Script 22 default=FALSE; script 71 propagates gross_invasion→gross_ete"),
    ("local_invasion_flag",          "B", "Script 22 default=FALSE; script 71 propagates gross_invasion→any"),
    ("tracheal_involvement_flag",    "B", "Script 22 default=FALSE; script 71 propagates tracheal_involvement"),
    ("esophageal_involvement_flag",  "B", "Script 22 default=FALSE; script 71 propagates esophageal_involvement"),
    ("strap_muscle_involvement_flag","B", "Script 22 default=FALSE; script 71 propagates strap_muscle"),
    ("reoperative_field_flag",       "B", "Script 22 default=FALSE; script 71 propagates reoperative_field"),
    ("drain_flag",                   "B", "Script 22 default=FALSE; script 71 propagates drain_placement"),
    ("operative_findings_raw",       "B", "Script 22 default=NULL; script 71 concatenates entity_value_norm"),
    # post-script-71 NLP enrichment fields (category C – entity type absent from vocabulary)
    ("berry_ligament_flag",          "C", "Entity type not in vocabulary; requires V2 extractor expansion"),
    ("frozen_section_flag",          "C", "Entity type not in vocabulary; requires V2 extractor expansion"),
    ("ebl_ml_nlp",                   "C", "Entity type not in vocabulary; requires V2 extractor expansion"),
    ("parathyroid_identified_count", "C", "Entity type not in vocabulary; requires V2 extractor expansion"),
    # structured fields (reliable)
    ("central_neck_dissection_flag", "A", "Derived from linkage_v3; reliable"),
    ("lateral_neck_dissection_flag", "A", "Derived from script 76 Phase D; reliable"),
]

CAT_LABELS = {
    "A": "Source-linked, reliable",
    "B": "NOT_PARSED (default=FALSE; script 71 propagates TRUE where found)",
    "C": "Source-absent (entity type missing from NLP vocabulary)",
}


def get_connection(use_md: bool):
    import duckdb
    if use_md:
        try:
            import toml
            token = os.environ.get("MOTHERDUCK_TOKEN") or toml.load(
                str(ROOT / ".streamlit" / "secrets.toml")
            )["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.environ["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    db = ROOT / "thyroid_master.duckdb"
    return duckdb.connect(str(db))


def safe_count(con, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def field_coverage(con, table: str, field: str) -> dict:
    """Return non-null / TRUE count and pct for a field."""
    total = safe_count(con, f"SELECT COUNT(*) FROM {table}")
    if total <= 0:
        return {"total": 0, "filled": 0, "pct": 0.0}

    try:
        # Boolean: count TRUE
        tp = con.execute(
            f"SELECT data_type FROM information_schema.columns "
            f"WHERE table_name='{table}' AND column_name='{field}' "
            f"AND table_schema='main' LIMIT 1"
        ).fetchone()
        dtype = (tp[0] if tp else "").upper()
        if "BOOL" in dtype:
            filled = safe_count(con, f"SELECT COUNT(*) FROM {table} WHERE {field} IS TRUE")
        else:
            filled = safe_count(
                con,
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {field} IS NOT NULL AND TRIM(CAST({field} AS VARCHAR)) != ''"
            )
    except Exception:
        filled = -1

    pct = round(100.0 * filled / total, 1) if total > 0 and filled >= 0 else 0.0
    return {"total": total, "filled": filled, "pct": pct}


def run_validation(con, table: str) -> list[dict]:
    rows = []
    for fname, cat, note in OPERATIVE_FIELDS:
        try:
            cov = field_coverage(con, table, fname)
        except Exception as e:
            cov = {"total": 0, "filled": -1, "pct": 0.0}
        rows.append({
            "field": fname,
            "category": cat,
            "category_label": CAT_LABELS[cat],
            "total_rows": cov["total"],
            "filled_count": cov["filled"],
            "fill_pct": cov["pct"],
            "notes": note,
            "status": (
                "OK" if cat == "A" else
                "POPULATED_BY_NLP" if cov["filled"] > 0 else
                "NOT_PARSED" if cat == "B" else
                "SOURCE_ABSENT"
            ),
        })
    return rows


def compare_local_md(con_local, con_md, table: str) -> dict:
    """Compare row counts between local and MotherDuck."""
    local_n = safe_count(con_local, f"SELECT COUNT(*) FROM {table}")
    md_n = safe_count(con_md, f"SELECT COUNT(*) FROM {table}")
    return {"local": local_n, "motherduck": md_n, "match": local_n == md_n}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB only")
    args = parser.parse_args()

    use_md = args.md or not args.local

    print("\n" + "=" * 72)
    print("  81 — Operative NLP Propagation Validation")
    print("=" * 72)

    con = get_connection(use_md)
    target = "operative_episode_detail_v2"
    mirror = "md_oper_episode_detail_v2"

    # ── 1. Validate canonical table ─────────────────────────────────────
    print(f"\n  Validating {target}...")
    results = run_validation(con, target)

    # ── 2. Validate mirror table ─────────────────────────────────────────
    print(f"  Validating mirror {mirror}...")
    mirror_results = run_validation(con, mirror)

    # ── 3. Local vs MotherDuck comparison ────────────────────────────────
    sync_status: dict = {}
    if use_md:
        try:
            db = ROOT / "thyroid_master.duckdb"
            import duckdb as _ddb
            con_local = _ddb.connect(str(db), read_only=True)
            sync_status = compare_local_md(con_local, con, target)
            con_local.close()
        except Exception as e:
            sync_status = {"error": str(e)}

    # ── 4. Print summary ─────────────────────────────────────────────────
    print(f"\n  ── Coverage Report: {target} ──")
    import textwrap
    fmt = "  {:<30s} {:>4s} {:>8s} {:>8s}  {}"
    print(fmt.format("Field", "Cat", "Filled", "Pct%", "Status"))
    print("  " + "-" * 70)
    for r in results:
        print(fmt.format(
            r["field"][:29],
            r["category"],
            str(r["filled_count"]),
            f"{r['fill_pct']:.1f}%",
            r["status"],
        ))

    cat_b_populated = sum(
        1 for r in results if r["category"] == "B" and r["fill_pct"] > 0
    )
    cat_b_total = sum(1 for r in results if r["category"] == "B")
    print(
        f"\n  Category B (NLP-populated): {cat_b_populated}/{cat_b_total} "
        "fields have any TRUE values"
    )
    if sync_status:
        print(f"\n  Local vs MotherDuck row count: {sync_status}")

    # ── 5. Identify missing propagation ──────────────────────────────────
    unpropagated = [
        r for r in results if r["category"] == "B" and r["fill_pct"] == 0
    ]
    if unpropagated:
        print("\n  ⚠ UNPROPAGATED FIELDS (script 71 not yet run or produced 0 entities):")
        for r in unpropagated:
            print(f"    - {r['field']}")
        print("  → Run: .venv/bin/python scripts/71_operative_nlp_to_motherduck.py --md")
    else:
        print("\n  ✓ All Category-B NLP fields populated (script 71 has run).")

    # ── 6. val_operative_nlp_propagation_v1 table ─────────────────────
    import pandas as pd
    df = pd.DataFrame(results)
    df["table_name"] = target
    df["validated_at"] = datetime.utcnow().isoformat()
    df["validator_script"] = "81"

    try:
        import tempfile, os
        tmp = tempfile.mktemp(suffix=".parquet")
        df.to_parquet(tmp)
        con.execute(
            f"CREATE OR REPLACE TABLE val_operative_nlp_propagation_v1 AS "
            f"SELECT * FROM read_parquet('{tmp}')"
        )
        os.unlink(tmp)
        print(
            f"\n  val_operative_nlp_propagation_v1 written "
            f"({len(df)} rows)"
        )
    except Exception as e:
        print(f"  WARNING: Could not write val table: {e}")

    # ── 7. Export CSV ──────────────────────────────────────────────────
    out_csv = EXPORTS_DIR / "val_operative_nlp_propagation_v1.csv"
    df.to_csv(out_csv, index=False)
    print(f"  Exported: {out_csv.relative_to(ROOT)}")

    # ── 8. Export JSON summary ─────────────────────────────────────────
    summary = {
        "validated_at": datetime.utcnow().isoformat(),
        "script": "81",
        "table": target,
        "total_fields": len(results),
        "cat_b_populated": cat_b_populated,
        "cat_b_total": cat_b_total,
        "unpropagated_count": len(unpropagated),
        "unpropagated_fields": [r["field"] for r in unpropagated],
        "sync_status": sync_status,
        "propagation_status": (
            "COMPLETE" if cat_b_populated == cat_b_total
            else f"PARTIAL ({cat_b_populated}/{cat_b_total})"
            if cat_b_populated > 0
            else "NOT_RUN"
        ),
    }
    out_json = EXPORTS_DIR / "operative_nlp_propagation_summary.json"
    out_json.write_text(json.dumps(summary, indent=2))
    print(f"  Exported: {out_json.relative_to(ROOT)}")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
