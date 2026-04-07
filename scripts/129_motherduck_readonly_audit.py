#!/usr/bin/env python3
"""Read-only MotherDuck catalog + row counts for mm audit (SELECT only; no DDL)."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

# Align with operator runbook / audit attribution (read-only inventory).
os.environ.setdefault("MOTHERDUCK_SESSION_HINT", "THYROID_2026")
os.environ.setdefault(
    "MOTHERDUCK_CUSTOM_USER_AGENT",
    "THYROID_2026_molecular/129_readonly_audit;kind=inventory",
)

KEYWORDS = [
    "patient",
    "identity",
    "mrn",
    "crosswalk",
    "linkage",
    "surgery",
    "operative",
    "imaging",
    "nodule",
    "fna",
    "aspirate",
    "molecular",
    "genomic",
    "path",
    "pathology",
    "validation",
    "audit",
    "timeline",
    "rai",
    "synoptic",
    "tumor",
    "specimen",
]

AUDIT_TABLES = [
    "mrn_crosswalk_v1",
    "linkage_master_v1",
    "operative_episode_detail_v2",
    "tumor_episode_master_v2",
    "fna_episode_master_v2",
    "molecular_test_episode_v2",
    "imaging_nodule_master_v1",
    "event_date_audit_v2",
    "patient_cross_domain_timeline_v2",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
    "fna_molecular_linkage_v3",
    "pathology_rai_linkage_v3",
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--out",
        type=Path,
        help="Write JSON here (default: stdout only)",
    )
    args = ap.parse_args()

    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb", env="prod")
    try:
        all_tabs = con.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog = current_database()
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY 1, 2
            """
        ).fetchall()

        matched: list[tuple[str, str, str]] = []
        for sch, name, typ in all_tabs:
            blob = f"{sch}.{name}".lower()
            if any(k in blob for k in KEYWORDS):
                matched.append((sch, name, typ))

        audit_detail: dict[str, object] = {}
        for logical in AUDIT_TABLES:
            found = None
            for sch, name, typ in all_tabs:
                if name.lower() == logical.lower():
                    found = (sch, name, typ)
                    break
            key = logical
            if not found:
                audit_detail[key] = {"exists": False}
                continue
            sch, name, typ = found
            fq = f'"{sch}"."{name}"' if sch != "main" else f"main.{name}"
            cols = con.execute(
                f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_catalog = current_database()
                  AND table_schema = '{sch}'
                  AND table_name = '{name.replace("'", "''")}'
                ORDER BY ordinal_position
                """
            ).fetchall()
            cnt = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()
            audit_detail[key] = {
                "exists": True,
                "qualified": fq,
                "table_type": typ,
                "row_count": int(cnt[0]) if cnt else None,
                "columns": [{"name": c[0], "type": c[1], "nullable": c[2]} for c in cols],
            }

        out = {
            "database": con.execute("SELECT current_database()").fetchone()[0],
            "keyword_filtered_objects": [
                {"schema": a, "name": b, "type": c} for a, b, c in matched
            ],
            "audit_tables": audit_detail,
        }
        text = json.dumps(out, indent=2)
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text, encoding="utf-8")
        print(text)
    finally:
        con.close()


if __name__ == "__main__":
    main()
