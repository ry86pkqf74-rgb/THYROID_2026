#!/usr/bin/env python3
"""Export rows still in `unresolved_linkage_gap` after imaging↔FNA linkage (script 129).

Writes CSV + manifest under studies/ for manual review / algorithm follow-up.

Run:
  .venv/bin/python scripts/152_linkage_gap_worklist_export.py --md
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

OUT_DIR = ROOT / "studies" / "20260413_source_truth_completeness_audit"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", action="store_true", help="MotherDuck (default)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Local thyroid_master.duckdb",
    )
    args = parser.parse_args()
    if not args.md and not args.local:
        args.md = True

    db_path = ROOT / "thyroid_master.duckdb"
    if args.md:
        con = connect_md_fail_closed(db_path)
    else:
        import duckdb

        con = duckdb.connect(str(db_path))

    sql = """
    SELECT *
    FROM v_imaging_nodule_linkage_classification_v1
    WHERE linkage_state = 'unresolved_linkage_gap'
    ORDER BY research_id, exam_id, nodule_id
    """
    df = con.execute(sql).fetchdf()
    con.close()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = OUT_DIR / f"linkage_gap_worklist_unresolved_{stamp}.csv"
    df.to_csv(csv_path, index=False)

    manifest = {
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_view": "v_imaging_nodule_linkage_classification_v1",
        "filter": "linkage_state = 'unresolved_linkage_gap'",
        "row_count": int(len(df)),
        "csv": str(csv_path.relative_to(ROOT)),
    }
    (OUT_DIR / f"linkage_gap_worklist_manifest_{stamp}.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
