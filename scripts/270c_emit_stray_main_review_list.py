#!/usr/bin/env python3
"""Script 270c — emit stray-main review CSV for clinical eyeball.

Reads scripts/output/270c_stray_schema_consolidation.csv, filters to
the 156 MIGRATE_TO_ARCHIVE_LEGACY rows in stray "Thyroid 2026 UPdated".
main, enriches with live row + column counts, and writes
scripts/output/270c_stray_main_review_list.csv for Logan to skim before
270d --execute.

Purpose: 270d will rename each of these 156 objects into archive_legacy
under names like main__<name>_<UTC>. If any of them is a working copy
of clinically meaningful content that got orphaned in the stray main
schema, it should be flagged for KEEP or re-routing BEFORE migration
buries it under a dated archive entry where nobody will find it.

Mode: read-only (live SELECT for column counts; no DDL writes).
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONSOLIDATION_CSV = OUT_DIR / "270c_stray_schema_consolidation.csv"
REVIEW_CSV = OUT_DIR / "270c_stray_main_review_list.csv"
REVIEW_LOG = OUT_DIR / "270c_stray_main_review_list.log"

ARCHIVE_DB = "Thyroid 2026 UPdated"
TARGET_SCHEMA = "main"
TARGET_DISPOSITION = "MIGRATE_TO_ARCHIVE_LEGACY"


def read_consolidation_rows(path: Path) -> list[dict]:
    """Read the 270c consolidation CSV; return rows matching the
    (schema=main, disposition=MIGRATE_TO_ARCHIVE_LEGACY) filter."""
    if not path.exists():
        raise SystemExit(f"missing input: {path}")
    out: list[dict] = []
    with path.open() as f:
        rows = list(csv.reader(f))
    header_idx = next(
        (i for i, r in enumerate(rows) if r and r[0] == "schema"), None
    )
    if header_idx is None:
        raise SystemExit(f"could not locate header row in {path}")
    header = rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    type_i = header.index("object_type")
    rc_i = header.index("row_count")
    disp_i = header.index("disposition")
    just_i = header.index("justification")
    proposed_i = header.index("proposed_target_name")
    for r in rows[header_idx + 1:]:
        if not r or len(r) <= proposed_i:
            continue
        if r[sch_i] != TARGET_SCHEMA or r[disp_i] != TARGET_DISPOSITION:
            continue
        out.append({
            "schema": r[sch_i],
            "name": r[name_i],
            "object_type": r[type_i],
            "row_count": r[rc_i],
            "disposition": r[disp_i],
            "justification": r[just_i],
            "proposed_target_name": r[proposed_i],
        })
    return out


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log("=== START 270c — stray-main review list emission ===")
    log(f"started_at: {started_at.isoformat()}")
    log(f"input:  {CONSOLIDATION_CSV}")
    log(f"output: {REVIEW_CSV}")

    rows = read_consolidation_rows(CONSOLIDATION_CSV)
    log(f"  consolidation rows matching filter: {len(rows)}")
    if not rows:
        log("  no rows to emit; aborting (this is unexpected)")
        REVIEW_LOG.write_text("".join(log_lines))
        return 1

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB} (search path locked)")

    # For each object, fetch live column count via DESCRIBE on the
    # cross-DB fully-qualified name. Row count comes from the CSV
    # (matches the live count at 270c run time, ~30 minutes ago).
    enriched: list[dict] = []
    failures = 0
    for r in rows:
        fq = f'"{ARCHIVE_DB}"."{TARGET_SCHEMA}"."{r["name"]}"'
        col_count: int | None = None
        column_sample: str = ""
        err: str | None = None
        try:
            describe_rows = con.execute(f"DESCRIBE {fq}").fetchall()
            col_count = len(describe_rows)
            # First 5 column names, semicolon-separated
            column_sample = ";".join(d[0] for d in describe_rows[:5])
        except Exception as e:
            err = str(e)[:160]
            failures += 1
        enriched.append({
            **r,
            "column_count": col_count,
            "column_sample_first5": column_sample,
            "describe_error": err,
        })

    log(f"  enriched: {len(enriched)} ({failures} describe failures)")

    # Write review CSV
    REVIEW_CSV.parent.mkdir(parents=True, exist_ok=True)
    with REVIEW_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270c_emit_stray_main_review_list.py",
            "generated_at", started_at.isoformat(),
            "purpose",
            "Logan clinical eyeball before 270d --execute renames these "
            "into archive_legacy",
        ])
        w.writerow([
            "schema", "name", "object_type", "row_count", "column_count",
            "column_sample_first5", "describe_error",
            "proposed_target_name", "justification", "logan_decision",
            "logan_notes",
        ])
        for r in sorted(enriched, key=lambda x: (
            -(int(x["row_count"]) if (x["row_count"] or "").isdigit() else 0),
            x["name"],
        )):
            w.writerow([
                r["schema"], r["name"], r["object_type"],
                r["row_count"] or "",
                "" if r["column_count"] is None else r["column_count"],
                r["column_sample_first5"],
                r["describe_error"] or "",
                r["proposed_target_name"], r["justification"],
                "",  # logan_decision: KEEP | REROUTE | MIGRATE_OK (blank = unreviewed)
                "",  # logan_notes
            ])
    log(f"  wrote {REVIEW_CSV} ({len(enriched)} rows, sorted by row_count DESC)")

    # Quick distribution log: top-10 by row count
    log("\n--- top 10 by row_count (for at-a-glance scan) ---")
    top = sorted(
        enriched,
        key=lambda x: -(int(x["row_count"]) if (x["row_count"] or "").isdigit() else 0),
    )[:10]
    for r in top:
        log(
            f"  {r['name']:<55} rows={r['row_count']:>10} "
            f"cols={r['column_count']}"
        )

    log(f"\n=== END 270c stray-main review emission ===")
    REVIEW_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
