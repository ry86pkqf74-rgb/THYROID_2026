#!/usr/bin/env python3
"""
85_materialization_performance_audit.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Performance audit of the script-26 materialization pipeline.

Reviews:
  A. Large table row counts and column counts → flag candidates for sort keys
  B. Checks whether hot-paths land on materialized tables vs raw views/CTEs
  C. Identifies duplicate md_* entries in MATERIALIZATION_MAP
  D. Recommends sort/partition keys for top N large tables
  E. Writes recommendations to docs/ and exports/

Performance principles documented here:
  1. Materialized TABLE beats VIEW for any table read >2x per dashboard load.
  2. Sort keys on dominant join column (research_id) improve MotherDuck scan.
  3. Phase 0 "hot tables" (streamlit_patient_header_v, master_cohort, etc.)
     must be materialized as TABLE, not left as VIEW.
  4. Avoid re-running scripts that re-materialize entire 100+ table maps when
     only 1 table changed – prefer targeted re-materialization.

Usage:
    .venv/bin/python scripts/85_materialization_performance_audit.py [--md] [--local]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / "final_md_optimization_20260314"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Tables to profile (hot-path first) ────────────────────────────────────────
PROFILE_TABLES = [
    # (table_name, dominant_join_col, expected_size_tier)
    # size tiers: small(<10k), medium(10k-500k), large(500k+)
    ("master_cohort",                          "research_id", "medium"),
    ("manuscript_cohort_v1",                   "research_id", "medium"),
    ("patient_analysis_resolved_v1",           "research_id", "medium"),
    ("episode_analysis_resolved_v1_dedup",     "research_id", "medium"),
    ("thyroid_scoring_py_v1",                  "research_id", "medium"),
    ("analysis_cancer_cohort_v1",              "research_id", "small"),
    ("operative_episode_detail_v2",            "research_id", "medium"),
    ("rai_treatment_episode_v2",               "research_id", "small"),
    ("molecular_test_episode_v2",              "research_id", "medium"),
    ("survival_cohort_enriched",               "research_id", "large"),
    ("longitudinal_lab_canonical_v1",          "research_id", "medium"),
    ("clinical_notes_long",                    "research_id", "large"),
    ("note_entities_complications",            "research_id", "large"),
    ("note_entities_staging",                  "research_id", "large"),
    ("fna_episode_master_v2",                  "research_id", "large"),
    ("path_synoptics",                         "research_id", "medium"),
    ("tumor_episode_master_v2",                "research_id", "medium"),
    ("streamlit_patient_header_v",             "research_id", "medium"),
    ("imaging_nodule_master_v1",               "research_id", "medium"),
    ("linkage_summary_v3",                     "research_id", "small"),
]

# ── Sort key recommendation rules ────────────────────────────────────────────
SORT_RECOMMENDATIONS = {
    "large":  "CREATE OR REPLACE TABLE {tbl}_sorted AS SELECT * FROM {tbl} ORDER BY {col};",
    "medium": "# Consider sort if p99 query >1s: CREATE OR REPLACE TABLE {tbl}_sorted AS SELECT * FROM {tbl} ORDER BY {col};",
    "small":  "# Sort not needed (<10k rows)",
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


def table_type(con, tbl: str) -> str:
    """Returns 'table', 'view', or 'missing'."""
    try:
        r = con.execute(
            f"SELECT table_type FROM information_schema.tables "
            f"WHERE table_name='{tbl}' AND table_schema='main' LIMIT 1"
        ).fetchone()
        return (r[0] or "").lower() if r else "missing"
    except Exception:
        return "missing"


def profile_table(con, tbl: str, join_col: str, size_tier: str) -> dict:
    import time
    result: dict = {
        "table": tbl, "join_col": join_col, "size_tier": size_tier,
        "type": table_type(con, tbl), "rows": -1, "cols": -1,
        "scan_ms": -1, "recommendation": "",
    }
    if result["type"] == "missing":
        result["recommendation"] = "MISSING – check materialization"
        return result

    t0 = time.monotonic()
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
        result["scan_ms"] = int((time.monotonic() - t0) * 1000)
        result["rows"] = int(row[0]) if row else 0
    except Exception as e:
        result["recommendation"] = f"ERROR: {str(e)[:80]}"
        return result

    try:
        cols = con.execute(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name='{tbl}' AND table_schema='main'"
        ).fetchone()
        result["cols"] = int(cols[0]) if cols else -1
    except Exception:
        pass

    # Hot-path check: view should be TABLE in production
    if result["type"] == "view" and size_tier in ("medium", "large"):
        result["recommendation"] = (
            "⚠ VIEW in hot-path – materialize as TABLE for performance: "
            f"CREATE OR REPLACE TABLE {tbl}_mat AS SELECT * FROM {tbl};"
        )
    elif size_tier == "large" and result["type"] == "base table":
        tmpl = SORT_RECOMMENDATIONS["large"]
        result["recommendation"] = tmpl.format(tbl=tbl, col=join_col)
    elif result["rows"] > 100_000:
        tmpl = SORT_RECOMMENDATIONS["medium"]
        result["recommendation"] = tmpl.format(tbl=tbl, col=join_col)
    else:
        result["recommendation"] = "OK"

    return result


def check_map_duplicates() -> list[str]:
    """Find duplicate destination names in MATERIALIZATION_MAP (script 26).

    Scans ONLY the lines within the list literal — from the line containing
    'MATERIALIZATION_MAP' up to the closing `]` — so that legitimate
    references to md_* names elsewhere in the file (SQL substitution strings,
    print statements, etc.) are NOT falsely reported as duplicates.
    """
    try:
        import re
        script26 = ROOT / "scripts" / "26_motherduck_materialize_v2.py"
        src_lines = script26.read_text().splitlines()

        in_map = False
        entries: list[str] = []
        for line in src_lines:
            # Start collecting when we enter the MAP definition
            if "MATERIALIZATION_MAP" in line and "=" in line and "[" in line:
                in_map = True
            if in_map:
                m = re.search(r'"(md_[^"]+)"', line)
                if m:
                    entries.append(m.group(1))
            # Stop collecting at the closing bracket of the list
            if in_map and line.strip() == "]":
                break

        seen: dict[str, int] = {}
        for e in entries:
            seen[e] = seen.get(e, 0) + 1
        return [k for k, v in seen.items() if v > 1]
    except Exception:
        return []


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()

    use_md = args.md or not args.local
    con = get_connection(use_md)

    print("\n" + "=" * 72)
    print("  85 — Materialization Performance Audit")
    print("=" * 72)
    print(f"  Profiling {len(PROFILE_TABLES)} tables...\n")

    rows = []
    for tbl, col, tier in PROFILE_TABLES:
        r = profile_table(con, tbl, col, tier)
        rows.append(r)
        icon = "✓" if r["type"] != "missing" else "✗"
        type_label = r["type"][:5] if r["type"] != "missing" else "MISS"
        print(
            f"  {icon}  {tbl:<47s} "
            f"{type_label:<6s} "
            f"{r['rows']:>8,} rows  "
            f"{r['scan_ms']:>5}ms"
        )
        if r["recommendation"] not in ("OK", ""):
            print(f"       → {r['recommendation'][:80]}")

    # ── MATERIALIZATION_MAP duplicate check ───────────────────────────────
    print("\n  MATERIALIZATION_MAP duplicate check...")
    dupes = check_map_duplicates()
    if dupes:
        print(f"  ⚠  {len(dupes)} duplicate destination(s) in MATERIALIZATION_MAP:")
        for d in dupes:
            print(f"    - {d}")
    else:
        print("  ✓  No duplicates in MATERIALIZATION_MAP")

    # ── Summary ───────────────────────────────────────────────────────────
    missing = [r for r in rows if r["type"] == "missing"]
    views_in_hotpath = [r for r in rows if r["type"] == "view"]
    slow_tables = [r for r in rows if r["scan_ms"] > 5000]

    print(f"\n  Summary:")
    print(f"    Missing tables:   {len(missing)}")
    print(f"    Views (hot-path): {len(views_in_hotpath)}")
    print(f"    Slow scans (>5s): {len(slow_tables)}")
    print(f"    MAP duplicates:   {len(dupes)}")

    # ── Export ────────────────────────────────────────────────────────────
    import pandas as pd
    df = pd.DataFrame(rows)
    df["profiled_at"] = datetime.utcnow().isoformat()
    df["map_duplicates"] = json.dumps(dupes)

    csv_out = EXPORTS_DIR / f"materialization_performance_{TIMESTAMP}.csv"
    df.to_csv(csv_out, index=False)
    print(f"\n  Exported: {csv_out.relative_to(ROOT)}")

    report = {
        "profiled_at": datetime.utcnow().isoformat(),
        "total_profiled": len(rows),
        "missing": [r["table"] for r in missing],
        "views_in_hotpath": [r["table"] for r in views_in_hotpath],
        "slow_scans": [r["table"] for r in slow_tables],
        "map_duplicates": dupes,
        "recommendations": [
            {"table": r["table"], "rec": r["recommendation"]}
            for r in rows if r["recommendation"] not in ("OK", "")
        ],
    }
    json_out = EXPORTS_DIR / f"materialization_performance_{TIMESTAMP}.json"
    json_out.write_text(json.dumps(report, indent=2))
    print(f"  Exported: {json_out.relative_to(ROOT)}")

    # ── Write val_* table ─────────────────────────────────────────────────
    try:
        import tempfile, os as _os
        tmp = tempfile.mktemp(suffix=".parquet")
        df.to_parquet(tmp, index=False)
        con.execute(
            f"CREATE OR REPLACE TABLE val_materialization_perf_v1 AS "
            f"SELECT * FROM read_parquet('{tmp}')"
        )
        _os.unlink(tmp)
        print("  ✓  val_materialization_perf_v1 written")
    except Exception as e:
        print(f"  WARNING: {e}")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
