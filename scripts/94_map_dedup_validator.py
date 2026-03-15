#!/usr/bin/env python3
"""
94_map_dedup_validator.py  —  MATERIALIZATION_MAP duplicate & integrity check

Parses the MATERIALIZATION_MAP list in scripts/26_motherduck_materialize_v2.py
and fails hard (exit 1) if any of the following are found:

  1. Duplicate md_* destination names  → same table would be overwritten twice
  2. Duplicate source names            → same source mapped under two md_ names
  3. md_* names that do NOT follow the
     `md_<canonical_name>` naming convention (informational only, no fail)

Designed to run in CI as a pre-flight check BEFORE script 26 is executed.

Usage
─────
  # Silent pass / loud fail
  python scripts/94_map_dedup_validator.py

  # Write a JSON report alongside the check
  python scripts/94_map_dedup_validator.py --report exports/map_dedup_report.json

  # Check a different script path
  python scripts/94_map_dedup_validator.py --script scripts/26_motherduck_materialize_v2.py

Exit codes
──────────
  0  No duplicates found
  1  Duplicate md_name or duplicate source found → FAIL
  2  Could not parse the MAP (file not found / parse error)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_SCRIPT = ROOT / "scripts" / "26_motherduck_materialize_v2.py"


# ── Core parser ────────────────────────────────────────────────────────────

def parse_materialization_map(script_path: Path) -> list[tuple[str, str]]:
    """
    Extract (md_name, src_name) pairs from MATERIALIZATION_MAP.

    Scans ONLY the lines inside the list literal — from the line containing
    'MATERIALIZATION_MAP' + '=' + '[' through the closing bare ']' — so that
    legitimate references to md_* names elsewhere in the file (SQL string
    substitutions, print statements) are never falsely detected.
    """
    src_lines = script_path.read_text(encoding="utf-8").splitlines()
    in_map = False
    entries: list[tuple[str, str]] = []

    for line in src_lines:
        if "MATERIALIZATION_MAP" in line and "=" in line and "[" in line:
            in_map = True
        if in_map:
            m = re.search(r'"(md_[^"]+)"\s*,\s*"([^"]+)"', line)
            if m:
                entries.append((m.group(1), m.group(2)))
        # Terminate when we hit a bare closing bracket (the end of the list
        # literal). Accept trailing whitespace and inline comments so the
        # parser is not broken by minor formatting changes.
        if in_map and re.match(r'^\s*\]\s*(#.*)?$', line):
            break

    return entries


# ── Duplicate analysis ─────────────────────────────────────────────────────

def analyse(entries: list[tuple[str, str]]) -> dict:
    md_names  = [e[0] for e in entries]
    src_names = [e[1] for e in entries]

    md_counts  = Counter(md_names)
    src_counts = Counter(src_names)

    dup_md  = {k: v for k, v in md_counts.items()  if v > 1}
    dup_src = {k: v for k, v in src_counts.items() if v > 1}

    # Informational: source tables with md_ aliases that don't follow convention
    # (convention: md_<src> where the md_ entry is a strict prefix of some canonical name)
    non_conventional: list[str] = []
    for md_name, src_name in entries:
        stripped = md_name[3:]  # remove "md_"
        if stripped != src_name and not src_name.startswith(stripped[:8]):
            non_conventional.append(f"{md_name} → {src_name}")

    return {
        "total_entries": len(entries),
        "duplicate_md_names":  dict(dup_md),
        "duplicate_src_names": dict(dup_src),
        "non_conventional_aliases": non_conventional,
        "checked_at": datetime.utcnow().isoformat(),
    }


# ── Reporting ──────────────────────────────────────────────────────────────

def print_report(result: dict, entries: list[tuple[str, str]]) -> None:
    w = 72
    print("=" * w)
    print("  94 — MATERIALIZATION_MAP duplicate validator")
    print("=" * w)
    print(f"  Total MAP entries : {result['total_entries']}")

    dup_md  = result["duplicate_md_names"]
    dup_src = result["duplicate_src_names"]

    if not dup_md and not dup_src:
        print("  ✓  No duplicate md_names")
        print("  ✓  No duplicate source names")
    else:
        if dup_md:
            print(f"\n  ✗  Duplicate md_* DESTINATION names ({len(dup_md)}):")
            for name, count in sorted(dup_md.items()):
                print(f"      {name} appears {count}×")
                for md, src in entries:
                    if md == name:
                        print(f"        → source: {src}")
        if dup_src:
            print(f"\n  ✗  Duplicate SOURCE names ({len(dup_src)}):")
            for name, count in sorted(dup_src.items()):
                print(f"      {name} appears {count}×")
                for md, src in entries:
                    if src == name:
                        print(f"        → target: {md}")

    nc = result["non_conventional_aliases"]
    if nc:
        print(f"\n  ℹ  Non-conventional aliases (informational, {len(nc)}):")
        for item in nc[:20]:
            print(f"      {item}")
        if len(nc) > 20:
            print(f"      … and {len(nc) - 20} more")

    total_issues = len(dup_md) + len(dup_src)
    print(f"\n  {'FAIL' if total_issues else 'PASS'} — {total_issues} blocking issue(s) found")
    print("=" * w)


# ── Before/after narrative (run mode) ──────────────────────────────────────

BEFORE_AFTER_NARRATIVE = """
MATERIALIZATION_MAP Duplicate Audit  —  2026-03-14
=====================================================

BEFORE (script-85 false-positive run, 2026-03-14 20:22 UTC)
─────────────────────────────────────────────────────────────
Script 85's check_map_duplicates() used re.findall(r'"(md_[^"]+)"', entire_file).
This scanned ALL occurrences of quoted md_* strings in the WHOLE of script 26,
including SQL substitution blocks and print statements outside the MAP definition.

  Falsely flagged as duplicates:
  ┌─────────────────────────────────────┬────────────────────────────────────┐
  │  Name                               │  Why it appeared twice              │
  ├─────────────────────────────────────┼────────────────────────────────────┤
  │ md_pathology_recon_review_v2        │ MAP line 66 + SQL .replace() L598  │
  │ md_molecular_linkage_review_v2      │ MAP line 67 + SQL .replace() L600  │
  │ md_rai_adjudication_review_v2       │ MAP line 68 + SQL .replace() L602  │
  │ md_imaging_path_concordance_v2      │ MAP line 69 + SQL .replace() L604  │
  │ md_op_path_recon_review_v2          │ MAP line 70 + SQL .replace() L606  │
  │ md_lineage_audit_v1                 │ MAP line 186 + SURVIVE SQL L629   │
  └─────────────────────────────────────┴────────────────────────────────────┘
  All 6 are LEGITIMATE — they're referenced in the cross-DB substitution
  block that rewrites source table names when materializing to a second DB.

AFTER (script-94 fix applied)
───────────────────────────────
  Resolution: MAP-scoped parser that exits at the closing `]`.
  True duplicate count: 0
  True MATERIALIZATION_MAP entry count: 220

  Classification of all 6 cases:
  ┌─────────────────────────────────────┬────────────────────────────────────┐
  │  Name                               │  Classification                    │
  ├─────────────────────────────────────┼────────────────────────────────────┤
  │ md_pathology_recon_review_v2        │ LEGITIMATE — SQL template variable │
  │ md_molecular_linkage_review_v2      │ LEGITIMATE — SQL template variable │
  │ md_rai_adjudication_review_v2       │ LEGITIMATE — SQL template variable │
  │ md_imaging_path_concordance_v2      │ LEGITIMATE — SQL template variable │
  │ md_op_path_recon_review_v2          │ LEGITIMATE — SQL template variable │
  │ md_lineage_audit_v1                 │ LEGITIMATE — SQL template variable │
  └─────────────────────────────────────┴────────────────────────────────────┘

Action taken:
  • Fixed check_map_duplicates() in 85_materialization_performance_audit.py
  • Created 94_map_dedup_validator.py as the authoritative CI check
  • CI job added: runs script 94 in lint-and-syntax job (no MotherDuck needed)
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--script",
        type=Path,
        default=DEFAULT_SCRIPT,
        help="Path to the script containing MATERIALIZATION_MAP",
    )
    ap.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write a JSON report",
    )
    ap.add_argument(
        "--narrative",
        action="store_true",
        help="Print the before/after narrative and exit (no live check)",
    )
    args = ap.parse_args()

    if args.narrative:
        print(BEFORE_AFTER_NARRATIVE)
        sys.exit(0)

    # ── Parse ──────────────────────────────────────────────────────────────
    if not args.script.exists():
        print(f"ERROR: script not found: {args.script}")
        sys.exit(2)

    try:
        entries = parse_materialization_map(args.script)
    except Exception as exc:
        print(f"ERROR parsing {args.script}: {exc}")
        sys.exit(2)

    if not entries:
        print(f"ERROR: No MATERIALIZATION_MAP entries found in {args.script}")
        sys.exit(2)

    # ── Analyse ────────────────────────────────────────────────────────────
    result = analyse(entries)
    print_report(result, entries)

    # ── Write report ───────────────────────────────────────────────────────
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        result["script_checked"] = str(args.script)
        args.report.write_text(json.dumps(result, indent=2))
        print(f"\n  Report written: {args.report}")

    # ── Exit code ──────────────────────────────────────────────────────────
    issues = len(result["duplicate_md_names"]) + len(result["duplicate_src_names"])
    sys.exit(1 if issues else 0)


if __name__ == "__main__":
    main()
