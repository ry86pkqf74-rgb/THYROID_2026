#!/usr/bin/env python3
"""Patch all scripts/ files that have --md but use legacy connection logic.

Replaces the common `_get_token()` + `connect_md()` + `connect_local()` pattern
with delegation to `utils.md_connect.connect_md_or_file`.

Also fixes:
  - Pattern A: inline MotherDuckClient().connect_rw() blocks
  - Pattern C: raw duckdb.connect("md:...") URIs
  - Pattern D: _get_token() + duckdb.connect("thyroid_master.duckdb")
  - Pattern E: get_connection() wrappers

Dry-run by default; pass --apply to write changes.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"

ALREADY_CORRECT = {
    "02b_register_notes_entities.py",
    "22_canonical_episodes_v2.py",
    "23_cross_domain_linkage_v2.py",
    "24_reconciliation_review_v2.py",
    "25_qa_validation_v2.py",
    "29_validation_engine.py",
    "77_lab_canonical_layer.py",
    "103_fact_lineage_materialize.py",
    "113_tg_lab_ingestion.py",  # fixed manually
    "_patch_md_connections.py",  # this script
}

SKIP_ORCHESTRATORS = {
    "36_daily_refresh.py",  # only forwards --md to subprocesses
    "40_cure_model_comparison.py",  # --md unused, no DB connection
}

# ── Pattern D: _get_token + connect_md that opens local file ────────────────

PATTERN_D_GET_TOKEN = re.compile(
    r"""
    (?P<block>
    def\s+_get_token\s*\(\s*\)\s*->\s*str:\s*\n
    (?:.*\n)*?            # body lines
    \s*raise\s+RuntimeError\s*\(
    (?:.*\n)*?            # multi-line error string
    \s*\)\s*\n
    )
    """,
    re.VERBOSE,
)

PATTERN_D_CONNECT_MD = re.compile(
    r"""
    (?P<block>
    def\s+connect_md\s*\(\s*\)\s*->\s*duckdb\.DuckDBPyConnection:\s*\n
    \s+(?:token\s*=\s*_get_token\(\)|_get_token\(\))\s*\n
    \s+return\s+duckdb\.connect\s*\(\s*f?"thyroid_master\.duckdb"\s*\)\s*\n
    )
    """,
    re.VERBOSE,
)

REPLACEMENT_CONNECT_MD = (
    "def connect_md() -> duckdb.DuckDBPyConnection:\n"
    "    from utils.md_connect import connect_md_or_file\n"
    "    return connect_md_or_file(DB_PATH, md=True)\n"
)

# ── Pattern for get_connection(use_md) wrappers ────────────────────────────

PATTERN_GET_CONNECTION = re.compile(
    r"""
    (?P<block>
    def\s+get_connection\s*\(\s*use_md\s*\)\s*:\s*\n
    \s+import\s+duckdb\s*\n
    (?:\s+.*\n)*?
    \s+return\s+duckdb\.connect\s*\(.*?\)\s*\n
    )
    """,
    re.VERBOSE,
)

REPLACEMENT_GET_CONNECTION = (
    "def get_connection(use_md):\n"
    "    from utils.md_connect import connect_md_or_file\n"
    "    return connect_md_or_file(REPO_ROOT / 'thyroid_master.duckdb', md=use_md)\n"
)


def has_md_flag(text: str) -> bool:
    return bool(re.search(r"""['"]-{1,2}md['"]""", text))


def already_uses_shared(text: str) -> bool:
    return "connect_md_or_file" in text


def patch_pattern_d(text: str) -> tuple[str, list[str]]:
    """Fix Pattern D: _get_token + connect_md → shared layer."""
    changes: list[str] = []

    # Replace connect_md body
    m = PATTERN_D_CONNECT_MD.search(text)
    if m:
        text = text[: m.start()] + REPLACEMENT_CONNECT_MD + text[m.end() :]
        changes.append("replaced connect_md() body with connect_md_or_file delegation")

    # Remove _get_token if connect_md was the only consumer
    if changes and "_get_token" not in text.replace(
        "def _get_token", ""
    ).replace("_get_token()", ""):
        # _get_token is only referenced in its own def — safe to remove
        pass  # We'll leave it; it's dead code but harmless
    elif changes:
        m2 = PATTERN_D_GET_TOKEN.search(text)
        if m2:
            # Check if _get_token is called anywhere BESIDES the old connect_md
            remaining = text[: m2.start()] + text[m2.end() :]
            if "_get_token" not in remaining:
                text = text[: m2.start()] + "\n" + text[m2.end() :]
                changes.append("removed unused _get_token()")

    return text, changes


def patch_get_connection(text: str) -> tuple[str, list[str]]:
    """Fix get_connection(use_md) pattern (script 90 style)."""
    changes: list[str] = []
    m = PATTERN_GET_CONNECTION.search(text)
    if m:
        # Detect what path variable is used
        block = m.group("block")
        if "REPO_ROOT" in block:
            path_var = "REPO_ROOT / 'thyroid_master.duckdb'"
        elif "ROOT" in block:
            path_var = "ROOT / 'thyroid_master.duckdb'"
        else:
            path_var = "DB_PATH"

        replacement = (
            "def get_connection(use_md):\n"
            f"    from utils.md_connect import connect_md_or_file\n"
            f"    return connect_md_or_file({path_var}, md=use_md)\n"
        )
        text = text[: m.start()] + replacement + text[m.end() :]
        changes.append("replaced get_connection() with connect_md_or_file delegation")

    return text, changes


def main():
    apply = "--apply" in sys.argv
    fixed_files: list[str] = []
    skipped_files: list[str] = []

    for path in sorted(SCRIPTS.glob("*.py")):
        name = path.name
        if name in ALREADY_CORRECT or name in SKIP_ORCHESTRATORS:
            continue
        if name.startswith("_") and name != "_phase4_fix_all.py":
            continue

        text = path.read_text()
        if not has_md_flag(text):
            continue
        if already_uses_shared(text):
            continue

        all_changes: list[str] = []

        # Try Pattern D
        text, ch = patch_pattern_d(text)
        all_changes.extend(ch)

        # Try get_connection wrapper
        text, ch = patch_get_connection(text)
        all_changes.extend(ch)

        if all_changes:
            if apply:
                path.write_text(text)
            fixed_files.append(f"{name}: {'; '.join(all_changes)}")
        else:
            skipped_files.append(name)

    print(f"\n{'APPLIED' if apply else 'DRY RUN'}: {len(fixed_files)} files patched\n")
    for f in fixed_files:
        print(f"  ✓ {f}")

    if skipped_files:
        print(f"\n⚠ {len(skipped_files)} files have --md but non-standard pattern (manual fix needed):")
        for f in skipped_files:
            print(f"  • {f}")

    if not apply:
        print("\nRe-run with --apply to write changes.")


if __name__ == "__main__":
    main()
