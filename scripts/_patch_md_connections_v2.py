#!/usr/bin/env python3
"""Comprehensive line-based patcher for MotherDuck connection patterns (v2).

Scans each script for connection functions and replaces them with
the shared `utils.md_connect.connect_md_or_file` layer.

Dry-run by default; pass --apply to write changes.
"""
from __future__ import annotations

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
    "113_tg_lab_ingestion.py",
    "_patch_md_connections.py",
    "_patch_md_connections_v2.py",
}

SKIP = {
    "36_daily_refresh.py",
    "40_cure_model_comparison.py",
}


def has_md_flag(text: str) -> bool:
    return "'--md'" in text or '"--md"' in text


def already_uses_shared(text: str) -> bool:
    return "connect_md_or_file" in text


def infer_db_path(text: str) -> str:
    if "REPO_ROOT" in text and "DB_PATH" not in text:
        return "REPO_ROOT / 'thyroid_master.duckdb'"
    return "DB_PATH"


def find_func_block(lines: list[str], start_idx: int) -> int:
    """Given the start of a function def, find the last line of its body."""
    if start_idx >= len(lines):
        return start_idx
    indent = len(lines[start_idx]) - len(lines[start_idx].lstrip())
    end = start_idx + 1
    while end < len(lines):
        line = lines[end]
        stripped = line.strip()
        if not stripped:
            end += 1
            continue
        line_indent = len(line) - len(line.lstrip())
        if line_indent <= indent and not stripped.startswith("#"):
            break
        end += 1
    # Trim trailing blank lines
    while end > start_idx + 1 and not lines[end - 1].strip():
        end -= 1
    return end


def patch_file(path: Path) -> tuple[str | None, str]:
    """Patch a file and return (change_description, new_content) or (None, '') if no change."""
    text = path.read_text()
    if not has_md_flag(text):
        return None, ""
    if already_uses_shared(text):
        return None, ""

    lines = text.split("\n")
    db_path = infer_db_path(text)
    changes = []

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Pattern: def connect_md() -> ...
        if stripped.startswith("def connect_md(") and "connect_md_or_file" not in stripped:
            end = find_func_block(lines, i)
            replacement = [
                "def connect_md() -> duckdb.DuckDBPyConnection:",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=True)",
            ]
            lines[i:end] = replacement
            changes.append("replaced connect_md()")
            break

        # Pattern: def get_connection(md: bool):
        if stripped.startswith("def get_connection(md:"):
            end = find_func_block(lines, i)
            replacement = [
                "def get_connection(md: bool):",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=md)",
            ]
            lines[i:end] = replacement
            changes.append("replaced get_connection(md: bool)")
            break

        # Pattern: def get_connection(use_md):
        if stripped.startswith("def get_connection(use_md"):
            end = find_func_block(lines, i)
            replacement = [
                "def get_connection(use_md):",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=use_md)",
            ]
            lines[i:end] = replacement
            changes.append("replaced get_connection(use_md)")
            break

        # Pattern: def connect(args) -> duckdb...:
        if (stripped.startswith("def connect(args)") and
                "connect_md_or_file" not in stripped):
            end = find_func_block(lines, i)
            replacement = [
                "def connect(args) -> duckdb.DuckDBPyConnection:",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=getattr(args, 'md', False))",
            ]
            lines[i:end] = replacement
            changes.append("replaced connect(args)")
            break

        # Pattern: def connect(use_md: bool = False, use_local: bool = False):
        if stripped.startswith("def connect(use_md:"):
            end = find_func_block(lines, i)
            replacement = [
                "def connect(use_md: bool = False, use_local: bool = False) -> duckdb.DuckDBPyConnection:",
                "    import os as _os",
                "    if use_local or _os.environ.get('USE_LOCAL_DUCKDB'):",
                "        path = _os.environ.get('LOCAL_DUCKDB_PATH', str(ROOT / 'thyroid_master_local.duckdb'))",
                "        return duckdb.connect(path)",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=use_md)",
            ]
            lines[i:end] = replacement
            changes.append("replaced connect(use_md, use_local)")
            break

        # Pattern: def _get_con(args):
        if stripped.startswith("def _get_con(args"):
            end = find_func_block(lines, i)
            replacement = [
                "def _get_con(args) -> duckdb.DuckDBPyConnection:",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=getattr(args, 'md', False))",
            ]
            lines[i:end] = replacement
            changes.append("replaced _get_con(args)")
            break

        # Pattern: def connect_duckdb(use_md...):
        if stripped.startswith("def connect_duckdb(") and "connect_md_or_file" not in stripped:
            end = find_func_block(lines, i)
            replacement = [
                "def connect_duckdb(use_md: bool = False):",
                "    from utils.md_connect import connect_md_or_file",
                f"    return connect_md_or_file({db_path}, md=use_md)",
            ]
            lines[i:end] = replacement
            changes.append("replaced connect_duckdb()")
            break

    if not changes:
        return None, ""

    # Now clean dead _get_token if no longer referenced
    new_text = "\n".join(lines)
    new_lines = new_text.split("\n")
    for i, line in enumerate(new_lines):
        if line.strip().startswith("def _get_token("):
            end = find_func_block(new_lines, i)
            remaining = "\n".join(new_lines[:i] + new_lines[end:])
            if "_get_token" not in remaining:
                new_lines[i:end] = []
                changes.append("removed dead _get_token()")
                break

    return "; ".join(changes), "\n".join(new_lines)


def main():
    apply = "--apply" in sys.argv
    fixed: list[str] = []
    skipped: list[str] = []

    for path in sorted(SCRIPTS.glob("*.py")):
        name = path.name
        if name in ALREADY_CORRECT or name in SKIP or name.startswith("_"):
            continue

        change, new_content = patch_file(path)
        if change:
            if apply:
                path.write_text(new_content)
            fixed.append(f"{name}: {change}")
        elif has_md_flag(path.read_text()) and not already_uses_shared(path.read_text()):
            skipped.append(name)

    print(f"\n{'APPLIED' if apply else 'DRY RUN'}: {len(fixed)} files patched\n")
    for f in fixed:
        print(f"  ✓ {f}")

    if skipped:
        print(f"\n⚠ {len(skipped)} files need manual fix:")
        for f in skipped:
            print(f"  • {f}")

    if not apply:
        print("\nRe-run with --apply to write changes.")


if __name__ == "__main__":
    main()
