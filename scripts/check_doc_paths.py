#!/usr/bin/env python3
"""Fail if tracked docs reference missing studies/ or docs/ paths (lightweight CI guard).

Scans markdown links and backtick-enclosed relative paths in a fixed allowlist of files.
Does not follow arbitrary URLs or absolute paths.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files that frequently deep-link to studies/ or docs/
DOC_SOURCES = [
    ROOT / "README.md",
    ROOT / "RELEASE_NOTES.md",
    ROOT / "truth_sync_summary.md",
    ROOT / "docs" / "motherduck_database_contract_v1.md",
    ROOT / "docs" / "motherduck_release_runbook_v2.md",
    ROOT / "docs" / "release_runbook.md",
    ROOT / "docs" / "REPO_STATUS.md",
    ROOT / "docs" / "final_master_database_contract.md",
    ROOT / "docs" / "final_source_of_truth_contract.md",
]

# Match (studies/...) or (docs/...) in markdown links; optional anchor after #
LINK_RE = re.compile(r"\]\((studies/[^)#\s]+|docs/[^)#\s]+)")
# Backtick paths `studies/...` or `docs/...`
BT_RE = re.compile(r"`(studies/[^`\s]+|docs/[^`\s]+)`")


def collect_paths(text: str) -> set[str]:
    out: set[str] = set()
    for m in LINK_RE.finditer(text):
        out.add(m.group(1).split("#", 1)[0].rstrip("/"))
    for m in BT_RE.finditer(text):
        out.add(m.group(1).split("#", 1)[0].rstrip("/"))
    return out


def _is_placeholder(rel: str) -> bool:
    """Skip template paths in docs (e.g. studies/<date>_foo, YYYYMMDD)."""
    if "<" in rel or ">" in rel:
        return True
    if "YYYYMMDD" in rel or "<tag>" in rel.lower() or "<date>" in rel.lower():
        return True
    return False


def main() -> int:
    missing: list[tuple[str, str]] = []
    for src in DOC_SOURCES:
        if not src.is_file():
            continue
        text = src.read_text(encoding="utf-8")
        for rel in sorted(collect_paths(text)):
            if _is_placeholder(rel):
                continue
            target = (ROOT / rel).resolve()
            try:
                target.relative_to(ROOT.resolve())
            except ValueError:
                missing.append((str(src.relative_to(ROOT)), rel))
                continue
            if not target.exists():
                missing.append((str(src.relative_to(ROOT)), rel))

    if missing:
        print("check_doc_paths: missing referenced paths:", file=sys.stderr)
        for src_file, rel_path in missing:
            print(f"  {src_file} -> {rel_path}", file=sys.stderr)
        return 1
    print("check_doc_paths: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
