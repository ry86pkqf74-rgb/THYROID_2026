#!/usr/bin/env python3
"""
Guardrail: verify version string consistency and banned overclaim phrases.

Run:
    python scripts/check_truth_sync.py

Returns exit code 0 if all checks pass, 1 if any fail.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BANNED_PHRASES = [
    r"every data point.{0,30}traceable",
    r"fully extracted",
    r"fully linked",
    r"100% traceable",
    r"complete traceability",
    r"fully verified",
]

ALLOWED_CONTEXTS = [
    "claims to avoid",
    "claims we should avoid",
    "overclaim",
    "removed",
    "rewritten",
    "~~",
    "banned",
    "do not",
    "must not",
    "check_truth_sync",
    "should avoid",
    "not supported",
    "why it fails",
    "no blanket",
    "are they fully",
    "successfully extracted",
    "successfully linked",
    "successfully",
    "were successfully",
]

VERSION_PATTERN = re.compile(r"v?(\d{4}\.\d{2}\.\d{2})")


def extract_dashboard_version() -> str | None:
    path = ROOT / "dashboard.py"
    for line in path.read_text().splitlines():
        m = re.search(r'_APP_VERSION\s*=\s*"([^"]+)"', line)
        if m:
            return m.group(1)
    return None


def extract_release_notes_version() -> str | None:
    path = ROOT / "RELEASE_NOTES.md"
    for line in path.read_text().splitlines():
        if "(Latest)" in line:
            m = VERSION_PATTERN.search(line)
            if m:
                return m.group(1)
    return None


def extract_citation_version() -> str | None:
    path = ROOT / "CITATION.cff"
    for line in path.read_text().splitlines():
        if line.startswith("version:"):
            m = VERSION_PATTERN.search(line)
            if m:
                return m.group(1)
    return None


def check_versions() -> list[str]:
    errors = []
    dash = extract_dashboard_version()
    rn = extract_release_notes_version()
    cit = extract_citation_version()

    dash_date = VERSION_PATTERN.search(dash) if dash else None
    dash_date_str = dash_date.group(1) if dash_date else None

    if dash_date_str and rn and dash_date_str != rn:
        errors.append(
            f"Version date mismatch: dashboard={dash_date_str}, "
            f"release_notes={rn}"
        )
    if dash_date_str and cit and dash_date_str != cit:
        errors.append(
            f"Version date mismatch: dashboard={dash_date_str}, "
            f"citation={cit}"
        )
    if rn and cit and rn != cit:
        errors.append(
            f"Version date mismatch: release_notes={rn}, citation={cit}"
        )

    return errors


def _is_allowed_context(line: str, nearby_lines: list[str] | None = None) -> bool:
    low = line.lower()
    if any(ctx in low for ctx in ALLOWED_CONTEXTS):
        return True
    if nearby_lines:
        for nl in nearby_lines:
            nl_low = nl.lower()
            if any(ctx in nl_low for ctx in ALLOWED_CONTEXTS):
                return True
    return False


def check_banned_phrases() -> list[str]:
    errors = []
    scan_globs = ["*.md", "*.py"]
    skip_dirs = {
        ".venv", ".git", "node_modules", "__pycache__", ".cursor",
        "exports", "studies",
    }
    skip_files = {"check_truth_sync.py", "AGENTS.md"}

    for glob in scan_globs:
        for path in ROOT.rglob(glob):
            if any(d in path.parts for d in skip_dirs):
                continue
            if path.name in skip_files:
                continue
            try:
                text = path.read_text(errors="replace")
            except Exception:
                continue
            lines = text.splitlines()
            for i, line in enumerate(lines, 1):
                nearby = lines[max(0, i - 8):i - 1]
                if _is_allowed_context(line, nearby):
                    continue
                for pattern in BANNED_PHRASES:
                    if re.search(pattern, line, re.IGNORECASE):
                        rel = path.relative_to(ROOT)
                        errors.append(
                            f"{rel}:{i}: banned phrase matches "
                            f"/{pattern}/ -> {line.strip()[:120]}"
                        )
    return errors


def main() -> int:
    print("=== Truth-Sync Guardrail ===\n")
    all_errors: list[str] = []

    print("1. Version consistency...")
    ver_errs = check_versions()
    all_errors.extend(ver_errs)
    for e in ver_errs:
        print(f"   FAIL: {e}")
    if not ver_errs:
        print("   PASS")

    print("\n2. Banned overclaim phrases...")
    phrase_errs = check_banned_phrases()
    all_errors.extend(phrase_errs)
    for e in phrase_errs:
        print(f"   FAIL: {e}")
    if not phrase_errs:
        print("   PASS")

    print(f"\n{'='*40}")
    if all_errors:
        print(f"FAILED: {len(all_errors)} issue(s) found")
        return 1
    else:
        print("ALL CHECKS PASSED")
        return 0


if __name__ == "__main__":
    sys.exit(main())
