#!/usr/bin/env python3
"""
apply_patches.py — idempotent patcher for the LLM extractor.

Applies two fixes needed for vLLM + Qwen2.5-72B-AWQ to work with
outlines_core 0.1.26 and Qwen's markdown-fenced JSON output format:

Bug 9 (outlines_core crash):
    Remove `response_format={"type": "json_object"}` argument from the
    vLLM `chat.completions.create()` call in extract_llm.py (~line 351).

Bug 10 (Qwen wraps output in ```json ... ```):
    Prepend fence-stripping logic before `json.loads(raw_json)` in
    extract_llm.py (~line 420).

Safe to re-run — detects existing patches and skips.

Usage:
    python apply_patches.py                       # patches ~/scratch_repo
    python apply_patches.py --repo /tmp/work_repo # different target
    python apply_patches.py --check               # report only, no writes
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

BUG9_NEEDLE = 'response_format={"type": "json_object"}'
BUG10_NEEDLE = "raw_json.startswith"
BUG10_TARGET = "data = json.loads(raw_json)"


def patch_extract_llm(path: Path, check: bool) -> tuple[int, int]:
    """Apply both patches. Returns (bug9_remaining_count, bug10_present_flag)."""
    lines = path.read_text().splitlines(keepends=True)

    # ── Bug 9 ── remove any line containing the needle ──────────────────
    new_lines: list[str] = []
    bug9_removed = 0
    for n, line in enumerate(lines, start=1):
        if BUG9_NEEDLE in line:
            bug9_removed += 1
            if check:
                print(f"  Bug9 hit at line {n}: {line.rstrip()}")
                new_lines.append(line)
            # else: skip (remove the line)
        else:
            new_lines.append(line)
    if not check and bug9_removed:
        print(f"  Bug9: removed {bug9_removed} occurrence(s)")
    elif bug9_removed == 0:
        print("  Bug9: already clean (0 occurrences)")

    # ── Bug 10 ── insert fence-strip before json.loads(raw_json) ───────
    text = "".join(new_lines)
    if BUG10_NEEDLE in text:
        print("  Bug10: already patched")
    else:
        inserted = False
        for i, line in enumerate(new_lines):
            if BUG10_TARGET in line:
                indent = line[: len(line) - len(line.lstrip())]
                fence_lines = [
                    f'{indent}if raw_json.startswith("```"):\n',
                    f'{indent}    raw_json = raw_json.split("\\n", 1)[-1].rsplit("```", 1)[0].strip()\n',
                ]
                if check:
                    print(f"  Bug10 would insert before line {i+1}: {line.rstrip()}")
                else:
                    new_lines = new_lines[:i] + fence_lines + new_lines[i:]
                    print(f"  Bug10: inserted fence-strip before line {i+1}")
                inserted = True
                break
        if not inserted:
            print(
                f"  Bug10: could not find target '{BUG10_TARGET}' "
                f"— manual patch required"
            )

    if not check:
        path.write_text("".join(new_lines))

    # ── Verify ──────────────────────────────────────────────────────────
    final_text = path.read_text()
    bug9_final = final_text.count(BUG9_NEEDLE)
    bug10_final = 1 if BUG10_NEEDLE in final_text else 0
    return bug9_final, bug10_final


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Apply Bug 9 + Bug 10 patches to extract_llm.py",
    )
    ap.add_argument(
        "--repo",
        default=str(Path.home() / "scratch_repo"),
        help="Path to the repo root (default: ~/scratch_repo)",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="Report what would change without writing",
    )
    args = ap.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    target = repo / "llm_extraction" / "extract_llm.py"
    if not target.exists():
        print(f"ERROR: {target} not found", file=sys.stderr)
        return 1

    print(f"Target: {target}")
    print(f"Mode:   {'CHECK (no changes)' if args.check else 'APPLY'}")
    print()
    bug9_remaining, bug10_present = patch_extract_llm(target, check=args.check)
    print()
    print(f"Final state:")
    print(f"  Bug9 remaining (want 0): {bug9_remaining}")
    print(f"  Bug10 present   (want 1): {bug10_present}")

    if bug9_remaining == 0 and bug10_present == 1:
        print("  STATUS: OK — patches in place")
        return 0
    print("  STATUS: INCOMPLETE", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
