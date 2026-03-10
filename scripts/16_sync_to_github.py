#!/usr/bin/env python3
"""
16_sync_to_github.py — Full GitHub Sync + Publication Tag

Stages all tracked artifacts (scripts, docs, notebooks, exports, studies),
commits with a structured message, pushes to origin/main, and ensures the
publication tag exists on both local and remote.

Adds or updates in this commit:
  • scripts/27_fix_legacy_episode_compatibility.py  (legacy compat layer)
  • dashboard.py                                     (graceful compat warning + footer)
  • scripts/16_sync_to_github.py                    (this file)
  • docs/QA_report.md, docs/pipeline_architecture_v2.md
  • RELEASE_NOTES.md, README.md, CITATION.cff
  • MANUSCRIPT_READY_CHECKLIST.md, data_dictionary.md
  • studies/proposal2_ete_staging/README.md
  • notebooks/01_publication_figures.ipynb

Usage:
    .venv/bin/python scripts/16_sync_to_github.py
    .venv/bin/python scripts/16_sync_to_github.py --dry-run
    .venv/bin/python scripts/16_sync_to_github.py --skip-tag
    .venv/bin/python scripts/16_sync_to_github.py --skip-push
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
TAG = "v2026.03.10-publication-ready"

COMMIT_MESSAGE = (
    "fix: legacy episode compatibility layer + dashboard graceful fallback\n\n"
    "- scripts/27_fix_legacy_episode_compatibility.py: creates 5 missing\n"
    "  tables (molecular_episode_v3, rai_episode_v3, validation_failures_v3,\n"
    "  tumor_episode_master_v2, linkage_summary_v2) as compatibility views\n"
    "  on top of modern stack (no duplication)\n"
    "- dashboard.py: replaces hard 'Missing critical tables' error with\n"
    "  graceful info warning; modern tables remain fully functional\n"
    "- dashboard.py: publication-ready footer banner on all pages\n"
    "- scripts/16_sync_to_github.py: idempotent sync + tag helper\n"
    "- data_dictionary.md: legacy ↔ modern table mapping section\n"
    "\n"
    "Dashboard error resolved: molecular_episode_v3, rai_episode_v3,\n"
    "validation_failures_v3, tumor_episode_master_v2, linkage_summary_v2\n"
    "all now auto-created from existing modern tables."
)

# Paths to stage (relative to REPO_ROOT)
STAGE_PATTERNS = [
    "scripts/13_performance_optimizations_pack.py",
    "scripts/14_final_publication_and_downgrade_prep.py",
    "scripts/15_final_validation_and_release.py",
    "scripts/16_sync_to_github.py",
    "scripts/27_fix_legacy_episode_compatibility.py",
    "dashboard.py",
    "docs/",
    "data_dictionary.md",
    "RELEASE_NOTES.md",
    "README.md",
    "CITATION.cff",
    "MANUSCRIPT_READY_CHECKLIST.md",
    "studies/proposal2_ete_staging/README.md",
    "notebooks/01_publication_figures.ipynb",
]


def run_cmd(
    cmd: list[str],
    check: bool = True,
    capture: bool = False,
    dry_run: bool = False,
) -> subprocess.CompletedProcess:
    display = " ".join(cmd)
    if dry_run:
        print(f"  [DRY RUN] {display}")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    result = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=capture, text=True
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip() if capture else ""
        print(f"[ERROR] Command failed: {display}")
        if stderr:
            print(f"        {stderr}")
        sys.exit(result.returncode)
    return result


def git_status(dry_run: bool) -> None:
    print("\n── Git status (before staging) ──────────────────────────────")
    run_cmd(["git", "status", "--short"], dry_run=False)
    print()


def stage_artifacts(dry_run: bool) -> None:
    print("── Staging artifacts ─────────────────────────────────────────")
    for pattern in STAGE_PATTERNS:
        path = REPO_ROOT / pattern
        if not path.exists():
            print(f"  [SKIP] not found: {pattern}")
            continue
        print(f"  git add {pattern}")
        run_cmd(["git", "add", pattern], dry_run=dry_run)


def commit(dry_run: bool) -> bool:
    """Return True if a commit was created."""
    result = run_cmd(
        ["git", "diff", "--cached", "--quiet"],
        check=False,
        capture=True,
        dry_run=False,
    )
    if result.returncode == 0 and not dry_run:
        print("  Nothing staged — skipping commit.")
        return False

    print(f"\n── Committing ────────────────────────────────────────────────")
    run_cmd(["git", "commit", "-m", COMMIT_MESSAGE], dry_run=dry_run)
    return True


def push(dry_run: bool) -> None:
    print("\n── Pushing to origin/main ────────────────────────────────────")
    run_cmd(["git", "push", "origin", "main"], dry_run=dry_run)


def ensure_tag(dry_run: bool, skip_push: bool) -> None:
    print(f"\n── Ensuring tag {TAG} ─────────────────────────────────────")
    # Check if tag already exists locally
    result = run_cmd(
        ["git", "tag", "-l", TAG],
        capture=True,
        check=False,
        dry_run=False,
    )
    if result.stdout.strip() == TAG:
        print(f"  Tag {TAG} already exists locally.")
    else:
        run_cmd(["git", "tag", TAG], dry_run=dry_run)
        print(f"  Created tag {TAG}.")

    if not skip_push:
        # Push tag to remote (ignore error if already pushed)
        result = run_cmd(
            ["git", "push", "origin", TAG],
            check=False,
            capture=True,
            dry_run=dry_run,
        )
        if result.returncode == 0:
            print(f"  Tag {TAG} pushed to remote.")
        else:
            stderr = result.stderr.strip() if result.stderr else ""
            if "already exists" in stderr or "Everything up-to-date" in stderr:
                print(f"  Tag {TAG} already on remote.")
            else:
                print(f"  [WARN] Tag push: {stderr[:120]}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync THYROID_2026 to GitHub + ensure publication tag.")
    ap.add_argument("--dry-run",    action="store_true", help="Print git commands without running them")
    ap.add_argument("--skip-tag",   action="store_true", help="Skip tag creation/push")
    ap.add_argument("--skip-push",  action="store_true", help="Commit but do not push")
    args = ap.parse_args()

    print("=" * 68)
    print("  THYROID_2026 — GitHub Sync")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    if args.dry_run:
        print("  MODE: DRY RUN — no git commands will execute")
    print("=" * 68)

    git_status(args.dry_run)
    stage_artifacts(args.dry_run)
    committed = commit(args.dry_run)

    if not args.skip_push:
        push(args.dry_run)

    if not args.skip_tag:
        ensure_tag(args.dry_run, args.skip_push)

    print()
    print("=" * 68)
    print("  GitHub sync complete.")
    if committed or args.dry_run:
        print(f"  Commit message: {COMMIT_MESSAGE.splitlines()[0]}")
    print(f"  Tag: {TAG}")
    print("=" * 68)
    print()
    print("  Next steps:")
    print("    1. Restart Streamlit: streamlit run dashboard.py")
    print("    2. Verify dashboard — legacy tables error should be gone")
    print("    3. Consider: Zenodo archive or analytic modeling phase")
    print()


if __name__ == "__main__":
    main()
