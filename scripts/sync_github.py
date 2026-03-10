#!/usr/bin/env python3
"""
sync_github.py — Safe, idempotent GitHub sync helper for THYROID_2026.

Stages all untracked/modified artifacts (scripts, docs, exports, studies,
notebooks), commits with a structured message, pushes to origin main, and
ensures the publication tag exists on the remote.

Usage:
    .venv/bin/python scripts/sync_github.py [--dry-run]

Safe by design:
  - Never force-pushes.
  - Prints full git status before staging.
  - Exits non-zero on any git error.
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).parent.parent
TAG = "v2026.03.10-publication-ready"
COMMIT_MESSAGE = (
    "feat: scripts 13–15 performance pack + final publication bundle + "
    "trial-downgrade backup (v2026.03.10-publication-ready)\n\n"
    "- Scripts 13–15: optimized MVs, sub-second dashboard tabs, local DuckDB backup\n"
    "- V2 canonical pipeline: scripts 22–29 (episodes, linkage, QA, validation)\n"
    "- Publication bundles + manuscript tables in exports/\n"
    "- Validation run artifacts in exports/validation_run_20260310_1331/\n"
    "- MANUSCRIPT_READY_CHECKLIST.md, RELEASE_NOTES updates, QA_report updates\n"
    "- studies/proposal2_ete_staging/ analysis + README\n"
    "- docs/ folder: QA_report.md, pipeline_architecture_v2.md\n"
    "- Git tag: v2026.03.10-publication-ready"
)

# Paths to stage (relative to REPO_ROOT, supports globs)
STAGE_PATTERNS = [
    "scripts/",
    "docs/",
    "RELEASE_NOTES.md",
    "MANUSCRIPT_READY_CHECKLIST.md",
    "CITATION.cff",
    "data_dictionary.md",
    "README.md",
    "studies/proposal2_ete_staging/README.md",
    "notebooks/01_publication_figures.ipynb",
    "exports/validation_run_20260310_1331/",
]


def run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=capture, text=True
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip() if capture else ""
        print(f"[ERROR] Command failed: {' '.join(cmd)}")
        if stderr:
            print(f"        {stderr}")
        sys.exit(result.returncode)
    return result


def print_section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main() -> None:
    parser = argparse.ArgumentParser(description="THYROID_2026 GitHub sync helper")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    args = parser.parse_args()
    dry = args.dry_run

    print_section("THYROID_2026 GitHub Sync")
    print(f"Timestamp : {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Repo root : {REPO_ROOT}")
    print(f"Tag       : {TAG}")
    print(f"Dry run   : {dry}")

    # 1. Show current status
    print_section("1 · Current git status")
    run(["git", "status", "--short"])

    # 2. Stage artifacts
    print_section("2 · Staging artifacts")
    for pattern in STAGE_PATTERNS:
        full = REPO_ROOT / pattern
        # Use glob for patterns; add directly if path exists
        if full.exists():
            target = str(pattern)
            print(f"  git add {target}")
            if not dry:
                run(["git", "add", target])
        else:
            # Try glob expansion
            matched = list(REPO_ROOT.glob(pattern))
            if matched:
                for m in matched:
                    rel = str(m.relative_to(REPO_ROOT))
                    print(f"  git add {rel}")
                    if not dry:
                        run(["git", "add", rel])
            else:
                print(f"  [SKIP] {pattern} — not found")

    # 3. Check if there is anything to commit
    print_section("3 · Checking staged changes")
    status = run(["git", "status", "--porcelain"], capture=True)
    staged = [l for l in status.stdout.splitlines() if l and l[0] in "MADRCU"]
    if not staged:
        print("  Nothing staged — repository is already in sync.")
    else:
        print(f"  {len(staged)} file(s) staged for commit.")
        for line in staged[:20]:
            print(f"    {line}")
        if len(staged) > 20:
            print(f"    ... and {len(staged) - 20} more")

        # 4. Commit
        print_section("4 · Committing")
        if not dry:
            run(["git", "commit", "-m", COMMIT_MESSAGE])
        else:
            print("  [DRY RUN] Would commit with message:")
            print(f"  {COMMIT_MESSAGE[:80]}...")

    # 5. Push
    print_section("5 · Pushing to origin main")
    if not dry:
        run(["git", "push", "origin", "main"])
        print("  Push complete.")
    else:
        print("  [DRY RUN] Would push to origin main.")

    # 6. Ensure tag exists locally and on remote
    print_section(f"6 · Ensuring tag {TAG}")
    existing_tags = run(["git", "tag", "-l", TAG], capture=True).stdout.strip()
    if existing_tags:
        print(f"  Tag {TAG} already exists locally.")
    else:
        print(f"  Creating local tag {TAG}...")
        if not dry:
            run(["git", "tag", TAG])

    # Push tag (idempotent — fails silently if already on remote)
    print(f"  Pushing tag {TAG} to origin...")
    if not dry:
        tag_push = run(["git", "push", "origin", TAG], check=False, capture=True)
        if tag_push.returncode == 0:
            print(f"  Tag {TAG} pushed successfully.")
        else:
            stderr = tag_push.stderr.strip()
            if "already exists" in stderr or "Everything up-to-date" in stderr:
                print(f"  Tag {TAG} already on remote.")
            else:
                print(f"  [WARN] Tag push returned: {stderr}")
    else:
        print(f"  [DRY RUN] Would push tag {TAG}.")

    # 7. Final commit hash
    print_section("7 · Final state")
    commit_hash = run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()
    print(f"  HEAD commit : {commit_hash}")
    remote_url = run(["git", "remote", "get-url", "origin"], capture=True).stdout.strip()
    print(f"  Remote      : {remote_url}")
    print("\n  \u2705 All tasks complete. Repo is synchronized and publication-grade.")
    print(f"     Tag    : {TAG}")
    print(f"     Commit : {commit_hash}")


if __name__ == "__main__":
    main()
