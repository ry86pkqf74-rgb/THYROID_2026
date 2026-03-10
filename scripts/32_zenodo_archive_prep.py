#!/usr/bin/env python3
"""
32_zenodo_archive_prep.py — Zenodo Archive Preparation

Assembles a self-contained archive directory suitable for upload to
Zenodo (or Dryad / Figshare). Copies code, exports, documentation,
and metadata into a single folder, then creates a .zenodo.json manifest.

Does NOT upload — the researcher reviews the bundle, then uploads
manually via the Zenodo web form or API.

Output: exports/zenodo_archive_v2026.03.10/
  ├── code/           (scripts/, notebooks/, app/, motherduck_client.py, dashboard.py)
  ├── data/           (CSV + Parquet from latest publication bundle)
  ├── docs/           (README, RELEASE_NOTES, data_dictionary, QA_report, CITATION.cff)
  ├── studies/        (proposal2_ete_staging/, analytic_models/)
  ├── .zenodo.json    (Zenodo metadata)
  └── MANIFEST.txt    (file listing with sizes)

Usage:
    .venv/bin/python scripts/32_zenodo_archive_prep.py
    .venv/bin/python scripts/32_zenodo_archive_prep.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TAG = "v2026.03.10-publication-ready"
ARCHIVE_DIR = ROOT / "exports" / f"zenodo_archive_{TAG.lstrip('v').replace('-publication-ready', '')}"


# ── Files to include ──────────────────────────────────────────────────────

CODE_FILES = [
    "motherduck_client.py",
    "dashboard.py",
    "requirements.txt",
    "runtime.txt",
]

CODE_DIRS = [
    "scripts",
    "notebooks",
    "app",
]

DOC_FILES = [
    "README.md",
    "RELEASE_NOTES.md",
    "data_dictionary.md",
    "CITATION.cff",
    "MANUSCRIPT_READY_CHECKLIST.md",
]

DOC_DIRS = [
    "docs",
]

STUDY_DIRS = [
    "studies/proposal2_ete_staging",
    "studies/analytic_models",
]

BUNDLE_CANDIDATES = [
    "exports/FINAL_RELEASE_v2026.03.10_20260310_0529",
    "exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414",
]

EXCLUDE_PATTERNS = {".pyc", "__pycache__", ".DS_Store", ".git", ".venv", ".dvc", "node_modules"}


def _should_skip(path: Path) -> bool:
    for part in path.parts:
        if part in EXCLUDE_PATTERNS:
            return True
    return False


def _sha256(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_tree(src: Path, dst: Path, manifest: list[dict], dry_run: bool) -> int:
    count = 0
    if not src.exists():
        return 0
    for item in sorted(src.rglob("*")):
        if item.is_dir() or _should_skip(item):
            continue
        rel = item.relative_to(ROOT)
        target = dst / rel
        if dry_run:
            print(f"  [DRY] {rel}")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)
            manifest.append({
                "path": str(rel),
                "size_bytes": item.stat().st_size,
                "sha256": _sha256(item),
            })
        count += 1
    return count


def _copy_file(src: Path, dst_dir: Path, manifest: list[dict], dry_run: bool) -> bool:
    if not src.exists():
        print(f"  [SKIP] {src.name} — not found")
        return False
    rel = src.relative_to(ROOT)
    target = dst_dir / rel
    if dry_run:
        print(f"  [DRY] {rel}")
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        manifest.append({
            "path": str(rel),
            "size_bytes": src.stat().st_size,
            "sha256": _sha256(src),
        })
    return True


def run(args: argparse.Namespace) -> int:
    print("=" * 68)
    print("  Zenodo Archive Preparation")
    print(f"  Tag: {TAG}")
    print(f"  Output: {ARCHIVE_DIR.relative_to(ROOT)}")
    if args.dry_run:
        print("  MODE: DRY RUN")
    print("=" * 68)

    manifest: list[dict] = []
    total_files = 0

    if not args.dry_run:
        if ARCHIVE_DIR.exists():
            shutil.rmtree(ARCHIVE_DIR)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # Code
    print("\n── Code ────────────────────────────────────────────────────────")
    for f in CODE_FILES:
        if _copy_file(ROOT / f, ARCHIVE_DIR, manifest, args.dry_run):
            total_files += 1
    for d in CODE_DIRS:
        n = _copy_tree(ROOT / d, ARCHIVE_DIR, manifest, args.dry_run)
        print(f"  {d}/: {n} files")
        total_files += n

    # Docs
    print("\n── Documentation ───────────────────────────────────────────────")
    for f in DOC_FILES:
        if _copy_file(ROOT / f, ARCHIVE_DIR, manifest, args.dry_run):
            total_files += 1
    for d in DOC_DIRS:
        n = _copy_tree(ROOT / d, ARCHIVE_DIR, manifest, args.dry_run)
        print(f"  {d}/: {n} files")
        total_files += n

    # Studies
    print("\n── Studies ─────────────────────────────────────────────────────")
    for d in STUDY_DIRS:
        n = _copy_tree(ROOT / d, ARCHIVE_DIR, manifest, args.dry_run)
        print(f"  {d}/: {n} files")
        total_files += n

    # Data bundle
    print("\n── Data (publication bundle) ────────────────────────────────────")
    bundle_src = None
    for candidate in BUNDLE_CANDIDATES:
        p = ROOT / candidate
        if p.exists():
            bundle_src = p
            break
    if bundle_src:
        data_dst = ARCHIVE_DIR / "data"
        if not args.dry_run:
            data_dst.mkdir(parents=True, exist_ok=True)
        n = 0
        for item in sorted(bundle_src.rglob("*")):
            if item.is_dir() or _should_skip(item):
                continue
            rel_to_bundle = item.relative_to(bundle_src)
            target = data_dst / rel_to_bundle
            if args.dry_run:
                print(f"  [DRY] data/{rel_to_bundle}")
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target)
                manifest.append({
                    "path": f"data/{rel_to_bundle}",
                    "size_bytes": item.stat().st_size,
                    "sha256": _sha256(item),
                })
            n += 1
        print(f"  {bundle_src.name}/: {n} files")
        total_files += n
    else:
        print("  [SKIP] No publication bundle found")

    # .zenodo.json
    zenodo_meta = {
        "title": "THYROID_2026: Thyroid Cancer Research Lakehouse",
        "description": (
            "A comprehensive thyroid cancer research lakehouse covering 11,673 patients "
            "across 13 base tables with 60+ engineered features, cross-domain entity "
            "extraction, temporal linkage, and interactive Streamlit dashboards backed "
            "by MotherDuck cloud DuckDB. Includes propensity-score matched ETE staging "
            "analysis and NSQIP surgical outcomes linkage."
        ),
        "upload_type": "dataset",
        "publication_date": "2026-03-10",
        "version": "2026.03.10",
        "access_right": "restricted",
        "license": {"id": "CC-BY-4.0"},
        "creators": [
            {"name": "Glosser, Logan", "affiliation": "Emory University"}
        ],
        "keywords": [
            "thyroid cancer",
            "papillary thyroid carcinoma",
            "clinical research",
            "DuckDB",
            "data lakehouse",
            "extrathyroidal extension",
            "AJCC 8th edition",
        ],
        "related_identifiers": [
            {
                "identifier": "https://github.com/ry86pkqf74-rgb/THYROID_2026",
                "relation": "isSupplementTo",
                "scheme": "url",
            }
        ],
        "notes": f"Generated from tag {TAG}. Random seed: 42.",
    }

    if not args.dry_run:
        with open(ARCHIVE_DIR / ".zenodo.json", "w") as f:
            json.dump(zenodo_meta, f, indent=2)

        # MANIFEST.txt
        with open(ARCHIVE_DIR / "MANIFEST.txt", "w") as f:
            f.write(f"# Zenodo Archive Manifest — {TAG}\n")
            f.write(f"# Generated: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write(f"# Total files: {total_files}\n\n")
            total_size = 0
            for entry in sorted(manifest, key=lambda e: e["path"]):
                sz = entry["size_bytes"]
                total_size += sz
                f.write(f"{entry['sha256'][:12]}  {sz:>10,}  {entry['path']}\n")
            f.write(f"\n# Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)\n")

    print(f"\n── Summary ─────────────────────────────────────────────────────")
    print(f"  Total files: {total_files}")
    if not args.dry_run and manifest:
        total_mb = sum(e["size_bytes"] for e in manifest) / 1024 / 1024
        print(f"  Total size:  {total_mb:.1f} MB")
        print(f"  Archive:     {ARCHIVE_DIR.relative_to(ROOT)}")
    print()
    print("  Next steps:")
    print("    1. Review the archive directory")
    print("    2. Verify no PHI/sensitive data included")
    print("    3. Upload to Zenodo (https://zenodo.org/deposit/new)")
    print("    4. Paste the .zenodo.json metadata when prompted")
    print("    5. Publish and record the DOI in CITATION.cff")
    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Prepare Zenodo archive bundle.")
    ap.add_argument("--dry-run", action="store_true", help="Print plan without copying files")
    args = ap.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
