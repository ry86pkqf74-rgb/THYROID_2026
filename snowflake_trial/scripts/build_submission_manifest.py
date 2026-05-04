"""Build SHA256 manifest for each M*_submission_package_v1_0/ directory.

Output: <pkg>/PACKAGE_MANIFEST.json with per-file SHA256 + byte size + last-modified.
Reviewers can use this to verify integrity vs whatever was submitted to journal.
"""
import os, sys, json, hashlib
from pathlib import Path
from datetime import datetime

REPO = Path("/Users/ros/THyroid 2026")
PACKAGES = [
    "M044_submission_package_v1_0",
    "M038_submission_package_v1_0",
    "M032_submission_package_v1_0",
    "M037_submission_package_v1_0",
    "M025_submission_package_v1_0",
    "M004_submission_package_v1_0",
]

def hash_file(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(64 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

for pkg_name in PACKAGES:
    pkg = REPO / pkg_name
    if not pkg.exists():
        print(f"  ⚠ {pkg_name} does not exist; skipping")
        continue
    files = []
    for p in sorted(pkg.rglob('*')):
        if p.is_file() and not p.name.startswith('.') and 'PACKAGE_MANIFEST' not in p.name:
            rel = p.relative_to(pkg)
            files.append({
                'path': str(rel),
                'sha256': hash_file(p),
                'size_bytes': p.stat().st_size,
                'mtime_utc': datetime.utcfromtimestamp(p.stat().st_mtime).isoformat() + 'Z',
            })
    manifest = {
        'package': pkg_name,
        'release_tag': 'pub_v1_1_20260504',
        'generated_at_utc': datetime.utcnow().isoformat() + 'Z',
        'n_files': len(files),
        'files': files,
    }
    out = pkg / 'PACKAGE_MANIFEST.json'
    out.write_text(json.dumps(manifest, indent=2))
    print(f"  ✓ {pkg_name}: {len(files)} files -> PACKAGE_MANIFEST.json")

print("DONE")
