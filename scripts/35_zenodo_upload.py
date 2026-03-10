#!/usr/bin/env python3
"""
35_zenodo_upload.py — Upload archive to Zenodo via REST API

Creates a new deposit, uploads the zip, fills metadata from .zenodo.json,
and publishes. Returns the DOI.

Prerequisites:
    1. Create a Zenodo Personal Access Token at https://zenodo.org/account/settings/applications/
       (Scopes: deposit:actions, deposit:write)
    2. Export: export ZENODO_TOKEN='your_token_here'

Usage:
    .venv/bin/python scripts/35_zenodo_upload.py
    .venv/bin/python scripts/35_zenodo_upload.py --sandbox   # Test on sandbox.zenodo.org first
    .venv/bin/python scripts/35_zenodo_upload.py --no-publish # Upload without publishing (review first)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
ZIP_PATH = ROOT / "exports" / "zenodo_archive_2026.03.10.zip"
META_PATH = ROOT / "exports" / "zenodo_archive_2026.03.10" / ".zenodo.json"

ZENODO_API = "https://zenodo.org/api"
SANDBOX_API = "https://sandbox.zenodo.org/api"


def get_token() -> str:
    token = os.getenv("ZENODO_TOKEN")
    if not token:
        raise RuntimeError(
            "ZENODO_TOKEN not set.\n\n"
            "Create one at: https://zenodo.org/account/settings/applications/\n"
            "Then: export ZENODO_TOKEN='your_token'\n"
        )
    return token


def load_metadata() -> dict:
    with open(META_PATH) as f:
        raw = json.load(f)

    return {
        "metadata": {
            "title": raw["title"],
            "upload_type": raw.get("upload_type", "dataset"),
            "description": raw["description"],
            "creators": [
                {"name": c["name"], "affiliation": c.get("affiliation", "")}
                for c in raw["creators"]
            ],
            "keywords": raw.get("keywords", []),
            "access_right": raw.get("access_right", "restricted"),
            "license": raw.get("license", {}).get("id", "CC-BY-4.0"),
            "publication_date": raw.get("publication_date", "2026-03-10"),
            "version": raw.get("version", "2026.03.10"),
            "notes": raw.get("notes", ""),
            "related_identifiers": raw.get("related_identifiers", []),
        }
    }


def run(args: argparse.Namespace) -> int:
    api = SANDBOX_API if args.sandbox else ZENODO_API
    env_label = "SANDBOX" if args.sandbox else "PRODUCTION"
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    if not ZIP_PATH.exists():
        print(f"ERROR: {ZIP_PATH} not found. Run scripts/32_zenodo_archive_prep.py first.")
        return 1

    print(f"=== Zenodo Upload ({env_label}) ===")
    print(f"  Archive: {ZIP_PATH.name} ({ZIP_PATH.stat().st_size / 1024 / 1024:.1f} MB)")
    print()

    # 1. Create new deposit
    print("1. Creating deposit...")
    r = requests.post(f"{api}/deposit/depositions", json={}, headers=headers)
    if r.status_code != 201:
        print(f"   FAILED: {r.status_code} — {r.text[:200]}")
        return 1
    dep = r.json()
    dep_id = dep["id"]
    bucket_url = dep["links"]["bucket"]
    print(f"   Deposit ID: {dep_id}")

    # 2. Upload file
    print("2. Uploading zip...")
    with open(ZIP_PATH, "rb") as f:
        r = requests.put(
            f"{bucket_url}/{ZIP_PATH.name}",
            data=f,
            headers={**headers, "Content-Type": "application/octet-stream"},
        )
    if r.status_code not in (200, 201):
        print(f"   FAILED: {r.status_code} — {r.text[:200]}")
        return 1
    print(f"   Upload complete ({ZIP_PATH.stat().st_size / 1024 / 1024:.1f} MB)")

    # 3. Set metadata
    print("3. Setting metadata...")
    metadata = load_metadata()
    r = requests.put(
        f"{api}/deposit/depositions/{dep_id}",
        json=metadata,
        headers={**headers, "Content-Type": "application/json"},
    )
    if r.status_code != 200:
        print(f"   FAILED: {r.status_code} — {r.text[:200]}")
        return 1
    print("   Metadata set successfully")

    # 4. Publish (or skip)
    if args.no_publish:
        print()
        print(f"   DRAFT CREATED — review at: {dep['links']['html']}")
        print(f"   To publish later: visit the URL above and click 'Publish'")
    else:
        print("4. Publishing...")
        r = requests.post(
            f"{api}/deposit/depositions/{dep_id}/actions/publish",
            headers=headers,
        )
        if r.status_code != 202:
            print(f"   FAILED: {r.status_code} — {r.text[:200]}")
            print(f"   Draft URL: {dep['links']['html']}")
            return 1

        pub = r.json()
        doi = pub.get("doi", "")
        record_url = pub["links"].get("record_html", pub["links"].get("html", ""))
        print(f"   PUBLISHED!")
        print(f"   DOI: {doi}")
        print(f"   URL: {record_url}")
        print()
        print(f"   Next: update CITATION.cff with doi: \"{doi}\"")

    print()
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload archive to Zenodo via REST API.")
    ap.add_argument("--sandbox", action="store_true", help="Use sandbox.zenodo.org")
    ap.add_argument("--no-publish", action="store_true", help="Create draft only (don't publish)")
    args = ap.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
