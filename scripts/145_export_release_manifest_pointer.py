#!/usr/bin/env python3
"""Write exports/release_manifests/LATEST_MANIFEST.json from live qa.release_manifest.

Ordering matches scripts/125_master_verified_views.py (latest_release CTE):
TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC.

Does not delete historical manifest JSON files elsewhere in exports/release_manifests/.

Usage:
  .venv/bin/python scripts/145_export_release_manifest_pointer.py --md
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "exports" / "release_manifests" / "LATEST_MANIFEST.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export latest qa.release_manifest row to LATEST_MANIFEST.json")
    p.add_argument("--md", action="store_true", help="Connect to MotherDuck (fail-closed).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN.")
    p.add_argument("--md-env", default=None, help="MotherDuck environment (dev|qa|prod) when using --md.")
    p.add_argument(
        "--output",
        type=Path,
        default=OUT,
        help=f"Output path (default: {OUT})",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("FATAL: pass --md to read live qa.release_manifest.")
        raise SystemExit(1)

    import os
    import sys

    sys.path.insert(0, str(ROOT))
    from utils.md_connect import connect_md_or_file

    if args.md_env and not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get("MOTHERDUCK_DB"):
        from motherduck_client import resolve_database_for_env

        os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(args.md_env)

    db_path = ROOT / "thyroid_master.duckdb"
    con = connect_md_or_file(
        db_path,
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
        custom_user_agent="THYROID_2026_export_release_manifest_pointer/1.0",
        env=args.md_env,
    )
    try:
        row = con.execute(
            """
            SELECT
                release_tag, git_sha, registry_version, tables_included, row_counts,
                created_at, created_by
            FROM qa.release_manifest
            ORDER BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            print("FATAL: qa.release_manifest is empty.")
            raise SystemExit(1)
        keys = (
            "release_tag",
            "git_sha",
            "registry_version",
            "tables_included",
            "row_counts",
            "created_at",
            "created_by",
        )
        payload = dict(zip(keys, row, strict=True))
    finally:
        con.close()

    rt = payload.get("release_tag")
    git_sha = payload.get("git_sha")
    created = payload.get("created_at")

    rel_path = "exports/release_manifests/LATEST_MANIFEST.json"
    try:
        rel_path = str(args.output.relative_to(ROOT))
    except (ValueError, AttributeError):
        rel_path = str(args.output)

    doc = {
        "manifest_id": f"live_{rt}" if rt is not None else None,
        "env": "prod",
        "overall_status": "LIVE_SNAPSHOT",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": str(git_sha) if git_sha is not None else None,
        "release_tag": str(rt) if rt is not None else None,
        "release_manifest_created_at": str(created) if created is not None else None,
        "path": rel_path,
        "role": "live_checkpoint",
        "authoritative_live_source": "qa.release_manifest table on MotherDuck (this file was exported from it)",
        "do_not_use_as_current_without_regeneration": False,
        "release_manifest_row": payload,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(doc, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
