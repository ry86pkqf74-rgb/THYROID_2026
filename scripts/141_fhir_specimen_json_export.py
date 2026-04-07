#!/usr/bin/env python3
"""Write local NDJSON + manifest from main.fhir_bundle_specimen_export_v1 (MotherDuck).

Reads analytic bundles only; does not mutate MotherDuck. Uses fail-closed MotherDuck attach
with custom_user_agent='specimen_fhir_export_v1' per governance.

Usage:
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md [--limit N]
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md --output-root exports

Environment: MOTHERDUCK_TOKEN or MD_SA_TOKEN; optional MOTHERDUCK_DATABASE.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
UA = "specimen_fhir_export_v1"

FHIR_TABLES = (
    "fhir_patient_deid_map_v1",
    "fhir_specimen_v1",
    "fhir_procedure_collection_v1",
    "fhir_encounter_v1",
    "fhir_episode_of_care_v1",
    "fhir_bundle_specimen_export_v1",
)


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export FHIR specimen bundles to NDJSON + manifest.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed (required for export).")
    p.add_argument(
        "--output-root",
        type=Path,
        default=ROOT / "exports",
        help="Directory under which fhir_specimen_<ts>/ is created",
    )
    p.add_argument("--limit", type=int, default=0, help="Max bundle rows (0 = all).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("FATAL: --md required (refusing local DuckDB for export attach).")
        sys.exit(1)

    from utils.md_connect import connect_md_fail_closed

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_root).resolve() / f"fhir_specimen_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    hint = __import__("os").environ.get("MOTHERDUCK_SESSION_HINT") or (
        "thyroid2026:fhir_export:" + _git_sha()[:7]
    )
    con = connect_md_fail_closed(
        DEFAULT_DB,
        custom_user_agent=UA,
        motherduck_session_hint=hint,
    )

    counts: dict[str, int] = {}
    for tbl in FHIR_TABLES:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()
            counts[tbl] = int(n[0]) if n else 0
        except Exception as e:
            counts[tbl] = -1
            counts[f"{tbl}_error"] = str(e)

    lim = f" LIMIT {int(args.limit)}" if args.limit and args.limit > 0 else ""
    rows = con.execute(
        f"SELECT cast(bundle_json AS VARCHAR) FROM main.fhir_bundle_specimen_export_v1 "
        f"ORDER BY specimen_id{lim}"
    ).fetchall()
    con.close()

    ndjson_path = out_dir / "specimen_bundles.ndjson"
    with ndjson_path.open("w", encoding="utf-8") as fh:
        for (raw,) in rows:
            obj = json.loads(raw)
            fh.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")

    manifest = {
        "export_kind": "specimen_fhir_analytic_v1",
        "build_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "custom_user_agent": UA,
        "output_dir": str(out_dir),
        "bundle_row_count": len(rows),
        "source_tables_main": {k: counts.get(k) for k in FHIR_TABLES},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Validate first line parses as Bundle
    sample = ndjson_path.read_text(encoding="utf-8").splitlines()[0] if rows else ""
    if sample:
        first = json.loads(sample)
        assert first.get("resourceType") == "Bundle"
        assert first.get("type") == "collection"

    print(f"Wrote {len(rows)} bundles to {ndjson_path}")
    print(f"Manifest: {out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
