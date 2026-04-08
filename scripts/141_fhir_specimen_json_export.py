#!/usr/bin/env python3
"""Write local NDJSON + manifest from main.fhir_bundle_specimen_export_v1 (MotherDuck).

Reads analytic bundles only; does not mutate MotherDuck. Uses fail-closed MotherDuck attach
with custom_user_agent='specimen_fhir_export_v1' per governance.

Usage:
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md [--limit N]
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling [--limit N]
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md --output-root exports
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --local-duckdb /path/to.db  # CI/offline only

Tokens (see ``motherduck_client.get_token`` / ``get_read_scaling_token``):
  --md              RW: ``MD_SA_TOKEN`` / ``MOTHERDUCK_TOKEN`` / ``.streamlit/secrets.toml``
  --read-scaling    Reader: ``MD_READ_SCALING_TOKEN`` (+ ``MD_READ_SCALING_SESSION_HINT`` optional)
  After writer snapshots, reviewers should ``REFRESH DATABASE`` on read-scaling before export.

Environment: optional MOTHERDUCK_DATABASE / MOTHERDUCK_DB for catalog selection.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
UA = "specimen_fhir_export_v1"

def _verify_motherduck_attached(con: Any) -> bool:
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        return any(
            "md:" in str(r) or "md_information_schema" in str(r) for r in dbs
        )
    except Exception:
        return False


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


def count_fhir_source_tables(con: Any) -> dict[str, int | str]:
    """Row counts (or error strings) for manifest ``source_tables_main``."""
    counts: dict[str, int | str] = {}
    for tbl in FHIR_TABLES:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()
            counts[tbl] = int(n[0]) if n else 0
        except Exception as e:
            counts[tbl] = -1
            counts[f"{tbl}_error"] = str(e)
    return counts


def run_export(
    con: Any,
    *,
    output_root: Path,
    limit: int,
    git_sha: str,
) -> tuple[Path, dict[str, Any]]:
    """Write ``specimen_bundles.ndjson`` + ``manifest.json`` under ``output_root/fhir_specimen_<ts>/``."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_root).resolve() / f"fhir_specimen_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    counts = count_fhir_source_tables(con)
    lim = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
    rows = con.execute(
        f"SELECT cast(bundle_json AS VARCHAR) FROM main.fhir_bundle_specimen_export_v1 "
        f"ORDER BY specimen_id{lim}"
    ).fetchall()

    ndjson_path = out_dir / "specimen_bundles.ndjson"
    with ndjson_path.open("w", encoding="utf-8") as fh:
        for (raw,) in rows:
            obj = json.loads(raw)
            fh.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")

    manifest: dict[str, Any] = {
        "export_kind": "specimen_fhir_analytic_v1",
        "build_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha,
        "custom_user_agent": UA,
        "output_dir": str(out_dir),
        "bundle_row_count": len(rows),
        "source_tables_main": {k: counts.get(k) for k in FHIR_TABLES},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    readme = "\n".join(
        [
            "# FHIR specimen bundle export (analytic, de-identified)",
            "",
            f"- **Build (UTC):** {manifest['build_timestamp_utc']}",
            f"- **Git SHA:** {manifest['git_sha']}",
            f"- **Query user-agent:** `{manifest['custom_user_agent']}`",
            f"- **Bundle rows (NDJSON lines):** {manifest['bundle_row_count']}",
            "",
            "## Source tables (`main`)",
            "",
        ]
        + [f"- `{tbl}`: {counts.get(tbl)}" for tbl in FHIR_TABLES]
        + [
            "",
            "Machine-readable metadata: `manifest.json`. One FHIR Bundle JSON object per line: `specimen_bundles.ndjson`.",
            "",
        ]
    )
    (out_dir / "README.md").write_text(readme, encoding="utf-8")

    sample = ndjson_path.read_text(encoding="utf-8").splitlines()[0] if rows else ""
    if sample:
        first = json.loads(sample)
        assert first.get("resourceType") == "Bundle"
        assert first.get("type") == "collection"

    return out_dir, manifest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export FHIR specimen bundles to NDJSON + manifest.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed with RW token (operator/export).")
    p.add_argument(
        "--read-scaling",
        action="store_true",
        help="MotherDuck read-scaling token only (reviewer). Run REFRESH DATABASE on this connection first for freshness.",
    )
    p.add_argument(
        "--local-duckdb",
        type=Path,
        default=None,
        help="Offline/CI: read from this DuckDB file (no MotherDuck; development tests only).",
    )
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
    modes = sum([bool(args.md), bool(args.read_scaling), args.local_duckdb is not None])
    if modes != 1:
        print("FATAL: pass exactly one of --md, --read-scaling, or --local-duckdb.")
        sys.exit(1)

    if args.local_duckdb is not None:
        import duckdb

        con = duckdb.connect(str(args.local_duckdb))
    elif args.read_scaling:
        import os

        from motherduck_client import MotherDuckClient

        hint = os.environ.get("MOTHERDUCK_SESSION_HINT") or (
            "thyroid2026:fhir_export_reader:" + _git_sha()[:7]
        )
        client = MotherDuckClient.for_env(
            custom_user_agent=UA,
            motherduck_session_hint=hint,
        )
        try:
            con = client.connect_read_scaling()
        except Exception as e:
            print(f"FATAL: read-scaling MotherDuck connection failed: {e}")
            sys.exit(1)
        if not _verify_motherduck_attached(con):
            con.close()
            print(
                "FATAL: --read-scaling connected but PRAGMA database_list shows no MotherDuck attach."
            )
            sys.exit(1)
        print("  MotherDuck read-scaling connection verified (fail-closed gate passed)")
    else:
        from utils.md_connect import connect_md_fail_closed

        hint = __import__("os").environ.get("MOTHERDUCK_SESSION_HINT") or (
            "thyroid2026:fhir_export:" + _git_sha()[:7]
        )
        con = connect_md_fail_closed(
            DEFAULT_DB,
            custom_user_agent=UA,
            motherduck_session_hint=hint,
        )

    sha = _git_sha()
    try:
        out_dir, manifest = run_export(
            con, output_root=args.output_root, limit=args.limit, git_sha=sha
        )
    finally:
        con.close()

    ndjson_path = out_dir / "specimen_bundles.ndjson"
    print(f"Wrote {manifest['bundle_row_count']} bundles to {ndjson_path}")
    print(f"Manifest: {out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
