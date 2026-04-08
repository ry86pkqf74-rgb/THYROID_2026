#!/usr/bin/env python3
"""Export de-identified FHIR specimen bundles to local NDJSON + manifest.

Reads ``main.fhir_bundle_specimen_export_v1`` when available; otherwise rebuilds
collection bundles from ``fhir_specimen_v1`` + ``fhir_procedure_collection_v1`` +
``fhir_encounter_v1`` + ``fhir_episode_of_care_v1`` using the same JSON shape as
``scripts/sql/138_specimen_fhir_tail_ddl.sql`` (no PHI; analytic resources only).

MotherDuck connections are fail-closed for ``--md`` / ``--read-scaling``, with
``custom_user_agent`` defaulting to ``specimen_fhir_export_restore_v1`` (override
``MOTHERDUCK_CUSTOM_USER_AGENT``). Stable session hint default:
``specimen_fhir_export_restore_v1`` (override ``MOTHERDUCK_SESSION_HINT``).

Usage:
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md [--limit N]
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling [--limit N]
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --md --output-root exports
  .venv/bin/python scripts/141_fhir_specimen_json_export.py --local-duckdb /path/to.db

Tokens: ``motherduck_client.get_token`` / ``get_read_scaling_token`` /
``motherduck.local.toml`` (gitignored). Never use read-scaling tokens for writes.

Environment: ``MOTHERDUCK_DATABASE`` / ``MOTHERDUCK_DB`` for catalog selection.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"

# Stable defaults (override via MOTHERDUCK_CUSTOM_USER_AGENT / MOTHERDUCK_SESSION_HINT).
DEFAULT_EXPORT_UA = "specimen_fhir_export_restore_v1"
DEFAULT_SESSION_HINT = "specimen_fhir_export_restore_v1"

# SQL aligned with scripts/sql/138_specimen_fhir_tail_ddl.sql (bundle table SELECT body).
_RECONSTRUCT_BUNDLE_SQL = """
SELECT
  json_object(
    'resourceType', 'Bundle',
    'type', 'collection',
    'timestamp', cast(current_timestamp AS VARCHAR),
    'entry', json_array(
      json_object(
        'resource', fs.resource_json,
        'url', 'Specimen/' || json_extract_string(fs.resource_json, '$.id')
      ),
      json_object(
        'resource', fp.resource_json,
        'url', 'Procedure/' || json_extract_string(fp.resource_json, '$.id')
      ),
      json_object(
        'resource', fe.resource_json,
        'url', 'Encounter/' || json_extract_string(fe.resource_json, '$.id')
      ),
      json_object(
        'resource', fo.resource_json,
        'url', 'EpisodeOfCare/' || json_extract_string(fo.resource_json, '$.id')
      )
    )
  ) AS bundle_json,
  fs.specimen_id
FROM main.fhir_specimen_v1 fs
JOIN main.fhir_procedure_collection_v1 fp ON fs.specimen_id = fp.specimen_id
JOIN main.fhir_encounter_v1 fe ON fs.specimen_id = fe.specimen_id
JOIN main.fhir_episode_of_care_v1 fo
  ON fe.episode_fhir_id = fo.episode_fhir_id
 AND fe.patient_fhir_id = fo.patient_fhir_id
ORDER BY fs.specimen_id
"""


def _ua_resolved() -> str:
    return (os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "").strip() or (
        DEFAULT_EXPORT_UA
    )


# Tests and static tools expect ``mod.UA`` after import (env may override).
UA = _ua_resolved()


def _verify_motherduck_attached(con: Any) -> bool:
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        return any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
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


def probe_catalog(con: Any) -> dict[str, Any]:
    """Lightweight catalog facts for manifest (no snapshot DDL)."""
    meta: dict[str, Any] = {
        "motherduck_attached": _verify_motherduck_attached(con),
        "snapshot_native_note": (
            "Native CREATE SNAPSHOT semantics are catalog-dependent; DuckLake and "
            "some MotherDuck databases reject OF database snapshot syntax — verify "
            "operationally before reader REFRESH DATABASE workflows."
        ),
    }
    try:
        row = con.execute("SELECT current_database()").fetchone()
        meta["current_database"] = row[0] if row else None
    except Exception as e:
        meta["current_database"] = None
        meta["current_database_error"] = str(e)
    try:
        meta["pragma_database_list"] = [
            list(map(str, r)) for r in con.execute("PRAGMA database_list").fetchall()
        ]
    except Exception as e:
        meta["pragma_database_error"] = str(e)
    return meta


def _main_table_exists(con: Any, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM main.{table_name} LIMIT 0").fetchall()
        return True
    except Exception:
        return False


def count_fhir_source_tables(con: Any) -> dict[str, int | str]:
    """Row counts (or errors) for manifest ``source_tables_main``."""
    counts: dict[str, int | str] = {}
    for tbl in FHIR_TABLES:
        try:
            if not _main_table_exists(con, tbl):
                counts[tbl] = "missing"
                continue
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()
            counts[tbl] = int(n[0]) if n else 0
        except Exception as e:
            counts[tbl] = "error"
            counts[f"{tbl}_error"] = str(e)
    return counts


def _fetch_bundle_rows_bundle_table(
    con: Any, limit: int
) -> tuple[list[tuple[Any, ...]], str | None]:
    lim = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
    try:
        rows = con.execute(
            "SELECT cast(bundle_json AS VARCHAR) FROM main.fhir_bundle_specimen_export_v1 "
            f"ORDER BY specimen_id{lim}"
        ).fetchall()
        return rows, None
    except Exception as e:
        return [], str(e)


def _fetch_bundle_rows_reconstructed(
    con: Any, limit: int
) -> tuple[list[tuple[Any, ...]], str | None]:
    required = (
        "fhir_specimen_v1",
        "fhir_procedure_collection_v1",
        "fhir_encounter_v1",
        "fhir_episode_of_care_v1",
    )

    missing = [t for t in required if not _main_table_exists(con, t)]
    if missing:
        return [], f"missing tables for reconstruction: {', '.join(missing)}"
    lim = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
    try:
        rows = con.execute(
            f"SELECT cast(bundle_json AS VARCHAR) FROM ({_RECONSTRUCT_BUNDLE_SQL}) t{lim}"
        ).fetchall()
        return rows, None
    except Exception as e:
        return [], str(e)


def resolve_bundle_rows(
    con: Any,
    *,
    limit: int,
    force_reconstruct: bool,
) -> tuple[list[tuple[Any, ...]], str, str | None]:
    """Return (rows, export_route, error_detail).

    export_route: ``bundle_table`` | ``reconstructed_from_resources``
    """
    if not force_reconstruct and _main_table_exists(con, "fhir_bundle_specimen_export_v1"):
        rows, err = _fetch_bundle_rows_bundle_table(con, limit)
        if err is None and rows:
            return rows, "bundle_table", None
        if err is None and not rows:
            r2, e2 = _fetch_bundle_rows_reconstructed(con, limit)
            if e2 is None:
                return r2, "reconstructed_from_resources", None
            return [], "reconstructed_from_resources", e2
        if err:
            r2, e2 = _fetch_bundle_rows_reconstructed(con, limit)
            if e2 is None:
                return r2, "reconstructed_from_resources", None
            return [], "bundle_table", f"{err}; reconstruct: {e2}"

    r2, e2 = _fetch_bundle_rows_reconstructed(con, limit)
    if e2 is None:
        return r2, "reconstructed_from_resources", None
    return [], "reconstructed_from_resources", e2


def run_export(
    con: Any,
    *,
    output_root: Path,
    limit: int,
    git_sha: str,
    force_reconstruct: bool,
    motherduck_session_hint: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Write specimen_bundles.ndjson + manifest under output_root/fhir_specimen_<ts>/."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_root).resolve() / f"fhir_specimen_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    counts = count_fhir_source_tables(con)
    catalog = probe_catalog(con)
    rows, export_route, route_err = resolve_bundle_rows(
        con, limit=limit, force_reconstruct=force_reconstruct
    )

    ndjson_path = out_dir / "specimen_bundles.ndjson"
    written = 0
    with ndjson_path.open("w", encoding="utf-8") as fh:
        for (raw,) in rows:
            if raw is None or str(raw).strip() == "":
                continue
            obj = json.loads(raw)
            fh.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
            written += 1

    md_attach = (
        os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or ""
    ).strip()

    manifest: dict[str, Any] = {
        "export_kind": "specimen_fhir_analytic_v1",
        "build_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha,
        "custom_user_agent": UA,
        "motherduck_session_hint": motherduck_session_hint or DEFAULT_SESSION_HINT,
        "source_catalog_env": md_attach or None,
        "source_database_probe": catalog,
        "export_route": export_route,
        "export_route_detail": route_err,
        "reconstructed_from_tables": (
            [
                "main.fhir_specimen_v1",
                "main.fhir_procedure_collection_v1",
                "main.fhir_encounter_v1",
                "main.fhir_episode_of_care_v1",
            ]
            if export_route == "reconstructed_from_resources"
            else None
        ),
        "preferred_source": "main.fhir_bundle_specimen_export_v1",
        "output_dir": str(out_dir),
        "bundle_row_count": written,
        "source_tables_main": {k: counts.get(k) for k in FHIR_TABLES},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    readme = "\n".join(
        [
            "# FHIR specimen bundle export (analytic, de-identified)",
            "",
            f"- **Build (UTC):** {manifest['build_timestamp_utc']}",
            f"- **Git SHA:** {manifest['git_sha']}",
            f"- **Export route:** `{export_route}`",
            f"- **Query user-agent:** `{manifest['custom_user_agent']}`",
            f"- **MotherDuck session hint:** `{manifest['motherduck_session_hint']}`",
            f"- **Bundle rows (NDJSON lines):** {manifest['bundle_row_count']}",
            "",
            "## Source tables (`main`)",
            "",
        ]
        + [f"- `{tbl}`: {counts.get(tbl)}" for tbl in FHIR_TABLES]
        + [
            "",
            "Machine-readable metadata: `manifest.json`. One FHIR Bundle JSON object per line:",
            "`specimen_bundles.ndjson`.",
            "",
            "## Reviewer read-scaling",
            "",
            "After a writer snapshot, run `REFRESH DATABASE` on the read-scaling connection",
            "before export; use `MD_READ_SCALING_TOKEN` + this script's `--read-scaling`.",
            "Do not commit tokens; use `motherduck.local.toml` or your secret manager.",
            "",
        ]
    )
    (out_dir / "README.md").write_text(readme, encoding="utf-8")

    if written:
        sample = ndjson_path.read_text(encoding="utf-8").splitlines()[0]
        if sample:
            first = json.loads(sample)
            assert first.get("resourceType") == "Bundle"
            assert first.get("type") == "collection"

    return out_dir, manifest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export FHIR specimen bundles to NDJSON + manifest.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed with RW token.")
    p.add_argument(
        "--read-scaling",
        action="store_true",
        help="MotherDuck read-scaling token only; REFRESH DATABASE on this connection first.",
    )
    p.add_argument(
        "--local-duckdb",
        type=Path,
        default=None,
        help="Offline/CI: read from this DuckDB file (no MotherDuck).",
    )
    p.add_argument(
        "--output-root",
        type=Path,
        default=ROOT / "exports",
        help="Directory under which fhir_specimen_<ts>/ is created",
    )
    p.add_argument("--limit", type=int, default=0, help="Max bundle rows (0 = all).")
    p.add_argument(
        "--force-reconstruct",
        action="store_true",
        help="Skip fhir_bundle_specimen_export_v1 and rebuild bundles from resource tables.",
    )
    return p.parse_args()


def _session_hint_cli() -> str:
    return (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip() or DEFAULT_SESSION_HINT


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
        from motherduck_client import MotherDuckClient

        hint = _session_hint_cli()
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

        hint = _session_hint_cli()
        con = connect_md_fail_closed(
            DEFAULT_DB,
            custom_user_agent=UA,
            motherduck_session_hint=hint,
        )

    sha = _git_sha()
    hint = _session_hint_cli()
    try:
        out_dir, manifest = run_export(
            con,
            output_root=args.output_root,
            limit=args.limit,
            git_sha=sha,
            force_reconstruct=args.force_reconstruct,
            motherduck_session_hint=hint,
        )
    finally:
        con.close()

    ndjson_path = out_dir / "specimen_bundles.ndjson"
    print(f"Wrote {manifest['bundle_row_count']} bundles to {ndjson_path}")
    print(f"Manifest: {out_dir / 'manifest.json'} (route={manifest['export_route']})")


if __name__ == "__main__":
    main()
