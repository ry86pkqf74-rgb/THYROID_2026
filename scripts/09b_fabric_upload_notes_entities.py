#!/usr/bin/env python3
"""
09b_fabric_upload_notes_entities.py

Upload note-entity and (optional) canonical-release Parquet files from local `processed/`
to a Microsoft Fabric Lakehouse Files area on OneLake (Azure Data Lake Storage Gen2 REST client).

Target layout — 2026 OneLake ABFSS paths:

  Entities: .../Files/note_entities_{domain}/part-000.parquet
  Canonical release (`--domain release` or per-table key): .../Files/<table_stem>/part-000.parquet

Auth:
  - Service principal (preferred): set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in .env
  - Fallback: DefaultAzureCredential (Azure CLI, managed identity, workload identity, etc.)

Environment (.env file in repo root):
  AZURE_TENANT_ID              Service principal tenant GUID
  AZURE_CLIENT_ID              Service principal client GUID
  AZURE_CLIENT_SECRET          Service principal client secret
  FABRIC_LAKEHOUSE_WORKSPACE_ID  Workspace GUID (filesystem on OneLake)
  LAKEHOUSE_NAME               Lakehouse display name (e.g. thyroid_lakehouse)
  ONELAKE_ACCOUNT_URL          Default: https://onelake.dfs.core.windows.net

Optional provenance columns added at upload time:
  fabric_upload_run_id, fabric_uploaded_at_utc, fabric_source_local_uri, fabric_upload_script_version

Delta / registered tables:
  mssparkutils and Spark are only available inside a Fabric notebook. This script prints a ready-to-paste
  notebook snippet to register Delta tables after Parquet land in Files/.

Usage (repo root):
  python scripts/09b_fabric_upload_notes_entities.py --domain all …
  python scripts/09b_fabric_upload_notes_entities.py --domain release …
  python scripts/09b_fabric_upload_notes_entities.py --domain staging --dry-run

Pip:
  azure-storage-file-datalake azure-identity pyarrow pandas adlfs python-dotenv
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Load .env from repo root before anything else ──────────────────────────
ROOT = Path(__file__).resolve().parent.parent
_env_path = ROOT / ".env"
try:
    from dotenv import load_dotenv
    load_dotenv(_env_path)
    if _env_path.exists():
        logging.getLogger("fabric_upload").info(f"  Loaded .env from {_env_path}")
except ImportError:
    pass  # python-dotenv optional; rely on shell env if not installed

PROCESSED = ROOT / "processed"
SCRIPT_VERSION = "09b_fabric_upload_notes_entities_v3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fabric_upload")

# Registry-driven domain→file mapping
sys.path.insert(0, str(ROOT))
_REGISTRY_LOADED = False
try:
    from llm_extraction.registry import load_registry as _load_registry

    _reg = _load_registry()
    DOMAIN_TO_FILE: dict[str, str] = _reg.domain_to_parquet_stem()
    CANONICAL_RELEASE_STEMS: tuple[str, ...] = tuple(
        v.duckdb_table for v in _reg.canonical_outputs.values()
    )
    _REGISTRY_LOADED = True
except Exception:
    DOMAIN_TO_FILE = {
        "staging":           "note_entities_staging",
        "genetics":          "note_entities_genetics",
        "procedures":        "note_entities_procedures",
        "operative_detail":  "note_entities_operative_detail",
        "complications":     "note_entities_complications",
        "medications":       "note_entities_medications",
        "problem_list":      "note_entities_problem_list",
        "llm":               "note_entities_llm",
    }
    CANONICAL_RELEASE_STEMS = (
        "canonical_extracted_fact_long_v1",
        "canonical_fact_quarantine_v1",
        "canonical_extracted_fact_long_v2",
        "canonical_fact_quarantine_v2",
        "note_extraction_runs",
    )


def _credential():
    """Return a token credential (service principal → DeviceCodeCredential fallback)."""
    from azure.identity import ClientSecretCredential, DeviceCodeCredential

    tenant    = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    secret    = os.environ.get("AZURE_CLIENT_SECRET")

    if tenant and client_id and secret:
        log.info("  Auth: ClientSecretCredential (service principal)")
        return ClientSecretCredential(
            tenant_id=tenant, client_id=client_id, client_secret=secret
        )
    # Suppress verbose azure-identity HTTP logging so the device code is visible
    import logging as _logging
    _logging.getLogger("azure").setLevel(_logging.WARNING)
    _logging.getLogger("urllib3").setLevel(_logging.WARNING)

    def _show_device_code(verification_uri, user_code, expires_on):
        print("\n" + "="*60)
        print("  MICROSOFT LOGIN REQUIRED")
        print("="*60)
        print(f"  1. Open: {verification_uri}")
        print(f"  2. Enter code: {user_code}")
        print("  3. Sign in with LGLOSSE@emory.edu")
        print(f"  (Expires: {expires_on})")
        print("="*60 + "\n", flush=True)

    log.info("  Auth: DeviceCodeCredential — follow the login prompt below")
    return DeviceCodeCredential(prompt_callback=_show_device_code)


def _datalake_clients(account_url: str, file_system: str):
    from azure.storage.filedatalake import DataLakeServiceClient

    service = DataLakeServiceClient(account_url=account_url, credential=_credential())
    fs = service.get_file_system_client(file_system=file_system)
    return service, fs


def _ensure_directory(fs_client, directory_path: str) -> None:
    """Create directory hierarchy idempotently."""
    parts = directory_path.strip("/").split("/")
    prefix = ""
    for p in parts:
        if not p:
            continue
        prefix = f"{prefix}/{p}" if prefix else p
        try:
            fs_client.create_directory(prefix)
        except Exception:
            pass  # directory already exists — fine


def _upload_bytes(fs_client, remote_path: str, data: bytes) -> None:
    directory = str(Path(remote_path).parent).replace("\\", "/")
    _ensure_directory(fs_client, directory)
    file_client = fs_client.get_file_client(remote_path)
    file_client.upload_data(data, overwrite=True)
    log.info(f"    ✓ uploaded {len(data):,} bytes → {remote_path}")


def _enrich_provenance(df: pd.DataFrame, run_id: str, local_path: Path) -> pd.DataFrame:
    out = df.copy()
    out["fabric_upload_run_id"]       = run_id
    out["fabric_uploaded_at_utc"]     = datetime.now(timezone.utc).isoformat()
    out["fabric_source_local_uri"]    = local_path.resolve().as_uri()
    out["fabric_upload_script_version"] = SCRIPT_VERSION
    return out


def fabric_notebook_snippet(
    workspace_id: str,
    lakehouse_name: str,
    *,
    files_subpath: str,
    delta_table: str,
) -> str:
    """
    PySpark / Fabric notebook snippet — run inside a Lakehouse notebook to
    convert the landed Parquet to Delta and register it as a table.
    `files_subpath` is the folder under Files/ (e.g. note_entities_staging or
    canonical_extracted_fact_long_v1).
    """
    lakehouse_fs = f"{lakehouse_name}.Lakehouse"
    parquet_path = (
        f"abfss://{workspace_id}@onelake.dfs.core.windows.net"
        f"/{lakehouse_fs}/Files/{files_subpath}/"
    )
    return f"""
# ── Paste into Fabric Notebook (attach to Lakehouse: {lakehouse_name}) ──
# Converts landed Parquet → Delta and registers as a managed table.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

parquet_path = "{parquet_path}"
df = spark.read.format("parquet").load(parquet_path)
(df.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("{delta_table}"))

print("Delta table ready:", "{delta_table}", "| rows:", df.count())
# ── end snippet ──
""".strip()


def try_mssparkutils_hint() -> None:
    try:
        from notebookutils import mssparkutils  # type: ignore
        log.info(f"  mssparkutils available: {mssparkutils}")
    except Exception:
        log.info(
            "  mssparkutils not in this runtime — use the printed Fabric notebook "
            "snippet to register Delta tables after upload."
        )


def _remote_path_note_entity(lakehouse_name: str, remote_prefix: str, domain: str) -> str:
    """
    OneLake path for note_entities: .../Files/note_entities_{domain}/part-000.parquet
    """
    lakehouse_fs_segment = f"{lakehouse_name}.Lakehouse"
    return (
        f"{lakehouse_fs_segment}/{remote_prefix.strip('/')}"
        f"/note_entities_{domain}/part-000.parquet"
    )


def _remote_path_release_table(lakehouse_name: str, remote_prefix: str, table_stem: str) -> str:
    """OneLake path for canonical/quarantine/run artefacts: .../Files/<stem>/part-000.parquet"""
    lakehouse_fs_segment = f"{lakehouse_name}.Lakehouse"
    return (
        f"{lakehouse_fs_segment}/{remote_prefix.strip('/')}/{table_stem}/part-000.parquet"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload note_entities Parquet files to OneLake / Fabric Lakehouse (2026)"
    )
    all_domain_choices = (
        "all | release | " + " | ".join(sorted(DOMAIN_TO_FILE)) + " | " + " | ".join(CANONICAL_RELEASE_STEMS)
    )
    parser.add_argument(
        "--domain",
        default="all",
        help=f"Upload target: {all_domain_choices}",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=PROCESSED,
        help=f"Directory containing note_entities_*.parquet (default: {PROCESSED})",
    )
    parser.add_argument(
        "--account-url",
        default=os.environ.get("ONELAKE_ACCOUNT_URL", "https://onelake.dfs.core.windows.net"),
        help="ADLS Gen2 / OneLake account URL",
    )
    parser.add_argument(
        "--file-system",
        default=os.environ.get("FABRIC_LAKEHOUSE_WORKSPACE_ID"),
        help="Workspace GUID — the ADLS Gen2 filesystem name on OneLake",
    )
    parser.add_argument(
        "--lakehouse-name",
        default=os.environ.get("LAKEHOUSE_NAME", ""),
        help="Lakehouse display name (e.g. thyroid_lakehouse). Required for correct ABFSS path.",
    )
    parser.add_argument(
        "--remote-prefix",
        default="Files",
        help="Top folder under the lakehouse (default: Files)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions and ABFSS paths without uploading",
    )
    parser.add_argument(
        "--skip-provenance-columns",
        action="store_true",
        help="Upload Parquet bytes as-is without adding fabric_* provenance columns",
    )
    args = parser.parse_args()

    # ── Validation ─────────────────────────────────────────────────────────
    if not args.file_system and not args.dry_run:
        log.error(
            "Missing workspace GUID. Set FABRIC_LAKEHOUSE_WORKSPACE_ID in .env "
            "or pass --file-system <guid>."
        )
        sys.exit(1)
    if not args.lakehouse_name and not args.dry_run:
        log.error(
            "Missing lakehouse name. Set LAKEHOUSE_NAME in .env "
            "or pass --lakehouse-name <name>."
        )
        sys.exit(1)

    file_system    = args.file_system    or "<YOUR_WORKSPACE_GUID>"
    lakehouse_name = args.lakehouse_name or "<YOUR_LAKEHOUSE_NAME>"

    # ── Upload job list: (kind, label_for_log, parquet_stem) ─────────────────
    if args.domain == "all":
        jobs = [
            ("entity", d, DOMAIN_TO_FILE[d]) for d in sorted(DOMAIN_TO_FILE.keys())
        ]
    elif args.domain == "release":
        jobs = [("release", s, s) for s in CANONICAL_RELEASE_STEMS]
    elif args.domain in DOMAIN_TO_FILE:
        jobs = [("entity", args.domain, DOMAIN_TO_FILE[args.domain])]
    elif args.domain in CANONICAL_RELEASE_STEMS:
        jobs = [("release", args.domain, args.domain)]
    else:
        log.error(f"Unknown --domain '{args.domain}'. See --help for choices.")
        sys.exit(1)

    processed_dir: Path = args.processed_dir.resolve()
    if not processed_dir.is_dir():
        log.error(f"processed-dir not found: {processed_dir}")
        sys.exit(1)

    try_mssparkutils_hint()

    run_id = str(uuid.uuid4())
    log.info("=" * 70)
    log.info("  FABRIC / ONELAKE UPLOAD — entities + optional canonical release (v3)")
    log.info(f"  Registry: {'YAML-driven' if _REGISTRY_LOADED else 'hardcoded fallback'}")
    log.info(f"  Domains: {len(DOMAIN_TO_FILE)}, Canonical: {len(CANONICAL_RELEASE_STEMS)}")
    log.info("=" * 70)
    log.info(f"  Account URL  : {args.account_url}")
    log.info(f"  Workspace ID : {file_system}")
    log.info(f"  Lakehouse    : {lakehouse_name}")
    log.info(f"  Source dir   : {processed_dir}")
    log.info(f"  Run ID       : {run_id}")

    # ── Connect ─────────────────────────────────────────────────────────────
    fs = None
    if args.dry_run:
        log.info("  [dry-run] Skipping authentication and upload")
    else:
        try:
            _, fs = _datalake_clients(args.account_url, file_system)
        except Exception as exc:
            log.error(f"Failed to connect to OneLake: {exc}")
            sys.exit(1)

    snippets: dict[str, str] = {}
    uploaded: list[str] = []
    skipped:  list[str] = []

    for kind, label, stem in jobs:
        local_path = processed_dir / f"{stem}.parquet"
        if not local_path.exists():
            log.warning(f"  ⚠ Skip missing file: {local_path.name}")
            skipped.append(label)
            continue

        if kind == "entity":
            remote_path = _remote_path_note_entity(lakehouse_name, args.remote_prefix, label)
            files_subpath = f"note_entities_{label}"
            delta_table = files_subpath
        else:
            remote_path = _remote_path_release_table(lakehouse_name, args.remote_prefix, stem)
            files_subpath = stem
            delta_table = stem

        abfss_uri = (
            f"abfss://{file_system}@onelake.dfs.core.windows.net/{remote_path}"
        )
        log.info(f"\n  [{label}] ({stem}.parquet)")
        log.info(f"    local  : {local_path.name}  ({local_path.stat().st_size / 1e6:.1f} MB)")
        log.info(f"    remote : {abfss_uri}")

        if args.dry_run:
            snippets[label] = fabric_notebook_snippet(
                file_system,
                lakehouse_name,
                files_subpath=files_subpath,
                delta_table=delta_table,
            )
            uploaded.append(label)
            continue

        try:
            assert fs is not None
            if args.skip_provenance_columns:
                raw = local_path.read_bytes()
                _upload_bytes(fs, remote_path, raw)
            else:
                df       = pd.read_parquet(local_path)
                enriched = _enrich_provenance(df, run_id, local_path)
                buf_path = processed_dir / f".fabric_tmp_{stem}_{run_id}.parquet"
                try:
                    enriched.to_parquet(buf_path, index=False)
                    _upload_bytes(fs, remote_path, buf_path.read_bytes())
                finally:
                    buf_path.unlink(missing_ok=True)
            uploaded.append(label)
        except Exception as exc:
            log.error(f"    Upload failed for '{label}': {exc}")
            sys.exit(1)

        snippets[label] = fabric_notebook_snippet(
            file_system,
            lakehouse_name,
            files_subpath=files_subpath,
            delta_table=delta_table,
        )

    # ── Manifest ────────────────────────────────────────────────────────────
    manifest = {
        "fabric_upload_run_id":  run_id,
        "uploaded_at_utc":       datetime.now(timezone.utc).isoformat(),
        "script_version":        SCRIPT_VERSION,
        "account_url":           args.account_url,
        "workspace_id":          file_system,
        "lakehouse_name":        lakehouse_name,
        "domains_uploaded":      uploaded,
        "domains_skipped":       skipped,
    }
    if not args.dry_run:
        manifest_path = processed_dir / f"fabric_upload_manifest_{run_id}.json"
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            log.info(f"\n  Manifest written: {manifest_path.name}")
        except Exception as exc:
            log.warning(f"  Could not write manifest: {exc}")

    # ── Fabric notebook snippets ─────────────────────────────────────────────
    if snippets:
        log.info("\n  ─── Fabric Notebook / Delta registration snippets ───")
        for dom, snip in snippets.items():
            log.info(f"\n# Domain: {dom}\n{snip}")

    log.info("\n" + "=" * 70)
    if args.dry_run:
        log.info(f"  DRY RUN COMPLETE | would upload {len(uploaded)} artefact(s)")
    else:
        log.info(f"  UPLOAD COMPLETE | {len(uploaded)} artefact(s) uploaded, {len(skipped)} skipped")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
