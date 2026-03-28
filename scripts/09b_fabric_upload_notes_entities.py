#!/usr/bin/env python3
"""
09b_fabric_upload_notes_entities.py

Upload note-entity Parquet files from local `processed/` to Microsoft Fabric
Lakehouse Files area on OneLake using the Azure Data Lake Storage Gen2 REST client.

Target layout (per domain):
  abfss://<lakehouse_workspace_id>@onelake.dfs.core.windows.net/Files/note_entities_{domain}/part-000.parquet

Auth:
  - Default: DefaultAzureCredential (Azure CLI, managed identity, env-based workload identity, etc.)
  - Service principal: set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Optional provenance columns added at upload time:
  fabric_upload_run_id, fabric_uploaded_at_utc, fabric_source_local_uri, fabric_upload_script_version

Delta / registered tables:
  mssparkutils and Spark are only available inside a Fabric notebook. This script prints a ready-to-paste
  notebook snippet to register Delta tables after Parquet land in Files/.

Usage (repo root):
  export FABRIC_LAKEHOUSE_WORKSPACE_ID='<lakehouse-guid>'
  python scripts/09b_fabric_upload_notes_entities.py --domain all
  python scripts/09b_fabric_upload_notes_entities.py --domain staging --dry-run

Pip:
  azure-storage-file-datalake azure-identity pyarrow pandas adlfs
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
SCRIPT_VERSION = "09b_fabric_upload_notes_entities_v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fabric_upload")

# Match notes_extraction/run_extraction.py domain → file stem
DOMAIN_TO_FILE = {
    "staging": "note_entities_staging",
    "genetics": "note_entities_genetics",
    "procedures": "note_entities_procedures",
    "complications": "note_entities_complications",
    "medications": "note_entities_medications",
    "problem_list": "note_entities_problem_list",
    "llm": "note_entities_llm",
}


def _credential():
    """Return a token credential (service principal or DefaultAzureCredential)."""
    import os

    from azure.identity import (
        ClientSecretCredential,
        DefaultAzureCredential,
    )

    tenant = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    secret = os.environ.get("AZURE_CLIENT_SECRET")
    if tenant and client_id and secret:
        log.info("  Auth: ClientSecretCredential (service principal)")
        return ClientSecretCredential(tenant_id=tenant, client_id=client_id, client_secret=secret)
    log.info("  Auth: DefaultAzureCredential")
    return DefaultAzureCredential(exclude_interactive_browser_credential=False)


def _datalake_clients(account_url: str, file_system: str):
    from azure.storage.filedatalake import DataLakeServiceClient

    service = DataLakeServiceClient(account_url=account_url, credential=_credential())
    fs = service.get_file_system_client(file_system=file_system)
    return service, fs


def _ensure_directory(fs_client, directory_path: str) -> None:
    """Create directory hierarchy if needed (idempotent)."""
    parts = directory_path.strip("/").split("/")
    prefix = ""
    for p in parts:
        if not p:
            continue
        prefix = f"{prefix}/{p}" if prefix else p
        try:
            fs_client.create_directory(prefix)
        except Exception:
            # Directory may already exist
            pass


def _upload_bytes(fs_client, remote_path: str, data: bytes) -> None:
    directory = str(Path(remote_path).parent).replace("\\", "/")
    _ensure_directory(fs_client, directory)
    file_client = fs_client.get_file_client(remote_path)
    file_client.upload_data(data, overwrite=True)


def _enrich_provenance(df: pd.DataFrame, run_id: str, local_path: Path) -> pd.DataFrame:
    out = df.copy()
    out["fabric_upload_run_id"] = run_id
    out["fabric_uploaded_at_utc"] = datetime.now(timezone.utc).isoformat()
    out["fabric_source_local_uri"] = local_path.resolve().as_uri()
    out["fabric_upload_script_version"] = SCRIPT_VERSION
    return out


def fabric_notebook_snippet(lakehouse_id: str, domain: str) -> str:
    """PySpark/Fabric notebook code using mssparkutils + Delta (run inside Lakehouse)."""
    remote_dir = f"Files/note_entities_{domain}"
    table = f"note_entities_{domain}"
    return f"""
# --- Paste into Fabric Notebook (attach to Lakehouse) ---
# Converts landed Parquet to Delta and registers a table (overwrite).

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

parquet_path = "abfss://{lakehouse_id}@onelake.dfs.core.windows.net/{remote_dir}/"
df = spark.read.format("parquet").load(parquet_path)
(df.write.format("delta").mode("overwrite")
 .saveAsTable("{table}"))

print("Delta table ready:", "{table}")
# --- end snippet ---
""".strip()


def try_mssparkutils_hint() -> None:
    """If running inside Fabric, mssparkutils exists; otherwise skip."""
    try:
        from notebookutils import mssparkutils  # type: ignore

        log.info(f"  mssparkutils available: {mssparkutils}")
    except Exception:
        log.info(
            "  mssparkutils not in this runtime — use the printed Fabric notebook snippet for Delta."
        )


def main() -> None:
    import os

    parser = argparse.ArgumentParser(description="Upload note_entities Parquet to OneLake / Lakehouse Files")
    parser.add_argument(
        "--domain",
        default="all",
        help=f"One of: all, {', '.join(sorted(DOMAIN_TO_FILE))}",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=PROCESSED,
        help="Directory containing note_entities_*.parquet",
    )
    parser.add_argument(
        "--account-url",
        default=os.environ.get(
            "ONELAKE_ACCOUNT_URL",
            "https://onelake.dfs.core.windows.net",
        ),
        help="ADLS Gen2 account URL (OneLake default)",
    )
    parser.add_argument(
        "--file-system",
        default=os.environ.get("FABRIC_LAKEHOUSE_WORKSPACE_ID"),
        help="Lakehouse workspace ID (GUID) = filesystem name on OneLake",
    )
    parser.add_argument(
        "--remote-prefix",
        default="Files",
        help="Top folder under the lakehouse (default Files)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List actions without uploading",
    )
    parser.add_argument(
        "--skip-provenance-columns",
        action="store_true",
        help="Upload Parquet bytes as-is without adding fabric_* columns",
    )
    args = parser.parse_args()

    if not args.file_system and not args.dry_run:
        log.error(
            "Missing lakehouse filesystem id. Set FABRIC_LAKEHOUSE_WORKSPACE_ID or pass --file-system."
        )
        sys.exit(1)
    file_system = args.file_system or "<YOUR_LAKEHOUSE_WORKSPACE_ID>"

    domains: list[str]
    if args.domain == "all":
        domains = sorted(DOMAIN_TO_FILE.keys())
    else:
        if args.domain not in DOMAIN_TO_FILE:
            log.error(f"Unknown domain {args.domain!r}")
            sys.exit(1)
        domains = [args.domain]

    processed_dir: Path = args.processed_dir.resolve()
    if not processed_dir.is_dir():
        log.error(f"Processed dir not found: {processed_dir}")
        sys.exit(1)

    try_mssparkutils_hint()

    run_id = str(uuid.uuid4())
    log.info("=" * 70)
    log.info("  FABRIC / ONELAKE UPLOAD — note_entities")
    log.info("=" * 70)
    log.info(f"  Account: {args.account_url}")
    log.info(f"  Filesystem (lakehouse id): {file_system}")
    log.info(f"  fabric_upload_run_id: {run_id}")

    fs = None
    if args.dry_run:
        log.info("  [dry-run] Skipping authentication and upload")
    else:
        try:
            _, fs = _datalake_clients(args.account_url, file_system)
        except Exception as exc:
            log.error(f"Failed to connect to Data Lake: {exc}")
            sys.exit(1)

    snippets: dict[str, str] = {}

    for domain in domains:
        stem = DOMAIN_TO_FILE[domain]
        local_path = processed_dir / f"{stem}.parquet"
        if not local_path.exists():
            log.warning(f"  Skip missing file: {local_path}")
            continue

        remote_path = f"{args.remote_prefix.rstrip('/')}/note_entities_{domain}/part-000.parquet"
        log.info(f"  {domain}: {local_path.name} → {remote_path}")

        if args.dry_run:
            snippets[domain] = fabric_notebook_snippet(file_system, domain)
            continue

        try:
            if args.skip_provenance_columns:
                assert fs is not None
                raw = local_path.read_bytes()
                _upload_bytes(fs, remote_path, raw)
            else:
                assert fs is not None
                df = pd.read_parquet(local_path)
                enriched = _enrich_provenance(df, run_id, local_path)
                buf_path = processed_dir / f".fabric_upload_{stem}_{run_id}.parquet"
                try:
                    enriched.to_parquet(buf_path, index=False)
                    _upload_bytes(fs, remote_path, buf_path.read_bytes())
                finally:
                    buf_path.unlink(missing_ok=True)
        except Exception as exc:
            log.error(f"    Upload failed for {domain}: {exc}")
            sys.exit(1)

        snippets[domain] = fabric_notebook_snippet(file_system, domain)

    manifest = {
        "fabric_upload_run_id": run_id,
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "domains": domains,
        "account_url": args.account_url,
        "file_system": file_system,
        "script_version": SCRIPT_VERSION,
    }
    manifest_path = processed_dir / f"fabric_upload_manifest_{run_id}.json"
    if not args.dry_run:
        try:
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            log.info(f"  Wrote manifest: {manifest_path}")
        except Exception as exc:
            log.warning(f"  Could not write manifest: {exc}")

    log.info("\n  --- Fabric notebook / Delta snippets (mssparkutils + Spark) ---")
    for dom, snip in snippets.items():
        log.info(f"\n# Domain: {dom}\n{snip}")

    log.info("\n" + "=" * 70)
    log.info("  UPLOAD COMPLETE" if not args.dry_run else "  DRY RUN COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
