#!/usr/bin/env python3
"""
02_ingest_to_motherduck.py — Upload HPC re-extraction results to MotherDuck.

Run this on your LOCAL machine after downloading the results tarball from HPC.

Usage:
    # 1. Download from HPC:
    scp <user>@login.hpc.emory.edu:~/thyroid_extract/thyroid_reextract_results.tar.gz .

    # 2. Extract:
    tar xzf thyroid_reextract_results.tar.gz -C hpc_results/

    # 3. Copy parquets into processed/ (so the repo has the updated files):
    cp hpc_results/note_entities_llm_*.parquet processed/

    # 4. Run this ingestion script:
    python hpc/02_ingest_to_motherduck.py

    # Or dry-run first:
    python hpc/02_ingest_to_motherduck.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
PROCESSED = REPO / "processed"
sys.path.insert(0, str(REPO))

# The 5 re-extracted domains and their MotherDuck table names
DOMAINS = {
    "synoptic_pathology_enrichment": {
        "parquet_stem": "note_entities_llm_synoptic_pathology_enrichment",
        "md_table": "note_entities_llm_synoptic_pathology_enrichment",
    },
    "tg_kinetics": {
        "parquet_stem": "note_entities_llm_tg_kinetics",
        "md_table": "note_entities_llm_tg_kinetics",
    },
    "dynamic_risk_response": {
        "parquet_stem": "note_entities_llm_dynamic_risk_response",
        "md_table": "note_entities_llm_dynamic_risk_response",
    },
    "us_nodule_dynamics": {
        "parquet_stem": "note_entities_llm_us_nodule_dynamics",
        "md_table": "note_entities_llm_us_nodule_dynamics",
    },
    "presenting_symptoms": {
        "parquet_stem": "note_entities_llm_presenting_symptoms",
        "md_table": "note_entities_llm_presenting_symptoms",
    },
}

# Target MotherDuck database (the canonical publication DB)
MD_DATABASE = "thyroid_canonical_publication_v1_0"


def get_motherduck_token() -> str:
    """Load MotherDuck token from the repo's helper or environment."""
    import os

    # Try environment variable first
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
    if token:
        return token

    # Try repo's motherduck_client helper
    try:
        from motherduck_client import get_token
        token = get_token()
        if token:
            return token
    except ImportError:
        pass

    # Try .env file
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO / ".env")
        token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
        if token:
            return token
    except ImportError:
        pass

    print("ERROR: MotherDuck token not found.")
    print("  Set MOTHERDUCK_TOKEN env var, or configure motherduck_client.py / .env")
    sys.exit(1)


def upload_domain(con, domain_key: str, cfg: dict, dry_run: bool) -> dict:
    """Upload a single domain's parquet to MotherDuck."""
    parquet_path = PROCESSED / f"{cfg['parquet_stem']}.parquet"
    md_table = f"main.{cfg['md_table']}"

    print(f"\n{'─'*60}")
    print(f"  DOMAIN: {domain_key}")
    print(f"  Parquet: {parquet_path}")
    print(f"  MD Table: {md_table}")

    if not parquet_path.exists():
        print(f"  ⚠ Parquet not found — skipping")
        return {"status": "missing", "rows": 0}

    df = pd.read_parquet(parquet_path)
    print(f"  Rows in parquet: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")

    if df.empty:
        print(f"  ⚠ Empty parquet — skipping")
        return {"status": "empty", "rows": 0}

    # Quick validation
    if "research_id" in df.columns:
        n_patients = df["research_id"].nunique()
        print(f"  Unique patients: {n_patients:,}")
    if "entity_domain" in df.columns:
        print(f"  Entity domains: {df['entity_domain'].unique().tolist()}")

    # Count entities with evidence (quality check)
    if "verification_status" in df.columns:
        verified = (df["verification_status"] == "verified_substring").sum()
        total = len(df)
        pct = (verified / total * 100) if total > 0 else 0
        print(f"  Verified entities: {verified:,}/{total:,} ({pct:.1f}%)")

    if dry_run:
        print(f"  [DRY RUN] Would replace {md_table} with {len(df):,} rows")
        return {"status": "dry_run", "rows": len(df)}

    # Strategy: DROP + CREATE to fully replace with new extraction
    # (these 5 domains had near-zero data, so full replacement is correct)
    abs_path = str(parquet_path.resolve())

    # Check if table exists
    table_exists = False
    try:
        con.execute(f"SELECT 1 FROM {md_table} LIMIT 1")
        table_exists = True
        old_count = con.execute(f"SELECT COUNT(*) FROM {md_table}").fetchone()[0]
        print(f"  Existing table: {old_count:,} rows")
    except Exception:
        print(f"  Table does not exist — will create")

    if table_exists:
        # Back up old table, then replace
        backup_name = f"{cfg['md_table']}_backup_{datetime.now().strftime('%Y%m%d')}"
        print(f"  Backing up to main.{backup_name} ...")
        con.execute(f"CREATE OR REPLACE TABLE main.{backup_name} AS SELECT * FROM {md_table}")
        con.execute(f"DROP TABLE {md_table}")

    print(f"  Creating {md_table} from parquet ...")
    con.execute(f"""
        CREATE TABLE {md_table} AS
        SELECT * FROM read_parquet('{abs_path}')
    """)

    new_count = con.execute(f"SELECT COUNT(*) FROM {md_table}").fetchone()[0]
    print(f"  ✓ {md_table}: {new_count:,} rows uploaded")

    return {"status": "uploaded", "rows": new_count}


def main():
    parser = argparse.ArgumentParser(
        description="Upload HPC re-extraction results to MotherDuck"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate only, do not write to MotherDuck")
    parser.add_argument("--domain", type=str, default=None,
                        help="Upload only this domain (default: all 5)")
    args = parser.parse_args()

    print("=" * 65)
    print("  HPC Re-Extraction → MotherDuck Ingestion")
    print(f"  Database: md:{MD_DATABASE}")
    print(f"  Dry run:  {args.dry_run}")
    print(f"  Time:     {datetime.now().isoformat()}")
    print("=" * 65)

    # Connect to MotherDuck
    import duckdb

    token = get_motherduck_token()
    print(f"\nMotherDuck token: SET (length={len(token)})")

    if not args.dry_run:
        con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
        print(f"Connected to md:{MD_DATABASE}")
    else:
        con = None
        print("[DRY RUN] Skipping MotherDuck connection — reading parquets only")

    # Filter to single domain if specified
    domains_to_process = DOMAINS
    if args.domain:
        if args.domain not in DOMAINS:
            print(f"ERROR: Unknown domain '{args.domain}'")
            print(f"  Available: {list(DOMAINS.keys())}")
            sys.exit(1)
        domains_to_process = {args.domain: DOMAINS[args.domain]}

    # Process each domain
    results = {}
    for domain_key, cfg in domains_to_process.items():
        results[domain_key] = upload_domain(con, domain_key, cfg, args.dry_run)

    # Summary
    print(f"\n{'='*65}")
    print("  UPLOAD SUMMARY")
    print(f"{'='*65}")
    for domain_key, result in results.items():
        status = result["status"]
        rows = result["rows"]
        icon = {"uploaded": "✓", "dry_run": "○", "missing": "✗", "empty": "⚠"}
        print(f"  {icon.get(status, '?')} {domain_key}: {status} ({rows:,} rows)")

    if not args.dry_run:
        con.close()
        print("\nDone. Tables updated in MotherDuck.")
        print("Next step: run the canonical rollup to update canonical_patient_master.")
    else:
        print("\n[DRY RUN] No changes made. Remove --dry-run to upload.")


if __name__ == "__main__":
    main()
