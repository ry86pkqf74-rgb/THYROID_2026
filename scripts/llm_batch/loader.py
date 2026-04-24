"""scripts/llm_batch/loader.py — Generic per-domain loader.

Reads output JSONL for a domain, creates/replaces the target table in
main, inserts all rows. Idempotent.

Shape of loader table (every domain uses this same shape):
  note_row_id        VARCHAR (research_id|note_type|note_index)
  research_id        VARCHAR
  note_type          VARCHAR
  note_index         VARCHAR
  source_workbook    VARCHAR
  source_sheet       VARCHAR
  source_column      VARCHAR
  parsed_json        VARCHAR    -- JSON string, domain-specific payload
  raw_llm_response   VARCHAR
  error              INTEGER
  extracted_at       VARCHAR
  llm_model          VARCHAR
  elapsed_s          DOUBLE
  build_ts           TIMESTAMP

Downstream per-domain UNNEST parsers build the typed Tier 2 tables.

Usage:
    python3 loader.py --manifest manifest.json --domain ete_subgrade
    python3 loader.py --manifest manifest.json --domain ALL
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import time

import duckdb


def get_token(repo_root: pathlib.Path) -> str:
    for var in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        if os.environ.get(var):
            return os.environ[var]
    toml = repo_root / "motherduck.local.toml"
    if toml.exists():
        for line in toml.read_text().splitlines():
            m = re.match(r"^\s*(MD_SA_TOKEN|MOTHERDUCK_TOKEN|motherduck_token)\s*=\s*[\"']?([^\"'#\s]+)", line)
            if m:
                return m.group(2)
    sys.exit("no MotherDuck token found")


def load_domain(con: duckdb.DuckDBPyConnection, domain_cfg: dict, batch_dir: pathlib.Path) -> None:
    """Load a domain's JSONL via DuckDB's native read_json_auto — one atomic CTAS.

    Why: executemany streams every row over the network and is fragile on long
    runs. A server-side CTAS uploads the JSONL once and DuckDB scans it natively.
    """
    name = domain_cfg["name"]
    table = domain_cfg["loader_table"]
    output_path = batch_dir / domain_cfg["output_jsonl"]
    if not output_path.exists():
        print(f"[{name}] SKIP: output JSONL not found: {output_path}")
        return
    # Quick sanity count
    with output_path.open() as f:
        n_lines = sum(1 for line in f if line.strip())
    print(f"[{name}] source lines: {n_lines:,}")
    if n_lines == 0:
        return

    abs_path = str(output_path.resolve()).replace("'", "''")
    # NOTE: read_json_auto against MotherDuck occasionally duplicates rows
    # (observed +3-8% over source lines on vascular/airway). We dedupe on
    # note_row_id via QUALIFY ROW_NUMBER after the scan.
    con.execute(f"""
    CREATE OR REPLACE TABLE {table} AS
    SELECT * EXCLUDE(_rn)
    FROM (
      SELECT
        research_id || '|' || note_type || '|' || COALESCE(CAST(note_index AS VARCHAR), '') AS note_row_id,
        research_id,
        note_type,
        COALESCE(CAST(note_index AS VARCHAR), '')                                           AS note_index,
        source_workbook,
        source_sheet,
        source_column,
        TO_JSON(parsed_json)                                                                AS parsed_json,
        raw_llm_response,
        COALESCE(error, 0)::INTEGER                                                         AS error,
        extracted_at,
        llm_model,
        COALESCE(elapsed_s, 0)::DOUBLE                                                      AS elapsed_s,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                                AS build_ts,
        ROW_NUMBER() OVER (
          PARTITION BY research_id, note_type, COALESCE(CAST(note_index AS VARCHAR), '')
          ORDER BY extracted_at DESC
        ) AS _rn
      FROM read_json_auto('{abs_path}', format='newline_delimited', union_by_name=true)
    )
    WHERE _rn = 1
    """)
    n = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id), SUM(error) FROM {table}"
    ).fetchone()
    print(f"[{name}] loaded: {n[0]:,} rows, {n[1]:,} distinct patients, err={n[2]} -> {table}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--domain", required=True)
    args = ap.parse_args()

    manifest_path = pathlib.Path(args.manifest)
    manifest = json.loads(manifest_path.read_text())
    batch_dir = manifest_path.parent
    repo_root = pathlib.Path(os.environ.get("REPO_ROOT", os.path.expanduser("~/THyroid 2026")))
    token = get_token(repo_root)
    con = duckdb.connect(f"md:{manifest['target_db']}?motherduck_token={token}")

    domains = manifest["domains"]
    if args.domain != "ALL":
        domains = [d for d in domains if d["name"] == args.domain]
        if not domains:
            sys.exit(f"domain {args.domain!r} not in manifest")
    for d in domains:
        print(f"\n===== load {d['name']} =====")
        load_domain(con, d, batch_dir)


if __name__ == "__main__":
    main()
