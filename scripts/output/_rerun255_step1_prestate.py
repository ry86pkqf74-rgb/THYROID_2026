#!/usr/bin/env python3
"""Step 1 — Capture pre-run row counts + sha256 hashes; archive the 2 Tg rollup tables."""
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_Q = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'

OUT = Path(__file__).resolve().parent
STATE_JSON = OUT / f"_rerun255_pre_state_{UTC}.json"

ROLLUPS = [
    "tg_postop_surveillance_windows_v1",
    "tg_timeline_patient_summary_v1",
]
PER_ANALYTE = [
    "canonical_labs_thyroglobulin_v1",
    "canonical_labs_tsh_v1",
    "canonical_labs_pth_v1",
    "canonical_labs_calcium_v1",
    "canonical_labs_vitamin_d_v1",
]
VIEWS = [
    "longitudinal_lab_VIEW_v1",
    "thyroglobulin_lab_VIEW_v1",
]

con = connect_locked()

def row_count(tbl: str) -> int:
    return con.execute(f'SELECT COUNT(*) FROM main."{tbl}"').fetchone()[0]

def distinct_rid(tbl: str) -> int:
    return con.execute(f'SELECT COUNT(DISTINCT research_id) FROM main."{tbl}"').fetchone()[0]

def sha256_table(tbl: str) -> str:
    """Stable sha256 over the sorted content of the table. We use MD5 per-row then MD5
    the concatenation to produce a deterministic digest. DuckDB doesn't expose SHA256
    on ROW, so fall back to a hash-of-hashes: hash(string_agg(md5(...))) order-invariant."""
    cols = con.execute(f"""
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='main' AND table_name='{tbl}'
      ORDER BY ordinal_position
    """).fetchall()
    col_list = ", ".join([f'COALESCE(CAST("{c[0]}" AS VARCHAR), \'\')' for c in cols])
    # hash each row, then hash the sorted aggregate
    q = f"""
      SELECT md5(string_agg(row_hash, ',' ORDER BY row_hash))
      FROM (
        SELECT md5(concat_ws('|', {col_list})) AS row_hash
        FROM main."{tbl}"
      )
    """
    return con.execute(q).fetchone()[0]

state = {"utc": UTC, "rollups": {}, "per_analyte": {}, "views": {}}

print("== ROLLUPS ==")
for t in ROLLUPS:
    rc = row_count(t)
    dr = distinct_rid(t)
    h = sha256_table(t)
    state["rollups"][t] = {"rows": rc, "distinct_research_id": dr, "hash": h}
    print(f"  {t}: rows={rc}, distinct_rid={dr}, hash={h[:16]}...")

print("== PER-ANALYTE CANONICALS ==")
for t in PER_ANALYTE:
    rc = row_count(t)
    h = sha256_table(t)
    state["per_analyte"][t] = {"rows": rc, "hash": h}
    print(f"  {t}: rows={rc}, hash={h[:16]}...")

print("== VIEWS ==")
for t in VIEWS:
    rc = row_count(t)
    state["views"][t] = {"rows": rc}
    print(f"  {t}: rows={rc}")

# Archive snapshots
print("== ARCHIVING ROLLUP TABLES (pre-255rerun) ==")
con.execute(f'CREATE SCHEMA IF NOT EXISTS {ARCHIVE_Q}')
for t in ROLLUPS:
    arch = f'{ARCHIVE_Q}."{t}_pre255rerun_{UTC}"'
    con.execute(f'CREATE TABLE {arch} AS SELECT * FROM main."{t}"')
    n = con.execute(f'SELECT COUNT(*) FROM {arch}').fetchone()[0]
    print(f"  archived {t} -> {arch}: {n} rows")
    state["rollups"][t]["archive"] = arch

# Write state file
STATE_JSON.write_text(json.dumps(state, indent=2))
print(f"\nPre-state written to {STATE_JSON}")
