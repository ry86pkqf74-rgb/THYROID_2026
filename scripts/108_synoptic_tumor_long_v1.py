#!/usr/bin/env python3
"""Build synoptic_tumor_long_v1: one row per populated tumor focus (slots 1–5) from path_synoptics.

Source: wide `processed/path_synoptics.parquet` (or table if already in DuckDB).
Outputs:
  - processed/synoptic_tumor_long_v1.parquet
  - CREATE OR REPLACE TABLE synoptic_tumor_long_v1 in local thyroid_master.duckdb
  - With --md: same table + md_synoptic_tumor_long_v1 on local DuckDB (requires token)

Provenance columns: source_file, source_column_prefix, tumor_index, synoptic_row_ix.

Usage:
  .venv/bin/python scripts/108_synoptic_tumor_long_v1.py
  .venv/bin/python scripts/108_synoptic_tumor_long_v1.py --md
"""
from __future__ import annotations

import argparse
import copy
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"
SOURCE_FILE = "All Diagnoses & synoptic 12_1_2025.xlsx -> path_synoptics.parquet"

# Field name -> path_synoptics column per tumor slot (only keys present in schema)
SLOT_MAP: dict[int, dict[str, str]] = {
    1: {
        "histologic_type": "tumor_1_histologic_type",
        "histologic_variant": "tumor_1_variant",
        "size_greatest_dimension_cm": "tumor_1_size_greatest_dimension_cm",
        "extrathyroidal_extension": "tumor_1_extrathyroidal_extension",
        "margin_status": "tumor_1_margin_status",
        "angioinvasion": "tumor_1_angioinvasion",
        "angioinvasion_quantify": "tumor_1_angioinvasion_quantify",
        "lymphatic_invasion": "tumor_1_lymphatic_invasion",
        "perineural_invasion": "tumor_1_perineural_invasion",
        "capsular_invasion": "tumor_1_capsular_invasion",
        "site": "tumor_1_site_laterality",
        "ln_involved": "tumor_1_ln_involved",
        "ln_examined": "tumor_1_ln_examined",
    },
    2: {
        "histologic_type": "tumor_2_histologic_type",
        "histologic_variant": "tumor_2_histologic_variants",
        "size_greatest_dimension_cm": "tumor_2_size_greatest_dimension_cm",
        "extrathyroidal_extension": "tumor_2_extrathyroidal_extension",
        "margin_status": "tumor_2_margin_status",
        "angioinvasion": "tumor_2_angioinvasion",
        "angioinvasion_quantify": "tumor_2_angioinvasion_quantify",
        "lymphatic_invasion": "tumor_2_lymphatic_invasion",
        "perineural_invasion": "tumor_2_perineural_invasion",
        "capsular_invasion": "tumor_2_capsular_invasion",
        "site": "tumor_2_site",
        "ln_involved": "tumor_2_lns_involved",
    },
    3: {
        "histologic_type": "tumor_3_histologic_type",
        "histologic_variant": "tumor_3_histologic_variant",
        "size_greatest_dimension_cm": "tumor_3_size_greatest_dimension_cm",
        "extrathyroidal_extension": "tumor_3_extrathyroidal_extension",
        "margin_status": "tumor_3_margin_status",
        "angioinvasion": "tumor_3_angioinvasion",
        "angioinvasion_quantify": "tumor_3_angioinvasion_quantify",
        "lymphatic_invasion": "tumor_3_lymphatic_invasion",
        "perineural_invasion": "tumor_3_perineural_invasion",
        "capsular_invasion": "tumor_3_capsular_invasion",
        "site": "tumor_3_site",
    },
    4: {
        "histologic_type": "tumor_4_histologic_type",
        "histologic_variant": "tumor_4_histologic_variant",
        "size_greatest_dimension_cm": "tumor_4_size_greatest_dimension_cm",
        "extrathyroidal_extension": "tumor_4_extrathyroidal_extension",
        "margin_status": "tumor_4_margin_status",
        "angioinvasion": "tumor_4_angioinvasion",
        "angioinvasion_quantify": "tumor_4_angioinvasion_quantify",
        "lymphatic_invasion": "tumor_4_lymphatic_invasion",
        "perineural_invasion": "tumor_4_perineural_invasion",
        "site": "tumor_4_site",
    },
    5: {
        "histologic_type": "tumor_5_histologic_type",
        "histologic_variant": "tumor_5_histologic_variant",
        "size_greatest_dimension_cm": "tumor_5_size_greatest_dimension_cm",
        "extrathyroidal_extension": "tumor_5_extrathyroidal_extension",
        "margin_status": "tumor_5_margin_status",
        "angioinvasion": "tumor_5_angioinvasion",
        "angioinvasion_quantify": "tumor_5_angioinvasion_quantify",
        "lymphatic_invasion": "tumor_5_lymphatic_invasion",
        "perineural_invasion": "tumor_5_perineural_invasion",
        "capsular_invasion": "tumor_5_capsular_invasion",
        "site": "tumor_5_site",
    },
}

def _col_nonempty_mask(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return s.notna()
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(("", "nan", "none", "null"))


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def build_long_frame(ps: pd.DataFrame) -> pd.DataFrame:
    ps_cols = set(ps.columns)
    slot_map = copy.deepcopy(SLOT_MAP)
    for ti, cmap in slot_map.items():
        for logical, col in list(cmap.items()):
            if col not in ps_cols:
                del cmap[logical]
    ps = ps.reset_index(drop=True)
    ps["_synoptic_row_ix"] = np.arange(1, len(ps) + 1, dtype=np.int64)
    git = _git_sha()
    parts: list[pd.DataFrame] = []
    base_cols = ["_synoptic_row_ix", "research_id", "surg_date", "thyroid_procedure"]

    for tumor_index, cmap in slot_map.items():
        slot_cols = list(cmap.values())
        if not slot_cols:
            continue
        mask = np.zeros(len(ps), dtype=bool)
        for c in slot_cols:
            mask |= _col_nonempty_mask(ps[c]).to_numpy()
        if not mask.any():
            continue
        chunk = ps.loc[mask, base_cols + list(cmap.values())].copy()
        chunk = chunk.rename(columns={v: k for k, v in cmap.items()})
        chunk = chunk.rename(columns={"_synoptic_row_ix": "synoptic_row_ix"})
        chunk["tumor_index"] = tumor_index
        chunk["source_table"] = "path_synoptics"
        chunk["source_path_file"] = SOURCE_FILE
        chunk["source_column_prefix"] = f"tumor_{tumor_index}_"
        chunk["build_git_sha"] = git
        # research_id int where possible
        if "research_id" in chunk.columns:
            chunk["research_id"] = pd.to_numeric(chunk["research_id"], errors="coerce").astype(
                "Int64"
            )
        parts.append(chunk)

    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    meta = [
        "synoptic_row_ix",
        "research_id",
        "surg_date",
        "thyroid_procedure",
        "tumor_index",
        "source_table",
        "source_path_file",
        "source_column_prefix",
        "build_git_sha",
    ]
    rest = sorted(c for c in out.columns if c not in meta)
    return out[meta + rest]


def materialize_local(df: pd.DataFrame) -> None:
    out = PROCESSED / "synoptic_tumor_long_v1.parquet"
    df.to_parquet(out, index=False)
    print(f"  Wrote {out} ({len(df):,} rows)")
    if not DB_PATH.exists():
        print("  Skip local DB — thyroid_master.duckdb not found")
        return
    con = duckdb.connect(str(DB_PATH))
    con.execute(
        f"CREATE OR REPLACE TABLE synoptic_tumor_long_v1 AS "
        f"SELECT * FROM read_parquet('{out.as_posix()}')"
    )
    con.close()
    print("  Local table synoptic_tumor_long_v1 refreshed")


def materialize_motherduck(df: pd.DataFrame) -> None:
    from utils.md_connect import connect_md_or_file

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False)
        con = connect_md_or_file(ROOT / "thyroid_master.duckdb", md=True, fail_closed=True)
        con.execute(
            f"CREATE OR REPLACE TABLE synoptic_tumor_long_v1 AS "
            f"SELECT * FROM read_parquet('{tmp_path}')"
        )
        con.execute(
            "CREATE OR REPLACE TABLE md_synoptic_tumor_long_v1 AS "
            "SELECT * FROM synoptic_tumor_long_v1"
        )
        con.close()
        print(
            "  MotherDuck tables synoptic_tumor_long_v1 + md_synoptic_tumor_long_v1 refreshed"
        )
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", action="store_true", help="Push to local DuckDB")
    args = ap.parse_args()

    pq = PROCESSED / "path_synoptics.parquet"
    if not pq.exists():
        raise SystemExit(f"Missing {pq}")
    ps = pd.read_parquet(pq)
    df = build_long_frame(ps)
    materialize_local(df)
    if args.md:
        materialize_motherduck(df)
    print(f"Done. Rows: {len(df):,}")


if __name__ == "__main__":
    main()
