#!/usr/bin/env python3
"""Append analyst-delivered institutional lab rows to main.longitudinal_lab_canonical_v1.

Deterministic identity: (research_id, lab_date, lab_name_standardized, source_lineage_key).
Re-running the same --ingestion-wave replaces only rows for that wave (prior waves stay).

No raw clinical note text is read or written. MotherDuck target uses fail-closed --md.

Usage:
  .venv/bin/python scripts/127_analyst_institutional_lab_append.py --md \\
      --input exports/incoming/final_lab_extract_YYYYMMDD.csv \\
      --ingestion-wave final_institutional_20260407

Expected CSV columns (headers):
  research_id (required)
  lab_date (required, ISO date)
  lab_name_standardized OR lab_name_raw (required)
  value_raw (required)
  source_lineage_key (required) — institutional order/result id, hash, or stable composite
  value_numeric (optional)
  unit_raw, unit_standardized, analyte_group, lab_date_status (optional)
  source_table (optional; default analyst_institutional_lab_deliverable)
  provenance_note (optional)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
SCRIPT_TAG = "127_analyst_institutional_lab_append.py"


REQUIRED_COLS = {"research_id", "lab_date", "value_raw", "source_lineage_key"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB path (unused when --md).")
    p.add_argument("--input", type=Path, required=True, help="Analyst lab CSV path.")
    p.add_argument(
        "--ingestion-wave",
        required=True,
        help="Unique wave label, e.g. final_institutional_20260407 (used for idempotent replace).",
    )
    p.add_argument("--dry-run", action="store_true", help="Validate and print counts only.")
    return p.parse_args()


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file

        return connect_md_or_file(Path(args.db_path), md=True, fail_closed=True)
    print("  FATAL: This script requires --md (no silent local fallback for institutional append).")
    sys.exit(1)


def build_frame(path: Path, ingestion_wave: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise SystemExit(f"  FATAL: CSV missing required columns: {sorted(missing)}")

    if "lab_name_standardized" not in df.columns and "lab_name_raw" not in df.columns:
        raise SystemExit("  FATAL: need lab_name_standardized or lab_name_raw")

    out = pd.DataFrame()
    out["research_id"] = pd.to_numeric(df["research_id"], errors="coerce").astype("Int64")
    out["lab_date"] = pd.to_datetime(df["lab_date"], errors="coerce").dt.date.astype(str)
    out["lab_date_status"] = df["lab_date_status"] if "lab_date_status" in df.columns else "exact_collection_date"
    out["lab_name_raw"] = (
        df["lab_name_raw"] if "lab_name_raw" in df.columns else df["lab_name_standardized"]
    )
    out["lab_name_standardized"] = (
        df["lab_name_standardized"] if "lab_name_standardized" in df.columns else df["lab_name_raw"]
    )
    out["analyte_group"] = (
        df["analyte_group"] if "analyte_group" in df.columns else "institutional_deliverable"
    )
    out["value_raw"] = df["value_raw"].astype(str)
    if "value_numeric" in df.columns:
        out["value_numeric"] = pd.to_numeric(df["value_numeric"], errors="coerce")
    else:
        out["value_numeric"] = pd.NA
    out["unit_raw"] = df["unit_raw"] if "unit_raw" in df.columns else None
    out["unit_standardized"] = df["unit_standardized"] if "unit_standardized" in df.columns else None
    out["reference_range"] = None
    out["abnormal_flag"] = None
    if "is_censored" in df.columns:
        out["is_censored"] = df["is_censored"].map(
            lambda x: str(x).lower() in ("1", "true", "yes", "t")
        )
    else:
        out["is_censored"] = out["value_raw"].astype(str).str.strip().str.startswith("<")
    out["source_table"] = (
        df["source_table"] if "source_table" in df.columns else "analyst_institutional_lab_deliverable"
    )
    out["source_script"] = SCRIPT_TAG
    out["ingestion_wave"] = ingestion_wave
    out["data_completeness_tier"] = "current_structured"
    note = df["provenance_note"] if "provenance_note" in df.columns else None
    key = df["source_lineage_key"].astype(str)
    if note is not None:
        out["provenance_note"] = (
            "lineage_key=" + key + " | " + note.fillna("").astype(str)
        ).str.strip()
    else:
        out["provenance_note"] = "lineage_key=" + key

    bad = out["research_id"].isna() | out["lab_date"].eq("NaT") | out["lab_name_standardized"].isna()
    if bad.any():
        raise SystemExit(f"  FATAL: {bad.sum()} row(s) failed coercion (research_id / lab_date / name)")

    return out


DEDUP_VIEW_SQL = """
CREATE OR REPLACE VIEW main.longitudinal_lab_deduped_v AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id,
                         lab_date,
                         lab_name_standardized,
                         COALESCE(CAST(value_numeric AS VARCHAR), value_raw)
            ORDER BY
                CASE
                    WHEN ingestion_wave LIKE 'wave_tg%'
                      OR ingestion_wave LIKE 'wave_tgab%' THEN 1
                    WHEN ingestion_wave LIKE 'wave_1%'
                      OR ingestion_wave LIKE 'wave_2%' THEN 2
                    WHEN ingestion_wave LIKE 'final_institutional%' THEN 0
                    ELSE 3
                END,
                source_script DESC
        ) AS _rn
    FROM main.longitudinal_lab_canonical_v1
)
SELECT * EXCLUDE (_rn) FROM ranked WHERE _rn = 1
"""


def main() -> None:
    args = parse_args()
    if not args.input.is_file():
        print(f"  FATAL: --input not found: {args.input}")
        sys.exit(1)

    wave = args.ingestion_wave.strip()
    frame = build_frame(args.input, wave)
    print(f"  Prepared {len(frame):,} lab row(s), ingestion_wave={wave}")

    if args.dry_run:
        print("  [dry-run] stopping before database write")
        return

    con = connect(args)
    try:
        if not con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'longitudinal_lab_canonical_v1'"
        ).fetchone()[0]:
            print("  FATAL: main.longitudinal_lab_canonical_v1 does not exist")
            sys.exit(1)

        pre = con.execute("SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1").fetchone()[0]
        con.execute(
            "DELETE FROM main.longitudinal_lab_canonical_v1 WHERE ingestion_wave = ?",
            [wave],
        )
        con.register("_lab_append", frame)
        cols = (
            "research_id, lab_date, lab_date_status, lab_name_raw, lab_name_standardized, "
            "analyte_group, value_raw, value_numeric, unit_raw, unit_standardized, "
            "reference_range, abnormal_flag, is_censored, source_table, source_script, "
            "ingestion_wave, data_completeness_tier, provenance_note"
        )
        con.execute(
            f"""
            INSERT INTO main.longitudinal_lab_canonical_v1 ({cols})
            SELECT {cols} FROM _lab_append
            """
        )
        con.unregister("_lab_append")
        post = con.execute("SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1").fetchone()[0]
        print(f"  [lab] longitudinal_lab_canonical_v1: {pre:,} → {post:,} rows (replaced wave '{wave}')")

        con.execute(DEDUP_VIEW_SQL)
        ded = con.execute("SELECT COUNT(*) FROM main.longitudinal_lab_deduped_v").fetchone()[0]
        print(f"  [lab] main.longitudinal_lab_deduped_v refreshed: {ded:,} rows")

        qc_path = Path(
            f"studies/_lab_append_qc_{wave}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        )
        qc_path.parent.mkdir(parents=True, exist_ok=True)
        by_wave = con.execute(
            """
            SELECT ingestion_wave, COUNT(*) AS n,
                   COUNT(DISTINCT research_id) AS pts
            FROM main.longitudinal_lab_canonical_v1
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchdf()
        qc_path.write_text(by_wave.to_json(orient="records", indent=2), encoding="utf-8")
        print(f"  [qc] wave summary written {qc_path}")
    finally:
        con.close()

    print("  [done] 127_analyst_institutional_lab_append.py")


if __name__ == "__main__":
    main()
