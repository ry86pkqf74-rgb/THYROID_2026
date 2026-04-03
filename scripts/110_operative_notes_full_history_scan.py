#!/usr/bin/env python3
"""
110_operative_notes_full_history_scan.py

Full-historical operative-note recovery for THYROID_2026.

Root causes addressed (see ADR in module doc below):
  1. clinical_notes_long.note_date uses extract_note_date() which, before 2026-03,
     scanned only the first ~500 characters. Many operative dictations omit a
     dateline in that window → note_date stayed NULL while note_text was valid.
     Diagnostics counting TRY_CAST(note_date AS DATE) < 2019 therefore *undercounted*
     pre-2019 surgery eras even when notes existed.
  2. Polars ingest (01_ingest_all_files.py) looked for op_note_* columns while
     standardized Excel headers are opnote_* → operative family was silently empty
     in parquet-only paths.

This script:
  - Optionally queries local DuckDB for baseline diagnostics (--diagnose-md).
  - Loads research_id → surg_date from the synoptic workbook (or local DuckDB
    path_synoptics when --md --use-md-surgery-dates).
  - Re-extracts all OPNote columns from Notes 12_1_25.xlsx via config/notes_column_map.csv
    using extended date scanning (50k chars) + surg_date fallback.
  - Optionally scans every .xlsx under raw/** and extra roots for additional
    operative-like text columns (heuristic, auditable).
  - Writes exports/operative_notes_full_history_<stamp>/ audit artifacts + parquet.
  - Optionally CREATE OR REPLACE TABLE raw.operative_notes_full_history_v2 on local DuckDB.

Usage (local artifacts only):
  .venv/bin/python scripts/110_operative_notes_full_history_scan.py \\
    --extra-root '/Users/you/Downloads/Active Master Files 2'

Usage (local DuckDB diagnostics + publish table):
  export LOCAL_DB_PATH='...'
  .venv/bin/python scripts/110_operative_notes_full_history_scan.py --md --diagnose-md --publish-md \\
    --extra-root '/Users/you/Downloads/Active Master Files 2'

Do NOT commit tokens. Load LOCAL_DB_PATH from your environment or secret manager.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from motherduck_client import MotherDuckClient  # noqa: E402
from utils.text_helpers import (  # noqa: E402
    clean_research_id,
    extract_note_date,
    make_note_row_id,
    safe_parse_date,
    standardize_columns,
    strip_phi,
)

VERSION_TAG = "v20260327_pre2019_full_scan_fix"
CONFIG_MAP = ROOT / "config" / "notes_column_map.csv"
DEFAULT_NOTES_NAMES = (
    "Notes 12_1_25.xlsx",
    "Notes 12_1_25.XLSX",
)
DEFAULT_SYNOPTIC_NAMES = (
    "All Diagnoses & synoptic 12_1_2025.xlsx",
    "All Diagnoses & synoptic 12_1_2025.XLSX",
)

OP_COL_NAME_HINT = re.compile(
    r"(op\s*note|operative|intra\s*-?op|dictation|op\s*report|procedure\s*note|"
    r"operation\s*note|or\s*note)",
    re.IGNORECASE,
)
OP_BODY_HINT = re.compile(
    r"(PREOPERATIVE\s+DIAGNOSIS|POSTOPERATIVE\s+DIAGNOSIS|"
    r"DESCRIPTION\s+OF\s+PROCEDURE|INDICATION\s+FOR\s+PROCEDURE)",
    re.IGNORECASE,
)
SURGEON_LINE = re.compile(r"^\s*SURGEON\s*:\s*(.+)$", re.MULTILINE | re.IGNORECASE)
PROC_LINE = re.compile(
    r"^\s*PROCEDURE\s*:\s*(.+)$", re.MULTILINE | re.IGNORECASE
)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def _collect_xlsx_paths(raw_dir: Path, extras: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for base in [raw_dir, *extras]:
        if not base.exists():
            continue
        for p in sorted(base.rglob("*.xlsx")):
            try:
                rp = p.resolve()
            except OSError:
                rp = p
            if rp not in seen:
                seen.add(rp)
                out.append(p)
    return out


def _find_first(names: tuple[str, ...], search_roots: Iterable[Path]) -> Path | None:
    for root in search_roots:
        if not root:
            continue
        for name in names:
            cand = root / name
            if cand.exists():
                return cand
    return None


def load_surgery_lookup_from_synoptic(path: Path) -> pd.DataFrame:
    """research_id, surg_date_iso (YYYY-MM-DD or NULL)."""
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet = "synoptics + Dx merged" if "synoptics + Dx merged" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
    df = standardize_columns(df)
    df = clean_research_id(df)
    if "surg_date" not in df.columns:
        return pd.DataFrame(columns=["research_id", "surg_date_iso"])
    out_rows = []
    for _, row in df.iterrows():
        rid = row.get("research_id")
        if pd.isna(rid):
            continue
        raw = row.get("surg_date")
        iso = safe_parse_date(raw)
        out_rows.append({"research_id": int(rid), "surg_date_iso": iso})
    lu = pd.DataFrame(out_rows)
    lu = lu.drop_duplicates(subset=["research_id"], keep="first")
    return lu


def load_surgery_lookup_motherduck(con) -> pd.DataFrame:
    # surg_date is often VARCHAR with mixed formats; native DATE also occurs.
    q = """
    SELECT CAST(research_id AS BIGINT) AS research_id,
           MIN(
               COALESCE(
                   TRY_CAST(surg_date AS DATE),
                   TRY_STRPTIME(TRIM(REGEXP_REPLACE(CAST(surg_date AS VARCHAR), ';', '')), '%Y-%m-%d'),
                   TRY_STRPTIME(TRIM(REGEXP_REPLACE(CAST(surg_date AS VARCHAR), ';', '')), '%m/%d/%Y'),
                   TRY_STRPTIME(TRIM(REGEXP_REPLACE(CAST(surg_date AS VARCHAR), ';', '')), '%m/%d/%y')
               )
           ) AS sd
    FROM path_synoptics
    WHERE research_id IS NOT NULL AND TRIM(COALESCE(CAST(surg_date AS VARCHAR), '')) <> ''
    GROUP BY 1
    """
    try:
        df = con.execute(q).df()
    except Exception:
        return pd.DataFrame(columns=["research_id", "surg_date_iso"])
    if df.empty:
        return pd.DataFrame(columns=["research_id", "surg_date_iso"])
    df["surg_date_iso"] = df["sd"].apply(
        lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None
    )
    return df[["research_id", "surg_date_iso"]].drop_duplicates("research_id", keep="first")


def mapped_notes_records(
    notes_path: Path,
    surgery_lu: pd.DataFrame,
) -> list[dict]:
    col_map = pd.read_csv(CONFIG_MAP)
    note_rows = col_map[col_map["is_note_like"] == True]  # noqa: E712
    surg = surgery_lu.set_index("research_id")["surg_date_iso"].to_dict()
    xl = pd.ExcelFile(notes_path, engine="openpyxl")
    recs: list[dict] = []

    for sheet_name in note_rows["sheet"].unique():
        if sheet_name not in xl.sheet_names:
            continue
        df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl")
        df = standardize_columns(df)
        df = clean_research_id(df)
        df = strip_phi(df)
        for _, mapping_row in note_rows[note_rows["sheet"] == sheet_name].iterrows():
            snake_col = mapping_row["source_column_snake"]
            note_type = mapping_row["proposed_note_type"]
            note_index = mapping_row["proposed_note_index"]
            if snake_col not in df.columns or pd.isna(note_type) or str(note_type).strip() == "":
                continue
            if str(note_type) != "op_note":
                continue
            note_index = (
                int(note_index)
                if pd.notna(note_index) and str(note_index).strip()
                else 1
            )
            for _, row in df.iterrows():
                rid = row.get("research_id")
                text = row.get(snake_col)
                if pd.isna(rid) or pd.isna(text) or not str(text).strip():
                    continue
                text_str = str(text).strip()
                nd_text = extract_note_date(text_str, max_scan_chars=50_000)
                sdate = surg.get(int(rid))
                nd_final = nd_text or sdate
                nd_src = (
                    "text_header"
                    if nd_text
                    else ("synoptic_surg_date" if sdate else "none")
                )
                proc = PROC_LINE.search(text_str)
                sur = SURGEON_LINE.search(text_str)
                recs.append(
                    {
                        "note_row_id": make_note_row_id(int(rid), sheet_name, snake_col),
                        "research_id": int(rid),
                        "note_type": "op_note",
                        "note_index": note_index,
                        "note_date": nd_final,
                        "note_date_from_text": nd_text,
                        "note_date_source": nd_src,
                        "synoptic_surg_date_fallback": sdate,
                        "note_text": text_str,
                        "source_sheet": sheet_name,
                        "source_column": snake_col,
                        "source_file": str(notes_path.resolve()),
                        "surgeon": (sur.group(1).strip()[:500] if sur else None),
                        "procedure_description": (proc.group(1).strip()[:2000] if proc else None),
                        "char_count": len(text_str),
                        "extraction_method": "notes_column_map",
                    }
                )
    return recs


def heuristic_scan_sheet(
    path: Path,
    sheet_name: str,
    surgery_lu: pd.DataFrame,
    max_rows_sample: int,
) -> list[dict]:
    """Scan non-Notes workbooks for extra operative columns (conservative)."""
    surg = surgery_lu.set_index("research_id")["surg_date_iso"].to_dict()
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    df = standardize_columns(df)
    if "research_id" not in df.columns:
        return []
    df = clean_research_id(df)
    df = strip_phi(df)
    recs: list[dict] = []
    stem_tag = path.stem[:80]
    for col in df.columns:
        if col == "research_id":
            continue
        if OP_COL_NAME_HINT.search(col):
            pass
        else:
            if df[col].dtype != object:
                continue
            sample = df[col].dropna().head(max_rows_sample)
            if sample.empty:
                continue
            joined = "\n".join(str(x)[:400] for x in sample.head(8))
            if not OP_BODY_HINT.search(joined):
                continue
        for _, row in df.iterrows():
            rid = row.get("research_id")
            text = row.get(col)
            if pd.isna(rid) or pd.isna(text) or not str(text).strip():
                continue
            text_str = str(text).strip()
            if len(text_str) < 120:
                continue
            nd_text = extract_note_date(text_str, max_scan_chars=50_000)
            sdate = surg.get(int(rid))
            nd_final = nd_text or sdate
            nd_src = (
                "text_header"
                if nd_text
                else ("synoptic_surg_date" if sdate else "none")
            )
            src_sheet = f"{stem_tag}::{sheet_name}"
            recs.append(
                {
                    "note_row_id": make_note_row_id(int(rid), src_sheet, col),
                    "research_id": int(rid),
                    "note_type": "op_note",
                    "note_index": 1,
                    "note_date": nd_final,
                    "note_date_from_text": nd_text,
                    "note_date_source": nd_src,
                    "synoptic_surg_date_fallback": sdate,
                    "note_text": text_str,
                    "source_sheet": sheet_name,
                    "source_column": col,
                    "source_file": str(path.resolve()),
                    "surgeon": None,
                    "procedure_description": None,
                    "char_count": len(text_str),
                    "extraction_method": "heuristic_xlsx_scan",
                }
            )
    return recs


def diagnose_motherduck(con) -> None:
    print("\n=== local DuckDB: clinical_notes_long operative baseline ===\n")
    try:
        q = """
        SELECT
            COUNT(*) AS total_notes,
            COUNT(
                CASE WHEN TRY_CAST(note_date AS DATE) < DATE '2019-01-01' THEN 1 END
            ) AS pre_2019_with_note_date,
            COUNT(
                CASE WHEN note_date IS NULL OR TRIM(CAST(note_date AS VARCHAR)) = '' THEN 1 END
            ) AS null_note_date,
            MIN(TRY_CAST(note_date AS DATE)) AS earliest_note_date,
            MAX(TRY_CAST(note_date AS DATE)) AS latest_note_date
        FROM clinical_notes_long
        WHERE LOWER(CAST(note_type AS VARCHAR)) LIKE '%op%note%'
           OR LOWER(CAST(note_type AS VARCHAR)) = 'opnote'
        """
        print(con.execute(q).fetchdf().to_string(index=False))
    except Exception as exc:
        print(f"(skip) {exc}")
    try:
        print("\n=== SHOW TABLES LIKE '%note%' ===\n")
        rows = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE LOWER(table_name) LIKE '%note%' ORDER BY 1,2"
        ).fetchall()
        for r in rows[:80]:
            print(r)
        if len(rows) > 80:
            print(f"... ({len(rows)} total, truncated)")
    except Exception as exc:
        print(f"(skip) {exc}")


def validation_report(df: pd.DataFrame, baseline_pre2019: int | None) -> None:
    if df.empty:
        print("No operative records produced.")
        return
    df = df.copy()
    df["year"] = pd.to_datetime(df["note_date"], errors="coerce").dt.year
    print("\n=== Rows by year (note_date resolved) ===")
    vc = df["year"].value_counts().sort_index()
    for y in range(2010, 2027):
        if y in vc.index:
            print(f"  {y}: {int(vc[y]):,}")
    pre = (df["note_date"].notna()) & (df["note_date"] < "2019-01-01")
    post = (df["note_date"].notna()) & (df["note_date"] >= "2019-01-01")
    print(f"\nPre-2019 (resolved note_date): {pre.sum():,}")
    print(f"2019+  (resolved note_date): {post.sum():,}")
    print(f"NULL/empty note_date:        {df['note_date'].isna().sum():,}")
    print(f"Unique patients (pre-2019 resolved): {df.loc[pre, 'research_id'].nunique():,}")
    if baseline_pre2019 is not None:
        print(
            f"\nDelta vs local DuckDB pre_2019_with_note_date: "
            f"+{max(0, int(pre.sum()) - baseline_pre2019):,} "
            f"(baseline was {baseline_pre2019})"
        )
    print("\n=== Sample pre-2019 note_text (<=500 chars, PHI-safe slice) ===")
    rich = df.loc[pre & (df["char_count"] >= 400)]
    samples = rich["note_text"].head(8) if len(rich) else df.loc[pre, "note_text"].head(8)
    for i, t in enumerate(samples, 1):
        s = str(t)[:500].replace("\n", " ")
        print(f"--- {i} ---\n{s}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("Usage")[0])
    parser.add_argument(
        "--extra-root",
        action="append",
        default=[],
        help="Additional directory to glob **/*.xlsx (repeatable).",
    )
    parser.add_argument(
        "--md",
        action="store_true",
        help="Use local DuckDB token for optional publish/diagnose steps.",
    )
    parser.add_argument(
        "--diagnose-md",
        action="store_true",
        help="Print baseline counts from live clinical_notes_long.",
    )
    parser.add_argument(
        "--publish-md",
        action="store_true",
        help="CREATE OR REPLACE TABLE raw.operative_notes_full_history_v2 on local DuckDB.",
    )
    parser.add_argument(
        "--use-md-surgery-dates",
        action="store_true",
        help="Prefer path_synoptics.surg_date on local DuckDB for fallback (instead of local synoptic xlsx).",
    )
    parser.add_argument(
        "--heuristic-scan",
        action="store_true",
        help="Also scan all xlsx sheets for operative-like columns (slower).",
    )
    parser.add_argument(
        "--skip-local-synoptic",
        action="store_true",
        help="Do not read synoptic Excel for surgery dates (use MD or none).",
    )
    args = parser.parse_args()

    extra_paths = [Path(p).expanduser() for p in (args.extra_root or [])]
    raw_dir = ROOT / "raw"
    search_roots = [raw_dir, *extra_paths, ROOT]
    notes_path = _find_first(DEFAULT_NOTES_NAMES, search_roots)
    if notes_path is None:
        print("ERROR: Notes workbook not found. Pass --extra-root pointing at Active Master Files.")
        sys.exit(1)

    synoptic_path = None if args.skip_local_synoptic else _find_first(
        DEFAULT_SYNOPTIC_NAMES, search_roots
    )

    baseline_pre2019 = None
    md_con = None
    if args.md and (args.diagnose_md or args.publish_md or args.use_md_surgery_dates):
        from utils.md_connect import connect_md_or_file
        md_con = connect_md_or_file(ROOT / "thyroid_master.duckdb", md=True)

    if args.diagnose_md and md_con:
        diagnose_motherduck(md_con)
        try:
            baseline_pre2019 = int(
                md_con.execute(
                    """
                SELECT COUNT(*) FROM clinical_notes_long
                WHERE (LOWER(CAST(note_type AS VARCHAR)) LIKE '%op%note%'
                       OR LOWER(CAST(note_type AS VARCHAR)) = 'opnote')
                  AND TRY_CAST(note_date AS DATE) < DATE '2019-01-01'
                """
                ).fetchone()[0]
            )
        except Exception:
            baseline_pre2019 = None

    if args.use_md_surgery_dates and md_con:
        surgery_lu = load_surgery_lookup_motherduck(md_con)
        print(f"\nSurgery lookup rows (local DuckDB path_synoptics): {len(surgery_lu):,}")
    elif synoptic_path is not None:
        surgery_lu = load_surgery_lookup_from_synoptic(synoptic_path)
        print(f"\nSurgery lookup rows (local synoptic): {len(surgery_lu):,}")
    else:
        surgery_lu = pd.DataFrame(columns=["research_id", "surg_date_iso"])
        print("\nWARNING: No surgery lookup — note_date fallback will be text-only.")

    all_recs: list[dict] = mapped_notes_records(notes_path, surgery_lu)

    if args.heuristic_scan:
        skip_names = {notes_path.name.lower()}
        for p in _collect_xlsx_paths(raw_dir, extra_paths):
            if p.name.lower() in skip_names:
                continue
            try:
                xl = pd.ExcelFile(p, engine="openpyxl")
            except Exception as exc:
                print(f"  SKIP unreadable {p}: {exc}")
                continue
            for sheet in xl.sheet_names:
                try:
                    all_recs.extend(heuristic_scan_sheet(p, sheet, surgery_lu, max_rows_sample=25))
                except Exception as exc:
                    print(f"  SKIP sheet {p.name}::{sheet}: {exc}")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = pd.DataFrame(all_recs)
    if df.empty:
        print("No records — exiting")
        sys.exit(1)

    # Dedupe by note_row_id (heuristic could overlap mapped — mapped wins if same id)
    df = df.drop_duplicates(subset=["note_row_id"], keep="first")
    df["ingestion_timestamp"] = ts
    df["resolved_layer_version"] = VERSION_TAG

    exp_dir = ROOT / "exports" / f"operative_notes_full_history_{_utc_stamp()}"
    exp_dir.mkdir(parents=True, exist_ok=True)
    out_pq = exp_dir / "operative_notes_full_history_v2.parquet"
    df.to_parquet(out_pq, index=False)

    file_list = [{"path": str(p.resolve())} for p in _collect_xlsx_paths(raw_dir, extra_paths)]
    manifest = {
        "resolved_layer_version": VERSION_TAG,
        "notes_source": str(notes_path.resolve()),
        "synoptic_source": str(synoptic_path.resolve()) if synoptic_path else None,
        "row_count": len(df),
        "unique_patients": int(df["research_id"].nunique()),
        "xlsx_inventory": file_list,
    }
    (exp_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"\nWrote {out_pq}  ({len(df):,} rows)")
    validation_report(df, baseline_pre2019)

    if args.publish_md and md_con:
        try:
            md_con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        except Exception:
            pass
        safe_path = str(out_pq.resolve()).replace("'", "''")
        ddl = (
            f"CREATE OR REPLACE TABLE raw.operative_notes_full_history_v2 AS "
            f"SELECT * FROM read_parquet('{safe_path}')"
        )
        md_con.execute(ddl)
        n = md_con.execute(
            "SELECT COUNT(*) FROM raw.operative_notes_full_history_v2"
        ).fetchone()[0]
        print(f"\nlocal DuckDB raw.operative_notes_full_history_v2 rows: {n:,}")

    if md_con:
        md_con.close()


if __name__ == "__main__":
    main()
