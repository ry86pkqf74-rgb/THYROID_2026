#!/usr/bin/env python3
"""
Scan primary Excel workbooks under raw/ for ATA / American Thyroid Association text,
then optionally rebuild thyroid scoring (including ATA 2015 initial risk) on local DuckDB via 51b.

Usage:
  .venv/bin/python scripts/112_ata_primary_source_scan_and_scoring.py
  .venv/bin/python scripts/112_ata_primary_source_scan_and_scoring.py --md

Requires LOCAL_DB_PATH or LOCAL_DB_PATH in the environment when using --md.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Primary raw/*.xlsx used by the lakehouse: union of
#   scripts/01_ingest_all_files.py FILE_MAP (each filename once),
#   scripts/09_local DuckDB_upload_verify_extract.py RAW_XLSX_SOURCES,
#   scripts/07_phase3_genetics_specimen.py (THYROSEQ_AFIRMA_12_5.xlsx),
#   scripts/41_ingest_thyroseq_excel.py default workbook + Thyroseq crosswalk inputs,
#   studies/nsqip_linkage (NSQIP + case-details spreadsheets when kept under raw/).
# Keep filenames identical to on-disk names (including spacing and typos such as
# "THyroid" / "Frozen sectin") so scans match ingest expectations.
PRIMARY_EXCEL = sorted(
    {
        # 01_ingest_all_files.FILE_MAP
        "THyroid Sizes, Stanardized_12_2_25.xlsx",
        "Nuclear_Med_final.xlsx",
        "FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx",
        "FINAL_UPDATE_BenignPath_12_8_WithText.xlsx",
        "Thyroid_Weight_Data_12_2_25.xlsx",
        "anti_thyroglobulin_antibody_wide_by_research_id_split.xlsx",
        "thyroglobulin_wide_by_research_id_split.xlsx",
        "FNAs_Rescored_Long_Format.xlsx",
        "Frozen sectin parsed.xlsx",
        "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx",
        "CT_thyroid_extraction_FINAL_11_20_25.xlsx",
        "mri_extraction__FINAL_11_20_25.xlsx",
        "parathyroid_notes_intent.xlsx",
        "FNAs 12_5_2025.xlsx",
        "US Nodules TIRADS 12_1_25.xlsx",
        "All Diagnoses & synoptic 12_1_2025.xlsx",
        "Imaging_12_1_25.xlsx",
        "Notes 12_1_25.xlsx",
        # 09_local DuckDB_upload_verify_extract.RAW_XLSX_SOURCES, 07 genetics, 41 Thyroseq
        "Thyroid all_Complications 12_1_25.xlsx",
        "THYROSEQ_AFIRMA_12_5.xlsx",
        "Thyroid OP Sheet data.xlsx",
        "Thyroseq Data Complete.xlsx",
        "genetic_testing_TumorPath_update_Final_Cleaned 12_11.xlsx",
        # NSQIP / custom export sometimes placed in raw/
        "Thyroid NSQIP dataset 2010-2023.xlsx",
        "Case_Details_and_Custom_Fields_Report-14-Dec-2025-1204.xlsx",
    }
)

# Word-safe ATA / American Thyroid mentions (minimize noise from unrelated “ata” substrings)
ATA_RE = re.compile(
    r"(?is)"
    r"(American\s+Thyroid(?:\s+Association)?|"
    r"\bATA\s*[-–]?\s*201[0-9]\b|"
    r"\bATA\s+(risk|guideline|recommendation|criteria|class|low|intermediate|high)\b|"
    r"\bper\s+ATA\b|"
    r"\baccording\s+to\s+(the\s+)?ATA\b|"
    r"\bATA\s+category\b|"
    r"\binitial\s+ATA\s+risk\b|"
    r"\bATA\s+response\b|"
    r"(?<![A-Za-z])ATA(?![A-Za-z]))"
)


def _research_id_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        s = str(c).strip().lower().replace("#", "")
        if "research" in s and "id" in s:
            return c
    return None


def _normalize_rid(val: object) -> int | str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int,)):
        return int(val)
    t = str(val).strip()
    if not t:
        return None
    try:
        return int(float(t))
    except ValueError:
        return t


def scan_workbook(path: Path, max_chars: int = 8000) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:  # noqa: BLE001
        return [{"file": path.name, "sheet": "__open_error__", "error": str(exc)}]

    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl", dtype=object)
        except Exception as exc:  # noqa: BLE001
            out.append({"file": path.name, "sheet": sheet, "error": str(exc)})
            continue
        if df.empty:
            continue
        rid_col = _research_id_column(df)
        for col in df.columns:
            ser = df[col]
            for idx, val in ser.items():
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                text = str(val)
                if len(text) > max_chars:
                    text = text[:max_chars]
                if not ATA_RE.search(text):
                    continue
                one_line = " ".join(text.split())[:240]
                rid: int | str | None = None
                if rid_col is not None:
                    try:
                        rid = _normalize_rid(df.loc[idx, rid_col])
                    except Exception:  # noqa: BLE001
                        rid = None
                out.append(
                    {
                        "file": path.name,
                        "sheet": sheet,
                        "row_index": int(idx) if not isinstance(idx, tuple) else str(idx),
                        "column": str(col),
                        "research_id": rid,
                        "snippet": one_line,
                    }
                )
    return out


def run_excel_scan(raw_dir: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_dir = ROOT / "exports" / f"ata_primary_source_scan_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, object]] = []
    for name in PRIMARY_EXCEL:
        p = raw_dir / name
        if not p.is_file():
            print(f"skip missing: {name}", flush=True)
            continue
        print(f"scanning {name} ...", flush=True)
        all_rows.extend(scan_workbook(p))

    errors = [r for r in all_rows if "error" in r]
    hits = [r for r in all_rows if "snippet" in r]

    if hits:
        pd.DataFrame(hits).to_csv(out_dir / "ata_excel_mentions_detail.csv", index=False)
        sm = (
            pd.DataFrame(hits)
            .groupby(["file", "sheet"], as_index=False)
            .size()
            .rename(columns={"size": "n_cell_hits"})
        )
        sm.to_csv(out_dir / "ata_excel_mentions_by_sheet.csv", index=False)
    if errors:
        pd.DataFrame(errors).to_csv(out_dir / "ata_scan_errors.csv", index=False)

    manifest = {
        "script": "112_ata_primary_source_scan_and_scoring.py",
        "utc": datetime.now(timezone.utc).isoformat(),
        "raw_dir": str(raw_dir),
        "n_workbooks_scanned": sum(1 for n in PRIMARY_EXCEL if (raw_dir / n).is_file()),
        "n_cell_hits": len(hits),
        "n_errors": len(errors),
        "out_dir": str(out_dir),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Excel scan: {len(hits)} cell hits -> {out_dir}", flush=True)
    return out_dir


def run_local DuckDB_scoring() -> int:
    from local DuckDB_client import get_token

    if not get_token():
        print(
            "No local DuckDB token. Export LOCAL_DB_PATH or LOCAL_DB_PATH, then re-run with --md.",
            file=sys.stderr,
        )
        return 2
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "51b_thyroid_scoring_python.py"), "--md"],
        cwd=str(ROOT),
        env=env,
    )
    return int(proc.returncode)


def main() -> None:
    ap = argparse.ArgumentParser(description="ATA Excel scan + optional local DuckDB scoring (51b).")
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=ROOT / "raw",
        help="Directory containing primary Excel files (default: raw/)",
    )
    ap.add_argument(
        "--skip-excel",
        action="store_true",
        help="Only run local DuckDB scoring (51b), skip Excel scan.",
    )
    ap.add_argument(
        "--md",
        action="store_true",
        help="After Excel scan, run scripts/51b_thyroid_scoring_python.py --md",
    )
    args = ap.parse_args()

    if not args.skip_excel:
        run_excel_scan(args.raw_dir.resolve())

    if args.md:
        code = run_local DuckDB_scoring()
        raise SystemExit(code)


if __name__ == "__main__":
    main()
