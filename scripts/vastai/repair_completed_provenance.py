#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


DEFAULT_SOURCE_WORKBOOK = "Notes 12_1_25.xlsx"
STRICT_FIELDS = [
    "research_id",
    "note_type",
    "linkage_date",
    "source_workbook",
    "source_sheet",
    "source_column",
    "note_index",
]


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "nat", "none"}:
        return ""
    return text


def _load_source_notes(path: Path, default_source_workbook: str, fallback_linkage_date: str) -> pd.DataFrame:
    source = pd.read_parquet(
        path,
        columns=[
            "note_row_id",
            "research_id",
            "note_type",
            "note_index",
            "note_date",
            "source_sheet",
            "source_column",
        ],
    ).copy()
    source["note_row_id"] = source["note_row_id"].astype(str)
    source["research_id"] = source["research_id"].map(_clean_text)
    source["note_type"] = source["note_type"].map(_clean_text)
    source["note_index"] = source["note_index"].map(_clean_text)
    source["note_date"] = source["note_date"].map(_clean_text)
    source["source_sheet"] = source["source_sheet"].map(_clean_text)
    source["source_column"] = source["source_column"].map(_clean_text)
    source["source_workbook"] = default_source_workbook
    source["linkage_date"] = source["note_date"]
    mask = source["linkage_date"].eq("")
    if fallback_linkage_date:
        source.loc[mask, "linkage_date"] = fallback_linkage_date
    return source.drop_duplicates(subset=["note_row_id"], keep="first")


def _fill_from_source(df: pd.DataFrame, field: str) -> pd.DataFrame:
    src_field = f"{field}__src"
    if field not in df.columns:
        df[field] = ""
    df[field] = df[field].map(_clean_text)
    if src_field in df.columns:
        mask = df[field].eq("")
        df.loc[mask, field] = df.loc[mask, src_field].map(_clean_text)
    return df


def _repair_artifact(path: Path, source: pd.DataFrame, backup_dir: Path | None, dry_run: bool) -> dict[str, object]:
    df = pd.read_parquet(path).copy()
    original_columns = list(df.columns)
    original_rows = len(df)
    original_unique = int(df["note_row_id"].astype(str).nunique())
    df["note_row_id"] = df["note_row_id"].astype(str)

    merged = df.merge(source, on="note_row_id", how="left", suffixes=("", "__src"), validate="many_to_one")

    for field in [
        "research_id",
        "note_type",
        "note_date",
        "linkage_date",
        "source_workbook",
        "source_sheet",
        "source_column",
        "note_index",
    ]:
        merged = _fill_from_source(merged, field)

    strict_missing = {
        field: int(merged[field].map(_clean_text).eq("").sum()) for field in STRICT_FIELDS
    }
    unresolved = {field: count for field, count in strict_missing.items() if count > 0}
    if unresolved:
        raise ValueError(f"{path.name} still has unresolved provenance fields: {json.dumps(unresolved, sort_keys=True)}")

    final_columns = list(original_columns)
    for field in ["linkage_date", "source_workbook", "source_sheet", "source_column", "note_index"]:
        if field not in final_columns:
            final_columns.append(field)
    repaired = merged[final_columns].copy()

    if len(repaired) != original_rows:
        raise ValueError(f"{path.name} row count changed unexpectedly: {original_rows} -> {len(repaired)}")
    repaired_unique = int(repaired["note_row_id"].astype(str).nunique())
    if repaired_unique != original_unique:
        raise ValueError(f"{path.name} unique note count changed unexpectedly: {original_unique} -> {repaired_unique}")

    if not dry_run:
        if backup_dir is not None:
            backup_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup_dir / path.name)
        repaired.to_parquet(path, index=False)

    return {
        "file": path.name,
        "rows": original_rows,
        "unique_note_row_id": repaired_unique,
        "repaired_fields": [field for field in STRICT_FIELDS if strict_missing[field] == 0],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing provenance fields in completed local V2 parquet artifacts")
    parser.add_argument("--source-parquet", type=Path, default=Path("processed/clinical_notes_long.parquet"))
    parser.add_argument("--artifacts-dir", type=Path, default=Path("output/v2_parquets"))
    parser.add_argument("--files", nargs="*", default=[])
    parser.add_argument("--default-source-workbook", default=DEFAULT_SOURCE_WORKBOOK)
    parser.add_argument(
        "--fallback-linkage-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Used only when note_date is blank; defaults to today's UTC date",
    )
    parser.add_argument("--backup-dir", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    source = _load_source_notes(
        args.source_parquet,
        default_source_workbook=args.default_source_workbook,
        fallback_linkage_date=args.fallback_linkage_date,
    )

    targets = [args.artifacts_dir / name for name in args.files] if args.files else sorted(args.artifacts_dir.glob("note_entities_llm*.parquet"))
    if not targets:
        raise SystemExit("No target parquet artifacts found")

    summaries = []
    for path in targets:
        summaries.append(_repair_artifact(path, source, args.backup_dir, args.dry_run))

    print(json.dumps({"artifacts_repaired": summaries}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())