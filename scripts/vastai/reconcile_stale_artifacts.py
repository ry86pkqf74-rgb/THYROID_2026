#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def load_jsonl_loose(path: Path) -> tuple[pd.DataFrame, list[tuple[int, str, str]]]:
    records = []
    parse_errors = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception as exc:
                parse_errors.append((line_number, str(exc), line[:200]))
    return pd.DataFrame(records), parse_errors


def normalize_result_json(value: str) -> str:
    text = "" if value is None else str(value).strip()
    if text in {"", "{}"}:
        return '{"entities": []}'
    return text


def enrich_lineage(df: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    keep = ["note_row_id", "source_workbook", "source_sheet", "source_column"]
    return df.drop(
        columns=[column for column in ["source_workbook", "source_sheet", "source_column"] if column in df.columns],
        errors="ignore",
    ).merge(source[keep], on="note_row_id", how="left")


def write_outputs(df: pd.DataFrame, stem: str, output_dir: Path) -> None:
    df.to_parquet(output_dir / f"{stem}.parquet", index=False)
    with (output_dir / f"{stem}.jsonl").open("w", encoding="utf-8") as handle:
        for record in df.to_dict(orient="records"):
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile stale staging/complications artifacts into comparison-only outputs")
    parser.add_argument("--source-parquet", type=Path, required=True)
    parser.add_argument("--staging-jsonl", type=Path, required=True)
    parser.add_argument("--complications-jsonl", type=Path, required=True)
    parser.add_argument("--complications-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    source = pd.read_parquet(
        args.source_parquet,
        columns=["note_row_id", "source_workbook", "source_sheet", "source_column"],
    )
    source["note_row_id"] = source["note_row_id"].astype(str)

    staging_partial, staging_errors = load_jsonl_loose(args.staging_jsonl)
    staging_partial["note_row_id"] = staging_partial["note_row_id"].astype(str)
    staging_partial = enrich_lineage(staging_partial, source)
    write_outputs(staging_partial, "note_entities_llm_staging_partial_clean_for_comparison", args.output_dir)

    complications_ckpt, complications_errors = load_jsonl_loose(args.complications_jsonl)
    complications_parquet = pd.read_parquet(args.complications_parquet)
    complications_parquet["note_row_id"] = complications_parquet["note_row_id"].astype(str)
    complications_parquet["result_json"] = complications_parquet["result_json"].map(normalize_result_json)
    complications_parquet["result_json_len"] = complications_parquet["result_json"].str.len()
    complications_parquet["extracted_at"] = complications_parquet["extracted_at"].fillna("")
    complications_repaired = (
        complications_parquet.sort_values(
            ["note_row_id", "result_json_len", "extracted_at"],
            ascending=[True, False, False],
        )
        .drop_duplicates(subset=["note_row_id"], keep="first")
        .drop(columns=["result_json_len"])
    )
    complications_repaired = enrich_lineage(complications_repaired, source)
    write_outputs(complications_repaired, "note_entities_llm_complications_repaired_for_comparison", args.output_dir)

    normalized_groups = complications_parquet.groupby("note_row_id")["result_json"].nunique().reset_index(name="normalized_result_json_nunique")
    conflict_count = int((normalized_groups["normalized_result_json_nunique"] > 1).sum())

    summary = {
        "staging_partial_rows": int(len(staging_partial)),
        "staging_partial_parse_errors": int(len(staging_errors)),
        "staging_partial_unique_note_row_id": int(staging_partial["note_row_id"].nunique()),
        "complications_original_rows_parseable": int(len(complications_ckpt)),
        "complications_parse_errors": int(len(complications_errors)),
        "complications_parquet_rows": int(len(complications_parquet)),
        "complications_parquet_unique_note_row_id": int(complications_parquet["note_row_id"].nunique()),
        "complications_repaired_rows": int(len(complications_repaired)),
        "complications_repaired_unique_note_row_id": int(complications_repaired["note_row_id"].nunique()),
        "complications_normalized_conflict_note_ids": conflict_count,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())