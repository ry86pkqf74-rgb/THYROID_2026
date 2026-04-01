#!/usr/bin/env python3
"""Repair completed recurrence_detailed outputs in place without re-running extraction."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve()


def _resolve_root() -> Path:
    for candidate in [SCRIPT_PATH.parent, *SCRIPT_PATH.parents]:
        if (candidate / "scripts" / "vastai" / "run_extraction_concurrent.py").exists():
            return candidate
        if (candidate / "scripts" / "run_extraction_concurrent.py").exists():
            return candidate
    raise ImportError("Could not resolve project root for recurrence sanitization")


ROOT = _resolve_root()
sys.path.insert(0, str(ROOT))


def _load_sanitize_result():
    candidates = [
        ROOT / "scripts" / "vastai" / "run_extraction_concurrent.py",
        ROOT / "scripts" / "run_extraction_concurrent.py",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        spec = importlib.util.spec_from_file_location("run_extraction_concurrent_dynamic", candidate)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module._sanitize_result
    raise ImportError("Could not locate run_extraction_concurrent.py for recurrence sanitization")


_sanitize_result = _load_sanitize_result()


def _load_checkpoint_rows(ckpt_path: Path) -> list[dict]:
    rows: list[dict] = []
    with open(ckpt_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _write_checkpoint_rows(ckpt_path: Path, rows: list[dict]) -> None:
    tmp_path = ckpt_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    tmp_path.replace(ckpt_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sanitize completed recurrence_detailed outputs in place")
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--source-parquet", type=Path, required=True)
    args = parser.parse_args()

    source_df = pd.read_parquet(args.source_parquet, columns=["note_row_id", "note_date", "note_text"])
    source_df["note_row_id"] = source_df["note_row_id"].astype(str)
    source_lookup = source_df.set_index("note_row_id", drop=False)

    rows = _load_checkpoint_rows(args.checkpoint)
    entities_before = 0
    entities_after = 0
    rows_changed = 0

    for row in rows:
        note_row_id = str(row.get("note_row_id", ""))
        source_row = source_lookup.loc[note_row_id] if note_row_id in source_lookup.index else None
        if isinstance(source_row, pd.DataFrame):
            source_row = source_row.iloc[0]
        if source_row is None:
            continue

        try:
            payload = json.loads(row.get("result_json", "{}"))
        except json.JSONDecodeError:
            payload = {}

        entities_before += len(payload.get("entities", [])) if isinstance(payload.get("entities", []), list) else 0
        sanitized = _sanitize_result("recurrence_detailed", payload, source_row)
        entities_after += len(sanitized.get("entities", []))
        if sanitized != payload:
            rows_changed += 1
            row["result_json"] = json.dumps(sanitized)

    _write_checkpoint_rows(args.checkpoint, rows)
    pd.DataFrame(rows).to_parquet(args.parquet, index=False)

    summary = {
        "rows": len(rows),
        "rows_changed": rows_changed,
        "entities_before": entities_before,
        "entities_after": entities_after,
        "entities_removed": entities_before - entities_after,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
