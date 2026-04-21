#!/usr/bin/env python3
"""
Build input parquets for the 2026-04-21 RunPod extraction round.

Produces two parquets:

  1. processed/remaining/round2_20260421/input_clinical_notes_long.parquet
       — All notes from main.clinical_notes_long, used by the three
         qwen2.5-32b re-extractions (pathology, cervical_ln_detail,
         tirads_granular). Matches the historical 11,037-row corpus shape
         used by the qwen3:32b runs (same source_workbook / source_sheet /
         source_column semantics).

  2. processed/remaining/round2_20260421/input_opnotes_only.parquet
       — OPNOTE rows only, used by the new esophageal_invasion extraction.

Output columns match what scripts/vastai/run_extraction_concurrent.py
expects: note_row_id (deterministic md5), research_id, note_text, note_type,
note_index, note_date (null — clinical_notes_long does not carry it),
source_workbook, source_sheet, source_column, preprocess_batch_id,
preprocessed_at_utc, preprocess_script_version.

Auth: motherduck_client.get_token() (repo-root module; do NOT use
scripts/_md_connect.py, which is a stub).
"""

from __future__ import annotations

import datetime
import os
import sys
import uuid
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import motherduck_client  # noqa: E402

SCRIPT_VERSION = "round2_20260421_v1"
BATCH_ID = str(uuid.uuid4())
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).isoformat()

OUT_DIR = ROOT / "processed" / "remaining" / "round2_20260421"
OUT_ALL = OUT_DIR / "input_clinical_notes_long.parquet"
OUT_OP = OUT_DIR / "input_opnotes_only.parquet"

# Minimum note length (chars). Match existing round behavior; most historical
# extractions filtered note_text LENGTH > 20 to drop stubs.
MIN_TEXT_LEN = 20


def _base_select_sql(where_extra: str) -> str:
    """Return a SELECT producing the run_extraction_concurrent.py input schema."""
    return f"""
        SELECT
            md5(
                CONCAT_WS(
                    '|',
                    research_id,
                    source_workbook,
                    source_sheet,
                    source_column,
                    CAST(note_index AS VARCHAR)
                )
            )                                                AS note_row_id,
            research_id::VARCHAR                             AS research_id,
            note_text                                        AS note_text,
            note_type                                        AS note_type,
            CAST(note_index AS VARCHAR)                      AS note_index,
            NULL::VARCHAR                                    AS note_date,
            source_workbook                                  AS source_workbook,
            source_sheet                                     AS source_sheet,
            source_column                                    AS source_column,
            '{BATCH_ID}'                                     AS preprocess_batch_id,
            '{NOW_UTC}'                                      AS preprocessed_at_utc,
            '{SCRIPT_VERSION}'                               AS preprocess_script_version
          FROM thyroid_pub.main.clinical_notes_long
         WHERE note_text IS NOT NULL
           AND LENGTH(note_text) > {MIN_TEXT_LEN}
           {where_extra}
    """


def main() -> int:
    token = motherduck_client.get_token()
    os.environ["motherduck_token"] = token
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL motherduck;")
    con.execute("LOAD motherduck;")
    con.execute("ATTACH 'md:thyroid_canonical_publication_v1_0' AS thyroid_pub;")

    # ---------- 1. All notes (pathology + cervical_ln_detail + tirads_granular) ----------
    con.execute(
        f"COPY ({_base_select_sql('')}) TO '{OUT_ALL}' (FORMAT PARQUET);"
    )
    n_all = con.execute(f"SELECT COUNT(*) FROM '{OUT_ALL}'").fetchone()[0]
    rids_all = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM '{OUT_ALL}'"
    ).fetchone()[0]
    dup_all = con.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT note_row_id FROM '{OUT_ALL}' GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    print(f"\nWrote {OUT_ALL}")
    print(f"  rows          : {n_all:,}")
    print(f"  unique rids   : {rids_all:,}")
    print(f"  dup row_ids   : {dup_all}")
    print("  by note_type:")
    for nt, n in con.execute(
        f"SELECT note_type, COUNT(*) FROM '{OUT_ALL}' GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall():
        print(f"    {nt:20s} {n:>6,}")

    # ---------- 2. OPNOTE-only (esophageal_invasion) ----------
    op_where = "AND note_type = 'OPNOTE'"
    con.execute(
        f"COPY ({_base_select_sql(op_where)}) TO '{OUT_OP}' (FORMAT PARQUET);"
    )
    n_op = con.execute(f"SELECT COUNT(*) FROM '{OUT_OP}'").fetchone()[0]
    rids_op = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM '{OUT_OP}'"
    ).fetchone()[0]
    dup_op = con.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT note_row_id FROM '{OUT_OP}' GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    print(f"\nWrote {OUT_OP}")
    print(f"  rows          : {n_op:,}")
    print(f"  unique rids   : {rids_op:,}")
    print(f"  dup row_ids   : {dup_op}")

    # Sanity-check guardrails
    if n_all < 10_000:
        print("\nWARNING: combined input row count below 10,000", file=sys.stderr)
        return 3
    if n_op < 4_000:
        print("\nWARNING: OPNOTE row count below 4,000", file=sys.stderr)
        return 3
    if dup_all > 0 or dup_op > 0:
        print("\nERROR: duplicate note_row_id values emitted", file=sys.stderr)
        return 4

    print(f"\nscript_version: {SCRIPT_VERSION}")
    print(f"batch_id:       {BATCH_ID}")
    print(f"preprocessed:   {NOW_UTC}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
