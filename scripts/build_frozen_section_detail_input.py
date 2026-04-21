#!/usr/bin/env python3
"""
Build frozen_section_detail input parquet.

Sources (each leg of the UNION ALL):
  1. path_synoptics — 8 narrative/structured columns describing frozen section:
       frozen_section_obtained, fs_pathology_frozen_section,
       carcinoma_identified_on_fs_sent_intraop, synoptic_diagnosis,
       path_diagnosis_summary, path_diagnosis_comment,
       ancillary_studies, other_findings
     (note_date = surg_date)
  2. clinical_notes_long — op notes (intra-op frozen section is documented
     extensively in surgeon dictation) + H&P (pre-op FS plan):
       opnote_1..opnote_4, h_p_1, h_p_2
     (note_date = surg_date via LEFT JOIN path_synoptics on research_id)

All legs carry full provenance: research_id, note_date, source_workbook,
source_sheet, source_column, note_index, plus a deterministic md5 note_row_id.

Writes: processed/remaining/9domain_v4/input_frozen_section_detail.parquet
"""

from __future__ import annotations

import datetime
import os
import sys
import uuid
from pathlib import Path

import duckdb


DOMAIN = "frozen_section_detail"
SCRIPT_VERSION = "v4_9domain_rerun_2026-04-19"
BATCH_ID = str(uuid.uuid4())
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).isoformat()

OUT_DIR = Path("processed/remaining/9domain_v4")
OUT_PATH = OUT_DIR / f"input_{DOMAIN}.parquet"

# path_synoptics whole-record columns — all have direct FS signal.
PATH_SYNOPTICS_COLUMNS: list[str] = [
    "frozen_section_obtained",
    "fs_pathology_frozen_section",
    "carcinoma_identified_on_fs_sent_intraop",
    "synoptic_diagnosis",
    "path_diagnosis_summary",
    "path_diagnosis_comment",
    "ancillary_studies",
    "other_findings",
]

# clinical_notes_long source columns — FS documented in op note dictation.
NOTE_COLUMNS: list[str] = [
    "opnote_1", "opnote_2", "opnote_3", "opnote_4",
    "h_p_1", "h_p_2",
]

PATH_SYNOPTICS_WB = "All Diagnoses & synoptic 12_1_2025.xlsx"
PATH_SYNOPTICS_SHEET = "synoptics + Dx merged"
NOTES_WB = "Notes 12_1_25.xlsx"
NOTES_SHEET = "Sheet2"
MIN_TEXT_LEN = 20


def build_union_sql() -> str:
    legs: list[str] = []

    # path_synoptics legs
    for col in PATH_SYNOPTICS_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR          AS research_id,
                   surg_date::VARCHAR            AS note_date,
                   'path_synoptics'              AS note_type,
                   '{PATH_SYNOPTICS_WB}'         AS source_workbook,
                   '{PATH_SYNOPTICS_SHEET}'      AS source_sheet,
                   '{col}'                       AS source_column,
                   '0'                           AS note_index,
                   "{col}"::VARCHAR              AS note_text
              FROM path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}"::VARCHAR) >= {MIN_TEXT_LEN}
            """
        )

    # clinical_notes_long legs (JOIN path_synoptics for surg_date)
    for col in NOTE_COLUMNS:
        legs.append(
            f"""
            SELECT cnl.research_id::VARCHAR                  AS research_id,
                   ps.surg_date::VARCHAR                     AS note_date,
                   cnl.note_type                             AS note_type,
                   '{NOTES_WB}'                              AS source_workbook,
                   '{NOTES_SHEET}'                           AS source_sheet,
                   cnl.source_column                         AS source_column,
                   cnl.note_index::VARCHAR                   AS note_index,
                   cnl.note_text                             AS note_text
              FROM clinical_notes_long cnl
         LEFT JOIN path_synoptics ps
                ON cnl.research_id = ps.research_id
             WHERE cnl.source_column = '{col}'
               AND cnl.note_text IS NOT NULL
               AND LENGTH(cnl.note_text) >= {MIN_TEXT_LEN}
            """
        )

    return "\nUNION ALL\n".join(legs)


def main() -> int:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        print("ERROR: MOTHERDUCK_TOKEN not set. Source .env.motherduck first.", file=sys.stderr)
        return 2

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(
        f'md:thyroid_canonical_publication_v1_0?motherduck_token={os.environ["MOTHERDUCK_TOKEN"]}'
    )

    inner = build_union_sql()

    copy_sql = f"""
    COPY (
      WITH source_rows AS (
        {inner}
      )
      SELECT
        md5(CONCAT_WS('|', research_id, source_workbook, source_column,
                           COALESCE(note_date, ''), note_index, note_text))  AS note_row_id,
        research_id,
        note_text,
        note_type,
        note_index,
        note_date,
        source_workbook,
        source_sheet,
        source_column,
        '{BATCH_ID}'        AS preprocess_batch_id,
        '{NOW_UTC}'         AS preprocessed_at_utc,
        '{SCRIPT_VERSION}'  AS preprocess_script_version
      FROM source_rows
    ) TO '{OUT_PATH}' (FORMAT PARQUET);
    """
    con.execute(copy_sql)

    n_rows = con.execute(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
    n_patients = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM '{OUT_PATH}'").fetchone()[0]
    by_col = con.execute(
        f"SELECT source_workbook, source_column, COUNT(*) FROM '{OUT_PATH}' GROUP BY 1,2 ORDER BY 3 DESC"
    ).fetchall()

    print(f"\nWrote {OUT_PATH}")
    print(f"  domain:          {DOMAIN}")
    print(f"  script_version:  {SCRIPT_VERSION}")
    print(f"  batch_id:        {BATCH_ID}")
    print(f"  total rows:      {n_rows:,}")
    print(f"  unique patients: {n_patients:,}")
    print(f"  by source:")
    for wb, col, cnt in by_col:
        print(f"    [{wb[:30]:30s}] {col:55s} {cnt:>6,}")

    if n_rows < 5_000:
        print("\nWARNING: row count below 5,000.", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
