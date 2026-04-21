#!/usr/bin/env python3
"""
Build parathyroid_detail input parquet.

Prompt entity types: gland_location, gland_cellularity, gland_weight,
gland_size, removal_intent, autotransplant, reimplantation_detail,
gland_count_total, gland_count_preserved, hypercellularity_grade,
parathyroid_frozen_section. Each gland is a SEPARATE entity.

Parathyroid identification / preservation / autotransplant is documented
extensively in op-note dictation (intra-operative event). The path_synoptics
synoptic has a small structured parathyroid section (5 columns) but richer
narrative lives in synoptic_diagnosis / path_diagnosis_comment / other_findings
when a parathyroid is incidentally resected. MRI rarely describes parathyroid
but has a dedicated column for the small fraction that does.

Sources:
  1. clinical_notes_long op notes (opnote_1..4, h_p_1, h_p_2 — pre-op
     hyperparathyroidism workup also matters).
     note_date = surg_date via LEFT JOIN path_synoptics on research_id.
  2. path_synoptics structured parathyroid columns (5):
       parathyroid_operation, parathyroid_glands,
       location_of_parathyroid_glands, parathyroid_gland_findings,
       parathyroid_gland_or_tissue_included_in_resected_specimen
  3. path_synoptics narrative cols that mention parathyroid (3):
       synoptic_diagnosis, path_diagnosis_comment, other_findings
  4. mri_imaging (2 cols where parathyroid is described):
       parathyroid_details, original_report
     note_date = date_of_exam

Writes: processed/remaining/9domain_v4/input_parathyroid_detail.parquet
"""

from __future__ import annotations

import datetime
import os
import sys
import uuid
from pathlib import Path

import duckdb


DOMAIN = "parathyroid_detail"
SCRIPT_VERSION = "v4_9domain_rerun_2026-04-19"
BATCH_ID = str(uuid.uuid4())
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).isoformat()

OUT_DIR = Path("processed/remaining/9domain_v4")
OUT_PATH = OUT_DIR / f"input_{DOMAIN}.parquet"

NOTE_COLUMNS: list[str] = [
    "opnote_1", "opnote_2", "opnote_3", "opnote_4",
    "h_p_1", "h_p_2",
]

PATH_STRUCTURED_PARATHYROID: list[str] = [
    "parathyroid_operation",
    "parathyroid_glands",
    "location_of_parathyroid_glands",
    "parathyroid_gland_findings",
    "parathyroid_gland_or_tissue_included_in_resected_specimen",
]

PATH_NARRATIVE: list[str] = [
    "synoptic_diagnosis",
    "path_diagnosis_comment",
    "other_findings",
]

MRI_COLUMNS: list[str] = [
    "parathyroid_details",
    "original_report",
]

NOTES_WB = "Notes 12_1_25.xlsx"
NOTES_SHEET = "Sheet2"
PATH_WB = "All Diagnoses & synoptic 12_1_2025.xlsx"
PATH_SHEET = "synoptics + Dx merged"
MRI_WB = "Imaging_12_1_25.xlsx"
MRI_SHEET = "MRI"
MIN_TEXT_LEN = 20


def build_union_sql() -> str:
    legs: list[str] = []

    # clinical_notes_long op notes + H&P
    for col in NOTE_COLUMNS:
        legs.append(
            f"""
            SELECT cnl.research_id::VARCHAR      AS research_id,
                   ps.surg_date::VARCHAR         AS note_date,
                   cnl.note_type                 AS note_type,
                   '{NOTES_WB}'                  AS source_workbook,
                   '{NOTES_SHEET}'               AS source_sheet,
                   cnl.source_column             AS source_column,
                   cnl.note_index::VARCHAR       AS note_index,
                   cnl.note_text                 AS note_text
              FROM clinical_notes_long cnl
         LEFT JOIN path_synoptics ps
                ON cnl.research_id = ps.research_id
             WHERE cnl.source_column = '{col}'
               AND cnl.note_text IS NOT NULL
               AND LENGTH(cnl.note_text) >= {MIN_TEXT_LEN}
            """
        )

    # path_synoptics structured parathyroid cols
    for col in PATH_STRUCTURED_PARATHYROID:
        legs.append(
            f"""
            SELECT research_id::VARCHAR     AS research_id,
                   surg_date::VARCHAR       AS note_date,
                   'path_synoptics'         AS note_type,
                   '{PATH_WB}'              AS source_workbook,
                   '{PATH_SHEET}'           AS source_sheet,
                   '{col}'                  AS source_column,
                   '0'                      AS note_index,
                   "{col}"::VARCHAR         AS note_text
              FROM path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}"::VARCHAR) >= {MIN_TEXT_LEN}
            """
        )

    # path_synoptics narrative catch-all
    for col in PATH_NARRATIVE:
        legs.append(
            f"""
            SELECT research_id::VARCHAR     AS research_id,
                   surg_date::VARCHAR       AS note_date,
                   'path_synoptics'         AS note_type,
                   '{PATH_WB}'              AS source_workbook,
                   '{PATH_SHEET}'           AS source_sheet,
                   '{col}'                  AS source_column,
                   '0'                      AS note_index,
                   "{col}"::VARCHAR         AS note_text
              FROM path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}"::VARCHAR) >= {MIN_TEXT_LEN}
            """
        )

    # mri_imaging parathyroid-describing cols
    for col in MRI_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR            AS research_id,
                   date_of_exam::VARCHAR           AS note_date,
                   'mri_imaging'                   AS note_type,
                   '{MRI_WB}'                      AS source_workbook,
                   '{MRI_SHEET}'                   AS source_sheet,
                   '{col}'                         AS source_column,
                   COALESCE(mri_label, '0')::VARCHAR AS note_index,
                   "{col}"::VARCHAR                AS note_text
              FROM mri_imaging
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}"::VARCHAR) >= {MIN_TEXT_LEN}
            """
        )

    return "\nUNION ALL\n".join(legs)


def main() -> int:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        print("ERROR: MOTHERDUCK_TOKEN not set.", file=sys.stderr)
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
    by_source = con.execute(
        f"SELECT note_type, source_column, COUNT(*) FROM '{OUT_PATH}' GROUP BY 1,2 ORDER BY 3 DESC"
    ).fetchall()

    print(f"\nWrote {OUT_PATH}")
    print(f"  domain:          {DOMAIN}")
    print(f"  script_version:  {SCRIPT_VERSION}")
    print(f"  batch_id:        {BATCH_ID}")
    print(f"  total rows:      {n_rows:,}")
    print(f"  unique patients: {n_patients:,}")
    print(f"  by source:")
    for nt, col, cnt in by_source:
        print(f"    [{nt:20s}] {col:55s} {cnt:>6,}")

    if n_rows < 5_000:
        print("\nWARNING: row count below 5,000.", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
