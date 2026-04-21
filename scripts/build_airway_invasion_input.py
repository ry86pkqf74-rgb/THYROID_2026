#!/usr/bin/env python3
"""
Build airway_invasion input parquet.

Airway / tracheal / vascular-encasement findings live primarily in imaging
narratives (CT, MRI) and surgical dictation. The pathology synoptic contains
only indirect signal via ETE / tumor-histology comments. The column set
below reflects that reality — ~70% of input rows come from ct_imaging /
mri_imaging, ~15% from op notes, ~15% from path_synoptics narrative.

Sources:
  1. ct_imaging (11 narrative cols: original_report, airway_tracheal_findings,
     airway_compromise_comment, thyroid_findings, thyroid_surgical_details,
     thyroid_details, tracheal_narrowing, tracheal_deviation,
     substernal_extension, lymph_node_findings, lymph_node_details)
     note_date = date_of_exam
  2. mri_imaging (6 narrative cols: original_report, thyroid_details,
     vocal_cords_details, lymph_node_details, parathyroid_details,
     substernal_extension)
     note_date = date_of_exam
  3. clinical_notes_long op notes (opnote_1..4) — surgeon airway findings
     note_date = surg_date via LEFT JOIN path_synoptics
  4. path_synoptics ETE narrative (microscopic_description, synoptic_diagnosis,
     tumor_1/2/3_histology_comment, tumor_1_margin_angiolymphatic_invasion_comment,
     tumor_2..5_margin_comment, other_findings)
     note_date = surg_date

Writes: processed/remaining/9domain_v4/input_airway_invasion.parquet
"""

from __future__ import annotations

import datetime
import os
import sys
import uuid
from pathlib import Path

import duckdb


DOMAIN = "airway_invasion"
SCRIPT_VERSION = "v4_9domain_rerun_2026-04-19"
BATCH_ID = str(uuid.uuid4())
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).isoformat()

OUT_DIR = Path("processed/remaining/9domain_v4")
OUT_PATH = OUT_DIR / f"input_{DOMAIN}.parquet"

CT_COLUMNS: list[str] = [
    "original_report",
    "airway_tracheal_findings",
    "airway_compromise_comment",
    "thyroid_findings",
    "thyroid_surgical_details",
    "thyroid_details",
    "tracheal_narrowing",
    "tracheal_deviation",
    "substernal_extension",
    "lymph_node_findings",
    "lymph_node_details",
]

MRI_COLUMNS: list[str] = [
    "original_report",
    "thyroid_details",
    "vocal_cords_details",
    "lymph_node_details",
    "parathyroid_details",
    "substernal_extension",
]

NOTE_COLUMNS: list[str] = [
    "opnote_1", "opnote_2", "opnote_3", "opnote_4",
]

# path_synoptics: narrative cols with ETE / airway-invasion signal.
PATH_SYNOPTICS_WHOLE: list[str] = [
    "microscopic_description",
    "synoptic_diagnosis",
    "path_diagnosis_summary",
    "path_diagnosis_comment",
    "other_findings",
]

# Per-tumor histology comments (tumors 1–3 only).
PATH_TUMOR_HIST_COMMENT: list[tuple[str, str]] = [
    ("tumor_1_histology_comment", "1"),
    ("tumor_2_histology_comment", "2"),
    ("tumor_3_histology_comment", "3"),
]

# Per-tumor margin comments (hybrid col for tumor 1).
PATH_TUMOR_MARGIN_COMMENT: list[tuple[str, str]] = [
    ("tumor_1_margin_angiolymphatic_invasion_comment", "1"),
    ("tumor_2_margin_comment", "2"),
    ("tumor_3_margin_comment", "3"),
    ("tumor_4_margin_comment", "4"),
    ("tumor_5_margin_comment", "5"),
]

CT_WB = "Imaging_12_1_25.xlsx"
CT_SHEET = "CT"
MRI_WB = "Imaging_12_1_25.xlsx"
MRI_SHEET = "MRI"
PATH_WB = "All Diagnoses & synoptic 12_1_2025.xlsx"
PATH_SHEET = "synoptics + Dx merged"
NOTES_WB = "Notes 12_1_25.xlsx"
NOTES_SHEET = "Sheet2"
MIN_TEXT_LEN = 20


def build_union_sql() -> str:
    legs: list[str] = []

    # ct_imaging legs
    for col in CT_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR            AS research_id,
                   date_of_exam::VARCHAR           AS note_date,
                   'ct_imaging'                    AS note_type,
                   '{CT_WB}'                       AS source_workbook,
                   '{CT_SHEET}'                    AS source_sheet,
                   '{col}'                         AS source_column,
                   COALESCE(ct_column, '0')::VARCHAR AS note_index,
                   "{col}"::VARCHAR                AS note_text
              FROM ct_imaging
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}"::VARCHAR) >= {MIN_TEXT_LEN}
            """
        )

    # mri_imaging legs (no ct_column equivalent — use mri_label as note_index)
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

    # clinical_notes_long op notes, surg_date via JOIN
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

    # path_synoptics whole-record legs
    for col in PATH_SYNOPTICS_WHOLE:
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

    # path_synoptics per-tumor histology + margin comments
    for col, ordinal in PATH_TUMOR_HIST_COMMENT + PATH_TUMOR_MARGIN_COMMENT:
        legs.append(
            f"""
            SELECT research_id::VARCHAR     AS research_id,
                   surg_date::VARCHAR       AS note_date,
                   'path_synoptics'         AS note_type,
                   '{PATH_WB}'              AS source_workbook,
                   '{PATH_SHEET}'           AS source_sheet,
                   '{col}'                  AS source_column,
                   '{ordinal}'              AS note_index,
                   "{col}"::VARCHAR         AS note_text
              FROM path_synoptics
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
