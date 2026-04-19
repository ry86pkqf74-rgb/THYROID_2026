#!/usr/bin/env python3
"""
Build synoptic_pathology_enrichment input parquet from path_synoptics.

v4 (2026-04-18) — 9-domain rerun trial. Column set is 17 columns selected by
relevance to the synoptic_pathology_enrichment_extraction_v1 prompt's entity
types (Ki-67, mitoses, angioinvasion, ENE, margin distance/location,
tumor variant, multifocality, pT/pN, LN ratio, largest met, ETE, PNI, LVI,
tumor necrosis, specimen type).

Deviations from the March brief (intentional):
  - DROPPED path_extended_gross_path (gross dissection narrative, low
    synoptic signal; prompt's entity types are microscopic/synoptic findings)
  - DROPPED clinical_information_pre_op_diagnosis (pre-operative text; cannot
    contain post-op synoptic findings by construction)
  - ADDED fs_pathology_frozen_section, path_special_studies, ancillary_studies,
    other_findings, tumor_1_ln_examined_comment — discovered via schema sweep,
    all carry direct-signal content for the prompt's entity types
  - REMOVED tumor_4_histology_comment / tumor_5_histology_comment (don't exist
    on path_synoptics; only tumors 1–3 have a _histology_comment column)
  - tumor_1 margin comment uses the hybrid schema field
    tumor_1_margin_angiolymphatic_invasion_comment (only tumor with combined
    commentary); tumors 2–5 use the margin-only _margin_comment columns.

Writes: processed/remaining/9domain_v4/input_synoptic_pathology_enrichment.parquet
"""

from __future__ import annotations

import datetime
import os
import sys
import uuid
from pathlib import Path

import duckdb


SCRIPT_VERSION = "v4_9domain_rerun_2026-04-18"
BATCH_ID = str(uuid.uuid4())
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).isoformat()

OUT_DIR = Path("processed/remaining/9domain_v4")
OUT_PATH = OUT_DIR / "input_synoptic_pathology_enrichment.parquet"

# Per-record (whole-synoptic) text columns. note_index = '0' for these.
# Selected for direct signal on the prompt's 17 entity types.
WHOLE_RECORD_COLUMNS: list[str] = [
    "synoptic_diagnosis",
    "path_diagnosis_summary",
    "path_diagnosis_comment",
    "microscopic_description",
    "fs_pathology_frozen_section",
    "path_special_studies",
    "ancillary_studies",
    "other_findings",
]

# Per-tumor histology comments (only tumors 1–3 have a _histology_comment
# column on path_synoptics; tumors 4–5 do not exist in this schema).
TUMOR_HISTOLOGY_COMMENT_COLUMNS: list[tuple[str, str]] = [
    ("tumor_1_histology_comment", "1"),
    ("tumor_2_histology_comment", "2"),
    ("tumor_3_histology_comment", "3"),
]

# Per-tumor margin / angiolymphatic commentary. Tumor 1's column is named
# _margin_angiolymphatic_invasion_comment (combined field, upstream schema
# quirk). Tumors 2–5 have margin-only _margin_comment columns. There is no
# standalone angiolymphatic comment column for any tumor.
TUMOR_MARGIN_COMMENT_COLUMNS: list[tuple[str, str]] = [
    ("tumor_1_margin_angiolymphatic_invasion_comment", "1"),
    ("tumor_2_margin_comment", "2"),
    ("tumor_3_margin_comment", "3"),
    ("tumor_4_margin_comment", "4"),
    ("tumor_5_margin_comment", "5"),
]

# Per-tumor lymph node commentary (tumor 1 only has a populated one).
TUMOR_LN_COMMENT_COLUMNS: list[tuple[str, str]] = [
    ("tumor_1_ln_examined_comment", "1"),
]


def build_union_sql() -> str:
    """Return the UNION ALL SQL of (research_id, source_column, note_index, note_text)."""
    legs: list[str] = []

    for col in WHOLE_RECORD_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR AS research_id,
                   surg_date::VARCHAR   AS note_date,
                   '{col}'              AS source_column,
                   '0'                  AS note_index,
                   "{col}"              AS note_text
              FROM thyroid_pub.main.path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}") > 20
            """
        )

    for col, ordinal in TUMOR_HISTOLOGY_COMMENT_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR AS research_id,
                   surg_date::VARCHAR   AS note_date,
                   '{col}'              AS source_column,
                   '{ordinal}'          AS note_index,
                   "{col}"              AS note_text
              FROM thyroid_pub.main.path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}") > 20
            """
        )

    for col, ordinal in TUMOR_MARGIN_COMMENT_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR AS research_id,
                   surg_date::VARCHAR   AS note_date,
                   '{col}'              AS source_column,
                   '{ordinal}'          AS note_index,
                   "{col}"              AS note_text
              FROM thyroid_pub.main.path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}") > 20
            """
        )

    for col, ordinal in TUMOR_LN_COMMENT_COLUMNS:
        legs.append(
            f"""
            SELECT research_id::VARCHAR AS research_id,
                   surg_date::VARCHAR   AS note_date,
                   '{col}'              AS source_column,
                   '{ordinal}'          AS note_index,
                   "{col}"              AS note_text
              FROM thyroid_pub.main.path_synoptics
             WHERE "{col}" IS NOT NULL AND LENGTH("{col}") > 20
            """
        )

    return "\nUNION ALL\n".join(legs)


def main() -> int:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        print("ERROR: MOTHERDUCK_TOKEN not set in env. Source .env.motherduck first.", file=sys.stderr)
        return 2

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL motherduck;")
    con.execute("LOAD motherduck;")
    con.execute("ATTACH 'md:thyroid_canonical_publication_v1_0' AS thyroid_pub;")

    inner = build_union_sql()

    copy_sql = f"""
    COPY (
      WITH source_rows AS (
        {inner}
      )
      SELECT
        md5(CONCAT_WS('|', research_id, 'path_synoptics', source_column, COALESCE(note_date,''), note_text)) AS note_row_id,
        research_id,
        note_text,
        'synoptic_pathology'                        AS note_type,
        note_index,
        note_date,
        'All Diagnoses & synoptic 12_1_2025.xlsx'   AS source_workbook,
        'synoptics + Dx merged'                     AS source_sheet,
        source_column,
        '{BATCH_ID}'                                AS preprocess_batch_id,
        '{NOW_UTC}'                                 AS preprocessed_at_utc,
        '{SCRIPT_VERSION}'                          AS preprocess_script_version
      FROM source_rows
    ) TO '{OUT_PATH}' (FORMAT PARQUET);
    """
    con.execute(copy_sql)

    # Verification
    n_rows = con.execute(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
    n_patients = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM '{OUT_PATH}'").fetchone()[0]
    by_col = con.execute(
        f"""
        SELECT source_column, COUNT(*) AS n
          FROM '{OUT_PATH}'
         GROUP BY 1
         ORDER BY 2 DESC
        """
    ).fetchall()

    print(f"\nWrote {OUT_PATH}")
    print(f"  script_version:  {SCRIPT_VERSION}")
    print(f"  batch_id:        {BATCH_ID}")
    print(f"  total rows:      {n_rows:,}")
    print(f"  unique patients: {n_patients:,}")
    print(f"  by source_column:")
    for col, cnt in by_col:
        print(f"    {col:55s} {cnt:>6,}")

    # Early sanity-check guardrails (match the brief's stop-and-ping rules)
    if n_rows < 5_000:
        print("\nWARNING: row count below 5,000 — flag to Logan before proceeding.", file=sys.stderr)
        return 3
    empties = [col for col, cnt in by_col if cnt == 0]
    if empties:
        print(f"\nWARNING: columns with zero rows: {empties}", file=sys.stderr)
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
