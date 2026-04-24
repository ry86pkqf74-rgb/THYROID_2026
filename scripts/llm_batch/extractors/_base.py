"""Shared helpers for per-domain candidate-note extractors.

Notes live in FOUR sources in thyroid_canonical_publication_v1_0:
  1. main.clinical_notes_long        — free-text OPNOTE/HP/DC_SUM/ED_NOTE/etc.
  2. main.path_synoptics             — structured synoptic cols + long free-text
                                        cols (path_diagnosis_comment,
                                        microscopic_description,
                                        path_extended_gross_path, tumor_*_comment)
  3. main.ct_imaging                 — .original_report, .airway_tracheal_findings,
                                        .thyroid_surgical_details, .lymph_node_findings
  4. main.mri_imaging                — .original_report, .thyroid_details

Each extractor picks which sources are relevant and passes a `sources` spec.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import sys

import duckdb


PATH_SYNOPTIC_TEXT_COLS = [
    "path_diagnosis_comment",
    "path_diagnosis_summary",
    "microscopic_description",
    "path_extended_gross_path",
    "synoptic_diagnosis",
    "tumor_1_histology_comment",
    "tumor_1_margin_angiolymphatic_invasion_comment",
    "tumor_1_ln_examined_comment",
    "tumor_2_comment",
    "tumor_2_histology_comment",
    "tumor_3_histology_comment",
    "other_findings",
]

CT_TEXT_COLS = [
    "original_report",
    "airway_tracheal_findings",
    "thyroid_surgical_details",
    "thyroid_findings",
    "lymph_node_findings",
]

MRI_TEXT_COLS = [
    "original_report",
    "thyroid_details",
    "parathyroid_details",
    "vocal_cords_details",
    "lymph_node_details",
]


def get_token(repo_root: pathlib.Path) -> str:
    for var in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        if os.environ.get(var):
            return os.environ[var]
    toml = repo_root / "motherduck.local.toml"
    if toml.exists():
        for line in toml.read_text().splitlines():
            m = re.match(r"^\s*(MD_SA_TOKEN|MOTHERDUCK_TOKEN|motherduck_token)\s*=\s*[\"']?([^\"'#\s]+)", line)
            if m:
                return m.group(2)
    sys.exit("no MotherDuck token found")


def connect() -> duckdb.DuckDBPyConnection:
    repo_root = pathlib.Path(os.environ.get("REPO_ROOT", os.path.expanduser("~/THyroid 2026")))
    return duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={get_token(repo_root)}")


def write_jsonl(rows: list[dict], out_path: pathlib.Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(rows)} rows -> {out_path}")


def _keyword_like_clause(col: str, keywords: list[str]) -> str:
    parts = [f"LOWER({col}) LIKE '%{k.lower()}%'" for k in keywords]
    return "(" + " OR ".join(parts) + ")"


def _unified_notes_sql(
    *,
    include_clinical: bool = True,
    include_path: bool = True,
    include_ct: bool = False,
    include_mri: bool = False,
    clinical_note_types: list[str] | None = None,
) -> str:
    parts: list[str] = []
    if include_clinical:
        if clinical_note_types:
            nt_filter = "AND note_type IN (" + ",".join(f"'{t}'" for t in clinical_note_types) + ")"
        else:
            nt_filter = ""
        parts.append(f"""
        SELECT
          research_id,
          note_type,
          CAST(note_index AS VARCHAR) AS note_index,
          source_workbook,
          source_sheet,
          source_column,
          note_text
        FROM main.clinical_notes_long
        WHERE note_text IS NOT NULL AND LENGTH(note_text) > 50
        {nt_filter}
        """)
    if include_path:
        # Build a single concatenated note_text per synoptic row from all
        # long free-text cols. The note_type stays 'synoptic_pathology' so
        # the LLM can be told what it's reading.
        concat = "\n---\n".join([f"COALESCE({c}, '')" for c in PATH_SYNOPTIC_TEXT_COLS])
        parts.append(f"""
        SELECT
          research_id,
          'synoptic_pathology' AS note_type,
          CAST(ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surg_date NULLS LAST) AS VARCHAR) AS note_index,
          source_workbook,
          'path_synoptics' AS source_sheet,
          'concat' AS source_column,
          CONCAT_WS('\n', {', '.join(PATH_SYNOPTIC_TEXT_COLS)}) AS note_text
        FROM main.path_synoptics
        WHERE ({' OR '.join(f"{c} IS NOT NULL AND LENGTH({c}) > 20" for c in PATH_SYNOPTIC_TEXT_COLS)})
        """)
    if include_ct:
        concat = ", ".join(CT_TEXT_COLS)
        parts.append(f"""
        SELECT
          research_id,
          'ct_imaging' AS note_type,
          CAST(ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY date_of_exam NULLS LAST) AS VARCHAR) AS note_index,
          source_workbook,
          'ct_imaging' AS source_sheet,
          ct_column AS source_column,
          CONCAT_WS('\n', {concat}) AS note_text
        FROM main.ct_imaging
        WHERE ({' OR '.join(f"{c} IS NOT NULL AND LENGTH({c}) > 20" for c in CT_TEXT_COLS)})
        """)
    if include_mri:
        concat = ", ".join(MRI_TEXT_COLS)
        parts.append(f"""
        SELECT
          research_id,
          'mri_imaging' AS note_type,
          CAST(ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY date_of_exam NULLS LAST) AS VARCHAR) AS note_index,
          NULL AS source_workbook,
          'mri_imaging' AS source_sheet,
          mri_label AS source_column,
          CONCAT_WS('\n', {concat}) AS note_text
        FROM main.mri_imaging
        WHERE ({' OR '.join(f"{c} IS NOT NULL AND LENGTH({c}) > 20" for c in MRI_TEXT_COLS)})
        """)
    return "\nUNION ALL\n".join(parts)


def fetch_notes(
    con: duckdb.DuckDBPyConnection,
    *,
    cohort_cte: str,
    keywords: list[str],
    include_clinical: bool = True,
    include_path: bool = True,
    include_ct: bool = False,
    include_mri: bool = False,
    clinical_note_types: list[str] | None = None,
    limit_per_patient: int | None = None,
) -> list[dict]:
    unified = _unified_notes_sql(
        include_clinical=include_clinical,
        include_path=include_path,
        include_ct=include_ct,
        include_mri=include_mri,
        clinical_note_types=clinical_note_types,
    )
    kw = _keyword_like_clause("n.note_text", keywords)
    row_filter = ""
    if limit_per_patient is not None:
        row_filter = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY n.research_id, n.note_type ORDER BY n.note_index) <= {limit_per_patient}"
    sql = f"""
    WITH cohort AS ({cohort_cte}),
    unified AS ({unified})
    SELECT
        n.research_id,
        n.note_type,
        n.note_index,
        n.source_workbook,
        n.source_sheet,
        n.source_column,
        n.note_text
    FROM unified n
    INNER JOIN cohort c USING (research_id)
    WHERE {kw}
    {row_filter}
    ORDER BY n.research_id, n.note_type, n.note_index
    """
    return [dict(zip([d[0] for d in con.description], r))
            for r in con.execute(sql).fetchall()]
