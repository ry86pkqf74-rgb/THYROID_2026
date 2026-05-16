"""Pull source data from BigQuery for a given task.

Writes a JSONL file in `runs/<task>/<run_id>/source.jsonl` with one row per
extraction unit. Schema:
    {
      "research_id": ...,
      "note_row_id": ...,   # for note-derived tasks
      "source_pk": ...,     # task-specific primary key
      "source_text": "...",
      "note_type": ...,     # if from clinical_notes_long
      "note_date": ...,
    }
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from google.cloud import bigquery

from ..config import BQ_CANONICAL, TASKS


def pull(
    task_id: str,
    *,
    limit: int | None = None,
    where: str | None = None,
    output_path: Path | str | None = None,
    project: str | None = None,
) -> Path:
    """Pull source rows for a task and write to JSONL.

    Returns the output path.
    """
    if task_id not in TASKS:
        raise KeyError(f"Unknown task '{task_id}'")
    spec = TASKS[task_id]

    sql = _build_sql(task_id, limit=limit, where=where)
    client = bigquery.Client(project=project)
    job = client.query(sql)
    rows = list(job.result())

    if output_path is None:
        output_path = Path(f"runs/{task_id}/source.jsonl")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w") as f:
        for r in rows:
            f.write(json.dumps(dict(r), default=str) + "\n")
    return output_path


def _build_sql(task_id: str, *, limit: int | None, where: str | None) -> str:
    """Build a SELECT statement appropriate to each task's source layout."""
    spec = TASKS[task_id]

    if task_id == "molecular":
        # raw_payload_json is BYTES — cast for the model
        sql = f"""
        SELECT
          molecular_result_id AS source_pk,
          research_id,
          assay_name,
          test_date_parsed AS event_date,
          SAFE_CONVERT_BYTES_TO_STRING(raw_payload_json) AS source_text
        FROM `{BQ_CANONICAL}.molecular_results`
        WHERE raw_payload_json IS NOT NULL
        """
    elif task_id == "synoptic":
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(surg_date AS STRING)) AS source_pk,
          research_id,
          surg_date AS event_date,
          CONCAT(
            COALESCE(synoptic_diagnosis, ''),
            '\\n---\\n',
            COALESCE(path_diagnosis_comment, ''),
            '\\n---\\n',
            COALESCE(microscopic_description, '')
          ) AS source_text
        FROM `{BQ_CANONICAL}.path_synoptics`
        WHERE (synoptic_diagnosis IS NOT NULL
            OR path_diagnosis_comment IS NOT NULL
            OR microscopic_description IS NOT NULL)
        """
    elif task_id == "ultrasound":
        # One row per (us_report_number, nodule_index) when description is present
        sql = f"""
        WITH unpivoted AS (
          SELECT research_id, us_report_number, ultrasound_date AS event_date, 1 AS nodule_idx, nodule_1_source_description AS source_text FROM `{BQ_CANONICAL}.ultrasound_reports` UNION ALL
          SELECT research_id, us_report_number, ultrasound_date, 2, nodule_2_source_description FROM `{BQ_CANONICAL}.ultrasound_reports` UNION ALL
          SELECT research_id, us_report_number, ultrasound_date, 3, nodule_3_source_description FROM `{BQ_CANONICAL}.ultrasound_reports` UNION ALL
          SELECT research_id, us_report_number, ultrasound_date, 4, nodule_4_source_description FROM `{BQ_CANONICAL}.ultrasound_reports` UNION ALL
          SELECT research_id, us_report_number, ultrasound_date, 5, nodule_5_source_description FROM `{BQ_CANONICAL}.ultrasound_reports`
        )
        SELECT
          CONCAT(us_report_number, '|n', CAST(nodule_idx AS STRING)) AS source_pk,
          research_id, event_date, nodule_idx, source_text
        FROM unpivoted
        WHERE source_text IS NOT NULL AND LENGTH(source_text) > 30
        """
    elif task_id in ("imaging_ct", "imaging_mri"):
        table = "ct_imaging" if task_id == "imaging_ct" else "mri_imaging"
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(date_of_exam AS STRING)) AS source_pk,
          research_id, date_of_exam AS event_date,
          original_report AS source_text
        FROM `{BQ_CANONICAL}.{table}`
        WHERE original_report IS NOT NULL AND LENGTH(original_report) > 100
        """
    elif task_id == "imaging_nm":
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(scandate AS STRING)) AS source_pk,
          research_id, scandate AS event_date,
          CONCAT(COALESCE(findings_text, ''), '\\n---\\n', COALESCE(impression_text, '')) AS source_text
        FROM `{BQ_CANONICAL}.nuclear_med`
        WHERE findings_text IS NOT NULL OR impression_text IS NOT NULL
        """
    elif task_id == "fna":
        sql = f"""
        SELECT
          CONCAT(research_id, '|fna', CAST(fna_index AS STRING)) AS source_pk,
          research_id, fna_date AS event_date,
          path_text AS source_text
        FROM `{BQ_CANONICAL}.fna_cytology`
        WHERE path_text IS NOT NULL AND LENGTH(path_text) > 50
        """
    elif task_id == "complications":
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(note_index AS STRING)) AS source_pk,
          research_id, note_type, note_index, note_text AS source_text
        FROM `{BQ_CANONICAL}.clinical_notes_long`
        WHERE note_type IN ('OPNOTE', 'HP', 'ENDOCRINE_FM', 'DC_SUM')
          AND note_text IS NOT NULL
        """
    elif task_id == "death":
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(note_index AS STRING)) AS source_pk,
          research_id, note_type, note_text AS source_text
        FROM `{BQ_CANONICAL}.clinical_notes_long`
        WHERE note_type = 'DEATH' AND note_text IS NOT NULL
        """
    elif task_id == "risk_factors":
        sql = f"""
        SELECT
          CONCAT(research_id, '|', CAST(note_index AS STRING)) AS source_pk,
          research_id, note_type, note_text AS source_text
        FROM `{BQ_CANONICAL}.clinical_notes_long`
        WHERE note_type = 'HP' AND note_text IS NOT NULL
        """
    else:
        raise NotImplementedError(f"No SQL builder for task '{task_id}'")

    if where:
        sql += f"\nAND ({where})"
    if limit:
        sql += f"\nLIMIT {limit}"
    return sql
