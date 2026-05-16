"""BQ I/O for verify command.

Pulls rows from a note_entities_* table along with source note text,
writes verification verdicts to pub_workspace.<table>_verified_v1.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from google.cloud import bigquery

from ..config import BQ_CANONICAL, BQ_WORKSPACE
from ..models.verifier import VerificationInput

SUPPORTED_TABLES = {
    "note_entities_complications",
    "note_entities_operative_detail",
    "note_entities_problem_list",
    "note_entities_procedures",
    "note_entities_staging",
    "note_entities_genetics",
    "note_entities_medications",
}


def pull_unverified(
    table: str,
    *,
    limit: int | None = None,
    entity_types: list[str] | None = None,
    project: str | None = None,
) -> Iterator[VerificationInput]:
    """Pull rows missing real verification (status NULL or 'unverified')."""
    if table not in SUPPORTED_TABLES:
        raise ValueError(f"Table {table} not supported. Choose from: {SUPPORTED_TABLES}")

    where = ["(e.verification_status IS NULL OR e.verification_status = 'unverified')"]
    if entity_types:
        types_list = ", ".join(f"'{t}'" for t in entity_types)
        where.append(f"e.entity_type IN ({types_list})")
    where_sql = " AND ".join(where)
    limit_sql = f"LIMIT {limit}" if limit else ""

    # Entity tables use lowercase note_type (h_p, op_note, dc_sum, endocrine_note...);
    # clinical_notes_long uses uppercase (HP, OPNOTE, DC_SUM, ENDOCRINE_FM...).
    # And entity.note_index is often NULL. So we normalize note_type and aggregate
    # all notes of the matching type per patient.
    sql = f"""
    WITH notes_agg AS (
      SELECT
        research_id,
        UPPER(REPLACE(note_type, '_', '')) AS note_type_key,
        STRING_AGG(note_text, ' ||| NOTE BREAK ||| ' ORDER BY note_index) AS combined_text
      FROM `{BQ_CANONICAL}.clinical_notes_long`
      WHERE note_text IS NOT NULL
      GROUP BY research_id, note_type_key
    )
    SELECT
      e.research_id,
      CONCAT(
        e.research_id, '|',
        CAST(e.note_row_id AS STRING), '|',
        e.entity_type, '|',
        COALESCE(CAST(e.evidence_global_start AS STRING),
                 CAST(e.evidence_start AS STRING), '0')
      ) AS source_pk,
      e.entity_type,
      COALESCE(e.entity_value_raw, '') AS entity_value_raw,
      COALESCE(e.entity_value_norm, '') AS entity_value_norm,
      COALESCE(e.present_or_negated, '') AS present_or_negated,
      COALESCE(e.confidence_score, 0.0) AS original_confidence,
      COALESCE(e.evidence_span, '') AS evidence_span,
      CAST(e.entity_date AS STRING) AS entity_date,
      COALESCE(n.combined_text, '') AS source_text
    FROM `{BQ_CANONICAL}.{table}` e
    LEFT JOIN notes_agg n
      ON n.research_id = e.research_id
     AND n.note_type_key = UPPER(REPLACE(e.note_type, '_', ''))
    WHERE {where_sql}
    {limit_sql}
    """
    client = bigquery.Client(project=project)
    for row in client.query(sql).result():
        yield VerificationInput(
            research_id=row.research_id,
            source_pk=row.source_pk,
            entity_type=row.entity_type,
            entity_value_raw=row.entity_value_raw,
            entity_value_norm=row.entity_value_norm,
            present_or_negated=row.present_or_negated,
            original_confidence=float(row.original_confidence or 0.0),
            evidence_span=row.evidence_span,
            entity_date=row.entity_date,
            source_text=row.source_text or "",
        )


VERIFIED_SCHEMA = [
    bigquery.SchemaField("research_id", "STRING"),
    bigquery.SchemaField("source_pk", "STRING"),
    bigquery.SchemaField("source_table", "STRING"),
    bigquery.SchemaField("entity_type", "STRING"),
    bigquery.SchemaField("verification_status", "STRING"),
    bigquery.SchemaField("agrees_with_original", "BOOL"),
    bigquery.SchemaField("corrected_value", "STRING"),
    bigquery.SchemaField("corrected_present_or_negated", "STRING"),
    bigquery.SchemaField("date_confidence", "FLOAT"),
    bigquery.SchemaField("evidence_present_in_source", "BOOL"),
    bigquery.SchemaField("verifier_reasoning", "STRING"),
    bigquery.SchemaField("verifier_run_id", "STRING"),
    bigquery.SchemaField("verifier_model_name", "STRING"),
    bigquery.SchemaField("verifier_prompt_version", "STRING"),
    bigquery.SchemaField("raw_verifier_response_sha256", "STRING"),
    bigquery.SchemaField("elapsed_seconds", "FLOAT"),
    bigquery.SchemaField("extraction_timestamp_utc", "TIMESTAMP"),
]


def push_verifications(
    source_table: str,
    results_jsonl: Path | str,
    *,
    project: str | None = None,
) -> str:
    target = f"{BQ_WORKSPACE}.{source_table}_verified_v1"
    client = bigquery.Client(project=project)
    table_ref = bigquery.TableReference.from_string(target)
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=VERIFIED_SCHEMA))

    rows = []
    with Path(results_jsonl).open() as f:
        for line in f:
            obj = json.loads(line)
            obj["source_table"] = source_table
            obj.setdefault("extraction_timestamp_utc", datetime.now(timezone.utc).isoformat())
            rows.append(obj)
    errors = client.insert_rows_json(target, rows)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")
    return target
