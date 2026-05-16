"""Push extraction results back to BigQuery.

Writes to `pub_canonical.note_entities_llm_<task>_v<n>` (table name from TaskSpec).
Schema follows the existing note_entities_llm_* pattern with full provenance.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

from ..config import BQ_CANONICAL, BQ_WORKSPACE, TASKS


SCHEMA = [
    bigquery.SchemaField("research_id", "STRING"),
    bigquery.SchemaField("source_pk", "STRING"),
    bigquery.SchemaField("note_row_id", "STRING"),
    bigquery.SchemaField("entity_domain", "STRING"),
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("result_json", "STRING"),
    bigquery.SchemaField("extraction_run_id", "STRING"),
    bigquery.SchemaField("extractor_name", "STRING"),
    bigquery.SchemaField("extractor_version", "STRING"),
    bigquery.SchemaField("model_name", "STRING"),
    bigquery.SchemaField("model_version", "STRING"),
    bigquery.SchemaField("prompt_version", "STRING"),
    bigquery.SchemaField("llm_provider", "STRING"),
    bigquery.SchemaField("llm_sdk", "STRING"),
    bigquery.SchemaField("llm_sdk_version", "STRING"),
    bigquery.SchemaField("raw_response_sha256", "STRING"),
    bigquery.SchemaField("verification_status", "STRING"),
    bigquery.SchemaField("confidence_score", "FLOAT"),
    bigquery.SchemaField("extraction_timestamp_utc", "TIMESTAMP"),
    bigquery.SchemaField("elapsed_seconds", "FLOAT"),
]


def push(
    task_id: str,
    results_jsonl: Path | str,
    *,
    workspace: bool = True,
    project: str | None = None,
) -> str:
    """Load a JSONL of extraction rows into BQ.

    By default writes to pub_workspace.<output_table>; pass workspace=False
    to write directly into pub_canonical.
    """
    if task_id not in TASKS:
        raise KeyError(f"Unknown task '{task_id}'")
    spec = TASKS[task_id]

    target_dataset = BQ_WORKSPACE if workspace else BQ_CANONICAL
    target_table = f"{target_dataset}.{spec.output_table}"

    client = bigquery.Client(project=project)

    rows = []
    with Path(results_jsonl).open() as f:
        for line in f:
            obj = json.loads(line)
            rows.append(_row_for_bq(task_id, obj))

    table_ref = bigquery.TableReference.from_string(target_table)
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)

    errors = client.insert_rows_json(target_table, rows)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")
    return target_table


def _row_for_bq(task_id: str, obj: dict) -> dict:
    """Coerce a result row into the standard BQ row shape."""
    spec = TASKS[task_id]
    return {
        "research_id": obj.get("research_id"),
        "source_pk": obj.get("source_pk"),
        "note_row_id": obj.get("note_row_id"),
        "entity_domain": spec.domain,
        "event_date": obj.get("event_date"),
        "result_json": json.dumps(obj.get("result")) if obj.get("result") else None,
        "extraction_run_id": obj.get("extraction_run_id"),
        "extractor_name": "thyroid-mlx-extract",
        "extractor_version": obj.get("extractor_version", "0.1.0"),
        "model_name": obj.get("model_name"),
        "model_version": obj.get("model_version"),
        "prompt_version": obj.get("prompt_version"),
        "llm_provider": "mlx-community",
        "llm_sdk": "mlx-lm",
        "llm_sdk_version": obj.get("llm_sdk_version", "0.20.0"),
        "raw_response_sha256": obj.get("raw_response_sha256"),
        "verification_status": obj.get("verification_status"),
        "confidence_score": obj.get("confidence_score"),
        "extraction_timestamp_utc": obj.get("extraction_timestamp_utc")
            or datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": obj.get("elapsed_seconds"),
    }
