"""
Helpers for imaging ↔ FNA linkage (multimodal v1).

Source of truth for imaging nodules: imaging_nodule_master_v1 (per project README / AGENTS).
"""
from __future__ import annotations


def normalize_specimen_key_sql(col_expr: str) -> str:
    """
    DuckDB SQL fragment: lower-case, strip non-alphanumeric, trim empty to NULL.
    col_expr should be a valid SQL expression (e.g. h.specimen_received).
    """
    return (
        "NULLIF(TRIM(REGEXP_REPLACE(LOWER(CAST("
        + col_expr
        + " AS VARCHAR)), '[^a-z0-9]', '', 'g')), '')"
    )