"""Provenance: run IDs, SHA256 of raw responses, lineage columns.

Keeps the BQ pattern from your existing note_entities_llm_* tables intact.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone


def new_run_id(task_id: str, model_key: str) -> str:
    """Generate a deterministic-ish run id: <task>_<model>_<utc>_<short_uuid>."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{task_id}_{model_key}_{ts}_{short}"


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
