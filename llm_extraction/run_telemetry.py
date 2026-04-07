"""
Per-extraction-run telemetry: LLM counters and persistence to note_extraction_runs.
"""

from __future__ import annotations

import hashlib
import json
import socket
import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from llm_extraction.vocab import EXTRACTOR_BUILD_VERSION
from utils.text_helpers import save_parquet


def new_extraction_run_id() -> str:
    return str(uuid4())


def hash_file_sha256(path: Path, *, chunk_size: int = 1 << 20) -> str:
    """Stream a file into SHA-256 without loading it whole (binary chunks only; no text decode)."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _git_head() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).resolve().parent.parent,
        )
        if out.returncode == 0:
            return out.stdout.strip()[:40]
    except (OSError, subprocess.SubprocessError):
        pass
    return None


@dataclass
class RunTelemetryContext:
    """Thread-safe counters for LLM extraction (parallel note workers)."""

    llm_disabled: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)
    api_failures: int = 0
    parse_failures: int = 0
    retry_attempts: int = 0

    def record_api_failure(self) -> None:
        with self._lock:
            self.api_failures += 1

    def record_parse_failure(self) -> None:
        with self._lock:
            self.parse_failures += 1

    def record_retry(self, n: int = 1) -> None:
        with self._lock:
            self.retry_attempts += n

    def failure_stage(self) -> str:
        if self.llm_disabled:
            return "llm_disabled"
        if self.api_failures > 0:
            return "llm_api_error"
        if self.parse_failures > 0:
            return "llm_parse_error"
        return "none"


def append_note_extraction_run(
    processed_dir: Path,
    *,
    run_id: str,
    started_at: str,
    completed_at: str,
    success: bool,
    failure_stage: str,
    retry_count: int,
    output_record_count: int,
    warnings: dict[str, Any] | list[Any] | str,
    domains_requested: str | None = None,
    research_id_filter_note: str | None = None,
    target_domain: str | None = None,
    input_path: str | None = None,
    input_file_size_bytes: int | None = None,
    input_mtime_utc: str | None = None,
    input_sha256: str | None = None,
    registry_schema_version: str | None = None,
    registry_digest: str | None = None,
) -> Path:
    """Append one run row to processed/note_extraction_runs.parquet.

    ``success`` should be True when ``failure_stage`` is ``none`` or ``llm_disabled``,
    and False for ``llm_api_error`` / ``llm_parse_error`` (partial LLM failure).
    """
    path = processed_dir / "note_extraction_runs.parquet"
    if isinstance(warnings, (dict, list)):
        warnings_str = json.dumps(warnings, default=str)
    else:
        warnings_str = str(warnings)

    row = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "success": success,
        "failure_stage": failure_stage,
        "retry_count": retry_count,
        "output_record_count": output_record_count,
        "warnings": warnings_str,
        "domains_requested": domains_requested,
        "research_id_filter": research_id_filter_note,
        "target_domain": target_domain,
        "extractor_build_version": EXTRACTOR_BUILD_VERSION,
        "hostname": socket.gethostname(),
        "git_commit": _git_head(),
        "input_path": input_path,
        "input_file_size_bytes": input_file_size_bytes,
        "input_mtime_utc": input_mtime_utc,
        "input_sha256": input_sha256,
        "registry_schema_version": registry_schema_version,
        "registry_digest": registry_digest,
    }
    if path.exists():
        prev = pd.read_parquet(path)
        for col, val in row.items():
            if col not in prev.columns:
                prev[col] = None
        out = pd.concat([prev, pd.DataFrame([row])], ignore_index=True)
    else:
        out = pd.DataFrame([row])
    save_parquet(out, path)
    return path
