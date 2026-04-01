"""Provenance, run telemetry, and quarantine logic for canonical fact long v1."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_fact_lineage_module():
    path = ROOT / "scripts" / "103_fact_lineage_materialize.py"
    spec = importlib.util.spec_from_file_location("fact_lineage_103", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


fact103 = _load_fact_lineage_module()


def test_quarantine_multi_surgery_large_gap():
    multi = {100}
    uni = pd.DataFrame(
        {
            "research_id": [100, 100, 200],
            "extraction_method": ["llm_openai", "llm_openai", "regex"],
            "entity_date": ["2020-01-01", "2020-06-01", None],
            "date_confidence": [0.9, 0.9, None],
            "ep_distance_days": [120, 5, None],
            "inferred_surgery_date": [pd.Timestamp("2019-06-01")] * 3,
        }
    )
    q, reason = fact103.quarantine_masks(uni, multi)
    assert q.iloc[0]  # multi + dist > 90
    assert "multi_surgery" in reason.iloc[0]
    assert not q.iloc[1]  # close enough episode match


def test_quarantine_low_confidence_llm_date():
    uni = pd.DataFrame(
        {
            "research_id": [1],
            "extraction_method": ["llm_github_models"],
            "entity_date": ["2021-01-01"],
            "date_confidence": [0.2],
            "ep_distance_days": [10],
            "inferred_surgery_date": [pd.Timestamp("2021-01-15")],
        }
    )
    q, reason = fact103.quarantine_masks(uni, set())
    assert q.iloc[0]
    assert "low_confidence" in reason.iloc[0]


def test_quarantine_temporal_conflict():
    uni = pd.DataFrame(
        {
            "research_id": [1],
            "extraction_method": ["llm_openai"],
            "entity_date": ["2015-01-01"],
            "date_confidence": [0.9],
            "ep_distance_days": [0],
            "inferred_surgery_date": [pd.Timestamp("2022-01-01")],
        }
    )
    q, reason = fact103.quarantine_masks(uni, set())
    assert q.iloc[0]
    assert "temporal_conflict" in reason.iloc[0]


def test_split_quarantine_columns():
    multi = {1}
    uni = pd.DataFrame(
        {
            "research_id": [1, 2],
            "fact_domain": ["llm", "staging"],
            "entity_type": ["x", "y"],
            "extraction_method": ["llm_openai", "regex"],
            "entity_date": [None, None],
            "date_confidence": [None, None],
            "ep_distance_days": [None, None],
            "inferred_surgery_episode_id": [None, 1],
            "inferred_surgery_date": [None, None],
            "clin_source_workbook": ["wb.xlsx", "wb.xlsx"],
        }
    )
    uni = fact103.add_contract_columns(uni, multi)
    clean, quar = fact103.split_quarantine(uni, multi)
    assert len(clean) + len(quar) == len(uni)
    assert "quarantine_reason" in quar.columns
    assert "quarantine_date" in quar.columns


def test_note_extraction_run_append_and_failure_stage(tmp_path):
    from notes_extraction.run_telemetry import (
        RunTelemetryContext,
        append_note_extraction_run,
    )

    ctx = RunTelemetryContext()
    ctx.llm_disabled = True
    assert ctx.failure_stage() == "llm_disabled"

    ctx2 = RunTelemetryContext()
    ctx2.record_api_failure()
    assert ctx2.failure_stage() == "llm_api_error"

    ctx3 = RunTelemetryContext()
    ctx3.record_parse_failure()
    assert ctx3.failure_stage() == "llm_parse_error"

    append_note_extraction_run(
        tmp_path,
        run_id="test-run-1",
        started_at="2026-01-01T00:00:00Z",
        completed_at="2026-01-01T01:00:00Z",
        success=True,
        failure_stage="llm_disabled",
        retry_count=0,
        output_record_count=0,
        warnings={"note": "ok"},
        domains_requested="staging",
        research_id_filter_note=None,
        target_domain=None,
    )
    df = pd.read_parquet(tmp_path / "note_extraction_runs.parquet")
    assert len(df) == 1
    assert df.iloc[0]["failure_stage"] == "llm_disabled"
    assert bool(df.iloc[0]["success"]) is True
    w = json.loads(df.iloc[0]["warnings"])
    assert w["note"] == "ok"


def test_entity_schema_includes_provenance_columns():
    from notes_extraction.vocab import ENTITY_SCHEMA_COLUMNS

    for col in (
        "extraction_run_id",
        "extractor_name",
        "extractor_version",
        "model_name",
        "prompt_version",
    ):
        assert col in ENTITY_SCHEMA_COLUMNS


def test_stamp_row_sets_regex_prompt_version():
    from notes_extraction.run_extraction import _stamp_row
    from notes_extraction.extract_regex import StagingExtractor

    ext = StagingExtractor()
    rec = {
        "research_id": 1,
        "note_row_id": "n1",
        "note_type": "op_note",
        "entity_type": "T",
        "entity_value_raw": "x",
        "entity_value_norm": "x",
        "present_or_negated": "present",
        "confidence": 0.9,
        "evidence_span": "",
        "evidence_start": 0,
        "evidence_end": 0,
        "entity_date": None,
        "note_date": None,
        "extraction_method": "regex",
        "extracted_at": "",
        "date_confidence": None,
        "source_line": None,
        "chunk_index": 0,
        "chunk_char_start": 0,
        "chunk_char_end": 0,
        "evidence_global_start": 0,
        "evidence_global_end": 0,
        "raw_response_sha256": None,
        "verification_status": "unverified",
        "verification_step": "none",
        "extraction_run_id": None,
        "extractor_name": None,
        "extractor_version": None,
        "model_name": None,
        "model_version": None,
        "prompt_version": None,
        "verifier_name": None,
        "verifier_version": None,
    }
    out = _stamp_row(ext, rec, "run-uuid")
    assert out["extraction_run_id"] == "run-uuid"
    assert out["extractor_name"] == "StagingExtractor"
    assert out["prompt_version"] == "regex_only"
    assert out["model_name"] is None
