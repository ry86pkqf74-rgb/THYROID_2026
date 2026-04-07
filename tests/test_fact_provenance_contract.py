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

from utils.provenance import quarantine_masks  # noqa: E402


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
    q, reason = quarantine_masks(uni, multi)
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
    q, reason = quarantine_masks(uni, set())
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
    q, reason = quarantine_masks(uni, set())
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
    from llm_extraction.run_telemetry import (
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
    from llm_extraction.vocab import ENTITY_SCHEMA_COLUMNS, PROVENANCE_FIELD_DEFAULTS

    for col in (
        "extraction_run_id",
        "extractor_name",
        "extractor_version",
        "model_name",
        "prompt_version",
        "entity_domain",
        "llm_provider",
        "provider_returned_model",
    ):
        assert col in ENTITY_SCHEMA_COLUMNS
    assert PROVENANCE_FIELD_DEFAULTS.get("verification_status") == "unverified"


def test_sort_entities_deterministic_stable_under_shuffle():
    """Row order from threaded completion order should not affect final parquet order."""
    from llm_extraction.vocab import sort_entities_deterministic

    df = pd.DataFrame(
        [
            {
                "research_id": 2,
                "note_row_id": "n2",
                "entity_domain": "imaging",
                "entity_type": "size",
                "entity_date": None,
                "note_date": "2020-01-01",
                "entity_value_norm": "a",
                "present_or_negated": "present",
                "chunk_index": 0,
                "source_line": 2,
                "evidence_global_start": 10,
                "evidence_global_end": 20,
                "extraction_method": "llm_github_models",
                "raw_response_sha256": "aa",
            },
            {
                "research_id": 1,
                "note_row_id": "n1",
                "entity_domain": "imaging",
                "entity_type": "size",
                "entity_date": None,
                "note_date": "2020-01-02",
                "entity_value_norm": "b",
                "present_or_negated": "present",
                "chunk_index": 0,
                "source_line": 1,
                "evidence_global_start": 5,
                "evidence_global_end": 15,
                "extraction_method": "llm_github_models",
                "raw_response_sha256": "bb",
            },
            {
                "research_id": 1,
                "note_row_id": "n1",
                "entity_domain": "imaging",
                "entity_type": "size",
                "entity_date": None,
                "note_date": "2020-01-02",
                "entity_value_norm": "a",
                "present_or_negated": "present",
                "chunk_index": 0,
                "source_line": 1,
                "evidence_global_start": 1,
                "evidence_global_end": 2,
                "extraction_method": "llm_github_models",
                "raw_response_sha256": "cc",
            },
        ]
    )
    s1 = sort_entities_deterministic(df.sample(frac=1, random_state=7).reset_index(drop=True))
    s2 = sort_entities_deterministic(df.sample(frac=1, random_state=42).reset_index(drop=True))
    pd.testing.assert_frame_equal(s1, s2)

    merged_a = sort_entities_deterministic(
        pd.concat([df.iloc[[0, 1]], df.iloc[[2]]], ignore_index=True)
    )
    merged_b = sort_entities_deterministic(
        pd.concat([df.iloc[[2]], df.iloc[[1, 0]]], ignore_index=True)
    )
    pd.testing.assert_frame_equal(merged_a, merged_b)


def test_sort_entities_deterministic_unifies_mixed_entity_date_types():
    """ISO strings and timestamps sort in chronological order for entity_date."""
    from llm_extraction.vocab import sort_entities_deterministic

    df = pd.DataFrame(
        [
            {
                "research_id": 1,
                "note_row_id": "n1",
                "entity_type": "t",
                "entity_date": "2020-01-15",
                "entity_value_norm": "later",
            },
            {
                "research_id": 1,
                "note_row_id": "n1",
                "entity_type": "t",
                "entity_date": pd.Timestamp("2020-01-02"),
                "entity_value_norm": "earlier",
            },
        ]
    )
    out = sort_entities_deterministic(df)
    assert out.iloc[0]["entity_value_norm"] == "earlier"
    assert out.iloc[1]["entity_value_norm"] == "later"


def test_note_extraction_run_input_fingerprint_and_registry(tmp_path):
    import json

    from llm_extraction.run_telemetry import append_note_extraction_run

    append_note_extraction_run(
        tmp_path,
        run_id="run-fp",
        started_at="2026-04-06T00:00:00Z",
        completed_at="2026-04-06T00:01:00Z",
        success=True,
        failure_stage="none",
        retry_count=0,
        output_record_count=1,
        warnings={"input_fingerprint_mode": "full"},
        domains_requested="staging",
        research_id_filter_note=None,
        target_domain=None,
        input_path="/processed/clinical_notes_long.parquet",
        input_file_size_bytes=1024,
        input_mtime_utc="2026-04-06T12:00:00+00:00",
        input_sha256="a" * 64,
        registry_schema_version="entity_schema_v3_x",
        registry_digest="b" * 64,
    )
    df = pd.read_parquet(tmp_path / "note_extraction_runs.parquet")
    row = df.iloc[0]
    assert row["input_path"].endswith("clinical_notes_long.parquet")
    assert int(row["input_file_size_bytes"]) == 1024
    assert "2026-04-06" in str(row["input_mtime_utc"])
    assert row["input_sha256"] == "a" * 64
    assert row["registry_schema_version"] == "entity_schema_v3_x"
    assert row["registry_digest"] == "b" * 64
    warn = json.loads(row["warnings"])
    assert warn["input_fingerprint_mode"] == "full"


def test_note_extraction_runs_backward_compat_old_rows(tmp_path):
    """Older parquet rows without fingerprint columns survive append with null new fields."""
    from llm_extraction.run_telemetry import append_note_extraction_run

    legacy = pd.DataFrame(
        [
            {
                "run_id": "legacy-1",
                "started_at": "2025-01-01T00:00:00Z",
                "completed_at": "2025-01-01T01:00:00Z",
                "success": True,
                "failure_stage": "none",
                "retry_count": 0,
                "output_record_count": 0,
                "warnings": "{}",
                "domains_requested": "staging",
                "research_id_filter": None,
                "target_domain": None,
                "extractor_build_version": "entity_schema_v2_2026-04-01",
                "hostname": "h",
                "git_commit": None,
            }
        ]
    )
    legacy.to_parquet(tmp_path / "note_extraction_runs.parquet", index=False)

    append_note_extraction_run(
        tmp_path,
        run_id="new-1",
        started_at="2026-04-06T00:00:00Z",
        completed_at="2026-04-06T00:01:00Z",
        success=True,
        failure_stage="none",
        retry_count=0,
        output_record_count=1,
        warnings={"k": "v"},
        domains_requested="genetics",
        research_id_filter_note=None,
        target_domain=None,
        input_path=str(tmp_path / "clinical_notes_long.parquet"),
        input_file_size_bytes=99,
        input_mtime_utc="2026-04-06T00:00:00+00:00",
        input_sha256="c" * 64,
        registry_schema_version="entity_schema_v3",
        registry_digest="d" * 64,
    )

    df = pd.read_parquet(tmp_path / "note_extraction_runs.parquet")
    assert len(df) == 2
    assert pd.isna(df.iloc[0]["input_sha256"])
    assert df.iloc[1]["input_sha256"] == "c" * 64
    assert df.iloc[1]["registry_digest"] == "d" * 64


def test_md_connect_uses_local_file_when_md_false(tmp_path):
    from utils.md_connect import connect_md_or_file

    db = tmp_path / "t.duckdb"
    con = connect_md_or_file(db, md=False)
    try:
        con.execute("SELECT 1")
    finally:
        con.close()
    assert db.exists()


def test_md_connect_md_flag_falls_back_to_file_without_token(tmp_path, monkeypatch):
    from utils import md_connect as md_mod

    monkeypatch.setattr(md_mod, "get_token", lambda prefer_service_account=False: None)
    db = tmp_path / "fallback.duckdb"
    con = md_mod.connect_md_or_file(db, md=True)
    try:
        con.execute("SELECT 1")
    finally:
        con.close()
    assert db.exists()


def test_provenance_hash_without_source_row_number():
    """Recoverable provenance via evidence text when excel/source_line is absent."""
    from utils.provenance import hash_evidence_span

    h = hash_evidence_span("  quoted phrase  ")
    assert h is not None and len(h) == 64
    assert hash_evidence_span(None) is None
    assert hash_evidence_span("") is None


def test_note_extraction_run_failed_llm_persisted(tmp_path):
    from llm_extraction.run_telemetry import append_note_extraction_run

    append_note_extraction_run(
        tmp_path,
        run_id="test-run-fail",
        started_at="2026-01-01T00:00:00Z",
        completed_at="2026-01-01T00:01:00Z",
        success=False,
        failure_stage="llm_api_error",
        retry_count=2,
        output_record_count=0,
        warnings={" err": "timeout"},
        domains_requested="staging",
        research_id_filter_note=None,
        target_domain=None,
    )
    df = pd.read_parquet(tmp_path / "note_extraction_runs.parquet")
    assert len(df) == 1
    assert bool(df.iloc[0]["success"]) is False
    assert df.iloc[0]["failure_stage"] == "llm_api_error"
    assert int(df.iloc[0]["retry_count"]) == 2


def test_empty_successful_extraction_zero_rows(tmp_path):
    from llm_extraction.run_telemetry import append_note_extraction_run

    append_note_extraction_run(
        tmp_path,
        run_id="test-empty-ok",
        started_at="2026-01-02T00:00:00Z",
        completed_at="2026-01-02T00:00:01Z",
        success=True,
        failure_stage="none",
        retry_count=0,
        output_record_count=0,
        warnings={},
        domains_requested="llm",
        research_id_filter_note=None,
        target_domain=None,
    )
    df = pd.read_parquet(tmp_path / "note_extraction_runs.parquet")
    assert bool(df.iloc[0]["success"]) is True
    assert int(df.iloc[0]["output_record_count"]) == 0


def test_stamp_row_sets_regex_prompt_version():
    from llm_extraction.run_extraction import _stamp_row
    from llm_extraction.extract_regex import StagingExtractor

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
        "entity_domain": None,
        "llm_provider": None,
        "llm_base_url": None,
        "llm_sdk": None,
        "llm_sdk_version": None,
        "provider_returned_model": None,
        "provider_system_fingerprint": None,
    }
    out = _stamp_row(ext, rec, "run-uuid", {})
    assert out["extraction_run_id"] == "run-uuid"
    assert out["extractor_name"] == "StagingExtractor"
    assert out["prompt_version"] == "regex_only"
    assert out["model_name"] is None
