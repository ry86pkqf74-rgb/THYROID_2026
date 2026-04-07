"""
Offline golden regression for LLM extraction parser + stamping + schema.

No live API calls: mock JSON under tests/fixtures/llm_regression/mock_llm/.
"""
from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.base import BaseExtractor  # noqa: E402
from llm_extraction.extract_llm import LLMExtractor  # noqa: E402
from llm_extraction.run_extraction import _stamp_row  # noqa: E402
from llm_extraction.vocab import ENTITY_SCHEMA_COLUMNS, sort_entities_deterministic  # noqa: E402

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "llm_regression"

# Must match values used when generating tests/fixtures/llm_regression/expected/*.json
REGRESSION_TRANSPORT: dict[str, str | None] = {
    "llm_provider": "github_models",
    "llm_base_url": "https://models.github.ai/inference/",
    "llm_sdk": "openai",
    "llm_sdk_version": "9.9.9",
    "provider_returned_model": "openai/gpt-4o-mini",
    "provider_system_fingerprint": "fp_test_gold",
}

_FIXED_EXTRACTED_AT = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def _load_cases() -> list[dict[str, Any]]:
    data = json.loads((FIXTURE_ROOT / "cases.json").read_text(encoding="utf-8"))
    assert isinstance(data, list)
    return data


CASES = _load_cases()


def _freeze_extract_datetime(monkeypatch: pytest.MonkeyPatch) -> None:
    import llm_extraction.extract_llm as el_mod

    orig = el_mod.datetime

    class _FrozenDateTime:
        strptime = orig.strptime

        @staticmethod
        def now(tz=None):
            return _FIXED_EXTRACTED_AT

    monkeypatch.setattr(el_mod, "datetime", _FrozenDateTime)


def _clear_llm_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "GITHUB_TOKEN",
        "OPENAI_API_KEY",
        "OPENAI_API_KEY_ID",
    ):
        monkeypatch.delenv(key, raising=False)


def _offline_extractor() -> LLMExtractor:
    ext = LLMExtractor()
    ext._api_key = "offline-ci"
    ext._provider = "github_models"
    ext._base_url = "https://models.github.ai/inference/"
    ext._model_id = "openai/gpt-4o-mini"
    return ext


def _bind_fake_call_llm(ext: LLMExtractor, case_id: str) -> None:
    fixture_root = FIXTURE_ROOT

    def _fake_call_llm(
        self: LLMExtractor,
        note_row_id: str,
        research_id: int,
        note_type: str,
        text: str,
        note_date: str | None = None,
        *,
        full_note_text: str,
        chunk_char_start: int,
        chunk_char_end: int,
        chunk_index: int = 0,
        operative: bool = False,
        domain: str | None = None,
    ):
        raw = (fixture_root / "mock_llm" / f"{case_id}.json").read_text(encoding="utf-8")
        return self._parse_llm_response(
            raw,
            note_row_id,
            research_id,
            note_type,
            chunk_text=text,
            full_note_text=full_note_text,
            note_date=note_date,
            chunk_char_start=chunk_char_start,
            chunk_char_end=chunk_char_end,
            chunk_index=chunk_index,
            llm_operative=operative,
            domain=domain,
            llm_transport=dict(REGRESSION_TRANSPORT),
        )

    ext._call_llm = types.MethodType(_fake_call_llm, ext)  # type: ignore[method-assign]


def _minimal_source_row(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "episode_id": None,
        "note_index": 1,
        "source_sheet": "fixture",
        "source_column": "A",
        "research_id": case["research_id"],
        "note_row_id": case["note_row_id"],
    }


def _rows_for_case(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    _clear_llm_env(monkeypatch)
    _freeze_extract_datetime(monkeypatch)
    ext = _offline_extractor()
    _bind_fake_call_llm(ext, case["case_id"])
    note_type = case["note_type"]
    matches = ext.extract(
        case["note_row_id"],
        case["research_id"],
        note_type,
        case["note_text"],
        note_date=case.get("note_date"),
        domain=case["domain"],
    )
    run_id = "gold-regression-run"
    src = _minimal_source_row(case)
    stamped: list[dict[str, Any]] = []
    for m in matches:
        d = m.to_dict()
        d["entity_domain"] = case["domain"]
        stamped.append(_stamp_row(ext, d, run_id, src, is_llm=True))
    return stamped


def _load_expected(case_id: str) -> list[dict[str, Any]]:
    data = json.loads((FIXTURE_ROOT / "expected" / f"{case_id}.json").read_text(encoding="utf-8"))
    assert isinstance(data, list)
    return data


def _assert_row_subset(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    for key, exp_val in expected.items():
        act_val = actual.get(key)
        if key == "confidence" or key == "date_confidence":
            if exp_val is None:
                assert act_val is None
            else:
                assert act_val is not None
                assert float(act_val) == pytest.approx(float(exp_val))
        else:
            assert act_val == exp_val, f"{key!r}: got {act_val!r}, expected {exp_val!r}"


@pytest.mark.parametrize("case", CASES, ids=[c["case_id"] for c in CASES])
def test_case_matches_golden_normalized_rows(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    stamped = _rows_for_case(case, monkeypatch)
    expected_rows = _load_expected(case["case_id"])
    assert len(stamped) == len(expected_rows)
    is_op = case["note_type"] in frozenset({"op_note", "OPNOTE"})
    ext = _offline_extractor()
    want_prompt_ver = ext._prompt_version(operative=is_op, domain=case["domain"])
    for stamped_row, exp in zip(stamped, expected_rows, strict=True):
        assert stamped_row["prompt_version"] == want_prompt_ver
        assert stamped_row["llm_prompt_version"] == want_prompt_ver
        assert stamped_row["extracted_at"] == _FIXED_EXTRACTED_AT.isoformat()
        assert stamped_row["extraction_timestamp_utc"] == _FIXED_EXTRACTED_AT.isoformat()
        assert stamped_row["extraction_run_id"] == "gold-regression-run"
        _assert_row_subset(stamped_row, exp)


@pytest.mark.parametrize("case", CASES, ids=[c["case_id"] for c in CASES])
def test_evidence_substring_policy(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    stamped = _rows_for_case(case, monkeypatch)
    note_text = case["note_text"]
    for row in stamped:
        if row["verification_status"] == "rejected":
            assert row["verification_step"] == "substring_check"
            assert case["case_id"] == "labs_bad_evidence"
        else:
            assert row["evidence_span"] in note_text


@pytest.mark.parametrize("case", CASES, ids=[c["case_id"] for c in CASES])
def test_entity_schema_columns_exact(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    stamped = _rows_for_case(case, monkeypatch)
    df = pd.DataFrame(stamped)
    for col in ENTITY_SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[ENTITY_SCHEMA_COLUMNS]
    BaseExtractor.validate_output(df)
    assert list(df.columns) == list(ENTITY_SCHEMA_COLUMNS)


def test_sort_entities_deterministic_stable_when_shuffled(monkeypatch: pytest.MonkeyPatch) -> None:
    all_rows: list[dict[str, Any]] = []
    for case in CASES:
        all_rows.extend(_rows_for_case(case, monkeypatch))
    df = pd.DataFrame(all_rows)
    for col in ENTITY_SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[ENTITY_SCHEMA_COLUMNS]
    s1 = sort_entities_deterministic(df.sample(frac=1, random_state=7).reset_index(drop=True))
    s2 = sort_entities_deterministic(df.sample(frac=1, random_state=42).reset_index(drop=True))
    pd.testing.assert_frame_equal(s1, s2)


def test_duckdb_register_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    stamped: list[dict[str, Any]] = []
    for case in CASES:
        stamped.extend(_rows_for_case(case, monkeypatch))
    df = pd.DataFrame(stamped)
    for col in ENTITY_SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[ENTITY_SCHEMA_COLUMNS]
    con = duckdb.connect(database=":memory:")
    try:
        con.register("entities", df)
        row_n = con.execute("SELECT COUNT(*) FROM entities").fetchone()
        assert row_n is not None
        n = row_n[0]
        assert int(n) == len(df)
    finally:
        con.close()


def test_llm_extractor_unavailable_without_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_llm_env(monkeypatch)
    ext = LLMExtractor()
    assert not ext.available
