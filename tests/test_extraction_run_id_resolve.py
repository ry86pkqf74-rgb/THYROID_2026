"""Tests for extraction_run_id timeline resolution."""

from __future__ import annotations

import pandas as pd

from utils.extraction_run_id_resolve import (
    backfill_extraction_run_id_column,
    resolve_extraction_run_id_series,
)


def test_preserves_existing_nonblank():
    runs = pd.DataFrame(
        [
            {"run_id": "early", "started_at": "2026-04-01T10:00:00+00:00", "success": True},
            {"run_id": "late", "started_at": "2026-04-01T12:00:00+00:00", "success": True},
        ]
    )
    df = pd.DataFrame(
        {
            "extraction_run_id": ["keep-me"],
            "extracted_at": ["2026-04-01T11:00:00+00:00"],
        }
    )
    out = resolve_extraction_run_id_series(df, runs)
    assert out.iloc[0] == "keep-me"


def test_latest_run_before_extracted_at():
    runs = pd.DataFrame(
        [
            {"run_id": "early", "started_at": "2026-04-01T10:00:00+00:00", "success": True},
            {"run_id": "late", "started_at": "2026-04-01T12:00:00+00:00", "success": True},
        ]
    )
    df = pd.DataFrame(
        {
            "extraction_run_id": [None],
            "extracted_at": ["2026-04-01T11:00:00+00:00"],
        }
    )
    out = resolve_extraction_run_id_series(df, runs)
    assert out.iloc[0] == "early"


def test_pre_telemetry_fallback_to_first_successful():
    runs = pd.DataFrame(
        [
            {"run_id": "first", "started_at": "2026-04-01T10:00:00+00:00", "success": True},
            {"run_id": "second", "started_at": "2026-04-01T12:00:00+00:00", "success": True},
        ]
    )
    df = pd.DataFrame(
        {
            "extraction_run_id": [None],
            "extracted_at": ["2026-03-31T23:00:00+00:00"],
        }
    )
    out = resolve_extraction_run_id_series(df, runs)
    assert out.iloc[0] == "first"


def test_skips_unsuccessful_runs_when_successful_exist():
    runs = pd.DataFrame(
        [
            {"run_id": "bad", "started_at": "2026-04-01T09:00:00+00:00", "success": False},
            {"run_id": "good", "started_at": "2026-04-01T10:00:00+00:00", "success": True},
        ]
    )
    df = pd.DataFrame(
        {
            "extraction_run_id": [None],
            "extracted_at": ["2026-04-01T10:30:00+00:00"],
        }
    )
    out = resolve_extraction_run_id_series(df, runs)
    assert out.iloc[0] == "good"


def test_backfill_mutates_column():
    runs = pd.DataFrame(
        [
            {"run_id": "a", "started_at": "2026-04-01T10:00:00+00:00", "success": True},
        ]
    )
    df = pd.DataFrame(
        {
            "extraction_run_id": [None, "x"],
            "extracted_at": ["2026-04-01T11:00:00+00:00", "2026-04-01T11:00:00+00:00"],
        }
    )
    out = backfill_extraction_run_id_column(df, runs)
    assert out["extraction_run_id"].iloc[0] == "a"
    assert out["extraction_run_id"].iloc[1] == "x"
