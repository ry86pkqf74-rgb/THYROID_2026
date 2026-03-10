"""Tests for utils/text_helpers.py — normalisation and hashing."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.text_helpers import (
    clean_research_id,
    make_note_row_id,
    standardize_columns,
    strip_phi,
    to_snake_case,
)


class TestResearchIdNormalization:
    def test_float_format_stripped(self):
        df = pd.DataFrame({"research_id": ["123.0", "456.0", "789.0"]})
        result = clean_research_id(df)
        assert list(result["research_id"]) == [123, 456, 789]

    def test_pure_int_strings(self):
        df = pd.DataFrame({"research_id": ["1", "22", "333"]})
        result = clean_research_id(df)
        assert list(result["research_id"]) == [1, 22, 333]

    def test_invalid_ids_dropped(self):
        df = pd.DataFrame({"research_id": ["123", "abc", "", None, "456"]})
        result = clean_research_id(df)
        assert list(result["research_id"]) == [123, 456]
        assert len(result) == 2

    def test_whitespace_stripped(self):
        df = pd.DataFrame({"research_id": [" 100 ", "200\t"]})
        result = clean_research_id(df)
        assert list(result["research_id"]) == [100, 200]

    def test_no_research_id_column(self):
        df = pd.DataFrame({"patient_id": [1, 2]})
        result = clean_research_id(df)
        assert "patient_id" in result.columns


class TestSnakeCaseConversion:
    def test_basic_conversion(self):
        assert to_snake_case("Research ID Number") == "research_id_number"

    def test_special_chars(self):
        assert to_snake_case("H&P-1") == "h_p_1"

    def test_multiple_spaces(self):
        assert to_snake_case("Other  notes") == "other_notes"

    def test_leading_trailing(self):
        assert to_snake_case("  Age at Surgery  ") == "age_at_surgery"


class TestStandardizeColumns:
    def test_research_id_alias(self):
        df = pd.DataFrame({"Research ID Number": [1], "Age": [55]})
        result = standardize_columns(df)
        assert "research_id" in result.columns
        assert "age" in result.columns

    def test_duplicate_handling(self):
        df = pd.DataFrame({"A": [1], "a": [2]})
        result = standardize_columns(df)
        assert len(result.columns) == 2
        assert "a" in result.columns
        assert "a_1" in result.columns

    def test_record_id_alias(self):
        df = pd.DataFrame({"Record_ID": [99]})
        result = standardize_columns(df)
        assert "research_id" in result.columns


class TestStripPhi:
    def test_phi_columns_removed(self):
        df = pd.DataFrame({
            "research_id": [1],
            "patient_last_nm": ["Smith"],
            "euh_mrn": ["12345"],
            "age": [55],
        })
        result = strip_phi(df)
        assert "patient_last_nm" not in result.columns
        assert "euh_mrn" not in result.columns
        assert "research_id" in result.columns
        assert "age" in result.columns


class TestNoteRowId:
    def test_deterministic(self):
        h1 = make_note_row_id(123, "Sheet2", "h_p_1")
        h2 = make_note_row_id(123, "Sheet2", "h_p_1")
        assert h1 == h2

    def test_different_inputs_differ(self):
        h1 = make_note_row_id(123, "Sheet2", "h_p_1")
        h2 = make_note_row_id(123, "Sheet2", "h_p_2")
        assert h1 != h2

    def test_is_hex_sha1(self):
        h = make_note_row_id(1, "S", "c")
        assert len(h) == 40
        int(h, 16)  # valid hex
