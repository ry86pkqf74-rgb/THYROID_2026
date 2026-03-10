"""Tests for notes_extraction/vocab.py — controlled vocabularies."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.vocab import (
    COMPLICATION_NORM,
    COMPLICATION_TYPES,
    ENTITY_SCHEMA_COLUMNS,
    GENE_NAMES,
    GENE_NORM,
    MEDICATION_NORM,
    MEDICATION_TYPES,
    NOTE_TYPES,
    PROCEDURE_NORM,
    PROCEDURE_TYPES,
    PROBLEM_TYPES,
    STAGING_COMPONENTS,
)


class TestNormMapCoverage:
    def test_procedure_norm_values_in_types(self):
        for alias, norm in PROCEDURE_NORM.items():
            assert norm in PROCEDURE_TYPES, f"'{norm}' (from alias '{alias}') not in PROCEDURE_TYPES"

    def test_complication_norm_values_in_types(self):
        for alias, norm in COMPLICATION_NORM.items():
            assert norm in COMPLICATION_TYPES, f"'{norm}' (from alias '{alias}') not in COMPLICATION_TYPES"

    def test_medication_norm_values_in_types(self):
        for alias, norm in MEDICATION_NORM.items():
            assert norm in MEDICATION_TYPES, f"'{norm}' (from alias '{alias}') not in MEDICATION_TYPES"

    def test_gene_norm_values_in_names(self):
        extra = {"RAS"}
        for alias, norm in GENE_NORM.items():
            assert norm in GENE_NAMES | extra, f"'{norm}' (from alias '{alias}') not in GENE_NAMES"


class TestVocabFormat:
    def test_note_types_lowercase(self):
        for nt in NOTE_TYPES:
            assert nt == nt.lower(), f"note_type '{nt}' should be lowercase"

    def test_procedure_types_snake_case(self):
        for pt in PROCEDURE_TYPES:
            assert " " not in pt, f"procedure_type '{pt}' should not contain spaces"
            assert pt == pt.lower(), f"procedure_type '{pt}' should be lowercase"

    def test_complication_types_snake_case(self):
        for ct in COMPLICATION_TYPES:
            assert " " not in ct, f"complication_type '{ct}' should not contain spaces"
            assert ct == ct.lower(), f"complication_type '{ct}' should be lowercase"

    def test_staging_components_exist(self):
        assert "T_stage" in STAGING_COMPONENTS
        assert "N_stage" in STAGING_COMPONENTS
        assert "M_stage" in STAGING_COMPONENTS
        assert "overall_stage" in STAGING_COMPONENTS

    def test_schema_columns_complete(self):
        required = {
            "research_id", "note_row_id", "note_type", "entity_type",
            "entity_value_raw", "entity_value_norm", "present_or_negated",
            "confidence", "evidence_span", "evidence_start", "evidence_end",
            "entity_date", "note_date",
            "extraction_method", "extracted_at",
        }
        assert required == set(ENTITY_SCHEMA_COLUMNS)

    def test_norm_map_keys_lowercase(self):
        for key in PROCEDURE_NORM:
            assert key == key.lower(), f"PROCEDURE_NORM key '{key}' should be lowercase"
        for key in COMPLICATION_NORM:
            assert key == key.lower(), f"COMPLICATION_NORM key '{key}' should be lowercase"
        for key in MEDICATION_NORM:
            assert key == key.lower(), f"MEDICATION_NORM key '{key}' should be lowercase"
        for key in GENE_NORM:
            assert key == key.lower(), f"GENE_NORM key '{key}' should be lowercase"
