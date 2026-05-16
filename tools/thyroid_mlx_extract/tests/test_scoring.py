"""Smoke tests for scoring + schemas.

These tests don't require mlx-lm (so they run on CI / non-Mac). They validate:
- Schemas parse correctly
- Scoring math (TP/FP/FN/F1) is correct
- JSON-block extraction handles prose around JSON
"""
import json
import tempfile
from pathlib import Path

import pandas as pd

from thyroid_mlx_extract.eval import scoring
from thyroid_mlx_extract.models.extractor import _extract_json_block
from thyroid_mlx_extract.schemas.molecular import Extraction as MolecularExtraction
from thyroid_mlx_extract.schemas.synoptic import Extraction as SynopticExtraction


def test_molecular_schema_minimal():
    e = MolecularExtraction(no_alterations_detected=True)
    assert e.no_alterations_detected is True
    assert e.variants == []


def test_molecular_schema_with_variant():
    e = MolecularExtraction(
        assay_name="ThyroSeq GC v3",
        variants=[
            {
                "gene": "BRAF",
                "protein_change": "V600E",
                "cdna_change": "c.1799T>A",
                "variant_allele_frequency_pct": 32.5,
                "classification": "pathogenic",
                "evidence_text": "BRAF V600E detected at VAF 32.5%",
                "confidence": 0.98,
            }
        ],
    )
    assert e.variants[0].gene == "BRAF"
    assert e.variants[0].variant_allele_frequency_pct == 32.5


def test_synoptic_schema_basic():
    e = SynopticExtraction(tumors=[{"tumor_index": 1, "ete_grade": "microscopic_only"}])
    assert e.tumors[0].tumor_index == 1
    assert e.tumors[0].ete_grade == "microscopic_only"


def test_json_block_extraction_with_prose():
    text = "Sure, here is the result: {\"a\": 1, \"b\": {\"c\": 2}} hope that helps!"
    block = _extract_json_block(text)
    assert json.loads(block) == {"a": 1, "b": {"c": 2}}


def test_json_block_handles_nested_braces():
    text = '{"x": "}{", "y": {"z": "{"}}'
    block = _extract_json_block(text)
    parsed = json.loads(block)
    assert parsed["x"] == "}{"


def test_scoring_basic_perfect():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Gold
        pd.DataFrame(
            [
                {"source_pk": "A", "field_path": "ete_grade", "gold_value": "microscopic_only", "gold_evidence_substring": ""},
                {"source_pk": "A", "field_path": "ki67", "gold_value": "5", "gold_evidence_substring": ""},
                {"source_pk": "B", "field_path": "ete_grade", "gold_value": "none", "gold_evidence_substring": ""},
            ]
        ).to_csv(td / "gold.csv", index=False)
        # Source
        (td / "src.jsonl").write_text(
            json.dumps({"source_pk": "A", "source_text": "microscopic_only 5"}) + "\n"
            + json.dumps({"source_pk": "B", "source_text": "none"}) + "\n"
        )
        # Perfect predictions
        (td / "pred.jsonl").write_text(
            json.dumps({
                "source_pk": "A", "model_name": "test", "success": True,
                "result": {"ete_grade": "microscopic_only", "ki67": "5"},
                "elapsed_seconds": 0.1,
            }) + "\n"
            + json.dumps({
                "source_pk": "B", "model_name": "test", "success": True,
                "result": {"ete_grade": "none"},
                "elapsed_seconds": 0.1,
            }) + "\n"
        )
        r = scoring.score(td / "gold.csv", td / "pred.jsonl", td / "src.jsonl")
        assert r.macro_f1 == 1.0
        assert r.parse_success_rate == 1.0


def test_scoring_handles_misses():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        pd.DataFrame(
            [
                {"source_pk": "A", "field_path": "x", "gold_value": "yes", "gold_evidence_substring": ""},
                {"source_pk": "A", "field_path": "y", "gold_value": "5", "gold_evidence_substring": ""},
            ]
        ).to_csv(td / "gold.csv", index=False)
        (td / "src.jsonl").write_text(json.dumps({"source_pk": "A", "source_text": "yes 5"}) + "\n")
        # Pred gets x right, misses y entirely
        (td / "pred.jsonl").write_text(
            json.dumps({
                "source_pk": "A", "model_name": "test", "success": True,
                "result": {"x": "yes"},
                "elapsed_seconds": 0.1,
            }) + "\n"
        )
        r = scoring.score(td / "gold.csv", td / "pred.jsonl", td / "src.jsonl")
        # x: TP=1, y: FN=1
        f1s = {s.field: s.f1 for s in r.per_field}
        assert f1s["x"] == 1.0
        assert f1s["y"] == 0.0
