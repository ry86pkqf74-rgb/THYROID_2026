"""
Afirma ingest helpers and normalized layer builders.

Run: .venv/bin/python -m pytest tests/test_afirma_ingest.py -v
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from utils.afirma_helpers import (
    canonicalize_afirma_columns,
    compute_afirma_row_hash,
    default_crosswalk_for_tests,
    expand_xpression_variants,
    harmonize_calls,
    parse_xpression_payload,
    resolve_afirma_assay_key,
)

ROOT = Path(__file__).resolve().parent.parent
FIX = ROOT / "tests" / "fixtures" / "afirma"


def _load_ingest42():
    p = ROOT / "scripts" / "42_ingest_afirma.py"
    spec = importlib.util.spec_from_file_location("ingest42", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


class TestCanonicalizeColumns:
    def test_aliases(self):
        df = pd.DataFrame(
            {
                "Research ID": [1],
                "Pt MRN": ["12"],
                "GEC_Result": ["Benign"],
                "specimen_key": ["S1"],
            },
        )
        out = canonicalize_afirma_columns(df)
        assert "research_id" in out.columns
        assert "mrn" in out.columns
        assert "gec_call" in out.columns
        assert "specimen_id" in out.columns


class TestAfirmaRowHash:
    def test_stable(self):
        base = {
            "research_id": 1,
            "mrn": "a",
            "dob": None,
            "specimen_id": "s",
            "accession": None,
            "test_date": None,
            "gec_call": None,
            "gsc_call": None,
            "panel_type": None,
            "bethesda": None,
            "xpression_variants": None,
        }
        h1 = compute_afirma_row_hash(base)
        h2 = compute_afirma_row_hash(base)
        assert h1 == h2
        assert len(h1) == 24


class TestHarmonizeCalls:
    def test_mapped(self):
        xw = default_crosswalk_for_tests()
        h, ok = harmonize_calls("Suspicious", xw)
        assert ok and h == "suspicious"

    def test_unmapped(self):
        xw = default_crosswalk_for_tests()
        h, ok = harmonize_calls("TotallyNonstandardLabel", xw)
        assert h is None and not ok


class TestXpressionPayload:
    def test_expand_snv(self):
        xw = default_crosswalk_for_tests()
        raw = [{"gene_symbol": "BRAF", "variant_class": "SNV", "protein_hgvs": "p.V600E"}]
        rows = expand_xpression_variants(raw, xw)
        assert len(rows) == 1
        assert rows[0]["gene_symbol"] == "BRAF"
        assert rows[0]["variant_class"] == "SNV"

    def test_parse_json_string(self):
        s = '[{"gene":"NRAS","variant_class":"SNV"}]'
        assert len(parse_xpression_payload(s)) == 1


class TestIngestIntegration:
    def test_panel_only_csv_produces_one_result_no_variants(self):
        mod = _load_ingest42()
        df0 = pd.read_csv(FIX / "panel_only.csv")
        raw = mod.ingest_afirma_frame(df0, "panel_only.csv")
        matches = pd.DataFrame(
            [
                {
                    "row_hash": raw.iloc[0]["row_hash"],
                    "matched_research_id": 999001,
                    "match_method": "source_research_id",
                    "match_confidence": 1.0,
                    "review_required": False,
                    "review_reason": "",
                },
            ],
        )
        xw = default_crosswalk_for_tests()
        assay = {k: v for k, v in mod.__dict__.items() if k == "ROOT"}  # dummy
        from utils.afirma_helpers import EMBEDDED_ASSAY_BY_KEY

        mr, mvl = mod.build_normalized_molecular_layers(raw, matches, xw, EMBEDDED_ASSAY_BY_KEY)
        assert len(mr) == 1
        assert len(mvl) == 0
        assert mr.iloc[0]["risk_call"] == "suspicious"
        assert "GEC:suspicious" in (mr.iloc[0]["interpretation_summary"] or "")

    def test_xa_row_emits_variant_long(self):
        mod = _load_ingest42()
        df0 = pd.read_csv(FIX / "with_xa_variants.csv")
        raw = mod.ingest_afirma_frame(df0, "xa.csv")
        matches = pd.DataFrame(
            [
                {
                    "row_hash": raw.iloc[0]["row_hash"],
                    "matched_research_id": 999002,
                    "match_method": "source_research_id",
                    "match_confidence": 1.0,
                    "review_required": False,
                    "review_reason": "",
                },
            ],
        )
        xw = default_crosswalk_for_tests()
        from utils.afirma_helpers import EMBEDDED_ASSAY_BY_KEY

        mr, mvl = mod.build_normalized_molecular_layers(raw, matches, xw, EMBEDDED_ASSAY_BY_KEY)
        assert len(mr) == 1
        assert len(mvl) == 1
        assert mvl.iloc[0]["gene_symbol"] == "BRAF"

    def test_unmapped_call_flags_qc(self):
        mod = _load_ingest42()
        df0 = pd.read_csv(FIX / "unmapped_call.csv")
        raw = mod.ingest_afirma_frame(df0, "u.csv")
        matches = pd.DataFrame(
            [
                {
                    "row_hash": raw.iloc[0]["row_hash"],
                    "matched_research_id": 999003,
                    "match_method": "source_research_id",
                    "match_confidence": 1.0,
                    "review_required": False,
                    "review_reason": "",
                },
            ],
        )
        xw = default_crosswalk_for_tests()
        from utils.afirma_helpers import EMBEDDED_ASSAY_BY_KEY

        mr, _ = mod.build_normalized_molecular_layers(raw, matches, xw, EMBEDDED_ASSAY_BY_KEY)
        flags = mr.iloc[0]["qc_flags"]
        assert "unmapped_gec_call" in flags


class TestAssayKeyResolution:
    def test_panel_type(self):
        xw = default_crosswalk_for_tests()
        assert resolve_afirma_assay_key("GEC+GSC", xw) == "afirma_combined"
