"""
Regression: governed molecular_results + molecular_variant_long match golden fixtures.

Golden JSON files use ingestion_run_id=golden_batch01 and fixed ingestion_ts (see DESIGN_MEMO).
"""

from __future__ import annotations

import importlib.util
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "tests" / "fixtures" / "molecular_ingest_golden"
FIX_AF = ROOT / "tests" / "fixtures" / "afirma"

FIX_TS = datetime(2026, 4, 8, 12, 0, 0)
FIX_BATCH = "golden_batch01"


def _load42():
    p = ROOT / "scripts" / "42_ingest_afirma.py"
    spec = importlib.util.spec_from_file_location("ingest42reg", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _load41():
    p = ROOT / "scripts" / "41_ingest_thyroseq_excel.py"
    spec = importlib.util.spec_from_file_location("ingest41reg", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _assert_frames_equal_jsonish(actual: pd.DataFrame, golden_path: Path) -> None:
    raw_txt = golden_path.read_text(encoding="utf-8").strip()
    if raw_txt == "[]" and len(actual) == 0:
        return
    expected = pd.read_json(golden_path)
    assert list(actual.columns) == list(expected.columns)
    assert len(actual) == len(expected)
    for c in actual.columns:
        av = actual[c].reset_index(drop=True)
        ev = expected[c].reset_index(drop=True)
        for i in range(len(av)):
            a, e = av.iloc[i], ev.iloc[i]
            if pd.isna(a) and pd.isna(e):
                continue
            if c in ("test_date_parsed", "ingestion_ts"):
                # ISO strings vs timestamps
                assert str(a) == str(e) or pd.to_datetime(a, errors="coerce") == pd.to_datetime(
                    e, errors="coerce"
                )
                continue
            assert (a == e) or (str(a) == str(e)), f"col={c} row={i} a={a!r} e={e!r}"


@pytest.mark.parametrize(
    "case,csv_name,rid",
    [
        ("panel_only", "panel_only.csv", 999001),
        ("with_xa", "with_xa_variants.csv", 999002),
        ("unmapped_call", "unmapped_call.csv", 999003),
    ],
)
def test_afirma_golden_molecular_layers(case: str, csv_name: str, rid: int) -> None:
    from utils.afirma_helpers import EMBEDDED_ASSAY_BY_KEY, default_crosswalk_for_tests

    mod = _load42()
    mod.BATCH_ID = FIX_BATCH

    df0 = pd.read_csv(FIX_AF / csv_name)
    raw = mod.ingest_afirma_frame(df0, csv_name)
    rh = raw.iloc[0]["row_hash"]
    matches = pd.DataFrame(
        [
            {
                "row_hash": rh,
                "matched_research_id": rid,
                "match_method": "source_research_id",
                "match_confidence": 1.0,
                "review_required": False,
                "review_reason": "",
            },
        ],
    )
    mr, mvl = mod.build_normalized_molecular_layers(
        raw, matches, default_crosswalk_for_tests(), EMBEDDED_ASSAY_BY_KEY,
    )
    mr = mr.copy()
    if len(mr):
        mr["ingestion_ts"] = FIX_TS
        mr["ingestion_run_id"] = FIX_BATCH
    mvl = mvl.copy()
    if len(mvl):
        mvl["ingestion_ts"] = FIX_TS

    _assert_frames_equal_jsonish(mr, GOLD / f"afirma_{case}_molecular_results.json")
    _assert_frames_equal_jsonish(mvl, GOLD / f"afirma_{case}_molecular_variant_long.json")


def test_thyroseq_synthetic_golden() -> None:
    from utils.thyroseq_helpers import compute_row_hash, normalize_dob, normalize_mrn, normalize_name

    mod = _load41()
    mod.BATCH_ID = FIX_BATCH

    base = {
        "Req Patient/Source Name": "DOE, JANE",
        "Pt. MRN": 123456,
        "Date of Birth": date(1980, 5, 1),
        "Thyroseq Mutation": "BRAF V600E positive",
        "Gene Fusions": None,
        "Pathology": "PTC",
        "Copy Number Alterations": None,
        "Gene Expression Profile": "positive",
        "ThyroSeq Test Date": date(2023, 11, 15),
        "source_file": "synthetic.xlsx",
        "source_sheet": "Sheet1",
        "source_row_number": 2,
        "ingestion_batch_id": FIX_BATCH,
        "imported_at": FIX_TS.isoformat(),
    }
    raw = pd.DataFrame([base])
    np = raw["Req Patient/Source Name"].apply(normalize_name)
    raw["mrn_norm"] = raw["Pt. MRN"].apply(normalize_mrn)
    raw["dob_norm"] = raw["Date of Birth"].apply(normalize_dob)
    raw["name_norm"] = np.apply(lambda d: d["name_norm"])
    raw["last_name_norm"] = np.apply(lambda d: d["last_name_norm"])
    raw["first_name_norm"] = np.apply(lambda d: d["first_name_norm"])
    raw["row_hash"] = raw.apply(lambda r: compute_row_hash(r.to_dict()), axis=1)

    matches = pd.DataFrame(
        [
            {
                "row_hash": raw.iloc[0]["row_hash"],
                "matched_research_id": 888001,
                "match_method": "exact_mrn_dob_name",
                "match_confidence": 1.0,
                "review_required": False,
                "review_reason": "",
            },
        ],
    )
    mr, mvl = mod.build_normalized_molecular_layers(raw, matches)
    mr = mr.copy()
    mr["ingestion_ts"] = FIX_TS
    mr["ingestion_run_id"] = FIX_BATCH
    mvl = mvl.copy()
    mvl["ingestion_ts"] = FIX_TS

    _assert_frames_equal_jsonish(mr, GOLD / "thyroseq_synthetic_molecular_results.json")
    _assert_frames_equal_jsonish(mvl, GOLD / "thyroseq_synthetic_molecular_variant_long.json")
