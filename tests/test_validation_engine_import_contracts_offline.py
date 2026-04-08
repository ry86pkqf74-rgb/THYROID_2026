"""Offline checks that ``scripts/29_validation_engine.py`` registry and release contracts stay wired."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_validation_engine_module():
    path = ROOT / "scripts" / "29_validation_engine.py"
    spec = importlib.util.spec_from_file_location("validation_engine_29", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def v29():
    return _load_validation_engine_module()


def test_all_validation_sql_includes_lab_provenance_and_linkage_surfaces(v29):
    names = [t[0] for t in v29.ALL_VALIDATION_SQL]
    assert "val_lab_canonical_v1" in names
    assert "val_provenance_traceability" in names
    assert "val_fact_provenance_v1" in names
    assert "val_unlinked_linkable" in names
    assert "val_chronology_anomalies" in names


def test_fact_release_threshold_contract_keys(v29):
    th = v29.FACT_RELEASE_THRESHOLDS
    assert th["pct_quarantine_max"] == 5.0
    assert 0 <= th["pct_source_file_id_min"] <= 100
    for k, v in th.items():
        assert isinstance(v, float | int), k
