"""Runs ``VAL_LAB_CANONICAL_SQL`` from script 29 against an in-memory lab stub (CI-safe)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_validation_engine_module():
    path = ROOT / "scripts" / "29_validation_engine.py"
    spec = importlib.util.spec_from_file_location("validation_engine_29b", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def v29():
    return _load_validation_engine_module()


def _create_lab_stub(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE longitudinal_lab_canonical_v1 (
            research_id BIGINT,
            lab_date DATE,
            lab_date_status VARCHAR,
            lab_name_raw VARCHAR,
            lab_name_standardized VARCHAR,
            analyte_group VARCHAR,
            value_raw VARCHAR,
            value_numeric DOUBLE,
            is_censored BOOLEAN,
            data_completeness_tier VARCHAR
        )
    """)


def _row(std: str, val: float, tier: str, dt: str = "2020-01-15") -> list:
    return [1, dt, "exact_collection_date", std, std, "g", str(val), val, False, tier]


def test_val_lab_canonical_sql_passes_clean_stub(v29):
    con = duckdb.connect(database=":memory:")
    _create_lab_stub(con)
    con.execute(
        "INSERT INTO longitudinal_lab_canonical_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        _row("thyroglobulin", 2.5, "current_structured"),
    )
    con.execute(v29.VAL_LAB_CANONICAL_SQL)
    statuses = [r[0] for r in con.execute(
        "SELECT DISTINCT validation_status FROM val_lab_canonical_v1"
    ).fetchall()]
    assert "FAIL" not in statuses
    con.close()


def test_val_lab_canonical_sql_warns_on_tg_oob(v29):
    con = duckdb.connect(database=":memory:")
    _create_lab_stub(con)
    con.execute(
        "INSERT INTO longitudinal_lab_canonical_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        _row("thyroglobulin", 200_000.0, "current_structured"),
    )
    con.execute(v29.VAL_LAB_CANONICAL_SQL)
    stats = con.execute(
        "SELECT validation_status, n_plausibility_violations FROM val_lab_canonical_v1"
    ).fetchall()
    assert any(s[0] == "WARN" and s[1] and s[1] > 0 for s in stats)
    con.close()


def test_val_lab_canonical_sql_fails_on_invalid_tier(v29):
    con = duckdb.connect(database=":memory:")
    _create_lab_stub(con)
    con.execute(
        "INSERT INTO longitudinal_lab_canonical_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        _row("thyroglobulin", 1.0, "not_a_valid_tier"),
    )
    con.execute(v29.VAL_LAB_CANONICAL_SQL)
    statuses = [r[0] for r in con.execute(
        "SELECT DISTINCT validation_status FROM val_lab_canonical_v1"
    ).fetchall()]
    assert "FAIL" in statuses
    con.close()
