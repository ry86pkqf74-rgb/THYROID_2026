"""In-memory contract tests for ``longitudinal_lab_canonical_v1`` (runs in CI without local DB)."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

_TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(_TESTS))

from lab_canonical_contract import (
    ALLOWED_DATE_STATUSES,
    ALLOWED_TIERS,
    PLAUSIBILITY_BOUNDS,
    REQUIRED_COLUMNS,
)


def _minimal_lab_table_sql() -> str:
    parts = [
        "research_id BIGINT",
        "lab_date DATE",
        "lab_date_status VARCHAR",
        "lab_name_raw VARCHAR",
        "lab_name_standardized VARCHAR",
        "analyte_group VARCHAR",
        "value_raw VARCHAR",
        "value_numeric DOUBLE",
        "unit_raw VARCHAR",
        "unit_standardized VARCHAR",
        "reference_range VARCHAR",
        "abnormal_flag VARCHAR",
        "is_censored BOOLEAN",
        "source_table VARCHAR",
        "source_script VARCHAR",
        "ingestion_wave BIGINT",
        "data_completeness_tier VARCHAR",
        "provenance_note VARCHAR",
    ]
    return f"CREATE TABLE longitudinal_lab_canonical_v1 ({', '.join(parts)})"


@pytest.fixture()
def mem_con():
    con = duckdb.connect(database=":memory:")
    con.execute(_minimal_lab_table_sql())
    yield con
    con.close()


def _insert_row(
    con: duckdb.DuckDBPyConnection,
    *,
    std: str = "thyroglobulin",
    val: float = 1.5,
    tier: str = "current_structured",
    lab_date: str = "2020-06-01",
    status: str = "exact_collection_date",
) -> None:
    con.execute(
        """
        INSERT INTO longitudinal_lab_canonical_v1 VALUES (
            ?::BIGINT, ?::DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            1,
            lab_date,
            status,
            std,
            std,
            "thyroid_tumor_markers",
            str(val),
            val,
            "ng/mL",
            "ng/mL",
            None,
            None,
            False,
            "test",
            "77",
            1,
            tier,
            None,
        ],
    )


def test_schema_contract_column_list_matches_script_expectations():
    con = duckdb.connect(database=":memory:")
    con.execute(_minimal_lab_table_sql())
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'longitudinal_lab_canonical_v1'"
    ).fetchall()}
    con.close()
    missing = set(REQUIRED_COLUMNS) - cols
    assert not missing, f"missing {missing}"


def test_tiers_and_date_status_enforced_in_memory(mem_con):
    _insert_row(mem_con)
    tiers = {r[0] for r in mem_con.execute(
        "SELECT DISTINCT data_completeness_tier FROM longitudinal_lab_canonical_v1"
    ).fetchall()}
    assert not (tiers - ALLOWED_TIERS)
    statuses = {r[0] for r in mem_con.execute(
        "SELECT DISTINCT lab_date_status FROM longitudinal_lab_canonical_v1"
    ).fetchall()}
    assert not (statuses - ALLOWED_DATE_STATUSES)


def test_plausibility_bounds_detect_out_of_range(mem_con):
    lo, hi = PLAUSIBILITY_BOUNDS["thyroglobulin"]
    _insert_row(mem_con, val=float(hi) + 1.0)
    n = mem_con.execute(
        "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 "
        "WHERE lab_name_standardized = 'thyroglobulin' "
        "  AND value_numeric IS NOT NULL "
        f"  AND (value_numeric < {lo} OR value_numeric > {hi})"
    ).fetchone()[0]
    assert n == 1
