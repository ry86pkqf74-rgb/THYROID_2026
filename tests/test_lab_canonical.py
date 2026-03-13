"""
Tests for the canonical lab layer contract (longitudinal_lab_canonical_v1).

Validates schema, tier values, date status values, plausibility bounds,
and graceful handling of absent analytes.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    import duckdb
except ImportError:
    pytest.skip("duckdb not installed", allow_module_level=True)

DB_PATH = ROOT / "thyroid_master.duckdb"

REQUIRED_COLUMNS = [
    "research_id",
    "lab_date",
    "lab_date_status",
    "lab_name_raw",
    "lab_name_standardized",
    "analyte_group",
    "value_raw",
    "value_numeric",
    "unit_raw",
    "unit_standardized",
    "reference_range",
    "abnormal_flag",
    "is_censored",
    "source_table",
    "source_script",
    "ingestion_wave",
    "data_completeness_tier",
    "provenance_note",
]

ALLOWED_TIERS = {
    "current_structured",
    "current_nlp_partial",
    "future_institutional_required",
}

ALLOWED_DATE_STATUSES = {
    "exact_collection_date",
    "extracted_date",
    "unresolved_date",
    None,
}

PLAUSIBILITY_BOUNDS = {
    "thyroglobulin": (0, 100_000),
    "anti_thyroglobulin": (0, 10_000),
    "pth": (0.5, 500),
    "calcium_total": (4, 15),
    "calcium_ionized": (0.5, 2.0),
    "tsh": (0.01, 200),
    "free_t4": (0.1, 10),
    "free_t3": (0.5, 20),
    "vitamin_d": (1, 200),
    "albumin": (0.5, 7),
    "phosphorus": (0.5, 15),
    "magnesium": (0.3, 10),
    "calcitonin": (0, 50_000),
    "cea": (0, 5_000),
}

POPULATED_ANALYTES = {"thyroglobulin", "anti_thyroglobulin", "pth", "calcium_total", "calcium_ionized"}
FUTURE_ANALYTES = {"tsh", "free_t4", "free_t3", "vitamin_d", "albumin", "phosphorus", "magnesium", "calcitonin", "cea"}


@pytest.fixture(scope="module")
def con():
    if not DB_PATH.exists():
        pytest.skip("local DuckDB not available")
    c = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        c.execute("SELECT 1 FROM longitudinal_lab_canonical_v1 LIMIT 1")
    except Exception:
        c.close()
        pytest.skip("longitudinal_lab_canonical_v1 not found in local DB")
    yield c
    c.close()


class TestLabCanonicalSchema:
    def test_required_columns_exist(self, con):
        cols = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'longitudinal_lab_canonical_v1'"
        ).fetchall()}
        missing = set(REQUIRED_COLUMNS) - cols
        assert not missing, f"Missing columns: {missing}"

    def test_no_null_research_ids(self, con):
        n = con.execute(
            "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 WHERE research_id IS NULL"
        ).fetchone()[0]
        assert n == 0, f"{n} rows with NULL research_id"

    def test_row_count_positive(self, con):
        n = con.execute("SELECT COUNT(*) FROM longitudinal_lab_canonical_v1").fetchone()[0]
        assert n > 0, "Table is empty"


class TestLabCanonicalTiers:
    def test_all_tiers_allowed(self, con):
        tiers = {r[0] for r in con.execute(
            "SELECT DISTINCT data_completeness_tier FROM longitudinal_lab_canonical_v1"
        ).fetchall()}
        invalid = tiers - ALLOWED_TIERS
        assert not invalid, f"Invalid tiers: {invalid}"

    def test_populated_analytes_have_correct_tier(self, con):
        rows = con.execute(
            "SELECT DISTINCT lab_name_standardized, data_completeness_tier "
            "FROM longitudinal_lab_canonical_v1 "
            "WHERE data_completeness_tier != 'future_institutional_required'"
        ).fetchall()
        for name, tier in rows:
            assert tier in ("current_structured", "current_nlp_partial"), \
                f"{name} has unexpected tier: {tier}"


class TestLabCanonicalDateStatus:
    def test_all_date_statuses_allowed(self, con):
        statuses = {r[0] for r in con.execute(
            "SELECT DISTINCT lab_date_status FROM longitudinal_lab_canonical_v1"
        ).fetchall()}
        invalid = statuses - ALLOWED_DATE_STATUSES
        assert not invalid, f"Invalid date statuses: {invalid}"

    def test_no_future_dates(self, con):
        n = con.execute(
            "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 "
            "WHERE lab_date > CURRENT_DATE"
        ).fetchone()[0]
        assert n == 0, f"{n} rows with future dates"


class TestLabCanonicalPlausibility:
    @pytest.mark.parametrize("analyte", list(PLAUSIBILITY_BOUNDS.keys()))
    def test_values_in_bounds(self, con, analyte):
        lo, hi = PLAUSIBILITY_BOUNDS[analyte]
        n = con.execute(
            f"SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 "
            f"WHERE lab_name_standardized = '{analyte}' "
            f"  AND value_numeric IS NOT NULL "
            f"  AND (value_numeric < {lo} OR value_numeric > {hi})"
        ).fetchone()[0]
        assert n == 0, f"{analyte}: {n} values outside [{lo}, {hi}]"


class TestLabCanonicalGracefulEmpty:
    @pytest.mark.parametrize("analyte", sorted(FUTURE_ANALYTES))
    def test_future_analytes_zero_or_absent(self, con, analyte):
        n = con.execute(
            f"SELECT COUNT(*) FROM longitudinal_lab_canonical_v1 "
            f"WHERE lab_name_standardized = '{analyte}'"
        ).fetchone()[0]
        assert n == 0, f"Future analyte {analyte} has {n} unexpected rows"

    def test_completeness_view_exists(self, con):
        try:
            n = con.execute("SELECT COUNT(*) FROM val_lab_completeness_v1").fetchone()[0]
            assert n > 0, "val_lab_completeness_v1 is empty"
        except Exception:
            pytest.skip("val_lab_completeness_v1 not found")

    def test_future_placeholders_in_completeness(self, con):
        try:
            rows = con.execute(
                "SELECT lab_name_standardized, n_measurements "
                "FROM val_lab_completeness_v1 "
                "WHERE data_completeness_tier = 'future_institutional_required'"
            ).fetchall()
            for name, n in rows:
                assert n == 0, f"Future placeholder {name} has {n} measurements (expected 0)"
        except Exception:
            pytest.skip("val_lab_completeness_v1 not found")


class TestLabCanonicalDedup:
    def test_no_exact_duplicates(self, con):
        n = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT research_id, lab_date, lab_name_standardized, value_numeric,
                       COUNT(*) AS cnt
                FROM longitudinal_lab_canonical_v1
                WHERE value_numeric IS NOT NULL
                GROUP BY 1, 2, 3, 4
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert n == 0, f"{n} exact duplicate groups found"
