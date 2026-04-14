"""Offline tests for check_verified_layer_release_integrity (119 release-mode)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_119():
    path = ROOT / "scripts" / "119_md_formalization_validate.py"
    spec = importlib.util.spec_from_file_location("md_formalization_validate_119", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod_119():
    return _load_119()


def _seed_passing_db(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS qa")
    con.execute(
        """
        CREATE TABLE main.canonical_extracted_fact_long_v2 AS
        SELECT * FROM (VALUES
            ('f1', 1, 'dom', 'n1', 'e1', 'v1', DATE '2020-01-01', 't1'),
            ('f2', 1, 'dom', 'n2', 'e2', 'v2', DATE '2020-01-02', 't1'),
            ('f3', 1, 'dom', 'n3', 'e3', 'v3', DATE '2020-01-03', 't1')
        ) AS t(fact_id, research_id, source_domain, source_object_id, entity_type, entity_value_norm, entity_date, release_tag)
        """
    )
    con.execute(
        """
        CREATE TABLE main.master_fact_long_verified_v1 AS
        SELECT * FROM main.canonical_extracted_fact_long_v2
        """
    )
    con.execute(
        "CREATE TABLE main.master_source_lineage_v1 AS SELECT * FROM main.master_fact_long_verified_v1"
    )
    con.execute(
        """
        CREATE TABLE main.master_patient_rollup_verified_v1 AS
        SELECT research_id, release_tag FROM main.master_fact_long_verified_v1 GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE TABLE qa.release_manifest (
            release_tag VARCHAR, git_sha VARCHAR, registry_version VARCHAR,
            tables_included VARCHAR, row_counts VARCHAR,
            created_at TIMESTAMP, created_by VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO qa.release_manifest VALUES
        ('t1', 'abc', NULL, NULL, NULL, current_timestamp, 'test')
        """
    )


def test_verified_layer_integrity_passes_minimal(mod_119) -> None:
    con = duckdb.connect(":memory:")
    try:
        _seed_passing_db(con)
        results = mod_119.ValidationResult("t")
        mod_119.check_verified_layer_release_integrity(con, results, strict=True)
        failed = [c for c in results.checks if c["status"] == "FAIL"]
        assert not failed, failed
    finally:
        con.close()


def test_verified_layer_integrity_fails_row_mismatch(mod_119) -> None:
    con = duckdb.connect(":memory:")
    try:
        _seed_passing_db(con)
        con.execute("INSERT INTO main.canonical_extracted_fact_long_v2 SELECT * FROM (VALUES ('f4', 2, 'd', 'x', 'e', 'v', NULL, 't1')) t(a,b,c,d,e,f,g,h)")
        results = mod_119.ValidationResult("t")
        mod_119.check_verified_layer_release_integrity(con, results, strict=True)
        failed = [c for c in results.checks if c["status"] == "FAIL"]
        assert any("canonical vs master_fact" in c["check"] for c in failed)
    finally:
        con.close()


def test_verified_layer_integrity_warns_natural_key_dupes(mod_119) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS qa")
        con.execute(
            """
            CREATE TABLE main.canonical_extracted_fact_long_v2 AS
            SELECT * FROM (VALUES
                ('f1', 1, 'dom', 'n1', 'e1', 'v1', DATE '2020-01-01', 't1'),
                ('f2', 1, 'dom', 'n1', 'e1', 'v1', DATE '2020-01-01', 't1')
            ) AS t(fact_id, research_id, source_domain, source_object_id, entity_type, entity_value_norm, entity_date, release_tag)
            """
        )
        con.execute("CREATE TABLE main.master_fact_long_verified_v1 AS SELECT * FROM main.canonical_extracted_fact_long_v2")
        con.execute("CREATE TABLE main.master_source_lineage_v1 AS SELECT * FROM main.master_fact_long_verified_v1")
        con.execute(
            """
            CREATE TABLE main.master_patient_rollup_verified_v1 AS
            SELECT research_id, release_tag FROM main.master_fact_long_verified_v1 GROUP BY 1, 2
            """
        )
        con.execute(
            """
            CREATE TABLE qa.release_manifest (
                release_tag VARCHAR, git_sha VARCHAR, registry_version VARCHAR,
                tables_included VARCHAR, row_counts VARCHAR,
                created_at TIMESTAMP, created_by VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO qa.release_manifest VALUES ('t1', 'abc', NULL, NULL, NULL, current_timestamp, 'test')"
        )
        results = mod_119.ValidationResult("t")
        mod_119.check_verified_layer_release_integrity(con, results, strict=True)
        warns = [c for c in results.checks if c["status"] == "WARN" and "natural-key" in c["check"]]
        assert warns
    finally:
        con.close()
