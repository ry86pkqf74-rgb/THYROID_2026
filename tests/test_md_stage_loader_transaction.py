"""Transactional rollback / commit tests for scripts/116_md_stage_loader.py (local DuckDB)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_116():
    path = ROOT / "scripts" / "116_md_stage_loader.py"
    spec = importlib.util.spec_from_file_location("md_stage_loader_116", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _v2_stage_table_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_schema = 'v2_stage'
        ORDER BY table_name
        """
    ).fetchall()
    return [r[0] for r in rows]


def test_rollback_after_inject_no_partial_v2_stage(tmp_path):
    mod = _load_116()
    pq_dir = tmp_path / "pq"
    pq_dir.mkdir()
    p1 = pq_dir / "st_a.parquet"
    p2 = pq_dir / "st_b.parquet"
    pd.DataFrame({"x": [10]}).to_parquet(p1)
    pd.DataFrame({"x": [20, 30]}).to_parquet(p2)
    domains = [
        ("dom_a", "st_a", "v2", "standard", p1),
        ("dom_b", "st_b", "v2", "standard", p2),
    ]
    db_path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        with pytest.raises(RuntimeError, match="__TEST_INJECT_AFTER_PARTIAL_LOAD__"):
            mod.run_v2_stage_transaction(
                con,
                domains,
                "sha",
                "reg",
                inject_after_n_domain_loads=1,
            )
        assert _v2_stage_table_names(con) == []
        inv = con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'v2_stage' AND table_name = 'load_inventory'
            """
        ).fetchone()[0]
        assert int(inv) == 0
    finally:
        con.close()


def test_commit_persists_row_counts(tmp_path):
    mod = _load_116()
    pq_dir = tmp_path / "pq"
    pq_dir.mkdir()
    p1 = pq_dir / "st_a.parquet"
    p2 = pq_dir / "st_b.parquet"
    pd.DataFrame({"x": [10]}).to_parquet(p1)
    pd.DataFrame({"x": [20, 30]}).to_parquet(p2)
    domains = [
        ("dom_a", "st_a", "v2", "standard", p1),
        ("dom_b", "st_b", "v2", "standard", p2),
    ]
    db_path = tmp_path / "ok.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        mod.run_v2_stage_transaction(
            con,
            domains,
            "sha",
            "reg",
            inject_after_n_domain_loads=None,
        )
        assert con.execute("SELECT COUNT(*) FROM v2_stage.load_inventory").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM v2_stage.st_a").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM v2_stage.st_b").fetchone()[0] == 2
    finally:
        con.close()
