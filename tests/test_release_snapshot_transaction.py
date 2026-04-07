"""Transactional rollback / commit tests for scripts/115_release_snapshot.py (local DuckDB)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_115():
    path = ROOT / "scripts" / "115_release_snapshot.py"
    spec = importlib.util.spec_from_file_location("release_snapshot_115", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _schema_exists(con: duckdb.DuckDBPyConnection, schema_name: str) -> bool:
    try:
        names = [r[0] for r in con.execute("SHOW SCHEMAS").fetchall()]
    except Exception:
        names = [
            r[0]
            for r in con.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        ]
    return schema_name in names


def test_rollback_after_inject_no_release_schema(tmp_path):
    mod = _load_115()
    db = tmp_path / "r.duckdb"
    con = duckdb.connect(str(db))
    tag = "20990101"
    schema_name = f"release_{tag}"
    try:
        con.execute("CREATE TABLE main.t1 AS SELECT 1 AS k UNION ALL SELECT 2")
        con.execute("CREATE TABLE main.t2 AS SELECT 3 AS k")
        with pytest.raises(RuntimeError, match="__TEST_INJECT_AFTER_PARTIAL_COPY__"):
            mod.run_release_snapshot_transaction(
                con,
                schema_name,
                ["t1", "t2"],
                tag,
                inject_after_n_table_copies=1,
            )
        assert not _schema_exists(con, schema_name)
        rel_tables = con.execute(
            f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            """
        ).fetchone()[0]
        assert int(rel_tables) == 0
    finally:
        con.close()


def test_commit_persists_release_row_counts(tmp_path):
    mod = _load_115()
    db = tmp_path / "ok.duckdb"
    con = duckdb.connect(str(db))
    tag = "20990102"
    schema_name = f"release_{tag}"
    try:
        con.execute("CREATE TABLE main.t1 AS SELECT 1 AS k UNION ALL SELECT 2")
        con.execute("CREATE TABLE main.t2 AS SELECT 3 AS k")
        row_counts = mod.run_release_snapshot_transaction(
            con,
            schema_name,
            ["t1", "t2"],
            tag,
            inject_after_n_table_copies=None,
        )
        assert row_counts == {"t1": 2, "t2": 1}
        assert con.execute(f"SELECT COUNT(*) FROM {schema_name}.t1").fetchone()[0] == 2
        assert con.execute(f"SELECT COUNT(*) FROM {schema_name}.t2").fetchone()[0] == 1
    finally:
        con.close()
