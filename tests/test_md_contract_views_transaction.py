"""Transactional rollback / commit tests for scripts/117_md_contract_views.py (local DuckDB)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_117():
    path = ROOT / "scripts" / "117_md_contract_views.py"
    spec = importlib.util.spec_from_file_location("md_contract_117", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _main_contract_table_rows(con: duckdb.DuckDBPyConnection, names: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for n in names:
        meta = con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = current_database()
              AND table_schema = 'main' AND table_name = ?
            """,
            [n],
        ).fetchone()
        assert meta is not None
        if int(meta[0]) == 0:
            out[n] = 0
        else:
            row = con.execute(f"SELECT COUNT(*) FROM main.{n}").fetchone()
            assert row is not None
            out[n] = int(row[0])
    return out


def test_rollback_after_inject_no_partial_main_tables(monkeypatch, tmp_path):
    mod = _load_117()
    freeze = tmp_path / "freeze"
    freeze.mkdir()
    pd.DataFrame({"id": [1]}).to_parquet(freeze / "t_ep1.parquet")
    pd.DataFrame({"id": [1, 2]}).to_parquet(freeze / "t_ep2.parquet")
    monkeypatch.setattr(
        mod,
        "EPISODE_TABLES",
        {
            "ep_one": "t_ep1.parquet",
            "ep_two": "t_ep2.parquet",
        },
    )
    monkeypatch.setattr(mod, "FREEZE_DIR", freeze)

    con = duckdb.connect(str(tmp_path / "db.duckdb"))
    args = SimpleNamespace(skip_canonical=True)
    try:
        with pytest.raises(RuntimeError, match="__TEST_INJECT_AFTER_PARTIAL_LOAD__"):
            mod._run_contract_writes_in_transaction(
                con, args, inject_after_n_table_loads=1
            )
        rows = _main_contract_table_rows(con, ["ep_one", "ep_two"])
        assert rows == {"ep_one": 0, "ep_two": 0}
    finally:
        con.close()


def test_commit_persists_episode_row_counts(monkeypatch, tmp_path):
    mod = _load_117()
    freeze = tmp_path / "freeze"
    freeze.mkdir()
    pd.DataFrame({"id": [1]}).to_parquet(freeze / "t_ep1.parquet")
    pd.DataFrame({"id": [1, 2]}).to_parquet(freeze / "t_ep2.parquet")
    monkeypatch.setattr(
        mod,
        "EPISODE_TABLES",
        {
            "ep_one": "t_ep1.parquet",
            "ep_two": "t_ep2.parquet",
        },
    )
    monkeypatch.setattr(mod, "FREEZE_DIR", freeze)
    monkeypatch.setattr(mod, "apply_ddl", lambda *a, **k: None)

    con = duckdb.connect(str(tmp_path / "db2.duckdb"))
    args = SimpleNamespace(skip_canonical=True)
    try:
        mod._run_contract_writes_in_transaction(
            con, args, inject_after_n_table_loads=None
        )
        assert con.execute("SELECT COUNT(*) FROM main.ep_one").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM main.ep_two").fetchone()[0] == 2
    finally:
        con.close()
