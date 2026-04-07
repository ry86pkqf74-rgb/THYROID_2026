"""Unit tests for scripts/smoke_test_md_connection.py (no live MotherDuck)."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_smoke_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "smoke_test_md_connection",
        ROOT / "scripts" / "smoke_test_md_connection.py",
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestSmokeMdConnectionScript:
    def test_local_mode_passes(self, monkeypatch, tmp_path) -> None:
        mod = _load_smoke_module()
        db = tmp_path / "smoke_local.duckdb"
        monkeypatch.setattr(mod, "DB_PATH", db)
        monkeypatch.setattr(sys, "argv", ["smoke_test_md_connection.py"])

        assert mod.main() == 0
        assert db.exists()

    def test_md_mode_no_token_exits_1(self, monkeypatch, tmp_path) -> None:
        mod = _load_smoke_module()
        monkeypatch.chdir(tmp_path)
        for key in (
            "MOTHERDUCK_TOKEN",
            "motherduck_token",
            "MD_SA_TOKEN",
            "MD_READ_SCALING_TOKEN",
            "MOTHERDUCK_READ_SCALING_TOKEN",
            "LOCAL_DB_PATH",
            "USE_LOCAL_DUCKDB",
        ):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr(mod, "DB_PATH", tmp_path / "unused.duckdb")
        monkeypatch.setattr(sys, "argv", ["smoke_test_md_connection.py", "--md"])

        with pytest.raises(SystemExit) as excinfo:
            mod.main()
        assert excinfo.value.code == 1

    def test_md_mode_non_motherduck_verification_exits_1(self, monkeypatch, tmp_path) -> None:
        mod = _load_smoke_module()
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(mod, "DB_PATH", tmp_path / "unused.duckdb")
        monkeypatch.setattr(sys, "argv", ["smoke_test_md_connection.py", "--md"])
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_fake_unit_test_token")

        class FakeCon:
            def __init__(self) -> None:
                self._sql = ""

            def execute(self, sql: str) -> FakeCon:
                self._sql = sql
                return self

            def fetchall(self) -> list[tuple[str, str, str]] | list[tuple[str, str]]:
                s = self._sql.lower()
                if "pragma database_list" in s:
                    # Deliberately no md: path nor md_information_schema row
                    return [("0", "main", str(tmp_path / "local_only.duckdb"))]
                return []

            def fetchone(self) -> tuple[str, ...] | tuple[str, str] | None:
                s = self._sql.lower()
                if "pragma version" in s:
                    return ("test_version",)
                if "current_catalog" in s:
                    return ("main", "main")
                return None

            def close(self) -> None:
                pass

        class FakeClient:
            config: object

            def connect_rw(self) -> FakeCon:
                return FakeCon()

            @classmethod
            def for_env(cls, *_a: object, **_k: object) -> FakeClient:
                inst = cls()
                inst.config = type("Cfg", (), {"database": "Thyroid 2026"})()
                return inst

        monkeypatch.setattr("utils.md_connect.MotherDuckClient", FakeClient)

        with pytest.raises(SystemExit) as excinfo:
            mod.main()
        assert excinfo.value.code == 1
