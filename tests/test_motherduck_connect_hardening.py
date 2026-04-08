"""MotherDuck connection hardening: URL shape, path bootstrap, fail-closed vs fallback.

No live cloud calls — ``duckdb.connect`` is mocked where needed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _isolate_motherduck_toml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Avoid picking up the developer's repo-root ``motherduck.local.toml`` during no-token tests."""
    import motherduck_client as mc

    monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", tmp_path / "__missing_motherduck_local__.toml")


def test_md_connect_inserts_repo_root_on_import() -> None:
    """``utils.md_connect`` must be importable without pre-loading ``sys.path`` hacks."""
    import utils.md_connect as mc

    assert str(Path(mc.__file__).resolve().parent.parent) in sys.path


class TestMotherDuckAttachUri:
    """Connection string pattern: ``md:{db}?motherduck_token=…&custom_user_agent=…&session_hint=…``."""

    def test_rw_uri_includes_token_ua_and_session_hint(self, monkeypatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_test_fake")
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("MOTHERDUCK_CUSTOM_USER_AGENT", raising=False)
        monkeypatch.delenv("MOTHERDUCK_SESSION_HINT", raising=False)
        monkeypatch.delenv("MOTHERDUCK_DATABASE", raising=False)
        monkeypatch.delenv("MOTHERDUCK_DB", raising=False)

        captured: dict[str, str] = {}

        class FakeCon:
            def execute(self, _sql: str) -> FakeCon:
                return self

            def close(self) -> None:
                pass

        def fake_connect(uri: str, **_kwargs: object) -> FakeCon:
            captured["uri"] = uri
            return FakeCon()

        monkeypatch.setattr("motherduck_client.duckdb.connect", fake_connect)

        from motherduck_client import MotherDuckClient, MotherDuckConfig

        cfg = MotherDuckConfig(
            database="Thyroid_Rw_Smoke",
            custom_user_agent="thyroid_hardening_tests/1.0",
            motherduck_session_hint="pytest-affinity-1",
        )
        client = MotherDuckClient(cfg)
        con = client.connect_rw()
        con.close()

        uri = captured["uri"]
        assert uri.startswith("md:Thyroid_Rw_Smoke?")
        assert "motherduck_token=md_test_fake" in uri
        assert "custom_user_agent=" in uri
        assert "thyroid_hardening_tests%2F1.0" in uri or "thyroid_hardening_tests" in uri
        assert "session_hint=" in uri
        assert "pytest-affinity-1" in uri

    def test_space_in_database_uses_md_question_mark_form(self, monkeypatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_x")
        monkeypatch.delenv("MOTHERDUCK_CUSTOM_USER_AGENT", raising=False)

        uris: list[str] = []

        class FakeCon:
            def execute(self, sql: str) -> FakeCon:
                self._sql = sql
                return self

            def close(self) -> None:
                pass

        def fake_connect(uri: str, **_kwargs: object) -> FakeCon:
            uris.append(uri)
            return FakeCon()

        monkeypatch.setattr("motherduck_client.duckdb.connect", fake_connect)

        from motherduck_client import MotherDuckClient, MotherDuckConfig

        cfg = MotherDuckConfig(
            database="Thyroid 2026",
            custom_user_agent="ua",
            motherduck_session_hint="hint",
        )
        MotherDuckClient(cfg).connect_rw().close()

        assert uris[0].startswith("md:?")
        assert "motherduck_token=" in uris[0]
        assert "session_hint=" in uris[0]


class TestMdConnectBehavior:
    def test_local_connection_without_md(self, tmp_path) -> None:
        from utils.md_connect import connect_md_or_file

        db = tmp_path / "local_only.duckdb"
        con = connect_md_or_file(db, md=False)
        try:
            assert con.execute("SELECT 1").fetchone() == (1,)
        finally:
            con.close()

    def test_md_without_token_non_fail_closed_uses_local_file(self, monkeypatch, tmp_path) -> None:
        from utils.md_connect import connect_md_or_file

        _isolate_motherduck_toml(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        for key in (
            "MOTHERDUCK_TOKEN",
            "motherduck_token",
            "MD_SA_TOKEN",
            "LOCAL_DB_PATH",
            "MD_READ_SCALING_TOKEN",
        ):
            monkeypatch.delenv(key, raising=False)
        db = tmp_path / "fallback.duckdb"
        con = connect_md_or_file(db, md=True, fail_closed=False)
        try:
            assert con.execute("SELECT 2").fetchone() == (2,)
        finally:
            con.close()

    def test_md_fail_closed_no_token_exits(self, monkeypatch, tmp_path) -> None:
        import utils.md_connect as md_mod

        _isolate_motherduck_toml(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        for key in (
            "MOTHERDUCK_TOKEN",
            "motherduck_token",
            "MD_SA_TOKEN",
            "LOCAL_DB_PATH",
        ):
            monkeypatch.delenv(key, raising=False)
        db = tmp_path / "nope.duckdb"
        with pytest.raises(SystemExit) as exc:
            md_mod.connect_md_or_file(db, md=True, fail_closed=True)
        assert exc.value.code == 1

    def test_connect_md_fail_closed_passes_attribution_kwargs(
        self, monkeypatch, tmp_path
    ) -> None:
        import utils.md_connect as md_mod

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_fake")
        calls: dict[str, object] = {}

        class FakeCon:
            def execute(self, sql: str) -> FakeCon:
                self._sql = sql.lower()
                return self

            def fetchall(self) -> list[tuple[object, ...]]:
                if "pragma database_list" in getattr(self, "_sql", ""):
                    return [("0", "main", "md:Thyroid 2026")]
                return []

            def fetchone(self) -> tuple[str] | None:
                return None

            def close(self) -> None:
                pass

        class FakeClient:
            config: object | None

            def __init__(self) -> None:
                self.connect_rw_called = False
                self.config = None

            def connect_rw(self) -> FakeCon:
                self.connect_rw_called = True
                return FakeCon()

            @classmethod
            def for_env(
                cls,
                env: str | None,
                *,
                use_service_account: bool = False,
                custom_user_agent: str | None = None,
                motherduck_session_hint: str | None = None,
            ) -> FakeClient:
                calls["custom_user_agent"] = custom_user_agent
                calls["motherduck_session_hint"] = motherduck_session_hint
                calls["use_service_account"] = use_service_account
                calls["env"] = env
                inst = cls()
                inst.config = type("Cfg", (), {"database": "Thyroid 2026"})()
                return inst

        monkeypatch.setattr(md_mod, "MotherDuckClient", FakeClient)

        db = tmp_path / "x.duckdb"
        con = md_mod.connect_md_fail_closed(
            db,
            custom_user_agent="smoke-agent/1",
            motherduck_session_hint="hint-from-test",
        )
        con.close()
        assert calls["custom_user_agent"] == "smoke-agent/1"
        assert calls["motherduck_session_hint"] == "hint-from-test"
