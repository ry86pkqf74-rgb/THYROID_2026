"""Tests for MotherDuck token precedence and read-scaling guardrails."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _no_file_motherduck_tokens(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point ``motherduck.local.toml`` at a non-existent path so tests do not read the dev machine file."""
    import motherduck_client as mc

    monkeypatch.setattr(
        mc,
        "LOCAL_MOTHERDUCK_TOML_PATH",
        tmp_path / "__test_missing_motherduck_local__.toml",
    )


class TestGetTokenPrecedence:
    def test_sa_wins_over_personal_when_both_set(self, monkeypatch):
        from motherduck_client import get_token

        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_personal")
        monkeypatch.setenv("MD_SA_TOKEN", "md_sa")
        assert get_token(prefer_service_account=False) == "md_sa"
        assert get_token(prefer_service_account=True) == "md_sa"

    def test_personal_wins_over_motherduck_token_alias(self, monkeypatch):
        from motherduck_client import get_token

        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_personal")
        monkeypatch.setenv("motherduck_token", "md_alias")
        assert get_token() == "md_personal"

    def test_motherduck_token_env_alias_when_only_alias_set(self, monkeypatch):
        from motherduck_client import get_token

        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.setenv("motherduck_token", "md_alias")
        assert get_token(prefer_service_account=False) == "md_alias"

    def test_get_token_from_motherduck_local_toml(self, monkeypatch, tmp_path):
        import motherduck_client as mc

        toml_path = tmp_path / "motherduck.local.toml"
        toml_path.write_text('MOTHERDUCK_TOKEN = "md_from_toml"\n', encoding="utf-8")
        monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", toml_path)
        for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token", "LOCAL_DB_PATH"):
            monkeypatch.delenv(key, raising=False)
        assert mc.get_token() == "md_from_toml"
        assert mc.token_mode() == "motherduck.local.toml:MOTHERDUCK_TOKEN"

    def test_sa_wins_in_motherduck_local_toml_when_both_keys_present(self, monkeypatch, tmp_path):
        import motherduck_client as mc

        toml_path = tmp_path / "motherduck.local.toml"
        toml_path.write_text(
            'MOTHERDUCK_TOKEN = "md_personal_toml"\nMD_SA_TOKEN = "md_sa_toml"\n',
            encoding="utf-8",
        )
        monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", toml_path)
        for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token", "LOCAL_DB_PATH"):
            monkeypatch.delenv(key, raising=False)
        assert mc.get_token() == "md_sa_toml"
        assert mc.token_mode() == "motherduck.local.toml:MD_SA_TOKEN"

    def test_env_wins_over_motherduck_local_toml(self, monkeypatch, tmp_path):
        import motherduck_client as mc

        toml_path = tmp_path / "motherduck.local.toml"
        toml_path.write_text('MOTHERDUCK_TOKEN = "md_from_toml"\n', encoding="utf-8")
        monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", toml_path)
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_from_env")
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        assert mc.get_token() == "md_from_env"

    def test_motherduck_local_toml_precedes_streamlit_secrets(self, monkeypatch, tmp_path):
        import motherduck_client as mc

        streamlit_dir = tmp_path / ".streamlit"
        streamlit_dir.mkdir()
        (streamlit_dir / "secrets.toml").write_text(
            'MOTHERDUCK_TOKEN = "md_from_streamlit"\n', encoding="utf-8"
        )
        toml_path = tmp_path / "motherduck.local.toml"
        toml_path.write_text('MOTHERDUCK_TOKEN = "md_from_root_toml"\n', encoding="utf-8")
        monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", toml_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        assert mc.get_token() == "md_from_root_toml"

    def test_get_read_scaling_token_from_motherduck_local_toml(self, monkeypatch, tmp_path):
        import motherduck_client as mc

        toml_path = tmp_path / "motherduck.local.toml"
        toml_path.write_text('MD_READ_SCALING_TOKEN = "md_rs_toml"\n', encoding="utf-8")
        monkeypatch.setattr(mc, "LOCAL_MOTHERDUCK_TOML_PATH", toml_path)
        for key in (
            "MD_READ_SCALING_TOKEN",
            "MOTHERDUCK_READ_SCALING_TOKEN",
            "MOTHERDUCK_TOKEN",
            "MD_SA_TOKEN",
        ):
            monkeypatch.delenv(key, raising=False)
        assert mc.get_read_scaling_token() == "md_rs_toml"
        assert mc.read_scaling_token_mode() == "motherduck.local.toml:MD_READ_SCALING_TOKEN"

    def test_get_token_never_returns_read_scaling_secret(self, monkeypatch, tmp_path):
        from motherduck_client import get_token

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_read_only")
        assert get_token(prefer_service_account=False) is None
        assert get_token(prefer_service_account=True) is None

    def test_token_mode_none_when_only_read_scaling(self, monkeypatch, tmp_path):
        from motherduck_client import token_mode

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        for key in (
            "MOTHERDUCK_TOKEN",
            "motherduck_token",
            "MD_SA_TOKEN",
            "LOCAL_DB_PATH",
        ):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs_only")
        monkeypatch.delenv("MOTHERDUCK_READ_SCALING_TOKEN", raising=False)
        assert token_mode() == "none"

    def test_token_mode_none_when_only_read_scaling_alias(self, monkeypatch, tmp_path):
        from motherduck_client import token_mode

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        for key in (
            "MOTHERDUCK_TOKEN",
            "motherduck_token",
            "MD_SA_TOKEN",
            "LOCAL_DB_PATH",
            "MD_READ_SCALING_TOKEN",
        ):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("MOTHERDUCK_READ_SCALING_TOKEN", "md_rs_alias_only")
        assert token_mode() == "none"


class TestReadScalingHelpers:
    def test_get_read_scaling_token_env_primary(self, monkeypatch):
        from motherduck_client import get_read_scaling_token

        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs")
        monkeypatch.setenv("MOTHERDUCK_READ_SCALING_TOKEN", "md_rs2")
        assert get_read_scaling_token() == "md_rs"

    def test_get_read_scaling_token_secondary_alias(self, monkeypatch):
        from motherduck_client import get_read_scaling_token

        monkeypatch.delenv("MD_READ_SCALING_TOKEN", raising=False)
        monkeypatch.setenv("MOTHERDUCK_READ_SCALING_TOKEN", "md_rs_alt")
        assert get_read_scaling_token() == "md_rs_alt"

    def test_read_scaling_token_mode(self, monkeypatch):
        from motherduck_client import read_scaling_token_mode

        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "x")
        assert read_scaling_token_mode() == "env:MD_READ_SCALING_TOKEN"


class TestReadScalingGuardrails:
    def test_connect_rw_raises_when_only_read_scaling_configured(self, monkeypatch, tmp_path):
        from motherduck_client import MotherDuckClient, ReadScalingTokenForbiddenError

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs_only")
        monkeypatch.delenv("USE_LOCAL_DUCKDB", raising=False)
        md = MotherDuckClient()
        with pytest.raises(ReadScalingTokenForbiddenError):
            md.connect_rw()

    def test_is_read_scaling_only_environment(self, monkeypatch, tmp_path):
        from motherduck_client import is_read_scaling_only_environment

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs")
        assert is_read_scaling_only_environment() is True

        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_rw")
        assert is_read_scaling_only_environment() is False

    def test_connect_read_scaling_requires_token(self, monkeypatch, tmp_path):
        from motherduck_client import MotherDuckClient

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MD_READ_SCALING_TOKEN", raising=False)
        monkeypatch.delenv("MOTHERDUCK_READ_SCALING_TOKEN", raising=False)
        monkeypatch.setenv("USE_LOCAL_DUCKDB", "")
        md = MotherDuckClient()
        with pytest.raises(RuntimeError, match="read-scaling"):
            md.connect_read_scaling()


class TestMdConnectFailClosedReadScaling:
    def test_fail_closed_exits_when_only_read_scaling(self, monkeypatch, tmp_path):
        import utils.md_connect as md_mod

        _no_file_motherduck_tokens(monkeypatch, tmp_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs")

        db = tmp_path / "x.duckdb"
        with pytest.raises(SystemExit) as exc:
            md_mod.connect_md_or_file(db, md=True, fail_closed=True)
        assert exc.value.code == 1
