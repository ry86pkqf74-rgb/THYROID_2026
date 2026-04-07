"""Tests for MotherDuck token precedence and read-scaling guardrails."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


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

    def test_get_token_never_returns_read_scaling_secret(self, monkeypatch, tmp_path):
        from motherduck_client import get_token

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

        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        monkeypatch.delenv("motherduck_token", raising=False)
        monkeypatch.setenv("MD_READ_SCALING_TOKEN", "md_rs")
        assert is_read_scaling_only_environment() is True

        monkeypatch.setenv("MOTHERDUCK_TOKEN", "md_rw")
        assert is_read_scaling_only_environment() is False

    def test_connect_read_scaling_requires_token(self, monkeypatch):
        from motherduck_client import MotherDuckClient

        monkeypatch.delenv("MD_READ_SCALING_TOKEN", raising=False)
        monkeypatch.delenv("MOTHERDUCK_READ_SCALING_TOKEN", raising=False)
        monkeypatch.setenv("USE_LOCAL_DUCKDB", "")
        md = MotherDuckClient()
        with pytest.raises(RuntimeError, match="read-scaling"):
            md.connect_read_scaling()


class TestMdConnectFailClosedReadScaling:
    def test_fail_closed_exits_when_only_read_scaling(self, monkeypatch, tmp_path):
        import utils.md_connect as md_mod

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
