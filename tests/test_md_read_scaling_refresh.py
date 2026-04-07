"""Unit tests for MotherDuck snapshot / refresh SQL helpers (no cloud)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class TestMdReadScalingRefreshSql:
    def test_create_snapshot_unnamed_quoted_db_with_space(self) -> None:
        from utils.md_read_scaling_refresh import sql_create_snapshot

        assert sql_create_snapshot("Thyroid 2026") == 'CREATE SNAPSHOT OF "Thyroid 2026"'

    def test_create_snapshot_named(self) -> None:
        from utils.md_read_scaling_refresh import sql_create_snapshot

        assert (
            sql_create_snapshot("Thyroid 2026", snapshot_name="post_etl")
            == 'CREATE SNAPSHOT post_etl OF "Thyroid 2026"'
        )

    def test_refresh_single(self) -> None:
        from utils.md_read_scaling_refresh import sql_refresh_database

        assert sql_refresh_database("Thyroid 2026", mode="single") == (
            'REFRESH DATABASE "Thyroid 2026"'
        )

    def test_refresh_all(self) -> None:
        from utils.md_read_scaling_refresh import sql_refresh_database

        assert sql_refresh_database(None, mode="all") == "REFRESH DATABASES"

    def test_rejects_injection(self) -> None:
        from utils.md_read_scaling_refresh import sql_create_snapshot

        with pytest.raises(ValueError):
            sql_create_snapshot("x; DROP SCHEMA main; --")


class TestDashboardReadScalingEnv:
    def test_prefer_and_attach_default_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from motherduck_client import (
            dashboard_allow_read_scaling_attach,
            dashboard_prefer_read_scaling_token_for_share,
        )

        for k in (
            "MOTHERDUCK_DASHBOARD_PREFER_READ_SCALING_TOKEN",
            "THYROID_DASHBOARD_PREFER_READ_SCALING_TOKEN",
            "MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH",
            "THYROID_DASHBOARD_ALLOW_READ_SCALING_ATTACH",
        ):
            monkeypatch.delenv(k, raising=False)
        assert dashboard_prefer_read_scaling_token_for_share() is False
        assert dashboard_allow_read_scaling_attach() is False

    def test_truthy_variants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from motherduck_client import (
            dashboard_allow_read_scaling_attach,
            dashboard_prefer_read_scaling_token_for_share,
        )

        monkeypatch.setenv("THYROID_DASHBOARD_PREFER_READ_SCALING_TOKEN", "1")
        monkeypatch.setenv("MOTHERDUCK_DASHBOARD_ALLOW_READ_SCALING_ATTACH", "true")
        assert dashboard_prefer_read_scaling_token_for_share() is True
        assert dashboard_allow_read_scaling_attach() is True
