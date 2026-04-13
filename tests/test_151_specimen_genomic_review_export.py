"""Offline smoke for scripts/151_specimen_genomic_review_queue_export.py."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import importlib.util

SPEC = ROOT / "scripts" / "151_specimen_genomic_review_queue_export.py"
mod = importlib.util.spec_from_file_location("sg151", SPEC)
assert mod and mod.loader
sg151 = importlib.util.module_from_spec(mod)
mod.loader.exec_module(sg151)


@pytest.fixture()
def mem_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE SCHEMA qa;
        CREATE TABLE qa.specimen_genomic_link_review_v1 (
          review_queue_id VARCHAR NOT NULL,
          genomic_assay_id VARCHAR NOT NULL,
          research_id BIGINT NOT NULL,
          molecular_episode_id BIGINT,
          reason_codes VARCHAR NOT NULL,
          conflict_summary VARCHAR,
          source_table VARCHAR,
          source_row_key VARCHAR,
          queued_at TIMESTAMP,
          review_status VARCHAR NOT NULL
        );
        INSERT INTO qa.specimen_genomic_link_review_v1 VALUES
          ('q1', 'g1', 1, 10, 'rc', 'long conflict text ' || repeat('x', 200),
           'src', 'key', TIMESTAMP '2026-01-01', 'open');
        CREATE TABLE main.specimen_genomic_assay_v1 (
          genomic_assay_id VARCHAR,
          linkage_confidence_tier VARCHAR
        );
        INSERT INTO main.specimen_genomic_assay_v1 VALUES ('g1', 'plausible_review');
        CREATE VIEW qa.v_diag_specimen_review_burden_v1 AS
        SELECT 'specimen_genomic_link_review'::VARCHAR AS queue_key,
               COALESCE(review_status, 'unknown')::VARCHAR AS review_status,
               COUNT(*)::BIGINT AS n_rows
        FROM qa.specimen_genomic_link_review_v1
        GROUP BY 1, 2;
        """
    )
    return con


def test_run_export_writes_files(tmp_path: Path, mem_con: duckdb.DuckDBPyConnection) -> None:
    out = tmp_path / "out"
    stats = sg151.run_export(mem_con, out)
    assert stats["detail_rows"] == 1
    assert (out / "summary.md").is_file()
    assert (out / "specimen_genomic_link_review_v1_all.csv").is_file()
    assert (out / "v_diag_specimen_review_burden_v1.csv").is_file()
    wl = list((out / "worklists").glob("*.csv"))
    assert len(wl) >= 1


def test_connect_read_scaling_fail_closed_exits_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    import utils.md_connect as mdc

    monkeypatch.setattr(mdc, "get_read_scaling_token", lambda: None)
    monkeypatch.setattr(mdc, "read_scaling_token_mode", lambda: "none")
    with pytest.raises(SystemExit) as exc:
        mdc.connect_read_scaling_fail_closed(md_env="prod")
    assert exc.value.code == 1


def test_validate_connection_args_rejects_md_with_read_scaling() -> None:
    ns = argparse.Namespace(md=True, read_scaling=True, md_sa=False)
    with pytest.raises(SystemExit) as exc:
        sg151.validate_connection_args(ns)
    assert exc.value.code == 1


def test_validate_connection_args_rejects_md_sa_without_md() -> None:
    ns = argparse.Namespace(md=False, read_scaling=False, md_sa=True)
    with pytest.raises(SystemExit) as exc:
        sg151.validate_connection_args(ns)
    assert exc.value.code == 1


def test_read_scaling_passes_kwargs_to_connect(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mem_con: duckdb.DuckDBPyConnection
) -> None:
    captured: dict[str, object] = {}

    def _fake_rs(**kwargs: object) -> duckdb.DuckDBPyConnection:
        captured.update(kwargs)
        return mem_con

    monkeypatch.setattr(sg151, "connect_read_scaling_fail_closed", _fake_rs)
    monkeypatch.setenv("MOTHERDUCK_CUSTOM_USER_AGENT", "ua_151_rs")
    args = argparse.Namespace(
        md=False,
        read_scaling=True,
        md_sa=False,
        md_env="qa",
        session_hint="cli_hint",
        db_path=str(tmp_path / "x.duckdb"),
    )
    sg151.validate_connection_args(args)
    sg151.get_connection(args)
    assert captured.get("md_env") == "qa"
    assert captured.get("custom_user_agent") == "ua_151_rs"
    assert captured.get("motherduck_session_hint") == "cli_hint"


def test_main_read_scaling_uses_patched_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mem_con: duckdb.DuckDBPyConnection
) -> None:
    captured: dict[str, bool] = {}

    def _fake_rs(**_kwargs: object) -> duckdb.DuckDBPyConnection:
        captured["called"] = True
        return mem_con

    monkeypatch.setattr(sg151, "connect_read_scaling_fail_closed", _fake_rs)
    out_root = tmp_path / "exports"
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "151_specimen_genomic_review_queue_export.py",
            "--read-scaling",
            "--output-root",
            str(out_root),
        ],
    )
    sg151.main()
    assert captured.get("called") is True
    bundles = sorted(out_root.glob("specimen_genomic_review_*"))
    assert len(bundles) == 1
    assert (bundles[0] / "summary.md").is_file()
