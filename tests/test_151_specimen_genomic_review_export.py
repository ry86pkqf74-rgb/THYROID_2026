"""Offline smoke for scripts/151_specimen_genomic_review_queue_export.py."""

from __future__ import annotations

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
