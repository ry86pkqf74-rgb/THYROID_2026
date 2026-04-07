"""Tests for scripts/127_analyst_institutional_lab_append.py — local DuckDB only."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent

DDL = """
CREATE TABLE main.longitudinal_lab_canonical_v1 (
    research_id BIGINT,
    lab_date VARCHAR,
    lab_date_status VARCHAR,
    lab_name_raw VARCHAR,
    lab_name_standardized VARCHAR,
    analyte_group VARCHAR,
    value_raw VARCHAR,
    value_numeric DOUBLE,
    unit_raw VARCHAR,
    unit_standardized VARCHAR,
    reference_range VARCHAR,
    abnormal_flag VARCHAR,
    is_censored BOOLEAN,
    source_table VARCHAR,
    source_script VARCHAR,
    ingestion_wave VARCHAR,
    data_completeness_tier VARCHAR,
    provenance_note VARCHAR
);
"""


def _load_lab127():
    path = ROOT / "scripts" / "127_analyst_institutional_lab_append.py"
    spec = importlib.util.spec_from_file_location("lab127", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


lab127 = _load_lab127()


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture
def tmp_lab_db(tmp_path: Path) -> Path:
    dbp = tmp_path / "lab_tx.duckdb"
    con = duckdb.connect(str(dbp))
    con.execute(DDL)
    con.execute(
        """
        INSERT INTO main.longitudinal_lab_canonical_v1 VALUES
        (1, '2024-01-01', 'exact', 'TSH', 'tsh', 'chem', '5', 5.0,
         NULL, NULL, NULL, NULL, false, 'x', 'seed', 'wave_keep', 'tier', 'p1'),
        (1, '2024-01-02', 'exact', 'TSH', 'tsh', 'chem', '6', 6.0,
         NULL, NULL, NULL, NULL, false, 'x', 'seed', 'wave_replace', 'tier', 'p2')
        """
    )
    con.execute(lab127.DEDUP_VIEW_SQL)
    con.close()
    return dbp


def _csv_for_wave(tmp_path: Path, wave: str) -> Path:
    p = tmp_path / "labs.csv"
    pd.DataFrame(
        {
            "research_id": [2],
            "lab_date": ["2024-06-01"],
            "lab_name_standardized": ["calcium"],
            "value_raw": ["9.1"],
            "source_lineage_key": ["k-new-1"],
        }
    ).to_csv(p, index=False)
    return p


def test_commit_persists_rows_and_refreshes_dedup(tmp_path: Path, tmp_lab_db: Path) -> None:
    csv = _csv_for_wave(tmp_path, "wave_replace")
    frame = lab127.build_frame(csv, "wave_replace")
    con = duckdb.connect(str(tmp_lab_db))
    try:
        pre, post = lab127.replace_lab_wave_in_transaction(con, frame, "wave_replace")
        assert pre == 2
        assert post == 2
        n_wave = _scalar(
            con,
            "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1 WHERE ingestion_wave = 'wave_replace'",
        )
        assert n_wave == 1
        ded = _scalar(con, "SELECT COUNT(*) FROM main.longitudinal_lab_deduped_v")
        assert ded >= 1
    finally:
        con.close()


def test_rollback_after_delete_restores_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, tmp_lab_db: Path) -> None:
    monkeypatch.setenv(lab127.LAB_APPEND_FAIL_AFTER_ENV, "after_delete")
    csv = _csv_for_wave(tmp_path, "wave_replace")
    frame = lab127.build_frame(csv, "wave_replace")
    con = duckdb.connect(str(tmp_lab_db))
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            lab127.replace_lab_wave_in_transaction(con, frame, "wave_replace")
        n = _scalar(con, "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1")
        assert n == 2
        n_wr = _scalar(
            con,
            "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1 WHERE ingestion_wave = 'wave_replace'",
        )
        assert n_wr == 1
    finally:
        con.close()
        monkeypatch.delenv(lab127.LAB_APPEND_FAIL_AFTER_ENV, raising=False)


def test_rollback_after_insert_restores_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, tmp_lab_db: Path) -> None:
    monkeypatch.setenv(lab127.LAB_APPEND_FAIL_AFTER_ENV, "after_insert")
    csv = _csv_for_wave(tmp_path, "wave_replace")
    frame = lab127.build_frame(csv, "wave_replace")
    con = duckdb.connect(str(tmp_lab_db))
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            lab127.replace_lab_wave_in_transaction(con, frame, "wave_replace")
        n = _scalar(con, "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1")
        assert n == 2
    finally:
        con.close()
        monkeypatch.delenv(lab127.LAB_APPEND_FAIL_AFTER_ENV, raising=False)
