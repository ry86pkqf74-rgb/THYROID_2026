"""Smoke tests for scripts/120_review_queue_triage.py (local DuckDB only)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_triage_module():
    spec = importlib.util.spec_from_file_location(
        "review_queue_triage_120",
        ROOT / "scripts" / "120_review_queue_triage.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


triage = _load_triage_module()


@pytest.fixture()
def memory_mrq_con():
    con = duckdb.connect()
    con.execute(
        """
        CREATE SCHEMA qa;
        CREATE TABLE qa.manual_review_queue (
            review_row_id INTEGER,
            run_label VARCHAR NOT NULL,
            research_id BIGINT,
            domain VARCHAR NOT NULL,
            entity_type VARCHAR,
            entity_value_norm VARCHAR,
            algorithm_status VARCHAR,
            review_reason VARCHAR,
            verification_status VARCHAR,
            reviewer VARCHAR,
            reviewed_at TIMESTAMP,
            loaded_at TIMESTAMP,
            promotion_approved VARCHAR,
            reviewer_evidence_span VARCHAR,
            reviewer_comment VARCHAR,
            reason_code VARCHAR
        );
        """
    )
    rows = [
        (
            0,
            "gate_a",
            1001,
            "imaging",
            "nodule_size",
            "x" * 100,
            "discordant_existing",
            "SECRET NOTE TEXT SHOULD NOT APPEAR IN WORKLIST CSV",
            None,
            None,
            None,
            "2026-01-01 00:00:00",
            None,
            "evidence " * 30,
            "comment " * 30,
            "R1",
        ),
        (
            1,
            "gate_a",
            1002,
            "imaging",
            "other",
            "ok",
            "existing_missing_fill_candidate",
            None,
            None,
            None,
            None,
            "2026-02-01 00:00:00",
            None,
            None,
            None,
            None,
        ),
        (
            2,
            "gate_a",
            1003,
            "pathology",
            "histology",
            "value",
            "discordant_existing",
            None,
            "confirmed_correct",
            "alice",
            "2026-03-01 00:00:00",
            "2026-03-01 12:00:00",
            "true",
            None,
            None,
            None,
        ),
    ]
    con.executemany(
        "INSERT INTO qa.manual_review_queue VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    yield con
    con.close()


def test_sanitize_worklist_truncates_and_omits_review_reason():
    df = pd.DataFrame(
        [
            {
                "review_reason": "should drop if column existed",
                "entity_value_norm": "a" * 200,
                "reviewer_comment": "b" * 200,
                "reviewer_evidence_span": "c" * 200,
            }
        ]
    )
    out = triage.sanitize_worklist_df(df)
    assert "review_reason" not in out.columns
    ev = out["entity_value_norm"].iloc[0]
    assert isinstance(ev, str)
    assert ev.endswith("...")
    assert len(ev) <= triage.TRUNC_ENTITY


def test_run_triage_writes_bundle(tmp_path, memory_mrq_con):
    reg = triage.load_registry()
    out = tmp_path / "bundle"
    stats = triage.run_triage(
        memory_mrq_con,
        out,
        registry=reg,
        oldest_limit=50,
        run_label=None,
    )
    assert stats["total_rows"] == 3
    assert stats["pending"] == 2
    assert stats["reviewed"] == 1
    assert (out / "counts_by_domain.csv").is_file()
    assert (out / "counts_by_verification_status.csv").is_file()
    assert (out / "counts_promotable_blocking.csv").is_file()
    assert (out / "domains_highest_pending_volume.csv").is_file()
    assert (out / "oldest_pending_rows.csv").is_file()
    assert (out / "summary.md").is_file()
    # Pending worklists: imaging has rows (2 pending include 2 imaging? 1 imaging pending + 1 pathology pending = 2
    # Row 0 imaging discordant pending, row 1 imaging fill pending -> 2 imaging worklist file tier_standard
    wdir = out / "worklists"
    assert wdir.is_dir()
    wl_files = list(wdir.glob("worklist__*.csv"))
    assert len(wl_files) == stats["worklist_files"]
    assert stats["worklist_files"] >= 1

    imaging_wl = next(f for f in wl_files if "imaging" in f.name.lower())
    m = pd.read_csv(imaging_wl)
    assert "review_reason" not in m.columns
    assert all(m["verification_status"].isna())
