"""Smoke tests for scripts/120_review_queue_triage.py (local DuckDB only)."""

from __future__ import annotations

import csv
import importlib.util
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent

# Simulated note-adjacent text that must never appear in exported CSV/Markdown.
MRQ_FORBIDDEN_SNAPSHOT = "SECRET NOTE TEXT SHOULD NOT APPEAR IN WORKLIST CSV"

MRQ_ROWS: list[tuple] = [
    (
        0,
        "gate_a",
        1001,
        "imaging",
        "nodule_size",
        "x" * 100,
        "discordant_existing",
        MRQ_FORBIDDEN_SNAPSHOT,
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


def _seed_manual_review_queue(con: duckdb.DuckDBPyConnection) -> None:
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
    con.executemany(
        "INSERT INTO qa.manual_review_queue VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        MRQ_ROWS,
    )


def _assert_bundle_exports_no_raw_notes(bundle: Path, *, forbidden: str) -> None:
    """Fail if note-adjacent snapshot text or review_reason columns leak into exports."""
    for path in bundle.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() in {".csv", ".md"}:
            body = path.read_text(encoding="utf-8", errors="replace")
            assert forbidden not in body, path

    for path in bundle.rglob("*.csv"):
        with path.open(newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle), None)
        assert header is not None, path
        assert "review_reason" not in header, path


@pytest.fixture()
def memory_mrq_con():
    con = duckdb.connect()
    _seed_manual_review_queue(con)
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
    assert (out / "counts_manuscript_quality_tiers.csv").is_file()
    tiers = pd.read_csv(out / "counts_manuscript_quality_tiers.csv")
    by_tier = dict(zip(tiers["manuscript_quality_tier"], tiers["n_rows"]))
    assert by_tier.get("A_pending_blocks_structural_release") == 2
    assert by_tier.get("D_human_review_identity_present") == 1
    assert (out / "counts_promotable_blocking.csv").is_file()
    assert (out / "domains_highest_pending_volume.csv").is_file()
    assert (out / "oldest_pending_rows.csv").is_file()
    assert (out / "summary.md").is_file()
    wdir = out / "worklists"
    assert wdir.is_dir()
    wl_files = list(wdir.glob("worklist__*.csv"))
    assert len(wl_files) == stats["worklist_files"]
    assert stats["worklist_files"] >= 1

    imaging_wl = next(f for f in wl_files if "imaging" in f.name.lower())
    m = pd.read_csv(imaging_wl)
    assert "review_reason" not in m.columns
    assert all(m["verification_status"].isna())

    _assert_bundle_exports_no_raw_notes(out, forbidden=MRQ_FORBIDDEN_SNAPSHOT)


def test_main_writes_timestamped_bundle(monkeypatch, tmp_path):
    """CLI entrypoint creates review_queue_triage_<UTC> under --output-root (local file DB)."""
    db_file = tmp_path / "triage_smoke.duckdb"
    con = duckdb.connect(str(db_file))
    _seed_manual_review_queue(con)
    con.close()

    out_root = tmp_path / "exports"
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "120_review_queue_triage.py",
            "--db-path",
            str(db_file),
            "--output-root",
            str(out_root),
            "--oldest-limit",
            "50",
        ],
    )
    triage.main()

    bundles = sorted(out_root.glob("review_queue_triage_*"))
    assert len(bundles) == 1
    bundle = bundles[0]
    assert re.fullmatch(r"review_queue_triage_\d{8}_\d{6}", bundle.name)

    assert (bundle / "summary.md").is_file()
    wl_files = list((bundle / "worklists").glob("worklist__*.csv"))
    assert len(wl_files) >= 1

    _assert_bundle_exports_no_raw_notes(bundle, forbidden=MRQ_FORBIDDEN_SNAPSHOT)
