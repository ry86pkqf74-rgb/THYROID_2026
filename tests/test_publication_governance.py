"""Tests for publication vs rehearsal rules (MRQ + promotion_review_decisions)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

from utils.publication_governance import (
    MRQ_SYNTHETIC_PLACEHOLDER_EXACT,
    is_mrq_synthetic_placeholder_verification_status,
    sql_count_mrq_synthetic_rows,
    sql_count_promotion_decisions_missing_batch,
)


def test_is_mrq_synthetic_placeholder_exact() -> None:
    assert is_mrq_synthetic_placeholder_verification_status(MRQ_SYNTHETIC_PLACEHOLDER_EXACT)
    assert is_mrq_synthetic_placeholder_verification_status(
        MRQ_SYNTHETIC_PLACEHOLDER_EXACT.lower()
    )


def test_is_mrq_synthetic_placeholder_markers() -> None:
    assert is_mrq_synthetic_placeholder_verification_status(
        "PREVIEW_synthetic_automation_only_suffix"
    )
    assert is_mrq_synthetic_placeholder_verification_status(
        "foo_not_manuscript_signoff_bar"
    )


def test_is_mrq_synthetic_placeholder_negative() -> None:
    assert not is_mrq_synthetic_placeholder_verification_status(None)
    assert not is_mrq_synthetic_placeholder_verification_status("")
    assert not is_mrq_synthetic_placeholder_verification_status("nan")
    assert not is_mrq_synthetic_placeholder_verification_status("confirmed_correct")
    assert not is_mrq_synthetic_placeholder_verification_status("auto_accepted_standard")


def test_sql_count_mrq_synthetic_rows_integration() -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE SCHEMA qa;
        CREATE TABLE qa.manual_review_queue (
            verification_status VARCHAR
        );
        INSERT INTO qa.manual_review_queue VALUES
            ('confirmed_correct'),
            (NULL),
            ('SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF');
        """
    )
    row = con.execute(sql_count_mrq_synthetic_rows()).fetchone()
    assert row is not None
    n = int(row[0])
    assert n == 1


def test_sql_count_promotion_decisions_missing_batch() -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE SCHEMA qa;
        CREATE TABLE qa.promotion_review_decisions (
            decision_batch_id VARCHAR
        );
        INSERT INTO qa.promotion_review_decisions VALUES ('b1'), (NULL), ('  ');
        """
    )
    row = con.execute(sql_count_promotion_decisions_missing_batch()).fetchone()
    assert row is not None
    n_bad = int(row[0])
    assert n_bad == 2


def test_126_rejects_synthetic_fill_with_release_mode() -> None:
    """126 defaults to release-mode; synthetic fill is rehearsal-only."""
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "126_final_master_release.py"),
            "--md",
            "--release-date",
            "20260401",
            "--synthetic-fill-mrq-verification",
            "SYNTH_TEST",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 1
    out = (proc.stdout or "") + (proc.stderr or "")
    assert "synthetic-fill-mrq-verification" in out.lower()
