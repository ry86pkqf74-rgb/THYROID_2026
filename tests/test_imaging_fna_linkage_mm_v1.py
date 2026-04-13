"""Synthetic scenarios for imaging_fna_linkage_mm_v1 (script 129)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_129():
    p = ROOT / "scripts" / "129_imaging_fna_linkage_mm_v1.py"
    spec = importlib.util.spec_from_file_location("link129", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def link129():
    sys.path.insert(0, str(ROOT))
    return _load_129()


def _materialize(con: duckdb.DuckDBPyConnection, link129) -> None:
    con.execute(link129.build_temp_wide_sql(con))
    con.execute(link129.LINK_TABLE_SQL)
    con.execute(link129.REVIEW_SQL)


def test_strict_release_requires_tumor_episode_master_v2(link129) -> None:
    """Release mode must not run without tumor_episode_master_v2 (preop window semantics)."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 (
            research_id INTEGER, nodule_id VARCHAR, exam_id VARCHAR, exam_date DATE,
            laterality VARCHAR, max_dimension_cm DOUBLE
        );
        INSERT INTO imaging_nodule_master_v1 VALUES
            (1, 'n1', 'e1', DATE '2024-01-01', 'right', 1.0);
        CREATE TABLE fna_episode_master_v2 (
            research_id INTEGER, fna_episode_id INTEGER, fna_date_native DATE,
            resolved_fna_date DATE, bethesda_category INTEGER, specimen_site_raw VARCHAR,
            laterality VARCHAR, pathology_diagnosis VARCHAR
        );
        INSERT INTO fna_episode_master_v2 VALUES
            (1, 1, DATE '2024-02-01', DATE '2024-02-01', 3, 'thyroid', 'right', NULL);
        """
    )
    with pytest.raises(RuntimeError, match="tumor_episode_master_v2"):
        link129.run(con, dry_run=False, motherduck=False, output_schema=None, strict_release=True)


def test_exact_specimen_match_primary(link129) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9001::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-01-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   'A1'::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS SELECT * FROM (
            SELECT 9001::INTEGER AS research_id,
                   1::INTEGER AS fna_episode_id,
                   DATE '2024-01-05' AS fna_date_native,
                   'left'::VARCHAR AS laterality,
                   NULL::VARCHAR AS specimen_site_raw
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_history AS SELECT * FROM (
            SELECT 9001::BIGINT AS research_id,
                   1::INTEGER AS fna_index,
                   'a1'::VARCHAR AS specimen_received,
                   1.0::DOUBLE AS nodule_size_cm
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE tumor_episode_master_v2 AS SELECT * FROM (
            SELECT 9001::INTEGER AS research_id, DATE '2024-06-01'::DATE AS surgery_date
        ) t
        """
    )
    _materialize(con, link129)
    row = con.execute(
        "SELECT is_primary_link, specimen_match_flag FROM imaging_fna_linkage_mm_v1"
    ).fetchone()
    assert row is not None
    assert row[0] is True
    assert row[1] is True


def test_dual_fna_temporal_deterministic_primary(link129) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9002::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-01-01' AS exam_date,
                   'right'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS
        SELECT * FROM (VALUES
            (9002::INTEGER, 1::INTEGER, DATE '2024-01-10', 'right'::VARCHAR, NULL::VARCHAR),
            (9002::INTEGER, 2::INTEGER, DATE '2024-01-12', 'right'::VARCHAR, NULL::VARCHAR)
        ) AS v(research_id, fna_episode_id, fna_date_native, laterality, specimen_site_raw)
        """
    )
    _materialize(con, link129)
    row_prim = con.execute(
        "SELECT BOOL_OR(is_primary_link) FROM imaging_fna_linkage_mm_v1"
    ).fetchone()
    assert row_prim is not None
    prim = row_prim[0]
    assert prim is True
    row_amb = con.execute(
        "SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1 WHERE review_reason = 'ambiguous_multimatch'"
    ).fetchone()
    assert row_amb is not None
    amb = row_amb[0]
    assert amb == 0


def test_discordant_side_in_review_not_linked(link129) -> None:
    """Two FNAs in 0–90d window: not singleton → lateral discord stays review-only, no link."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9003::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-02-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS
        SELECT * FROM (VALUES
            (9003::INTEGER, 1::INTEGER, DATE '2024-02-05', 'right'::VARCHAR, NULL::VARCHAR),
            (9003::INTEGER, 2::INTEGER, DATE '2024-02-12', 'right'::VARCHAR, NULL::VARCHAR)
        ) AS v(research_id, fna_episode_id, fna_date_native, laterality, specimen_site_raw)
        """
    )
    _materialize(con, link129)
    row_link = con.execute("SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1").fetchone()
    assert row_link is not None
    n_link = row_link[0]
    assert n_link == 0
    row_rev = con.execute(
        "SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1 "
        "WHERE review_reason = 'discordant_laterality'"
    ).fetchone()
    assert row_rev is not None
    n_rev = row_rev[0]
    assert n_rev >= 1


def test_singleton_discordant_lateral_links_relaxed(link129) -> None:
    """Single FNA candidate in window, lateral discord, no specimen → relaxed_singleton_temporal."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9003::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-02-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS SELECT * FROM (
            SELECT 9003::INTEGER AS research_id,
                   1::INTEGER AS fna_episode_id,
                   DATE '2024-02-05' AS fna_date_native,
                   'right'::VARCHAR AS laterality,
                   NULL::VARCHAR AS specimen_site_raw
        ) t
        """
    )
    _materialize(con, link129)
    row = con.execute(
        """
        SELECT match_path, relaxed_eligibility_flag, is_primary_link
        FROM imaging_fna_linkage_mm_v1
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "relaxed_singleton_temporal"
    assert row[1] is True
    assert row[2] is True
    n_rev = con.execute(
        """
        SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1
        WHERE review_reason = 'discordant_laterality'
        """
    ).fetchone()
    assert n_rev is not None
    assert n_rev[0] == 0


def test_size_drift_gt_20pct_excluded(link129) -> None:
    """Sizes differ by >40% so row stays out of linkage and remains review-only."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9004::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-03-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   2.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS SELECT * FROM (
            SELECT 9004::INTEGER AS research_id,
                   1::INTEGER AS fna_episode_id,
                   DATE '2024-03-08' AS fna_date_native,
                   'left'::VARCHAR AS laterality,
                   NULL::VARCHAR AS specimen_site_raw
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_history AS SELECT * FROM (
            SELECT 9004::BIGINT AS research_id,
                   1::INTEGER AS fna_index,
                   NULL::VARCHAR AS specimen_received,
                   1.0::DOUBLE AS nodule_size_cm
        ) t
        """
    )
    _materialize(con, link129)
    row_n0 = con.execute("SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1").fetchone()
    assert row_n0 is not None
    assert row_n0[0] == 0
    row_sz = con.execute(
        "SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1 "
        "WHERE review_reason = 'size_drift_gt_20pct'"
    ).fetchone()
    assert row_sz is not None
    assert row_sz[0] >= 1


def test_relaxed_size_drift_25pct_links_with_flag(link129) -> None:
    """20–40% drift in 0–90d window is eligible via relaxed tier (not >40%)."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9004::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-03-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   2.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS SELECT * FROM (
            SELECT 9004::INTEGER AS research_id,
                   1::INTEGER AS fna_episode_id,
                   DATE '2024-03-08' AS fna_date_native,
                   'left'::VARCHAR AS laterality,
                   NULL::VARCHAR AS specimen_site_raw
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_history AS SELECT * FROM (
            SELECT 9004::BIGINT AS research_id,
                   1::INTEGER AS fna_index,
                   NULL::VARCHAR AS specimen_received,
                   1.5::DOUBLE AS nodule_size_cm
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE tumor_episode_master_v2 AS SELECT * FROM (
            SELECT 9004::INTEGER AS research_id, DATE '2024-06-01'::DATE AS surgery_date
        ) t
        """
    )
    _materialize(con, link129)
    row = con.execute(
        """
        SELECT is_primary_link, match_path, relaxed_eligibility_flag
        FROM imaging_fna_linkage_mm_v1
        """
    ).fetchone()
    assert row is not None
    assert row[0] is True
    assert row[1] == "relaxed_size_drift_40pct"
    assert row[2] is True


def test_relaxed_lateral_specimen_links_with_flag(link129) -> None:
    """Laterality discord but matching specimen accession in 0–90d → relaxed primary."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9006::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-02-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   'S99'::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS SELECT * FROM (
            SELECT 9006::INTEGER AS research_id,
                   1::INTEGER AS fna_episode_id,
                   DATE '2024-02-05' AS fna_date_native,
                   'right'::VARCHAR AS laterality,
                   NULL::VARCHAR AS specimen_site_raw
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_history AS SELECT * FROM (
            SELECT 9006::BIGINT AS research_id,
                   1::INTEGER AS fna_index,
                   's99'::VARCHAR AS specimen_received,
                   1.0::DOUBLE AS nodule_size_cm
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE tumor_episode_master_v2 AS SELECT * FROM (
            SELECT 9006::INTEGER AS research_id, DATE '2024-06-01'::DATE AS surgery_date
        ) t
        """
    )
    _materialize(con, link129)
    row = con.execute(
        """
        SELECT COUNT(*) FROM review_queue_imaging_fna_mm_v1
        WHERE review_reason = 'discordant_laterality'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == 0
    prim = con.execute(
        """
        SELECT match_path, relaxed_eligibility_flag, is_primary_link
        FROM imaging_fna_linkage_mm_v1
        """
    ).fetchone()
    assert prim is not None
    assert prim[0] == "relaxed_lateral_specimen"
    assert prim[1] is True
    assert prim[2] is True


def test_same_day_multi_fna_ordinals(link129) -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9005::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-04-01' AS exam_date,
                   'isthmus'::VARCHAR AS laterality,
                   0.9::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 AS
        SELECT * FROM (VALUES
            (9005::INTEGER, 1::INTEGER, DATE '2024-04-01', 'isthmus'::VARCHAR, NULL::VARCHAR),
            (9005::INTEGER, 2::INTEGER, DATE '2024-04-01', 'isthmus'::VARCHAR, NULL::VARCHAR)
        ) AS v(research_id, fna_episode_id, fna_date_native, laterality, specimen_site_raw)
        """
    )
    _materialize(con, link129)
    ords = con.execute(
        "SELECT ordinal_in_nodule FROM imaging_fna_linkage_mm_v1 ORDER BY fna_episode_id"
    ).fetchall()
    assert [r[0] for r in ords] == [1, 2]
    row_prim2 = con.execute(
        "SELECT BOOL_OR(is_primary_link) FROM imaging_fna_linkage_mm_v1"
    ).fetchone()
    assert row_prim2 is not None
    assert row_prim2[0] is True


def test_coalesce_resolved_fna_when_native_null(link129) -> None:
    """Use COALESCE(fna_date_native, resolved_fna_date) to match confirmation v1 EXISTS logic."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 AS SELECT * FROM (
            SELECT 9010::INTEGER AS research_id,
                   'n1'::VARCHAR AS nodule_id,
                   'ex1'::VARCHAR AS exam_id,
                   DATE '2024-01-01' AS exam_date,
                   'left'::VARCHAR AS laterality,
                   1.0::DOUBLE AS max_dimension_cm,
                   NULL::VARCHAR AS accession_number
        ) t
        """
    )
    con.execute(
        """
        CREATE TABLE fna_episode_master_v2 (
            research_id INTEGER,
            fna_episode_id INTEGER,
            fna_date_native DATE,
            resolved_fna_date DATE,
            laterality VARCHAR,
            specimen_site_raw VARCHAR
        );
        INSERT INTO fna_episode_master_v2 VALUES
            (9010, 1, NULL, DATE '2024-01-10', 'left', NULL);
        """
    )
    con.execute(
        """
        CREATE TABLE tumor_episode_master_v2 AS SELECT * FROM (
            SELECT 9010::INTEGER AS research_id, DATE '2024-06-01'::DATE AS surgery_date
        ) t
        """
    )
    _materialize(con, link129)
    row = con.execute(
        "SELECT COUNT(*) FROM imaging_fna_linkage_mm_v1 WHERE is_primary_link"
    ).fetchone()
    assert row is not None
    n = int(row[0])
    assert n == 1
