"""Tests for utils.canonical_nodule_linkage + script 149 assumptions."""
from __future__ import annotations

import duckdb
import pytest

from utils.canonical_nodule_linkage import (
    canonical_nodule_linkage_sql,
    discordance_sql,
    manual_review_queue_sql,
    qc_summary_sql,
)


@pytest.fixture
def linkage_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE imaging_nodule_master_v1 (
            research_id INTEGER,
            nodule_id VARCHAR,
            exam_id VARCHAR,
            exam_date DATE,
            nodule_number INTEGER,
            laterality VARCHAR,
            location_raw VARCHAR,
            max_dimension_cm DOUBLE,
            tirads_category VARCHAR,
            tirads_reported INTEGER,
            tirads_acr_recalculated INTEGER,
            source_table VARCHAR
        );
        INSERT INTO imaging_nodule_master_v1 VALUES
        (1001, 'n1', 'e1', DATE '2024-01-10', 1, 'right', 'right mid', 1.2,
         'TR4', NULL, NULL, 'test');

        CREATE TABLE imaging_fna_linkage_mm_v1 (
            research_id INTEGER,
            nodule_id VARCHAR,
            fna_episode_id INTEGER,
            fna_event_date DATE,
            match_path VARCHAR,
            specimen_match_flag BOOLEAN,
            n_candidates_for_nodule INTEGER,
            is_primary_link BOOLEAN,
            ordinal_in_nodule INTEGER
        );
        INSERT INTO imaging_fna_linkage_mm_v1 VALUES
        (1001, 'n1', 1, DATE '2024-02-01', 'specimen_key', TRUE, 1, TRUE, 1);

        CREATE TABLE fna_episode_master_v2 (
            research_id INTEGER,
            fna_episode_id INTEGER,
            fna_date_native DATE,
            bethesda_category INTEGER,
            bethesda_raw VARCHAR,
            specimen_site_raw VARCHAR,
            laterality VARCHAR
        );
        INSERT INTO fna_episode_master_v2 VALUES
        (1001, 1, DATE '2024-02-01', 3, 'III', 'right', 'right');

        CREATE TABLE fna_molecular_linkage_v3 (
            research_id INTEGER,
            fna_episode_id INTEGER,
            molecular_episode_id INTEGER,
            score_rank INTEGER,
            linkage_confidence_tier VARCHAR,
            linkage_score DOUBLE,
            n_candidates INTEGER
        );
        INSERT INTO fna_molecular_linkage_v3 VALUES
        (1001, 1, 10, 1, 'exact_match', 0.95, 1);

        CREATE TABLE molecular_test_episode_v2 (
            research_id INTEGER,
            molecular_episode_id INTEGER,
            test_date_native DATE,
            platform VARCHAR,
            result VARCHAR,
            overall_result_class VARCHAR
        );
        INSERT INTO molecular_test_episode_v2 VALUES
        (1001, 10, DATE '2024-02-15', 'ThyroSeq', 'positive', 'positive');

        CREATE TABLE preop_surgery_linkage_v3 (
            research_id INTEGER,
            preop_episode_id INTEGER,
            preop_type VARCHAR,
            surgery_episode_id INTEGER,
            surgery_date DATE,
            score_rank INTEGER,
            linkage_confidence_tier VARCHAR,
            linkage_score DOUBLE,
            n_candidates INTEGER
        );
        INSERT INTO preop_surgery_linkage_v3 VALUES
        (1001, 1, 'fna', 100, DATE '2024-04-01', 1, 'high_confidence', 0.9, 1);

        CREATE TABLE surgery_pathology_linkage_v3 (
            research_id INTEGER,
            surgery_episode_id INTEGER,
            path_surgery_id INTEGER,
            tumor_ordinal INTEGER,
            score_rank INTEGER,
            linkage_confidence_tier VARCHAR,
            linkage_score DOUBLE,
            n_candidates INTEGER
        );
        INSERT INTO surgery_pathology_linkage_v3 VALUES
        (1001, 100, 100, 1, 1, 'high_confidence', 0.92, 1);

        CREATE TABLE tumor_episode_master_v2 (
            research_id INTEGER,
            surgery_episode_id INTEGER,
            tumor_ordinal INTEGER,
            primary_histology VARCHAR,
            t_stage VARCHAR,
            n_stage VARCHAR,
            margin_status VARCHAR,
            tumor_size_cm DOUBLE,
            multifocality_flag BOOLEAN,
            number_of_tumors INTEGER
        );
        INSERT INTO tumor_episode_master_v2 VALUES
        (1001, 100, 1, 'PTC classic', 'T2', 'N0', 'negative', 1.5, FALSE, 1);

        CREATE TABLE operative_episode_detail_v2 (
            research_id INTEGER,
            surgery_episode_id INTEGER,
            surgery_date_native DATE
        );
        INSERT INTO operative_episode_detail_v2 VALUES
        (1001, 100, DATE '2024-04-01');
        """
    )
    return con


def test_canonical_linkage_happy_path(linkage_db: duckdb.DuckDBPyConnection) -> None:
    df = linkage_db.execute(canonical_nodule_linkage_sql()).df()
    assert len(df) == 1
    row = df.iloc[0]
    assert row["research_id"] == 1001
    assert row["fna_date_first"] is not None
    assert int(row["molecular_episode_id"]) == 10
    assert int(row["surgery_episode_id_linked"]) == 100
    assert row["final_histology"] == "PTC classic"
    assert bool(row["manual_review_needed_flag"]) is False


def test_manual_review_bethesda34_without_mol(linkage_db: duckdb.DuckDBPyConnection) -> None:
    linkage_db.execute("DELETE FROM fna_molecular_linkage_v3")
    linkage_db.execute("DELETE FROM molecular_test_episode_v2")
    df = linkage_db.execute(canonical_nodule_linkage_sql()).df()
    assert bool(df.iloc[0]["manual_review_needed_flag"]) is True


def test_qc_and_discordance_run(linkage_db: duckdb.DuckDBPyConnection) -> None:
    qc = linkage_db.execute(qc_summary_sql()).df()
    assert qc.iloc[0]["n_imaging_nodule_rows"] == 1
    disc = linkage_db.execute(discordance_sql()).df()
    assert len(disc) >= 1
    mrq = linkage_db.execute(manual_review_queue_sql()).df()
    assert isinstance(mrq.shape[0], int)
