"""Offline tests: specimen–genomics binding (140), ThyroSeq JSON ordinality, cross-tumor isolation."""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def _load_mod140():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "sgb140", ROOT / "scripts" / "140_md_specimen_genomics_binding.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_thyroseq_and_afirma_rows_have_specimen_id() -> None:
    mod140 = _load_mod140()
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa")
    con.execute("""
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, molecular_episode_id BIGINT, fna_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, surgery_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, score_rank BIGINT);
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id BIGINT, tumor_ordinal BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.specimen_tumor_focus_v1 (
      research_id BIGINT, surgery_episode_id BIGINT, specimen_id VARCHAR, specimen_focus_id VARCHAR);
    CREATE TABLE main.genetic_testing (research_id BIGINT, test_platform VARCHAR);
    CREATE TABLE main.thyroseq_molecular_enrichment (
      research_id BIGINT, source_row_hash VARCHAR, fusion_genes_json VARCHAR, allele_fractions_json VARCHAR);

    INSERT INTO main.molecular_test_episode_v2 VALUES
      (1, 10, 'ThyroSeq v3', DATE '2020-06-01'),
      (1, 11, 'Afirma', DATE '2020-06-15');
    INSERT INTO main.fna_molecular_linkage_v3 VALUES
      (1, 10, 100, 'exact_match', 1.0, 1),
      (1, 11, 101, 'exact_match', 1.0, 1);
    INSERT INTO main.preop_surgery_linkage_v3 VALUES
      (1, 100, 1000, 'high_confidence', 1),
      (1, 101, 1000, 'high_confidence', 1);
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES
      (1, 1000, 1000, 1, 'high_confidence', 1.0, 1);
    INSERT INTO main.specimen_tumor_focus_v1 VALUES (1, 1000, 'SPEC_A', 'FOC_A');
    INSERT INTO main.genetic_testing VALUES (1, 'Afirma');
    INSERT INTO main.thyroseq_molecular_enrichment VALUES (
      1, 'hash1', '["FU1","FU2"]', '{"BRAF": 0.12}');
    """)
    mod140.apply_specimen_genomics_binding(con)
    r = con.execute(
        """
        SELECT platform, specimen_id, linkage_confidence_tier
        FROM main.specimen_genomic_assay_v1
        WHERE source_table IN ('molecular_test_episode_v2', 'genetic_testing')
          AND payload_explode_ord = 0
        ORDER BY platform
        """
    ).fetchall()
    assert all(row[1] == "SPEC_A" for row in r if row[0] is not None)
    assert all(row[2] == "exact" for row in r)


def test_no_cross_tumor_specimen_overwrite() -> None:
    mod140 = _load_mod140()
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa")
    con.execute("""
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, molecular_episode_id BIGINT, fna_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, surgery_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, score_rank BIGINT);
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id BIGINT, tumor_ordinal BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.specimen_tumor_focus_v1 (
      research_id BIGINT, surgery_episode_id BIGINT, specimen_id VARCHAR, specimen_focus_id VARCHAR);

    INSERT INTO main.molecular_test_episode_v2 VALUES
      (9, 900, 'ThyroSeq', DATE '2021-01-01'),
      (9, 901, 'ThyroSeq', DATE '2021-06-01');
    INSERT INTO main.fna_molecular_linkage_v3 VALUES
      (9, 900, 9000, 'exact_match', 1.0, 1),
      (9, 901, 9001, 'exact_match', 1.0, 1);
    INSERT INTO main.preop_surgery_linkage_v3 VALUES
      (9, 9000, 7001, 'high_confidence', 1),
      (9, 9001, 7002, 'high_confidence', 1);
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES
      (9, 7001, 7001, 1, 'high_confidence', 1.0, 1),
      (9, 7002, 7002, 1, 'high_confidence', 1.0, 1);
    INSERT INTO main.specimen_tumor_focus_v1 VALUES
      (9, 7001, 'SPEC_L', 'FOC_L'),
      (9, 7002, 'SPEC_R', 'FOC_R');
    """)
    mod140.apply_specimen_genomics_binding(con, has_genetic=False, has_thyroseq=False)
    rows = con.execute(
        """SELECT molecular_episode_id, specimen_id FROM main.specimen_genomic_assay_v1
           WHERE source_table = 'molecular_test_episode_v2' ORDER BY molecular_episode_id"""
    ).fetchall()
    assert rows == [(900, "SPEC_L"), (901, "SPEC_R")]


def test_exploded_json_row_counts_stable() -> None:
    mod140 = _load_mod140()
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa")
    con.execute("""
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, molecular_episode_id BIGINT, fna_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, surgery_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, score_rank BIGINT);
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id BIGINT, tumor_ordinal BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.specimen_tumor_focus_v1 (
      research_id BIGINT, surgery_episode_id BIGINT, specimen_id VARCHAR, specimen_focus_id VARCHAR);
    CREATE TABLE main.thyroseq_molecular_enrichment (
      research_id BIGINT, source_row_hash VARCHAR, fusion_genes_json VARCHAR, allele_fractions_json VARCHAR);

    INSERT INTO main.molecular_test_episode_v2 VALUES (3, 30, 'ThyroSeq panel', DATE '2019-01-01');
    INSERT INTO main.fna_molecular_linkage_v3 VALUES (3, 30, 300, 'exact_match', 1.0, 1);
    INSERT INTO main.preop_surgery_linkage_v3 VALUES (3, 300, 3000, 'high_confidence', 1);
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES (3, 3000, 3000, 1, 'high_confidence', 1.0, 1);
    INSERT INTO main.specimen_tumor_focus_v1 VALUES (3, 3000, 'SX', 'FX');
    INSERT INTO main.thyroseq_molecular_enrichment VALUES (
      3, 'stable_hash', '["a","b","c"]', '[]');
    """)
    mod140.apply_specimen_genomics_binding(con, has_genetic=False, has_thyroseq=True)
    c1 = con.execute(
        "SELECT COUNT(*) FROM main.specimen_genomic_assay_v1 WHERE payload_field = 'fusion_genes_json'"
    ).fetchone()[0]
    mod140.apply_specimen_genomics_binding(con, has_genetic=False, has_thyroseq=True)
    c2 = con.execute(
        "SELECT COUNT(*) FROM main.specimen_genomic_assay_v1 WHERE payload_field = 'fusion_genes_json'"
    ).fetchone()[0]
    assert c1 == c2 == 3
    ordinals = con.execute(
        """SELECT payload_explode_ord FROM main.specimen_genomic_assay_v1
           WHERE payload_field = 'fusion_genes_json' ORDER BY payload_explode_ord"""
    ).fetchall()
    assert [r[0] for r in ordinals] == [1, 2, 3]


def test_multifocal_clears_focus_goes_to_qa() -> None:
    mod140 = _load_mod140()
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa")
    con.execute("""
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, molecular_episode_id BIGINT, fna_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, surgery_episode_id BIGINT,
      linkage_confidence_tier VARCHAR, score_rank BIGINT);
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id BIGINT, tumor_ordinal BIGINT,
      linkage_confidence_tier VARCHAR, linkage_score DOUBLE, score_rank BIGINT);
    CREATE TABLE main.specimen_tumor_focus_v1 (
      research_id BIGINT, surgery_episode_id BIGINT, specimen_id VARCHAR, specimen_focus_id VARCHAR);

    INSERT INTO main.molecular_test_episode_v2 VALUES (5, 50, 'Panel', DATE '2022-01-01');
    INSERT INTO main.fna_molecular_linkage_v3 VALUES (5, 50, 500, 'exact_match', 1.0, 1);
    INSERT INTO main.preop_surgery_linkage_v3 VALUES (5, 500, 5000, 'high_confidence', 1);
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES (5, 5000, 5000, 1, 'high_confidence', 1.0, 1);
    INSERT INTO main.specimen_tumor_focus_v1 VALUES
      (5, 5000, 'SM', 'F1'),
      (5, 5000, 'SM', 'F2');
    """)
    mod140.apply_specimen_genomics_binding(con, has_genetic=False, has_thyroseq=False)
    row = con.execute(
        """SELECT specimen_focus_id, linkage_confidence_tier
           FROM main.specimen_genomic_assay_v1 WHERE research_id = 5"""
    ).fetchone()
    assert row[0] is None
    assert row[1] == "plausible_review"
    nq = con.execute("SELECT COUNT(*) FROM qa.specimen_genomic_link_review_v1").fetchone()[0]
    assert nq >= 1
