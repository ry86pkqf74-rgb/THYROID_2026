"""Offline tests: qa.v_diag_* views (142) after identity + FHIR tail + genomics binding."""

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


def test_qa_diagnostic_views_empty_on_happy_path() -> None:
    """142 views: no duplicate FP, orphans, broken refs, or provenance gaps (in-memory spine)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa;")
    con.execute("""
    CREATE TABLE main.synoptic_tumor_long_v1 (
      synoptic_row_ix BIGINT, research_id BIGINT, surg_date VARCHAR, thyroid_procedure VARCHAR,
      tumor_index BIGINT, site VARCHAR, histologic_type VARCHAR
    );
    CREATE TABLE main.path_synoptics_encounter_qc_v1 AS
    SELECT * FROM (VALUES
      (1::BIGINT, '2020-01-15'::VARCHAR, 'PTC'::VARCHAR, DATE '2020-01-15', 1::BIGINT, 'tier1'::VARCHAR),
      (1::BIGINT, '2020-02-01'::VARCHAR, 'PTC'::VARCHAR, DATE '2020-02-01', 1::BIGINT, 'tier1'::VARCHAR)
    ) t(research_id, surg_date, tumor_1_histologic_type, surg_date_canonical, encounter_synoptic_row_ix, surg_date_parse_tier);
    INSERT INTO main.synoptic_tumor_long_v1 VALUES
      (1, 1, '2020-01-15', 'thyroidectomy', 1, 'R', 'PTC'),
      (2, 1, '2020-02-01', 'thyroidectomy', 2, 'L', 'PTC');
    CREATE TABLE main.surgery_pathology_linkage_v3 (
      research_id BIGINT, surgery_episode_id BIGINT, path_surgery_id VARCHAR, tumor_ordinal BIGINT,
      day_gap BIGINT, surg_lat VARCHAR, path_lat VARCHAR, n_candidates BIGINT,
      linkage_score DOUBLE, score_rank BIGINT, linkage_confidence_tier VARCHAR,
      linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN
    );
    INSERT INTO main.surgery_pathology_linkage_v3 VALUES
      (1, 100, 'A', 1, 0, 'R', 'R', 1, 0.9, 1, 'high_confidence', '', TRUE),
      (1, 200, 'B', 2, 0, 'L', 'L', 1, 0.9, 1, 'high_confidence', '', TRUE);
    CREATE TABLE main.fna_molecular_linkage_v3 (
      research_id BIGINT, fna_episode_id BIGINT, molecular_episode_id BIGINT,
      fna_date_native DATE, test_date_native DATE, day_gap BIGINT, laterality VARCHAR, platform VARCHAR,
      n_candidates BIGINT, linkage_score DOUBLE, score_rank BIGINT,
      linkage_confidence_tier VARCHAR, linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN
    );
    CREATE TABLE main.preop_surgery_linkage_v3 (
      research_id BIGINT, preop_episode_id BIGINT, preop_type VARCHAR, surgery_episode_id BIGINT,
      preop_date DATE, surgery_date DATE, day_gap BIGINT, score_rank BIGINT, n_candidates BIGINT,
      linkage_confidence_tier VARCHAR, linkage_reason_summary VARCHAR, analysis_eligible_link_flag BOOLEAN,
      preop_lat VARCHAR, surg_lat VARCHAR
    );
    CREATE TABLE main.molecular_test_episode_v2 (research_id BIGINT, molecular_episode_id BIGINT,
      platform VARCHAR, test_date_native DATE);
    INSERT INTO main.molecular_test_episode_v2 VALUES
      (1, 10, 'ThyroSeq v3', DATE '2020-06-01'),
      (1, 11, 'Afirma', DATE '2020-06-15');
    INSERT INTO main.fna_molecular_linkage_v3 VALUES
      (1, 100, 10, NULL, DATE '2020-06-01', 0, NULL, 'ThyroSeq v3', 1, 1.0, 1, 'exact_match', '', TRUE),
      (1, 101, 11, NULL, DATE '2020-06-15', 0, NULL, 'Afirma', 1, 1.0, 1, 'exact_match', '', TRUE);
    INSERT INTO main.preop_surgery_linkage_v3 VALUES
      (1, 100, 'fna', 100, NULL, NULL, 0, 1, 1, 'high_confidence', '', TRUE, NULL, NULL),
      (1, 101, 'fna', 200, NULL, NULL, 0, 1, 1, 'high_confidence', '', TRUE, NULL, NULL);
    CREATE TABLE main.tumor_episode_master_v2 (
      research_id BIGINT, surgery_episode_id BIGINT, surgery_date VARCHAR
    );
    INSERT INTO main.tumor_episode_master_v2 VALUES
      (1, 100, '2020-01-15'),
      (1, 200, '2020-02-01');
    """)
    ident = (
        (ROOT / "scripts/sql/139_specimen_identity_layer_ddl.sql")
        .read_text(encoding="utf-8")
        .replace("__BUILD_RUN_ID__", "pytest_qa_diag")
    )
    tail = (ROOT / "scripts/sql/138_specimen_fhir_tail_ddl.sql").read_text(encoding="utf-8")
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(ident)
    con.execute(tail)
    mod140 = _load_mod140()
    mod140.apply_specimen_genomics_binding(con)
    con.execute(diag_sql)

    assert con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_duplicate_master_fp_v1").fetchone()[0] == 0
    assert (
        con.execute(
            "SELECT COUNT(*) FROM ( SELECT focus_fingerprint_sha256 FROM "
            "main.specimen_tumor_focus_v1 GROUP BY 1 HAVING COUNT(*) > 1) _t"
        ).fetchone()[0]
        == 0
    )
    assert (
        con.execute(
            "SELECT COUNT(*) FROM main.specimen_tumor_focus_v1 f "
            "LEFT JOIN main.specimen_master_v1 m ON f.specimen_id = m.specimen_id "
            "WHERE m.specimen_id IS NULL"
        ).fetchone()[0]
        == 0
    )
    assert (
        con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_genomic_master_v1").fetchone()[0] == 0
    )
    assert con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1").fetchone()[0] == 0
    m = con.execute(
        "SELECT n_missing_identity_run FROM qa.v_diag_specimen_provenance_master_v1"
    ).fetchone()
    nmiss_f = con.execute(
        "SELECT COUNT(*) FILTER (WHERE TRIM(COALESCE(identity_build_run_id, '')) = '') "
        "FROM main.specimen_tumor_focus_v1"
    ).fetchone()[0]
    assert m is not None and int(m[0] or 0) == 0
    assert int(nmiss_f or 0) == 0
    g = con.execute(
        "SELECT n_high_tier_null_specimen FROM qa.v_diag_specimen_provenance_genomic_v1"
    ).fetchone()
    assert g is not None and int(g[0] or 0) == 0
    con.execute("SELECT * FROM qa.v_diag_specimen_review_burden_v1 LIMIT 5").fetchall()


def test_contract_view_column_sets_documented() -> None:
    """Guardrail: diagnostic view names stay stable for 119_md_formalization_validate.py."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "md119", ROOT / "scripts" / "119_md_formalization_validate.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    names = set(mod.SPECIMEN_FHIR_DIAG_VIEWS)
    assert "v_diag_specimen_fhir_broken_refs_v1" in names
    assert names == {
        "v_diag_specimen_duplicate_master_fp_v1",
        "v_diag_specimen_orphan_genomic_master_v1",
        "v_diag_specimen_fhir_broken_refs_v1",
        "v_diag_specimen_provenance_master_v1",
        "v_diag_specimen_provenance_genomic_v1",
        "v_diag_specimen_review_burden_v1",
    }
