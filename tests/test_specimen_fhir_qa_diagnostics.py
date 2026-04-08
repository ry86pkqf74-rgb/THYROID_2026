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


def _load_mod119():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "md119", ROOT / "scripts" / "119_md_formalization_validate.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _happy_path_db() -> duckdb.DuckDBPyConnection:
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
    con.execute(ident)
    con.execute(tail)
    mod140 = _load_mod140()
    mod140.apply_specimen_genomics_binding(con)
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(diag_sql)
    return con


def test_qa_diagnostic_views_empty_on_happy_path() -> None:
    """142 views: no duplicate FP, orphans, broken refs, or provenance gaps (in-memory spine)."""
    con = _happy_path_db()

    d0 = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_duplicate_master_fp_v1").fetchone()
    assert d0 is not None and d0[0] == 0
    d0f = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_duplicate_focus_fp_v1").fetchone()
    assert d0f is not None and d0f[0] == 0
    d2 = con.execute(
        "SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_focus_master_v1"
    ).fetchone()
    assert d2 is not None and d2[0] == 0
    d2gf = con.execute(
        "SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_genomic_focus_v1"
    ).fetchone()
    assert d2gf is not None and d2gf[0] == 0
    d3 = con.execute(
        "SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_genomic_master_v1"
    ).fetchone()
    assert d3 is not None and d3[0] == 0
    d4 = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1").fetchone()
    assert d4 is not None and d4[0] == 0
    m = con.execute(
        "SELECT n_missing_identity_run FROM qa.v_diag_specimen_provenance_master_v1"
    ).fetchone()
    pf = con.execute(
        "SELECT n_missing_identity_run, n_rows FROM qa.v_diag_specimen_provenance_focus_v1"
    ).fetchone()
    assert pf is not None and int(pf[0] or 0) == 0 and int(pf[1] or 0) > 0
    assert m is not None and int(m[0] or 0) == 0
    g = con.execute(
        "SELECT n_high_tier_null_specimen FROM qa.v_diag_specimen_provenance_genomic_v1"
    ).fetchone()
    assert g is not None and int(g[0] or 0) == 0
    con.execute("SELECT * FROM qa.v_diag_specimen_review_burden_v1 LIMIT 5").fetchall()

    met = con.execute(
        """
        SELECT n_focus_rows, n_duplicate_fp_groups, n_orphan_focus_master,
               n_orphan_genomic_focus, n_missing_focus_provenance
        FROM qa.t_diag_specimen_focus_qa_metrics_v1
        """
    ).fetchone()
    assert met is not None
    assert int(met[0] or 0) > 0
    for i in range(1, 5):
        assert int(met[i] or 0) == 0


def test_focus_metrics_table_matches_view_counts() -> None:
    con = _happy_path_db()

    def _cnt(sql: str) -> int:
        row = con.execute(sql).fetchone()
        assert row is not None and row[0] is not None
        return int(row[0])

    n_dup_v = _cnt("SELECT COUNT(*) FROM qa.v_diag_specimen_duplicate_focus_fp_v1")
    n_orph_v = _cnt("SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_focus_master_v1")
    n_ogf_v = _cnt("SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_genomic_focus_v1")
    n_pf_v = _cnt(
        "SELECT n_missing_identity_run FROM qa.v_diag_specimen_provenance_focus_v1"
    )
    met = con.execute("SELECT * FROM qa.t_diag_specimen_focus_qa_metrics_v1").fetchone()
    assert met is not None
    assert int(met[1]) == n_dup_v
    assert int(met[3]) == n_orph_v
    assert int(met[4]) == n_ogf_v
    assert int(met[5]) == n_pf_v


def test_v_diag_orphan_genomic_focus_detects_bad_reference() -> None:
    con = _happy_path_db()
    con.execute(
        """
        UPDATE main.specimen_genomic_assay_v1
        SET specimen_focus_id = 'spf_definitely_missing'
        WHERE genomic_assay_id = (
          SELECT genomic_assay_id FROM main.specimen_genomic_assay_v1
          WHERE specimen_focus_id IS NOT NULL
          LIMIT 1
        )
        """
    )
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(diag_sql)
    row_n = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_genomic_focus_v1").fetchone()
    assert row_n is not None and int(row_n[0]) >= 1
    met = con.execute("SELECT n_orphan_genomic_focus FROM qa.t_diag_specimen_focus_qa_metrics_v1").fetchone()
    assert met is not None and int(met[0] or 0) >= 1


def test_v_diag_duplicate_focus_fingerprint_detected() -> None:
    con = _happy_path_db()
    con.execute(
        """
        CREATE OR REPLACE TABLE main.specimen_tumor_focus_v1 AS
        SELECT * FROM main.specimen_tumor_focus_v1
        UNION ALL
        (
          SELECT * REPLACE ('spf_dup_pytest'::VARCHAR AS specimen_focus_id)
          FROM main.specimen_tumor_focus_v1
          LIMIT 1
        )
        """
    )
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(diag_sql)
    row_nd = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_duplicate_focus_fp_v1").fetchone()
    assert row_nd is not None and int(row_nd[0]) >= 1
    row_mg = con.execute(
        "SELECT n_duplicate_fp_groups FROM qa.t_diag_specimen_focus_qa_metrics_v1"
    ).fetchone()
    assert row_mg is not None and int(row_mg[0]) >= 1


def test_v_diag_orphan_focus_master_detected() -> None:
    con = _happy_path_db()
    con.execute(
        """
        INSERT INTO main.specimen_tumor_focus_v1
        SELECT * REPLACE (
          'spf_orphan_master'::VARCHAR AS specimen_focus_id,
          'sm_not_in_master'::VARCHAR AS specimen_id
        )
        FROM main.specimen_tumor_focus_v1
        LIMIT 1
        """
    )
    con.execute(
        """
        UPDATE main.specimen_tumor_focus_v1 SET focus_fingerprint_sha256 = sha256(
          concat('orph_focus_fp|', specimen_focus_id)
        ) WHERE specimen_focus_id = 'spf_orphan_master'
        """
    )
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(diag_sql)
    row_of = con.execute("SELECT COUNT(*) FROM qa.v_diag_specimen_orphan_focus_master_v1").fetchone()
    assert row_of is not None and int(row_of[0]) >= 1


def test_v_diag_provenance_focus_detects_blank_build_run() -> None:
    con = _happy_path_db()
    con.execute(
        """
        UPDATE main.specimen_tumor_focus_v1
        SET identity_build_run_id = ''
        WHERE specimen_focus_id = (
          SELECT specimen_focus_id FROM main.specimen_tumor_focus_v1 LIMIT 1
        )
        """
    )
    diag_sql = (ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8")
    con.execute(diag_sql)
    row_pr = con.execute(
        "SELECT n_missing_identity_run FROM qa.v_diag_specimen_provenance_focus_v1"
    ).fetchone()
    assert row_pr is not None and int(row_pr[0]) >= 1


def test_check_13_fails_on_focus_diagnostic_defect_when_layer_complete() -> None:
    """Integrity defects must FAIL even when strict=False if all specimen/FHIR objects exist."""
    con = _happy_path_db()
    con.execute(
        """
        UPDATE main.specimen_genomic_assay_v1
        SET specimen_focus_id = 'spf_missing_for_check13'
        WHERE genomic_assay_id = (
          SELECT genomic_assay_id FROM main.specimen_genomic_assay_v1
          WHERE specimen_focus_id IS NOT NULL
          LIMIT 1
        )
        """
    )
    con.execute((ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8"))

    mod119 = _load_mod119()
    for strict in (False, True):
        results = mod119.ValidationResult("pytest")
        mod119.check_specimen_fhir_layer(con, results, strict=strict)
        fail_names = {c["check"] for c in results.checks if c["status"] == "FAIL"}
        assert any(
            "Specimen/FHIR QA diagnostics (142 surfaces + focus metrics)" == name for name in fail_names
        ), f"expected FAIL for focus orphan when strict={strict!r}"


def test_check_13_fails_on_metrics_mismatch_when_layer_complete() -> None:
    """Stale or broken t_diag rollups must FAIL (authoritative cross-check vs list views)."""
    con = _happy_path_db()
    con.execute(
        """
        UPDATE main.specimen_genomic_assay_v1
        SET specimen_focus_id = 'spf_metrics_mismatch_check13'
        WHERE genomic_assay_id = (
          SELECT genomic_assay_id FROM main.specimen_genomic_assay_v1
          WHERE specimen_focus_id IS NOT NULL
          LIMIT 1
        )
        """
    )
    con.execute(
        """
        UPDATE qa.t_diag_specimen_focus_qa_metrics_v1
        SET n_orphan_genomic_focus = 0
        """
    )
    mod119 = _load_mod119()
    results = mod119.ValidationResult("pytest")
    mod119.check_specimen_fhir_layer(con, results, strict=False)
    fail_names = {c["check"] for c in results.checks if c["status"] == "FAIL"}
    assert "Specimen/FHIR QA diagnostics (142 surfaces + focus metrics)" in fail_names


def test_v_diag_encounter_episode_flags_missing_episode_row() -> None:
    """If EpisodeOfCare row is removed, encounter JSON ref must surface as encounter_episode."""
    con = _happy_path_db()
    eid = con.execute(
        "SELECT episode_fhir_id FROM main.fhir_encounter_v1 LIMIT 1"
    ).fetchone()
    assert eid is not None
    con.execute(
        "DELETE FROM main.fhir_episode_of_care_v1 WHERE episode_fhir_id = ?",
        [eid[0]],
    )
    con.execute((ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8"))
    row = con.execute(
        """SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1
           WHERE issue = 'encounter_episode'"""
    ).fetchone()
    assert row is not None and int(row[0]) >= 1


def test_v_diag_encounter_episode_patient_mismatch_detected() -> None:
    con = _happy_path_db()
    ref = con.execute(
        """SELECT json_extract_string(resource_json, '$.episodeOfCare[0].reference')
           FROM main.fhir_encounter_v1 LIMIT 1"""
    ).fetchone()
    assert ref is not None and ref[0]
    con.execute(
        """
        UPDATE main.fhir_episode_of_care_v1
        SET patient_fhir_id = 'ffffffffffffffff'
        WHERE fhir_id = ?
        """,
        [ref[0]],
    )
    con.execute((ROOT / "scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql").read_text(encoding="utf-8"))
    row = con.execute(
        """SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1
           WHERE issue = 'encounter_episode_patient_mismatch'"""
    ).fetchone()
    assert row is not None and int(row[0]) >= 1


def test_contract_view_column_sets_documented() -> None:
    """Guardrail: diagnostic view/table names stay stable for 119 Check 13 and release gate."""
    import sys

    sys.path.insert(0, str(ROOT))
    from utils.specimen_fhir_release_gate import (
        SPECIMEN_FHIR_DIAG_TABLES as gate_tables,
        SPECIMEN_FHIR_DIAG_VIEWS as gate_views,
    )

    mod119 = _load_mod119()
    names = set(mod119.SPECIMEN_FHIR_DIAG_VIEWS)
    assert names == set(gate_views)
    assert mod119.SPECIMEN_FHIR_DIAG_TABLES == gate_tables
    assert "v_diag_specimen_duplicate_focus_fp_v1" in names
    assert "v_diag_specimen_orphan_genomic_focus_v1" in names
    assert "v_diag_specimen_provenance_focus_v1" in names
    assert mod119.SPECIMEN_FHIR_DIAG_TABLES == ("t_diag_specimen_focus_qa_metrics_v1",)
