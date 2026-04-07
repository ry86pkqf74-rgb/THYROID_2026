"""Tests for multimodal_contract_v1 release layer (script 128)."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_mm128():
    path = ROOT / "scripts" / "128_multimodal_contract_mm_v1.py"
    spec = importlib.util.spec_from_file_location("mm128", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _seed_minimal_upstream(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE linkage_master_v1 (
            research_id INTEGER, canonical_research_id INTEGER,
            euh_mrn VARCHAR, linkage_method VARCHAR, confidence DOUBLE
        );
        INSERT INTO linkage_master_v1 VALUES
            (100, 100, 'MRN1', 'direct', 1.0),
            (200, 100, 'MRN1', 'mrn_crosswalk', 0.95);

        CREATE TABLE mrn_crosswalk_v1 AS
        SELECT * FROM (VALUES
            (100, 'MRN1', CAST(NULL AS VARCHAR), 100, 'direct'),
            (200, 'MRN1', CAST(NULL AS VARCHAR), 100, 'mrn_crosswalk')
        ) t(research_id, euh_mrn, tec_mrn, canonical_research_id, linkage_method);

        CREATE TABLE operative_episode_detail_v2 (
            research_id INTEGER, surgery_episode_id INTEGER,
            surgery_date_native DATE, procedure_raw VARCHAR, procedure_normalized VARCHAR,
            laterality VARCHAR, central_neck_dissection_flag BOOLEAN,
            lateral_neck_dissection_flag BOOLEAN
        );
        INSERT INTO operative_episode_detail_v2 VALUES
            (100, 1, DATE '2020-06-01', 'thyroidectomy', 'thyroidectomy', 'right', FALSE, FALSE),
            (200, 1, DATE '2021-01-01', 'lobectomy', 'lobectomy', 'left', FALSE, FALSE);

        CREATE TABLE tumor_episode_master_v2 (
            research_id INTEGER, surgery_episode_id INTEGER, tumor_ordinal INTEGER,
            surgery_date VARCHAR, date_status VARCHAR,
            primary_histology VARCHAR, tumor_size_cm DOUBLE,
            t_stage VARCHAR, n_stage VARCHAR, overall_stage VARCHAR,
            laterality VARCHAR, multifocality_flag BOOLEAN
        );
        INSERT INTO tumor_episode_master_v2 VALUES
            (100, 1, 1, '2020-06-01', 'exact_source_date', 'PTC', 1.2,
             'T1', 'N0', 'I', 'right', FALSE),
            (100, 1, 2, '2020-06-01', 'exact_source_date', 'PTC', 0.5,
             'T1', 'N0', 'I', 'right', TRUE);

        CREATE TABLE fna_episode_master_v2 AS
        SELECT * FROM (VALUES
            (100, 1, DATE '2020-05-01', DATE '2020-05-01', 5,
             'thyroid', 'right', NULL::VARCHAR, NULL::VARCHAR)
        ) v(research_id, fna_episode_id, fna_date_native, resolved_fna_date,
            bethesda_category, specimen_site_raw, laterality, pathology_diagnosis);

        CREATE TABLE molecular_test_episode_v2 AS
        SELECT * FROM (VALUES
            (100, 1, DATE '2020-05-15', CAST('2020-05-15' AS VARCHAR),
             'ThyroSeq', 'positive', TRUE, FALSE)
        ) v(research_id, molecular_episode_id, test_date_native, resolved_test_date,
            platform, overall_result_class, braf_flag, ras_flag);

        CREATE TABLE imaging_nodule_master_v1 (
            research_id INTEGER, exam_date DATE, nodule_number INTEGER,
            exam_id VARCHAR, nodule_id VARCHAR, laterality VARCHAR, max_dimension_cm DOUBLE,
            tirads_reported INTEGER, suspicious_flag BOOLEAN,
            composition VARCHAR, echogenicity VARCHAR, shape VARCHAR,
            margins VARCHAR, calcifications VARCHAR, tirads_category VARCHAR,
            source_table VARCHAR
        );
        INSERT INTO imaging_nodule_master_v1 VALUES
            (100, DATE '2020-04-01', 1, 'ex1', 'nod1', 'right', 1.0, 4, TRUE,
             'solid', 'hypoechoic', 'oval', 'smooth', 'none', 'TR4', 'test');

        CREATE TABLE event_date_audit_v2 (
            domain VARCHAR, research_id INTEGER, native_date VARCHAR,
            resolved_date VARCHAR, date_status VARCHAR, date_confidence INTEGER,
            anchor_source VARCHAR, source_table VARCHAR
        );
        INSERT INTO event_date_audit_v2 VALUES
            ('fna', 100, '2020-05-01', '2020-05-01', 'ok', 100, 'fna', 'test'),
            ('molecular', 100, '2020-05-15', '2020-05-15', 'ok', 100, 'mol', 'test');

        CREATE TABLE patient_cross_domain_timeline_v2 (
            research_id INTEGER, event_type VARCHAR, domain VARCHAR,
            event_date DATE, episode_id INTEGER, event_detail VARCHAR
        );
        INSERT INTO patient_cross_domain_timeline_v2 VALUES
            (100, 'surgery', 'operative', DATE '2020-06-01', 1, 'x');

        CREATE TABLE surgery_pathology_linkage_v3 (
            research_id INTEGER, surgery_episode_id INTEGER, path_surgery_id INTEGER,
            tumor_ordinal INTEGER, surg_date DATE, path_date DATE, day_gap INTEGER,
            surg_lat VARCHAR, path_lat VARCHAR, path_size_cm DOUBLE,
            n_candidates INTEGER, linkage_score DOUBLE, score_rank INTEGER,
            linkage_confidence_tier VARCHAR, linkage_reason_summary VARCHAR,
            analysis_eligible_link_flag BOOLEAN
        );
        INSERT INTO surgery_pathology_linkage_v3 VALUES
            (100, 1, 1, 1, DATE '2020-06-01', DATE '2020-06-01', 0,
             'right', 'right', 1.2, 1, 0.9, 1, 'exact_match', 'ok', TRUE),
            (100, 1, 1, 2, DATE '2020-06-01', DATE '2020-06-01', 0,
             'right', 'right', 0.5, 1, 0.88, 1, 'high_confidence', 'ok2', TRUE);

        CREATE TABLE preop_surgery_linkage_v3 AS
        SELECT * FROM (VALUES
            (100, 1, 'fna', 1, DATE '2020-05-01', DATE '2020-06-01', 30, 30,
             'right', 'right', 1, 0.85, 1, 'high_confidence', '+30d_to_surgery+right',
             TRUE)
        ) t(research_id, preop_episode_id, preop_type, surgery_episode_id,
            preop_date, surgery_date, day_gap, abs_gap, preop_lat, surg_lat,
            n_candidates, linkage_score, score_rank, linkage_confidence_tier,
            linkage_reason_summary, analysis_eligible_link_flag);

        CREATE TABLE fna_molecular_linkage_v3 AS
        SELECT * FROM (VALUES
            (100, 1, 1, DATE '2020-05-01', DATE '2020-05-15', 14, 14,
             'right', 'ThyroSeq', 1, 0.8, 1, 'high_confidence', 'ok', TRUE)
        ) t(research_id, fna_episode_id, molecular_episode_id, fna_date_native,
            test_date_native, day_gap, abs_gap, laterality, platform,
            n_candidates, linkage_score, score_rank, linkage_confidence_tier,
            linkage_reason_summary, analysis_eligible_link_flag);

        CREATE TABLE pathology_rai_linkage_v3 AS
        SELECT * FROM (VALUES
            (100, 1, 1, DATE '2020-06-01', DATE '2020-08-01', 61, 61,
             'definite_received', 100.0, 1, 0.75, 1, 'plausible', 'ok', TRUE)
        ) t(research_id, surgery_episode_id, rai_episode_id, surgery_date, rai_date,
            days_post_surgery, abs_days, rai_assertion_status, dose_mci,
            n_candidates, linkage_score, score_rank, linkage_confidence_tier,
            linkage_reason_summary, analysis_eligible_link_flag);
        """
    )


class TestMultimodalContractBuild:
    SCHEMA = "mm_contract_dev"

    def test_deterministic_person_id(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        mod.build_all(con, self.SCHEMA)
        p1 = con.execute(
            f"SELECT person_id FROM {self.SCHEMA}.dim_patient_mm_v1 "
            "WHERE canonical_research_id = 100"
        ).fetchone()[0]
        assert p1.startswith("mmv1_p_")
        n = con.execute(
            f"SELECT COUNT(DISTINCT person_id) FROM {self.SCHEMA}.dim_patient_mm_v1"
        ).fetchone()[0]
        assert n == 1

    def test_tumor_rows_not_collapsed(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        mod.build_all(con, self.SCHEMA)
        cnt = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.fact_tumor_mm_v1 WHERE research_id = 100"
        ).fetchone()[0]
        assert cnt == 2
        m = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_multitumor_expansion_mm_v1"
        ).fetchone()[0]
        assert m == 0

    def test_nodes_invariant_clean_on_happy_path(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        mod.build_all(con, self.SCHEMA)
        n = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_nodes_invariant_mm_v1"
        ).fetchone()[0]
        assert n == 0

    def test_primary_link_requires_unique_non_weak(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute(
            """
            INSERT INTO surgery_pathology_linkage_v3 VALUES
                (200, 1, 1, 1, DATE '2021-01-01', DATE '2021-01-01', 0,
                 'left', 'right', 1.0, 2, 0.6, 1, 'plausible', 'multi', TRUE);
            """
        )
        con.execute(
            """
            INSERT INTO tumor_episode_master_v2 VALUES
                (200, 1, 1, '2021-01-01', 'exact_source_date', 'PTC', 1.0,
                 'T2', 'N0', 'I', 'left', FALSE);
            """
        )
        mod.build_all(con, self.SCHEMA)
        amb = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_ambiguous_multimodal_linkage_mm_v1 "
            "WHERE domain = 'surgery_pathology' AND research_id = '200'"
        ).fetchone()[0]
        assert amb >= 1


class TestMultimodalContractViolations:
    SCHEMA = "mm_contract_dev"

    def test_side_lobe_mismatch_detected(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute("DELETE FROM surgery_pathology_linkage_v3 WHERE research_id = 100")
        con.execute(
            """
            INSERT INTO surgery_pathology_linkage_v3 VALUES
                (100, 1, 1, 1, DATE '2020-06-01', DATE '2020-06-01', 0,
                 'right', 'left', 1.2, 1, 0.9, 1, 'exact_match', 'lat_bad', TRUE);
            """
        )
        mod.build_all(con, self.SCHEMA)
        n = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_side_lobe_mismatch_mm_v1"
        ).fetchone()[0]
        assert n >= 1

    def test_preop_temporal_violation(self):
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute("DELETE FROM preop_surgery_linkage_v3")
        con.execute(
            """
            INSERT INTO preop_surgery_linkage_v3 VALUES
                (100, 1, 'fna', 1, DATE '2020-07-01', DATE '2020-06-01', -30, 30,
                 'right', 'right', 1, 0.9, 1, 'exact_match', 'badorder', TRUE);
            """
        )
        mod.build_all(con, self.SCHEMA)
        n = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_preop_temporal_order_mm_v1"
        ).fetchone()[0]
        assert n >= 1


class TestReleaseValidationMetrics:
    SCHEMA = "mm_contract_dev"

    def test_release_validation_metrics_populated(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        mod.build_all(con, self.SCHEMA)
        m = mod.collect_release_validation_metrics(con, self.SCHEMA)
        assert "blocking_validation_row_counts" in m
        assert "imaging_fna_link_flags" in m
        assert "ambiguous_multimodal_by_domain" in m
        assert "review_queue_by_reason" in m
        assert m["blocking_validation_row_counts"]["val_nodes_invariant_mm_v1"] == 0

    def test_review_queue_deltas_vs_prior_artifact(self, tmp_path: Path) -> None:
        mod = _load_mm128()
        prior = {
            "release_validation_metrics": {
                "review_queue_by_reason": {"ambiguous_multimatch": 10, "discordant_laterality": 2},
            }
        }
        pp = tmp_path / "prior.json"
        pp.write_text(json.dumps(prior), encoding="utf-8")
        cur = {"ambiguous_multimatch": 8, "discordant_laterality": 5, "size_drift_gt_20pct": 1}
        delta = mod.compute_review_queue_deltas(cur, mod.load_prior_gate_artifact(pp))
        assert delta["available"] is True
        assert delta["by_reason"]["ambiguous_multimatch"] == -2
        assert delta["by_reason"]["discordant_laterality"] == 3
        assert delta["net_change_review_queue"] == 2


class TestStrictReleaseGate:
    SCHEMA = "mm_contract_dev"

    def test_missing_upstream_raises_without_bootstrap(self) -> None:
        con = duckdb.connect(":memory:")
        for t in (
            "operative_episode_detail_v2",
            "tumor_episode_master_v2",
            "molecular_test_episode_v2",
            "imaging_nodule_master_v1",
        ):
            con.execute(f"CREATE TABLE {t} (research_id INTEGER)")
            con.execute(f"INSERT INTO {t} VALUES (1)")
        from scripts.mm_contract_upstream import ensure_upstream_sources

        with pytest.raises(RuntimeError, match="Missing required upstream"):
            ensure_upstream_sources(
                con, self.SCHEMA, section=lambda _m: None, allow_bootstrap=False
            )

    def test_strict_release_passes_on_clean_seed(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        boot = mod.build_all(con, self.SCHEMA, strict_release=True)
        mod.assert_strict_release_passes(
            con, self.SCHEMA, bootstrapped_upstream=boot
        )

    def test_strict_release_fails_on_blocking_validation(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute("DELETE FROM surgery_pathology_linkage_v3 WHERE research_id = 100")
        con.execute(
            """
            INSERT INTO surgery_pathology_linkage_v3 VALUES
                (100, 1, 1, 1, DATE '2020-06-01', DATE '2020-06-01', 0,
                 'right', 'left', 1.2, 1, 0.9, 1, 'exact_match', 'lat_bad', TRUE);
            """
        )
        boot = mod.build_all(con, self.SCHEMA, strict_release=True)
        with pytest.raises(RuntimeError, match="Strict release failed"):
            mod.assert_strict_release_passes(
                con, self.SCHEMA, bootstrapped_upstream=boot
            )

    def test_strict_gate_rejects_bootstrapped_upstream_metadata(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.SCHEMA};")
        with pytest.raises(RuntimeError, match="Strict release failed"):
            mod.assert_strict_release_passes(
                con,
                self.SCHEMA,
                bootstrapped_upstream=["linkage_master_v1"],
            )


class TestImagingFnaContractIntegration:
    SCHEMA = "mm_contract_dev"

    def test_temporal_link_matches_fact_surrogate_ids(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        mod.build_all(con, self.SCHEMA)
        ei = con.execute(
            f"SELECT imaging_fact_id FROM {self.SCHEMA}.fact_imaging_mm_v1 "
            "WHERE research_id = 100"
        ).fetchone()
        ef = con.execute(
            f"SELECT fna_fact_id FROM {self.SCHEMA}.fact_fna_mm_v1 WHERE research_id = 100"
        ).fetchone()
        assert ei is not None and ef is not None
        expect_img = ei[0]
        expect_fna = ef[0]
        row = con.execute(
            f"SELECT imaging_id, fna_id, is_primary_link, link_confidence FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        assert row is not None
        assert row[0] == expect_img
        assert row[1] == expect_fna
        assert row[2] is True
        assert row[3] == pytest.approx(0.85)

    def test_ambiguous_multi_fna_flags_on_contract_link(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute(
            """
            INSERT INTO fna_episode_master_v2 VALUES
                (100, 2, DATE '2020-05-15', DATE '2020-05-15', 4,
                 'thyroid', 'right', NULL::VARCHAR, NULL::VARCHAR)
            """
        )
        mod.build_all(con, self.SCHEMA)
        pr = con.execute(
            f"SELECT BOOL_OR(is_primary_link) FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        am = con.execute(
            f"SELECT BOOL_OR(flag_ambiguous_linkage) FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        mu = con.execute(
            f"SELECT BOOL_OR(flag_multi_fna_nodule) FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        assert pr is not None and am is not None and mu is not None
        prim = pr[0]
        amb = am[0]
        multi = mu[0]
        assert prim is False
        assert amb is True
        assert multi is True

    def test_discordant_laterality_surfaces_in_blockers_not_in_link(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute("UPDATE fna_episode_master_v2 SET laterality = 'left' WHERE research_id = 100")
        mod.build_all(con, self.SCHEMA)
        nl = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        nb = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.val_imaging_fna_contract_blockers_mm_v1 "
            "WHERE review_reason = 'discordant_laterality'"
        ).fetchone()
        assert nl is not None and nb is not None
        n_link = nl[0]
        n_blk = nb[0]
        assert n_link == 0
        assert n_blk >= 1

    def test_size_drift_gt_20pct_surfaces_in_review(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute(
            """
            CREATE TABLE fna_history AS
            SELECT * FROM (
                SELECT 100::BIGINT AS research_id, 1::INTEGER AS fna_index,
                       NULL::VARCHAR AS specimen_received, 0.5::DOUBLE AS nodule_size_cm
            ) t
            """
        )
        mod.build_all(con, self.SCHEMA)
        nl2 = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        nr = con.execute(
            f"SELECT COUNT(*) FROM {self.SCHEMA}.review_queue_imaging_fna_mm_v1 "
            "WHERE review_reason = 'size_drift_gt_20pct'"
        ).fetchone()
        assert nl2 is not None and nr is not None
        n_link = nl2[0]
        n_rev = nr[0]
        assert n_link == 0
        assert n_rev >= 1

    def test_same_day_fna_ordinals_preserved(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute("DELETE FROM fna_episode_master_v2")
        con.execute(
            """
            INSERT INTO fna_episode_master_v2 VALUES
                (100, 1, DATE '2020-05-01', DATE '2020-05-01', 5,
                 'thyroid', 'right', NULL::VARCHAR, NULL::VARCHAR),
                (100, 2, DATE '2020-05-01', DATE '2020-05-01', 5,
                 'thyroid', 'right', NULL::VARCHAR, NULL::VARCHAR)
            """
        )
        mod.build_all(con, self.SCHEMA)
        ords = con.execute(
            f"SELECT ordinal_in_nodule FROM {self.SCHEMA}.link_imaging_fna_mm_v1 ORDER BY fna_episode_id"
        ).fetchall()
        assert [r[0] for r in ords] == sorted([r[0] for r in ords])
        assert sorted([r[0] for r in ords]) == [1, 2]

    def test_exact_specimen_match_primary_in_contract(self) -> None:
        mod = _load_mm128()
        con = duckdb.connect(":memory:")
        _seed_minimal_upstream(con)
        con.execute(
            """
            CREATE TABLE fna_history AS
            SELECT * FROM (
                SELECT 100::BIGINT AS research_id, 1::INTEGER AS fna_index,
                       'accx'::VARCHAR AS specimen_received, 1.0::DOUBLE AS nodule_size_cm
            ) t
            """
        )
        con.execute(
            """
            UPDATE imaging_nodule_master_v1
            SET max_dimension_cm = 1.0
            WHERE research_id = 100
            """
        )
        con.execute("ALTER TABLE imaging_nodule_master_v1 ADD COLUMN accession_number VARCHAR")
        con.execute(
            "UPDATE imaging_nodule_master_v1 SET accession_number = 'ACCX' WHERE research_id = 100"
        )
        mod.build_all(con, self.SCHEMA)
        row = con.execute(
            f"SELECT is_primary_link, specimen_match_flag, match_path, link_confidence FROM {self.SCHEMA}.link_imaging_fna_mm_v1"
        ).fetchone()
        assert row is not None
        assert row[0] is True
        assert row[1] is True
        assert row[2] == "specimen_key"
        assert row[3] == pytest.approx(1.0)
