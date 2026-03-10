"""Tests for cross-domain linkage confidence tier assignment."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class TestImagingFnaLinkageConfidence:
    def _setup_and_link(self, con, img_date, fna_date, img_lat=None, fna_lat=None):
        con.execute("CREATE OR REPLACE TABLE imaging_nodule_long_v2 AS SELECT 1 AS research_id, 'n1' AS nodule_id, 'ex1' AS imaging_exam_id, 'us' AS modality, CAST(? AS DATE) AS exam_date_native, ? AS laterality, 1.0 AS size_cm_max, NULL AS linked_fna_episode_id", [img_date, img_lat])
        con.execute("CREATE OR REPLACE TABLE fna_episode_master_v2 AS SELECT 1 AS research_id, 'f1' AS fna_episode_id, CAST(? AS DATE) AS fna_date_native, ? AS laterality, NULL AS bethesda_category, NULL AS specimen_site_raw, NULL AS linked_molecular_episode_id, NULL AS linked_surgery_episode_id", [fna_date, fna_lat])
        # Run the linkage SQL inline
        con.execute("""
            CREATE OR REPLACE TABLE imaging_fna_linkage_v2 AS
            WITH img AS (
                SELECT research_id, nodule_id, imaging_exam_id, modality,
                       exam_date_native, laterality, size_cm_max
                FROM imaging_nodule_long_v2
                WHERE exam_date_native IS NOT NULL
            ),
            fna AS (
                SELECT research_id, fna_episode_id, fna_date_native, laterality AS fna_lat
                FROM fna_episode_master_v2
                WHERE fna_date_native IS NOT NULL
            ),
            candidates AS (
                SELECT
                    img.research_id, img.nodule_id, img.imaging_exam_id,
                    fna.fna_episode_id,
                    img.exam_date_native AS img_date, fna.fna_date_native AS fna_date,
                    ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) AS day_gap,
                    CASE
                        WHEN img.laterality = fna.fna_lat THEN TRUE
                        WHEN img.laterality IS NULL OR fna.fna_lat IS NULL THEN NULL
                        ELSE FALSE
                    END AS laterality_match,
                    CASE
                        WHEN img.exam_date_native = fna.fna_date_native
                             AND COALESCE(img.laterality = fna.fna_lat, TRUE) THEN 'exact_match'
                        WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 7
                             THEN 'high_confidence'
                        WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 90
                             AND COALESCE(img.laterality = fna.fna_lat, TRUE) THEN 'plausible'
                        WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 365
                             THEN 'weak'
                        ELSE 'unlinked'
                    END AS linkage_confidence
                FROM img JOIN fna ON img.research_id = fna.research_id
            )
            SELECT * FROM candidates WHERE linkage_confidence != 'unlinked'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id, nodule_id
                ORDER BY CASE linkage_confidence
                    WHEN 'exact_match' THEN 1 WHEN 'high_confidence' THEN 2
                    WHEN 'plausible' THEN 3 WHEN 'weak' THEN 4 ELSE 5
                END, day_gap
            ) = 1
        """)
        rows = con.execute("SELECT linkage_confidence FROM imaging_fna_linkage_v2").fetchall()
        return rows[0][0] if rows else "unlinked"

    def test_exact_match_same_date_same_laterality(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-03-15", "2024-03-15", "right_lobe", "right_lobe") == "exact_match"

    def test_exact_match_same_date_null_laterality(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-03-15", "2024-03-15", None, None) == "exact_match"

    def test_high_confidence_within_7_days(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-03-15", "2024-03-20") == "high_confidence"

    def test_plausible_within_90_days_compatible_laterality(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-03-15", "2024-05-15", "right_lobe", "right_lobe") == "plausible"

    def test_weak_within_365_days(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-01-01", "2024-11-01") == "weak"

    def test_unlinked_beyond_365_days(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2022-01-01", "2024-01-01") == "unlinked"

    def test_same_date_different_laterality_high_confidence(self):
        con = duckdb.connect()
        result = self._setup_and_link(con, "2024-03-15", "2024-03-15", "right_lobe", "left_lobe")
        assert result == "high_confidence"


class TestPathologyRaiLinkageConfidence:
    def _setup_and_link(self, con, surg_date, rai_date, assertion_status="definite_received"):
        con.execute("""CREATE OR REPLACE TABLE tumor_episode_master_v2 AS
            SELECT 1 AS research_id, 's1' AS surgery_episode_id,
                   CAST(? AS DATE) AS surgery_date, 1 AS tumor_ordinal,
                   NULL AS t_stage, NULL AS primary_histology, NULL AS laterality,
                   FALSE AS histology_discordance_flag, FALSE AS t_stage_discordance_flag,
                   1 AS confidence_rank""", [surg_date])
        con.execute("""CREATE OR REPLACE TABLE rai_treatment_episode_v2 AS
            SELECT 1 AS research_id, 'r1' AS rai_episode_id,
                   CAST(? AS DATE) AS resolved_rai_date, ? AS rai_assertion_status,
                   NULL AS dose_mci, NULL AS linked_surgery_episode_id""",
            [rai_date, assertion_status])
        con.execute("""
            CREATE OR REPLACE TABLE pathology_rai_linkage_v2 AS
            WITH surg AS (
                SELECT research_id, surgery_episode_id, surgery_date AS surgery_date_val
                FROM tumor_episode_master_v2
            ),
            rai AS (
                SELECT research_id, rai_episode_id, resolved_rai_date, rai_assertion_status
                FROM rai_treatment_episode_v2 WHERE resolved_rai_date IS NOT NULL
            ),
            candidates AS (
                SELECT surg.research_id, surg.surgery_episode_id, rai.rai_episode_id,
                    surg.surgery_date_val, rai.resolved_rai_date,
                    DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) AS days_surg_to_rai,
                    CASE
                        WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) BETWEEN 14 AND 180
                             THEN 'high_confidence'
                        WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) BETWEEN 1 AND 365
                             THEN 'plausible'
                        WHEN DATEDIFF('day', surg.surgery_date_val, rai.resolved_rai_date) < 0
                             AND rai.rai_assertion_status IN ('historical', 'ambiguous')
                             THEN 'weak'
                        ELSE 'unlinked'
                    END AS linkage_confidence
                FROM surg JOIN rai ON surg.research_id = rai.research_id
                WHERE surg.surgery_date_val IS NOT NULL
            )
            SELECT * FROM candidates WHERE linkage_confidence != 'unlinked'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id, rai_episode_id
                ORDER BY CASE linkage_confidence
                    WHEN 'high_confidence' THEN 1 WHEN 'plausible' THEN 2 WHEN 'weak' THEN 3 ELSE 4
                END, ABS(days_surg_to_rai)
            ) = 1
        """)
        rows = con.execute("SELECT linkage_confidence FROM pathology_rai_linkage_v2").fetchall()
        return rows[0][0] if rows else "unlinked"

    def test_high_confidence_30_days_post_surgery(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-03-01", "2024-03-31") == "high_confidence"

    def test_plausible_200_days_post_surgery(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-01-01", "2024-07-20") == "plausible"

    def test_weak_historical_rai_before_surgery(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-06-01", "2024-01-01", "historical") == "weak"

    def test_unlinked_rai_before_surgery_non_historical(self):
        con = duckdb.connect()
        assert self._setup_and_link(con, "2024-06-01", "2024-01-01", "definite_received") == "unlinked"
