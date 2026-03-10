"""Tests for V2 extractor enrichment fields and chronology constraints.

Validates that:
  1. RAIDetailExtractor populates scan_findings_raw, iodine_avidity_flag,
     stimulated_tg/tsh, and pre/post scan flags.
  2. OperativeDetailExtractor populates rln_monitoring_flag, rln_finding_raw,
     parathyroid/ete/tracheal/esophageal/strap/drain flags, and
     operative_findings_raw.
  3. FNA-before-molecular chronology is enforced (reverse order = weak tier).
  4. Preop-before-surgery chronology is enforced (reverse order = weak tier).
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_rai_v2 import RAIDetailExtractor
from notes_extraction.extract_operative_v2 import OperativeDetailExtractor

NOTE_ROW_ID = "test_enrich_001"
RESEARCH_ID = 42


class TestRAIEnrichmentFields:
    @pytest.fixture
    def ext(self) -> RAIDetailExtractor:
        return RAIDetailExtractor()

    def test_scan_findings_raw_populated(self, ext):
        text = (
            "Post-therapy scan at day 7: Uptake in the thyroid bed consistent "
            "with remnant tissue. No evidence of distant metastases. Focal "
            "uptake in right lateral neck level III lymph node."
        )
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        scan_findings = [m for m in matches if m.entity_type == "rai_scan_finding"]
        assert len(scan_findings) >= 1

    def test_iodine_avidity_flag(self, ext):
        text = "Post-therapy scan: iodine-avid uptake in thyroid bed. Avid lymph node."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        avidity = [m for m in matches if m.entity_type == "rai_avidity"]
        assert len(avidity) >= 1
        assert any(m.entity_value_norm == "avid" for m in avidity)

    def test_stimulated_tg_extraction(self, ext):
        text = "Stimulated thyroglobulin 2.5 ng/mL prior to RAI."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        stim_tg = [m for m in matches if m.entity_type == "rai_stimulated_tg"]
        assert len(stim_tg) >= 1
        assert "2.5" in stim_tg[0].entity_value_norm

    def test_stimulated_tsh_extraction(self, ext):
        text = "TSH 45 mIU/L. Patient adequately stimulated for RAI therapy."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        stim_tsh = [m for m in matches if m.entity_type == "rai_stimulated_tsh"]
        assert len(stim_tsh) >= 1
        assert "45" in stim_tsh[0].entity_value_norm

    def test_pre_scan_flag(self, ext):
        text = "Pre-treatment whole body scan showed uptake in thyroid bed."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        pre = [m for m in matches if m.entity_type == "rai_pre_scan"]
        assert len(pre) >= 1

    def test_post_scan_flag(self, ext):
        text = "Post-therapy scan at 7 days demonstrates thyroid bed uptake."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", text)
        post = [m for m in matches if m.entity_type == "rai_post_scan"]
        assert len(post) >= 1

    def test_empty_text_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", "") == []

    def test_none_text_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, "nuclear_medicine", None) == []


class TestOperativeEnrichmentFields:
    @pytest.fixture
    def ext(self) -> OperativeDetailExtractor:
        return OperativeDetailExtractor()

    def test_rln_monitoring_flag(self, ext):
        text = "Intraoperative nerve monitoring (NIM) was used throughout."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        nm = [m for m in matches if m.entity_type == "nerve_monitoring"]
        assert len(nm) >= 1

    def test_rln_finding_raw(self, ext):
        text = "The recurrent laryngeal nerve was identified and preserved."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        rln = [m for m in matches if m.entity_type == "rln_finding"]
        assert len(rln) >= 1
        assert rln[0].entity_value_norm == "rln_preserved"

    def test_parathyroid_autograft_flag(self, ext):
        text = "Parathyroid autotransplanted into the sternocleidomastoid muscle."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        pt = [m for m in matches if m.entity_type == "parathyroid_autograft"]
        assert len(pt) >= 1

    def test_gross_ete_flag(self, ext):
        text = "Tumor demonstrated gross extrathyroidal extension into strap muscles."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        ete = [m for m in matches if m.entity_type == "gross_invasion"]
        assert len(ete) >= 1

    def test_tracheal_involvement_flag(self, ext):
        text = "Tracheal shaving was performed where tumor was adherent."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        trach = [m for m in matches if m.entity_type == "tracheal_involvement"]
        assert len(trach) >= 1

    def test_drain_flag(self, ext):
        text = "JP drain placed in the thyroid bed prior to closure."
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", text)
        drain = [m for m in matches if m.entity_type == "drain_placement"]
        assert len(drain) >= 1

    def test_operative_findings_combined(self, ext, sample_detailed_op_note):
        matches = ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", sample_detailed_op_note)
        types = {m.entity_type for m in matches}
        assert "nerve_monitoring" in types
        assert "rln_finding" in types

    def test_empty_text_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", "") == []

    def test_none_text_returns_empty(self, ext):
        assert ext.extract(NOTE_ROW_ID, RESEARCH_ID, "op_note", None) == []


class TestFNAMolecularChronology:
    """FNA should precede molecular test; reverse order gets weak tier."""

    def _link_fna_molecular(self, con, fna_date, mol_date):
        con.execute("""
            CREATE OR REPLACE TABLE fna_episode_master_v2 AS
            SELECT 1 AS research_id, 'f1' AS fna_episode_id,
                   CAST(? AS DATE) AS fna_date_native, NULL AS laterality,
                   NULL AS bethesda_category, NULL AS specimen_site_raw,
                   NULL AS linked_molecular_episode_id,
                   NULL AS linked_surgery_episode_id
        """, [fna_date])
        con.execute("""
            CREATE OR REPLACE TABLE molecular_test_episode_v2 AS
            SELECT 1 AS research_id, 'm1' AS molecular_episode_id,
                   CAST(? AS DATE) AS molecular_date_native,
                   NULL AS platform_normalized, NULL AS result_normalized,
                   NULL AS linked_fna_episode_id
        """, [mol_date])
        con.execute("""
            CREATE OR REPLACE TABLE fna_molecular_linkage_v2 AS
            WITH candidates AS (
                SELECT
                    f.research_id, f.fna_episode_id, m.molecular_episode_id,
                    f.fna_date_native, m.molecular_date_native,
                    DATEDIFF('day', f.fna_date_native, m.molecular_date_native) AS days_fna_to_mol,
                    CASE
                        WHEN f.fna_date_native = m.molecular_date_native THEN 'exact_match'
                        WHEN DATEDIFF('day', f.fna_date_native, m.molecular_date_native) BETWEEN 1 AND 14
                             THEN 'high_confidence'
                        WHEN DATEDIFF('day', f.fna_date_native, m.molecular_date_native) BETWEEN 15 AND 90
                             THEN 'plausible'
                        WHEN DATEDIFF('day', f.fna_date_native, m.molecular_date_native) BETWEEN -7 AND 180
                             THEN 'weak'
                        ELSE 'unlinked'
                    END AS confidence_tier
                FROM fna_episode_master_v2 f
                JOIN molecular_test_episode_v2 m ON f.research_id = m.research_id
                WHERE f.fna_date_native IS NOT NULL AND m.molecular_date_native IS NOT NULL
            )
            SELECT * FROM candidates WHERE confidence_tier != 'unlinked'
        """)
        rows = con.execute("SELECT confidence_tier FROM fna_molecular_linkage_v2").fetchall()
        return rows[0][0] if rows else "unlinked"

    def test_fna_before_molecular_high_confidence(self):
        con = duckdb.connect()
        assert self._link_fna_molecular(con, "2024-03-01", "2024-03-08") == "high_confidence"

    def test_fna_same_day_exact_match(self):
        con = duckdb.connect()
        assert self._link_fna_molecular(con, "2024-03-01", "2024-03-01") == "exact_match"

    def test_molecular_before_fna_weak(self):
        """Molecular 5 days BEFORE FNA: should be weak (reverse chronology)."""
        con = duckdb.connect()
        result = self._link_fna_molecular(con, "2024-03-10", "2024-03-05")
        assert result == "weak", f"Expected 'weak' for reverse order, got '{result}'"

    def test_molecular_far_before_fna_unlinked(self):
        """Molecular 30 days BEFORE FNA: should be unlinked."""
        con = duckdb.connect()
        result = self._link_fna_molecular(con, "2024-04-01", "2024-03-01")
        assert result == "unlinked"


class TestPreopSurgeryChronology:
    """Preop imaging should precede surgery; reverse order gets weak tier."""

    def _link_preop_surgery(self, con, preop_date, surg_date):
        con.execute("""
            CREATE OR REPLACE TABLE imaging_exam_summary_v2 AS
            SELECT 1 AS research_id, 'ex1' AS imaging_exam_id,
                   'ultrasound' AS modality,
                   CAST(? AS DATE) AS exam_date_native, 1 AS nodule_count
        """, [preop_date])
        con.execute("""
            CREATE OR REPLACE TABLE operative_episode_detail_v2 AS
            SELECT 1 AS research_id, 's1' AS surgery_episode_id,
                   'total_thyroidectomy' AS procedure_type,
                   CAST(? AS DATE) AS surgery_date_native,
                   NULL AS linked_preop_imaging_id
        """, [surg_date])
        con.execute("""
            CREATE OR REPLACE TABLE preop_surgery_linkage_v2 AS
            WITH candidates AS (
                SELECT
                    img.research_id, img.imaging_exam_id, surg.surgery_episode_id,
                    img.exam_date_native, surg.surgery_date_native,
                    DATEDIFF('day', img.exam_date_native, surg.surgery_date_native) AS days_preop_to_surg,
                    CASE
                        WHEN DATEDIFF('day', img.exam_date_native, surg.surgery_date_native) BETWEEN 0 AND 14
                             THEN 'high_confidence'
                        WHEN DATEDIFF('day', img.exam_date_native, surg.surgery_date_native) BETWEEN 15 AND 90
                             THEN 'plausible'
                        WHEN DATEDIFF('day', img.exam_date_native, surg.surgery_date_native) BETWEEN -7 AND 365
                             THEN 'weak'
                        ELSE 'unlinked'
                    END AS confidence_tier
                FROM imaging_exam_summary_v2 img
                JOIN operative_episode_detail_v2 surg ON img.research_id = surg.research_id
                WHERE img.exam_date_native IS NOT NULL AND surg.surgery_date_native IS NOT NULL
            )
            SELECT * FROM candidates WHERE confidence_tier != 'unlinked'
        """)
        rows = con.execute("SELECT confidence_tier FROM preop_surgery_linkage_v2").fetchall()
        return rows[0][0] if rows else "unlinked"

    def test_preop_before_surgery_high_confidence(self):
        con = duckdb.connect()
        assert self._link_preop_surgery(con, "2024-03-01", "2024-03-10") == "high_confidence"

    def test_preop_same_day_high_confidence(self):
        con = duckdb.connect()
        assert self._link_preop_surgery(con, "2024-03-01", "2024-03-01") == "high_confidence"

    def test_preop_60_days_before_plausible(self):
        con = duckdb.connect()
        assert self._link_preop_surgery(con, "2024-01-15", "2024-03-15") == "plausible"

    def test_surgery_before_preop_weak(self):
        """Surgery 5 days BEFORE preop: within -7 window, should be weak."""
        con = duckdb.connect()
        result = self._link_preop_surgery(con, "2024-03-10", "2024-03-05")
        assert result == "weak", f"Expected 'weak' for reverse order, got '{result}'"

    def test_surgery_far_before_preop_unlinked(self):
        """Surgery 30 days BEFORE preop: should be unlinked."""
        con = duckdb.connect()
        result = self._link_preop_surgery(con, "2024-04-01", "2024-03-01")
        assert result == "unlinked"
